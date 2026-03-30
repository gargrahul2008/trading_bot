#!/usr/bin/env python3
"""
MEXC portfolio snapshot — period-based, appends one row per run.

Q, A, R are computed by replaying ALL events from scratch:
  1. Manual positions (initial holdings before bot started)
  2. Every bot trade (BUY / SELL) in chronological order across all trade files

Formula at each snapshot:
  Actual PnL = R + (Z - A) × Q
  where Z = current market price (CMP)

Period columns cover only the window [last_snapshot, now):
  cycles, rebalancing ETH, rebalancing PnL

Usage:
    python3 scripts/mexc_snapshot.py \\
        --state            strategies/pct_ladder/state/mexc_state_2026_03_05_v1.json \\
        --trades           strategies/pct_ladder/state/mexc_trades.jsonl \\
                           strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \\
                           strategies/pct_ladder/state/mexc_trades_2026_03_03.jsonl \\
                           strategies/pct_ladder/state/mexc_trades_2026_03_03_v1.jsonl \\
                           strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --manual-positions strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json \\
        --csv              strategies/pct_ladder/state/mexc_snapshots.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import sys
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

HEADERS = [
    # Period window
    "period_start_ts",
    "period_end_ts",
    # Period: ladder cycles
    "period_cycles",
    "period_avg_cycle_pnl",
    "period_cycle_pnl",
    # Period: rebalancing
    "period_rebal_eth_bought",
    "period_rebal_eth_sold",
    "period_rebal_net_eth",
    "period_rebal_avg_price",
    "period_rebal_pnl",
    # Cumulative state (full: manual holdings + bot trades)
    "Q_eth_holding",
    "A_eth_avg_cost",
    "R_realized_pnl",
    # CMP and PnL formula
    "Z_eth_cmp",
    "unrealized_pnl",       # (Z - A) × Q
    "actual_pnl",           # R + (Z - A) × Q
    # Cash and portfolio
    "cash_usdc",
    "portfolio_value",      # cash + Q × Z
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _r(x, places: int = 4) -> str:
    try:
        q = Decimal("0." + "0" * places)
        return str(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    except Exception:
        return str(x)


def _parse_ts(ts_str: str) -> datetime.datetime:
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return datetime.datetime.fromtimestamp(0, tz=UTC)


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S%z")


# ── running Q / A / R state ───────────────────────────────────────────────────

def _apply_buy(Q: Decimal, A: Decimal, R: Decimal,
               qty: Decimal, price: Decimal):
    """BUY q shares at p: update weighted average cost."""
    if qty <= D0:
        return Q, A, R
    new_Q = Q + qty
    if new_Q > D0:
        new_A = (Q * A + qty * price) / new_Q
    else:
        new_A = price
    return new_Q, new_A, R


def _apply_sell(Q: Decimal, A: Decimal, R: Decimal,
                qty: Decimal, price: Decimal):
    """SELL q shares at s: lock in realized PnL = (s - A) × q.
    Q is allowed to go negative (bot sells from the manual ETH buffer).
    """
    if qty <= D0:
        return Q, A, R
    realized = (price - A) * qty
    new_Q = Q - qty          # no floor — bot can sell against manual holdings
    new_R = R + realized
    return new_Q, A, new_R  # A unchanged after sell


# ── data loading ──────────────────────────────────────────────────────────────

def fetch_eth_price(symbol: str = "ETHUSDC") -> Decimal:
    url = f"https://api.mexc.com/api/v3/ticker/price?symbol={symbol}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return _dec(resp.json()["price"])


def load_cash(state_path: str) -> Decimal:
    with open(state_path, encoding="utf-8") as f:
        state = json.load(f)
    return _dec(state.get("cash") or "0")


def load_manual_positions(path: str, symbol: str) -> list[tuple]:
    """
    Returns list of (ts_utc, side, qty, price, reason) for manual positions.
    manual_positions JSON format: [{ts, symbol, qty, buy_price}, ...]
    """
    events = []
    try:
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        for r in rows:
            if str(r.get("symbol") or "") != symbol:
                continue
            qty   = _dec(r.get("qty") or "0")
            price = _dec(r.get("buy_price") or "0")
            if qty <= D0 or price <= D0:
                continue
            ts_utc = _parse_ts(str(r.get("ts") or ""))
            events.append((ts_utc, "BUY", qty, price, "manual"))
    except FileNotFoundError:
        pass
    return sorted(events, key=lambda e: e[0])


def load_bot_trades(trades_paths: list[str], symbol: str) -> list[tuple]:
    """Returns deduplicated list of (ts_utc, side, qty, price, reason) from all trade files."""
    seen_order_ids: set[str] = set()
    events = []
    for trades_path in trades_paths:
        try:
            with open(trades_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    r = json.loads(line)
                    if r.get("event") != "FILL":
                        continue
                    oid = str(r.get("order_id") or "")
                    if oid and oid in seen_order_ids:
                        continue
                    if oid:
                        seen_order_ids.add(oid)
                    qty = _dec(r.get("qty") or "0")
                    if qty <= D0:
                        continue
                    side   = str(r.get("side") or "").upper()
                    price  = _dec(r.get("price") or "0")
                    ts     = _parse_ts(r.get("ts") or "")
                    reason = str(r.get("reason") or "")
                    events.append((ts, side, qty, price, reason))
        except FileNotFoundError:
            pass
    return sorted(events, key=lambda e: e[0])


def compute_QAR(manual_events: list[tuple], bot_events: list[tuple],
                up_to: datetime.datetime | None = None) -> tuple[Decimal, Decimal, Decimal]:
    """
    Replay all events chronologically up to `up_to` to compute Q, A, R.
    Manual positions treated as BUY events at their recorded price.
    """
    Q, A, R = D0, D0, D0
    all_events = sorted(manual_events + bot_events, key=lambda e: e[0])
    for ts, side, qty, price, _reason in all_events:
        if up_to is not None and ts >= up_to:
            break
        if side == "BUY":
            Q, A, R = _apply_buy(Q, A, R, qty, price)
        else:
            Q, A, R = _apply_sell(Q, A, R, qty, price)
    return Q, A, R


def last_price_before(manual_events: list[tuple], bot_events: list[tuple],
                      up_to: datetime.datetime) -> Decimal:
    """Return the last trade price before up_to, or D0 if none."""
    all_events = sorted(manual_events + bot_events, key=lambda e: e[0])
    last_p = D0
    for ts, _side, _qty, price, _reason in all_events:
        if ts >= up_to:
            break
        if price > D0:
            last_p = price
    return last_p


def compute_period_metrics(manual_events: list[tuple], bot_events: list[tuple],
                           period_start: datetime.datetime,
                           period_end: datetime.datetime) -> dict:
    """
    Cycle count and rebalancing metrics for trades within [period_start, period_end).
    Uses running-average cost formula for accurate period PnL.
    """
    cycle_count  = 0
    rb_buy_qty   = D0
    rb_buy_cost  = D0
    rb_sell_qty  = D0
    rb_sell_cost = D0
    rb_pnl       = D0
    cyc_pnl      = D0

    # Replay all events (manual + bot) up to period_end to get correct A at each point
    all_events = sorted(manual_events + bot_events, key=lambda e: e[0])

    Q, A, R = D0, D0, D0
    for ts, side, qty, price, reason in all_events:
        if ts >= period_end:
            break
        if side == "BUY":
            Q, A, R = _apply_buy(Q, A, R, qty, price)
            if ts >= period_start and reason != "manual":
                if "rebalanc" in reason:
                    rb_buy_qty  += qty
                    rb_buy_cost += qty * price
        else:
            # Capture A before sell
            A_before_sell = A
            Q, A, R = _apply_sell(Q, A, R, qty, price)
            if ts >= period_start:
                delta = (price - A_before_sell) * qty
                if "rebalanc" in reason:
                    rb_sell_qty  += qty
                    rb_sell_cost += qty * price
                    rb_pnl       += delta
                else:
                    cycle_count += 1
                    cyc_pnl     += delta

    rb_total_qty  = rb_buy_qty + rb_sell_qty
    rb_avg_price  = (rb_buy_cost + rb_sell_cost) / rb_total_qty if rb_total_qty > D0 else D0
    rb_net_eth    = rb_buy_qty - rb_sell_qty
    avg_cycle_pnl = cyc_pnl / cycle_count if cycle_count > 0 else D0

    return {
        "cycle_count":    cycle_count,
        "avg_cycle_pnl":  avg_cycle_pnl,
        "cycle_pnl":      cyc_pnl,
        "rb_buy_qty":     rb_buy_qty,
        "rb_sell_qty":    rb_sell_qty,
        "rb_net_eth":     rb_net_eth,
        "rb_avg_price":   rb_avg_price,
        "rb_pnl":         rb_pnl,
    }


def read_last_snapshot_ts(csv_path: str) -> datetime.datetime:
    epoch = datetime.datetime.fromtimestamp(0, tz=UTC)
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return epoch
    try:
        with open(csv_path, encoding="utf-8") as f:
            last = None
            for row in csv.DictReader(f):
                last = row
        if last and last.get("period_end_ts"):
            return _parse_ts(last["period_end_ts"])
    except Exception:
        pass
    return epoch


# ── main ──────────────────────────────────────────────────────────────────────

def snapshot(state_path: str, trades_paths: list[str], csv_path: str,
             manual_positions_path: str | None, symbol: str,
             as_of: datetime.datetime | None = None) -> None:

    now_utc      = as_of if as_of is not None else datetime.datetime.now(UTC)
    period_start = read_last_snapshot_ts(csv_path)

    # ── cash from state ──
    cash = load_cash(state_path)

    # ── load all events ──
    manual_events = load_manual_positions(manual_positions_path, symbol) if manual_positions_path else []
    bot_events    = load_bot_trades(trades_paths, symbol)

    # ── Q, A, R from replay up to now_utc ──
    Q, A, R = compute_QAR(manual_events, bot_events, up_to=now_utc)

    # ── period metrics ──
    t = compute_period_metrics(manual_events, bot_events, period_start, now_utc)

    # ── price: live for real-time runs; last trade price for historical (--as-of) ──
    if as_of is None:
        try:
            Z = fetch_eth_price(symbol)
        except Exception as e:
            print(f"WARNING: live price fetch failed ({e}), using last trade price", file=sys.stderr)
            Z = last_price_before(manual_events, bot_events, now_utc) or A
    else:
        Z = last_price_before(manual_events, bot_events, now_utc) or A

    # ── formula: Actual PnL = R + (Z - A) × Q ──
    unrealized = (Z - A) * Q if Q > D0 else D0
    actual_pnl = R + unrealized
    port_value = cash + Q * Z

    row = {
        "period_start_ts":         _fmt_ist(period_start) if period_start.timestamp() > 0 else "epoch",
        "period_end_ts":           _fmt_ist(now_utc),
        "period_cycles":           str(t["cycle_count"]),
        "period_avg_cycle_pnl":    _r(t["avg_cycle_pnl"], 4),
        "period_cycle_pnl":        _r(t["cycle_pnl"], 4),
        "period_rebal_eth_bought": _r(t["rb_buy_qty"],  5),
        "period_rebal_eth_sold":   _r(t["rb_sell_qty"], 5),
        "period_rebal_net_eth":    _r(t["rb_net_eth"],  5),
        "period_rebal_avg_price":  _r(t["rb_avg_price"], 4),
        "period_rebal_pnl":        _r(t["rb_pnl"], 4),
        "Q_eth_holding":           _r(Q, 5),
        "A_eth_avg_cost":          _r(A, 4),
        "R_realized_pnl":          _r(R, 4),
        "Z_eth_cmp":               _r(Z, 4),
        "unrealized_pnl":          _r(unrealized, 4),
        "actual_pnl":              _r(actual_pnl, 4),
        "cash_usdc":               _r(cash, 4),
        "portfolio_value":         _r(port_value, 4),
    }

    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print(f"[snapshot] {_fmt_ist(period_start)} → {_fmt_ist(now_utc)}")
    print(f"  Period  : cycles={t['cycle_count']}  cycle_pnl={_r(t['cycle_pnl'],2)}"
          f"  rebal_net={_r(t['rb_net_eth'],4)}  rebal_pnl={_r(t['rb_pnl'],2)}")
    print(f"  Full Q,A,R: Q={_r(Q,5)} ETH  A={_r(A,4)}  R={_r(R,2)}")
    print(f"  Formula : Z={_r(Z,4)}  (Z-A)×Q={_r(unrealized,2)}  Actual PnL={_r(actual_pnl,2)}")
    print(f"  Cash    : {_r(cash,2)} USDC  |  Portfolio = {_r(port_value,2)} USDC")
    if manual_events:
        manual_Q = sum(e[2] for e in manual_events)
        print(f"  (includes {len(manual_events)} manual positions totalling {_r(manual_Q,5)} ETH)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state",             required=True)
    ap.add_argument("--trades",            required=True, nargs="+",
                    help="One or more trade .jsonl files in chronological order")
    ap.add_argument("--csv",               required=True)
    ap.add_argument("--manual-positions",  default=None)
    ap.add_argument("--symbol",            default="ETHUSDC")
    ap.add_argument("--as-of",             default=None,
                    help="Override 'now' for backfill, e.g. 2026-03-10T07:00:00+05:30")
    args = ap.parse_args()
    as_of = _parse_ts(args.as_of) if args.as_of else None
    snapshot(
        state_path=args.state,
        trades_paths=args.trades,
        csv_path=args.csv,
        manual_positions_path=args.manual_positions,
        symbol=args.symbol,
        as_of=as_of,
    )


if __name__ == "__main__":
    main()
