#!/usr/bin/env python3
"""
mexc_lifo.py — LIFO trade journal for MEXC strategy.

Produces one row per trade/event with LIFO cost basis and clean P&L columns.

Columns:
    datetime     — trade time in IST
    side         — BUY / SELL / MANUAL_BUY / DEPOSIT / WITHDRAW
    reason
    qty
    price
    Q_after      — ETH held after this trade
    avg_cost     — LIFO weighted avg cost of remaining lots
    cash_after
    trade_pnl    — LIFO realized P&L (SELLs only)
    cum_trade_pnl— running total of LIFO realized P&L
    pv           — Q_after × price + cash_after
    stock_pnl    — Q_after × (price − prev_price): price-shift impact on remaining ETH
    cycle_bot_pnl— for cycle SELLs: pv_delta − stock_pnl (trading contribution)
    cycle_formula_pnl — qty × price × pct (formula estimate)
    retained_eth — cumulative net ETH accumulated by strategy (band drift + rebalances)

State is saved to <out>.state.json for efficient append-mode runs.

Usage:
    python3 scripts/mexc_lifo.py \\
        --trades  strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --manual  strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json \\
        --capital strategies/pct_ladder/state/capital_flows_2026_03_05_v1.json \\
        --pct     0.004 \\
        --out     strategies/pct_ladder/state/mexc_lifo.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

HEADERS = [
    "datetime",
    "side",
    "reason",
    "qty",
    "price",
    "Q_after",
    "avg_cost",
    "cash_after",
    "trade_pnl",
    "cum_trade_pnl",
    "pv",
    "stock_pnl",
    "cycle_bot_pnl",
    "cycle_formula_pnl",
    "retained_eth",
]


# ─────────────────────────── helpers ────────────────────────────────────────

def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _r(x, places: int = 2) -> str:
    try:
        q = Decimal("0." + "0" * places)
        return str(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    except Exception:
        return str(x)


def _parse_ts(s: str) -> datetime.datetime:
    try:
        dt = datetime.datetime.fromisoformat(s)
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except Exception:
        return datetime.datetime.fromtimestamp(0, tz=UTC)


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S IST")


def _is_rebalance(reason: str) -> bool:
    return bool(re.search(r'rebalance|rebal', reason, re.IGNORECASE))


# ──────────────────────────── LIFO lot utils ────────────────────────────────

def lifo_avg_cost(lots: list[dict]) -> Decimal:
    """Weighted average cost of remaining LIFO lots."""
    total_qty   = sum(_dec(lot["qty"]) for lot in lots)
    total_value = sum(_dec(lot["qty"]) * _dec(lot["price"]) for lot in lots)
    if total_qty <= D0:
        return D0
    return total_value / total_qty


def lifo_sell(lots: list[dict], sell_qty: Decimal, sell_price: Decimal) -> Decimal:
    """
    Consume sell_qty from the LIFO lot stack (most-recent lot first).
    Modifies `lots` in place.
    Returns LIFO realized P&L.
    """
    realized = D0
    remaining = sell_qty
    while remaining > D0 and lots:
        lot      = lots[-1]
        lot_qty  = _dec(lot["qty"])
        lot_px   = _dec(lot["price"])
        take     = min(remaining, lot_qty)
        realized += take * (sell_price - lot_px)
        remaining -= take
        if take >= lot_qty:
            lots.pop()
        else:
            lot["qty"] = str(lot_qty - take)
    return realized


# ─────────────────────────── event loading ──────────────────────────────────

def load_all_events(trades_paths: list[str],
                    manual_path: str | None,
                    capital_path: str | None) -> list[tuple]:
    """
    Returns sorted list of (ts_utc, event_type, qty, price, reason, order_id).
    event_type: BUY / SELL / MANUAL_BUY / DEPOSIT / WITHDRAW
    """
    events: list[tuple] = []

    if manual_path:
        try:
            for m in json.load(open(manual_path, encoding="utf-8")):
                qty   = _dec(m.get("qty", 0))
                price = _dec(m.get("buy_price", 0))
                if qty <= D0 or price <= D0:
                    continue
                ts = _parse_ts(str(m.get("ts", "")))
                events.append((ts, "MANUAL_BUY", qty, price, "manual_eth_purchase", ""))
        except FileNotFoundError:
            pass

    if capital_path:
        try:
            for f in json.load(open(capital_path, encoding="utf-8")):
                delta = _dec(f.get("delta", 0))
                if delta == D0:
                    continue
                ts    = _parse_ts(str(f.get("ts", "")))
                etype = "DEPOSIT" if delta > D0 else "WITHDRAW"
                events.append((ts, etype, abs(delta), D0, f.get("note", ""), ""))
        except FileNotFoundError:
            pass

    seen: set[str] = set()
    for path in trades_paths:
        try:
            for line in open(path, encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("event") != "FILL":
                    continue
                oid = str(r.get("order_id") or "")
                if oid and oid in seen:
                    continue
                if oid:
                    seen.add(oid)
                qty = _dec(r.get("qty") or "0")
                if qty <= D0:
                    continue
                ts     = _parse_ts(r.get("ts") or "")
                side   = str(r.get("side") or "").upper()
                price  = _dec(r.get("price") or "0")
                reason = str(r.get("reason") or "")
                events.append((ts, side, qty, price, reason, oid))
        except FileNotFoundError:
            pass

    events.sort(key=lambda e: e[0])
    return events


# ─────────────────────────── sidecar state ──────────────────────────────────

def _state_path(csv_path: str) -> str:
    return csv_path + ".state.json"


def load_state(csv_path: str) -> dict | None:
    """Load persisted LIFO state from sidecar JSON. Returns None if not found."""
    sp = _state_path(csv_path)
    if not os.path.exists(sp):
        return None
    try:
        with open(sp, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_state(csv_path: str, state: dict) -> None:
    sp = _state_path(csv_path)
    with open(sp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# ─────────────────────────── core processing ────────────────────────────────

def process_events(
    events: list[tuple],
    after_ts: datetime.datetime,
    lots: list[dict],          # mutable LIFO lot stack [{qty, price}, ...]
    cash: Decimal,
    prev_price: Decimal,
    prev_pv: Decimal,
    cum_trade_pnl: Decimal,
    retained_eth: Decimal,
    last_buy_qty: Decimal,
    pct: Decimal,
    seen_oids: set[str],
) -> list[dict]:
    """
    Process events strictly after after_ts.
    Returns list of row dicts. Mutates lots, seen_oids in place.
    """
    rows: list[dict] = []

    for ts, etype, qty, price, reason, oid in events:
        if oid and oid in seen_oids:
            continue
        if ts <= after_ts:
            continue

        Q_before = sum(_dec(lot["qty"]) for lot in lots)

        # ── DEPOSIT / WITHDRAW ───────────────────────────────────────────────
        if etype in ("DEPOSIT", "WITHDRAW"):
            cash_delta = qty if etype == "DEPOSIT" else -qty
            cash      += cash_delta
            Q_after    = Q_before
            avg        = lifo_avg_cost(lots)
            pv         = Q_after * prev_price + cash   # price unchanged
            pv_delta   = pv - prev_pv
            rows.append({
                "datetime":         _fmt_ist(ts),
                "side":             etype,
                "reason":           reason,
                "qty":              _r(qty, 2),
                "price":            _r(prev_price, 2),
                "Q_after":          _r(Q_after, 6),
                "avg_cost":         _r(avg, 2),
                "cash_after":       _r(cash, 2),
                "trade_pnl":        "",
                "cum_trade_pnl":    _r(cum_trade_pnl, 2),
                "pv":               _r(pv, 2),
                "stock_pnl":        "0.00",
                "cycle_bot_pnl":    "",
                "cycle_formula_pnl":"",
                "retained_eth":     _r(retained_eth, 6),
            })
            prev_pv = pv
            if oid:
                seen_oids.add(oid)
            continue

        # ── BUY / MANUAL_BUY ────────────────────────────────────────────────
        if etype in ("BUY", "MANUAL_BUY"):
            if etype == "BUY" and not _is_rebalance(reason):
                last_buy_qty = qty
            lots.append({"qty": str(qty), "price": str(price)})
            cash -= qty * price
            Q_after = Q_before + qty
            avg     = lifo_avg_cost(lots)
            pv      = Q_after * price + cash
            pv_delta= pv - prev_pv
            # stock_pnl: remaining ETH (Q_after) × price move during this period
            stock   = Q_after * (price - prev_price)
            if etype == "BUY" and _is_rebalance(reason):
                retained_eth += qty
            rows.append({
                "datetime":         _fmt_ist(ts),
                "side":             etype,
                "reason":           reason,
                "qty":              _r(qty, 6),
                "price":            _r(price, 2),
                "Q_after":          _r(Q_after, 6),
                "avg_cost":         _r(avg, 2),
                "cash_after":       _r(cash, 2),
                "trade_pnl":        "",
                "cum_trade_pnl":    _r(cum_trade_pnl, 2),
                "pv":               _r(pv, 2),
                "stock_pnl":        _r(stock, 2),
                "cycle_bot_pnl":    "",
                "cycle_formula_pnl":"",
                "retained_eth":     _r(retained_eth, 6),
            })

        # ── SELL ─────────────────────────────────────────────────────────────
        elif etype == "SELL":
            trade_pnl = lifo_sell(lots, qty, price)
            cash += qty * price
            Q_after = Q_before - qty
            avg     = lifo_avg_cost(lots)
            pv      = Q_after * price + cash
            pv_delta= pv - prev_pv
            cum_trade_pnl += trade_pnl

            # stock_pnl: remaining Q_after × price move during this period
            stock   = Q_after * (price - prev_price)

            is_cycle = not _is_rebalance(reason)
            c_bot  = c_formula = ""
            if is_cycle:
                c_bot    = _r(pv_delta - stock, 2)
                c_formula= _r(qty * price * pct, 2)
                # retained_eth: difference between last buy qty and this sell qty
                retained_eth += last_buy_qty - qty
            else:
                # rebalance sell removes ETH from retained
                retained_eth -= qty

            rows.append({
                "datetime":         _fmt_ist(ts),
                "side":             "SELL",
                "reason":           reason,
                "qty":              _r(qty, 6),
                "price":            _r(price, 2),
                "Q_after":          _r(Q_after, 6),
                "avg_cost":         _r(avg, 2),
                "cash_after":       _r(cash, 2),
                "trade_pnl":        _r(trade_pnl, 2),
                "cum_trade_pnl":    _r(cum_trade_pnl, 2),
                "pv":               _r(pv, 2),
                "stock_pnl":        _r(stock, 2),
                "cycle_bot_pnl":    c_bot,
                "cycle_formula_pnl":c_formula,
                "retained_eth":     _r(retained_eth, 6),
            })

        prev_pv    = pv
        prev_price = price
        if oid:
            seen_oids.add(oid)

    return rows, cash, prev_price, prev_pv, cum_trade_pnl, retained_eth, last_buy_qty


# ─────────────────────────── main ───────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades",  required=True, nargs="+")
    ap.add_argument("--manual",  default=None)
    ap.add_argument("--capital", default=None)
    ap.add_argument("--pct",     type=float, default=0.004,
                    help="Ladder step as decimal (default 0.004 = 0.4%%)")
    ap.add_argument("--out",     required=True)
    args = ap.parse_args()

    pct    = Decimal(str(args.pct))
    events = load_all_events(args.trades, args.manual, args.capital)

    state  = load_state(args.out)
    csv_exists = os.path.exists(args.out) and os.path.getsize(args.out) > 0

    if state and csv_exists:
        # ── APPEND MODE ───────────────────────────────────────────────────
        after_ts      = _parse_ts(state["last_ts"])
        lots          = state["lots"]          # list of {qty, price}
        cash          = _dec(state["cash"])
        prev_price    = _dec(state["prev_price"])
        prev_pv       = _dec(state["prev_pv"])
        cum_trade_pnl = _dec(state["cum_trade_pnl"])
        retained_eth  = _dec(state["retained_eth"])
        last_buy_qty  = _dec(state["last_buy_qty"])
        seen_oids     = set(state.get("seen_oids", []))

        rows, cash, prev_price, prev_pv, cum_trade_pnl, retained_eth, last_buy_qty = \
            process_events(events, after_ts, lots, cash, prev_price, prev_pv,
                           cum_trade_pnl, retained_eth, last_buy_qty, pct, seen_oids)

        if rows:
            with open(args.out, "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=HEADERS).writerows(rows)
            # Update state with latest event ts
            last_row = rows[-1]
            last_ts_ist = last_row["datetime"].replace(" IST", "+05:30")
            last_ts_utc = datetime.datetime.fromisoformat(last_ts_ist).astimezone(UTC)
            _update_state(args.out, last_ts_utc, lots, cash, prev_price, prev_pv,
                          cum_trade_pnl, retained_eth, last_buy_qty, seen_oids)
            print(f"Appended {len(rows)} row(s) → {args.out}")
        else:
            print("No new trades.")
        return

    # ── FULL BUILD ────────────────────────────────────────────────────────
    lots: list[dict]  = []
    cash              = D0
    prev_price        = D0
    prev_pv           = D0
    cum_trade_pnl     = D0
    retained_eth      = D0
    last_buy_qty      = D0
    seen_oids: set[str] = set()
    epoch = datetime.datetime.fromtimestamp(0, tz=UTC)

    rows, cash, prev_price, prev_pv, cum_trade_pnl, retained_eth, last_buy_qty = \
        process_events(events, epoch, lots, cash, prev_price, prev_pv,
                       cum_trade_pnl, retained_eth, last_buy_qty, pct, seen_oids)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        w.writerows(rows)

    # Save sidecar state
    if rows:
        last_row = rows[-1]
        last_ts_ist = last_row["datetime"].replace(" IST", "+05:30")
        last_ts_utc = datetime.datetime.fromisoformat(last_ts_ist).astimezone(UTC)
        _update_state(args.out, last_ts_utc, lots, cash, prev_price, prev_pv,
                      cum_trade_pnl, retained_eth, last_buy_qty, seen_oids)

    sells      = [r for r in rows if r["side"] == "SELL" and r["trade_pnl"]]
    cycle_rows = [r for r in rows if r["cycle_bot_pnl"] != ""]
    print(f"Written {len(rows)} rows → {args.out}")
    print(f"  {len(sells)} sells  |  {len(cycle_rows)} cycles")
    if sells:
        total = _dec(rows[-1]["cum_trade_pnl"])
        print(f"  Cumulative LIFO trade P&L: {_r(total, 2)}")
        print(f"  Retained ETH: {_r(retained_eth, 6)}")
    print("State saved → " + _state_path(args.out))


def _update_state(csv_path, last_ts_utc, lots, cash, prev_price, prev_pv,
                  cum_trade_pnl, retained_eth, last_buy_qty, seen_oids):
    save_state(csv_path, {
        "last_ts":       last_ts_utc.isoformat(),
        "lots":          lots,
        "cash":          str(cash),
        "prev_price":    str(prev_price),
        "prev_pv":       str(prev_pv),
        "cum_trade_pnl": str(cum_trade_pnl),
        "retained_eth":  str(retained_eth),
        "last_buy_qty":  str(last_buy_qty),
        "seen_oids":     list(seen_oids),
    })


if __name__ == "__main__":
    main()
