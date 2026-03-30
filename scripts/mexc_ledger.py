#!/usr/bin/env python3
"""
mexc_ledger.py  —  Per-trade ledger starting from a snapshot checkpoint.

Opens with a known-good snapshot row (Q, cash, portfolio) and resets
average cost A to Z (current price at checkpoint) for a clean baseline.
Every trade after the checkpoint updates Q, A, cash and shows trade_pnl.

  BUY  → Q increases, A recalculated (weighted avg), cash decreases, trade_pnl = 0
  SELL → Q decreases, A unchanged,  cash increases,  trade_pnl = (price - A) × qty
  cum_pnl accumulates sell trade_pnl from zero at the checkpoint.

Manual positions added to the manual-positions file after the checkpoint date
are automatically included as BUY entries and update A accordingly.

Usage:
    python3 scripts/mexc_ledger.py \\
        --snapshots       strategies/pct_ladder/state/mexc_snapshots.csv \\
        --opening-ts      "2026-03-24T23:00" \\
        --trades          strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --manual-positions strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json \\
        --symbol          ETHUSDC \\
        --out             strategies/pct_ladder/state/mexc_ledger.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

HEADERS = [
    "ts",
    "event",        # OPEN / BUY / SELL / DEPOSIT / WITHDRAW
    "reason",
    "qty",
    "price",
    "Q_before",
    "Q_after",
    "A_before",
    "A_after",
    "A_change",     # A_after - A_before  (non-zero only on BUY)
    "cash_change",  # -qty*price on BUY, +qty*price on SELL
    "cash_after",
    "trade_pnl",    # (price - A_before) * qty on SELL, 0 on BUY
    "cum_pnl",      # running total of trade_pnl from checkpoint
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _r(x, places: int = 5) -> str:
    try:
        q = Decimal("0." + "0" * places)
        return str(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    except Exception:
        return str(x)


def _r2(x) -> str:
    return _r(x, 2)


def _parse_ts(ts_str: str) -> datetime.datetime:
    """Parse ISO timestamp to UTC datetime. Handles epoch and missing tz."""
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return datetime.datetime.fromtimestamp(0, tz=UTC)


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S%z")


# ── snapshot lookup ───────────────────────────────────────────────────────────

def find_opening_snapshot(snapshots_path: str, opening_ts_str: str) -> dict:
    """
    Find the snapshot row whose period_end_ts most closely matches opening_ts_str.
    Bare timestamps (no tz) are treated as IST. Returns the row dict.
    """
    try:
        dt = datetime.datetime.fromisoformat(opening_ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)   # treat bare timestamps as IST
        target = dt.astimezone(UTC)
    except Exception:
        target = _parse_ts(opening_ts_str)
    best_row = None
    best_delta = None

    with open(snapshots_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row_ts = _parse_ts(row.get("period_end_ts") or "")
            delta = abs((row_ts - target).total_seconds())
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_row = row

    if best_row is None:
        raise SystemExit(f"No snapshot rows found in {snapshots_path}")
    if best_delta > 3600:
        raise SystemExit(
            f"Closest snapshot to '{opening_ts_str}' is {best_row['period_end_ts']} "
            f"({best_delta/3600:.1f}h away). Check --opening-ts."
        )
    return best_row


# ── data loading ──────────────────────────────────────────────────────────────

def load_manual_positions(path: str, symbol: str,
                           after_ts: datetime.datetime) -> list[tuple]:
    """Manual BUY events strictly after after_ts."""
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
            ts = _parse_ts(str(r.get("ts") or ""))
            if ts <= after_ts:
                continue
            events.append((ts, "BUY", qty, price, "manual"))
    except FileNotFoundError:
        pass
    return events


def load_capital_flows(path: str, after_ts: datetime.datetime) -> list[tuple]:
    """
    Capital flows (USDC deposits / withdrawals) strictly after after_ts.
    Format: [{ts, delta, note}, ...]  — delta>0 = deposit, delta<0 = withdrawal.
    Returns (ts, event, qty=|delta|, price=0, reason=note).
    """
    events = []
    try:
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        for r in rows:
            ts    = _parse_ts(str(r.get("ts") or ""))
            if ts <= after_ts:
                continue
            delta = _dec(r.get("delta") or "0")
            if delta == D0:
                continue
            event  = "DEPOSIT" if delta > D0 else "WITHDRAW"
            reason = str(r.get("note") or "")
            events.append((ts, event, abs(delta), D0, reason))
    except FileNotFoundError:
        pass
    return events


def load_bot_trades(trades_paths: list[str], symbol: str,
                    after_ts: datetime.datetime) -> list[tuple]:
    """Bot FILL events strictly after after_ts, deduplicated by order_id."""
    seen: set[str] = set()
    events: list[tuple] = []
    for path in trades_paths:
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
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
                    if ts <= after_ts:
                        continue
                    side   = str(r.get("side") or "").upper()
                    price  = _dec(r.get("price") or "0")
                    reason = str(r.get("reason") or "")
                    events.append((ts, side, qty, price, reason))
        except FileNotFoundError:
            pass
    return events


# ── core ──────────────────────────────────────────────────────────────────────

def generate(snapshots_path: str, opening_ts_str: str,
             trades_paths: list[str], manual_positions_path: str | None,
             capital_flows_path: str | None,
             symbol: str, out_path: str) -> None:

    # ── opening snapshot ──
    snap = find_opening_snapshot(snapshots_path, opening_ts_str)
    open_ts   = _parse_ts(snap["period_end_ts"])
    open_Q    = _dec(snap["Q_eth_holding"])
    open_A    = _dec(snap["Z_eth_cmp"])     # reset A to Z (mark-to-market baseline)
    open_cash = _dec(snap["cash_usdc"])
    open_port = _dec(snap["portfolio_value"])

    print(f"Opening snapshot : {snap['period_end_ts']}")
    print(f"  Q={_r(open_Q,5)}  A(reset)={_r2(open_A)}  cash={_r2(open_cash)}  portfolio={_r2(open_port)}")
    print(f"  (original A_avg_cost was {snap.get('A_eth_avg_cost','?')} — reset to Z={_r2(open_A)})")

    # ── load events after checkpoint ──
    manual_events = []
    if manual_positions_path:
        manual_events = load_manual_positions(manual_positions_path, symbol, open_ts)

    flow_events = []
    if capital_flows_path:
        flow_events = load_capital_flows(capital_flows_path, open_ts)

    bot_events = load_bot_trades(trades_paths, symbol, open_ts)

    all_events = sorted(manual_events + flow_events + bot_events, key=lambda e: e[0])
    print(f"Events after checkpoint: {len(bot_events)} bot trades  +  {len(manual_events)} manual ETH  +  {len(flow_events)} cash flows  =  {len(all_events)} total")

    # ── build ledger rows ──
    rows: list[dict] = []

    # Opening entry
    rows.append({
        "ts":         _fmt_ist(open_ts),
        "event":      "OPEN",
        "reason":     "checkpoint",
        "qty":        "",
        "price":      _r2(open_A),          # show opening price = Z
        "Q_before":   "",
        "Q_after":    _r(open_Q, 5),
        "A_before":   "",
        "A_after":    _r2(open_A),
        "A_change":   "",
        "cash_change": "",
        "cash_after": _r2(open_cash),
        "trade_pnl":  "0.00",
        "cum_pnl":    "0.00",
    })

    Q       = open_Q
    A       = open_A
    cash    = open_cash
    cum_pnl = D0

    for ts, event_type, qty, price, reason in all_events:
        Q_before = Q
        A_before = A
        side = event_type   # alias for clarity in the block below

        if side == "BUY":
            new_Q = Q + qty
            new_A = (Q * A + qty * price) / new_Q if new_Q > D0 else price
            cash_change = -(qty * price)
            trade_pnl   = D0
            Q = new_Q
            A = new_A
        elif side == "SELL":
            new_Q = Q - qty
            new_A = A          # A unchanged on sell
            cash_change = qty * price
            trade_pnl   = (price - A_before) * qty
            cum_pnl    += trade_pnl
            Q = new_Q
            A = new_A
        elif side in ("DEPOSIT", "WITHDRAW"):
            # Cash only — Q and A unchanged
            cash_change = qty if side == "DEPOSIT" else -qty
            trade_pnl   = D0

        cash += cash_change

        rows.append({
            "ts":          _fmt_ist(ts),
            "event":       event_type,
            "reason":      reason,
            "qty":         _r(qty, 5),
            "price":       _r2(price),
            "Q_before":    _r(Q_before, 5),
            "Q_after":     _r(Q, 5),
            "A_before":    _r2(A_before),
            "A_after":     _r2(A),
            "A_change":    _r2(A - A_before),
            "cash_change": _r2(cash_change),
            "cash_after":  _r2(cash),
            "trade_pnl":   _r2(trade_pnl),
            "cum_pnl":     _r2(cum_pnl),
        })

    # ── write CSV ──
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWritten {len(rows)} rows (1 opening + {len(all_events)} trades) to {out_path}")
    print(f"  Final  Q={_r(Q,5)}  A={_r2(A)}  cash={_r2(cash)}  cum_pnl={_r2(cum_pnl)}")

    # Print last 10 trades
    recent = rows[-10:]
    print()
    print(f"{'Timestamp':<26} {'Ev':<4} {'Qty':>8} {'Price':>9} {'Q_after':>9} {'A_after':>9} {'CashChg':>10} {'TrdPnl':>8} {'CumPnl':>9}")
    print("-" * 100)
    for r in recent:
        print(
            f"{r['ts']:<26} "
            f"{r['event']:<4} "
            f"{r['qty']:>8} "
            f"{r['price']:>9} "
            f"{r['Q_after']:>9} "
            f"{r['A_after']:>9} "
            f"{r['cash_change']:>10} "
            f"{r['trade_pnl']:>8} "
            f"{r['cum_pnl']:>9}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots",        required=True,
                    help="Path to mexc_snapshots.csv")
    ap.add_argument("--opening-ts",       required=True,
                    help="Checkpoint timestamp e.g. '2026-03-24T23:00'")
    ap.add_argument("--trades",           required=True, nargs="+",
                    help="Trade .jsonl files")
    ap.add_argument("--manual-positions", default=None,
                    help="manual_positions JSON file (ETH buys)")
    ap.add_argument("--capital-flows",    default=None,
                    help="capital_flows JSON file (USDC deposits/withdrawals)")
    ap.add_argument("--symbol",           default="ETHUSDC")
    ap.add_argument("--out",              required=True,
                    help="Output ledger CSV path")
    args = ap.parse_args()
    generate(
        snapshots_path=args.snapshots,
        opening_ts_str=args.opening_ts,
        trades_paths=args.trades,
        manual_positions_path=args.manual_positions,
        capital_flows_path=args.capital_flows,
        symbol=args.symbol,
        out_path=args.out,
    )


if __name__ == "__main__":
    main()
