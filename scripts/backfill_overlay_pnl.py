#!/usr/bin/env python3
"""
One-time backfill of pnl_new.csv from the cutoff date onwards.

Reads Q0/C0 from mexc_ledger.csv (last row before cutoff), then replays
every post-cutoff FILL from the trade JSONL and writes a row per trade.
Mark price for historical rows = fill price (best available at trade time).

Usage:
    python3 scripts/backfill_overlay_pnl.py
"""
from __future__ import annotations

import csv
import datetime
import json
import os
import sys
from decimal import Decimal
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CUTOFF_TS   = "2026-04-13T00:00:00+00:00"
LEDGER_PATH = "strategies/pct_ladder/state/mexc_ledger.csv"
TRADES_PATH = "strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl"
OUT_PATH    = "strategies/pct_ladder/state/pnl_new.csv"

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc

FIELDS = [
    "ts", "event", "trade_type", "symbol", "side", "qty", "fill_price",
    "overlay_qty", "overlay_cash", "mark_price", "overlay_pnl",
    "rebal_overlay_qty", "rebal_overlay_cash", "rebal_pnl",
    "baseline_qty", "baseline_cash", "baseline_value", "account_value_est",
]

D0 = Decimal("0")


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _to_utc_iso(ts_str: str) -> str:
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(UTC).isoformat()
    except Exception:
        return ts_str


def read_baseline() -> tuple:
    """Return (Q0, C0) from last ledger row before CUTOFF_TS."""
    last_row = None
    with open(LEDGER_PATH, "r", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if _to_utc_iso(str(row.get("ts", ""))) < CUTOFF_TS:
                last_row = row
    if last_row is None:
        raise SystemExit("No ledger rows found before cutoff. Check LEDGER_PATH and CUTOFF_TS.")
    Q0 = _dec(last_row["Q_after"])
    C0 = _dec(last_row["cash_after"])
    print(f"Baseline from ledger row ts={last_row['ts']}")
    print(f"  Q0 = {Q0} ETH")
    print(f"  C0 = {C0} USDC")
    return Q0, C0


def load_post_cutoff_trades() -> list:
    """Return list of FILL dicts with ts >= CUTOFF_TS, in chronological order."""
    trades = []
    with open(TRADES_PATH, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                t = json.loads(raw)
            except Exception:
                continue
            if t.get("event") != "FILL":
                continue
            if _dec(t.get("qty", 0)) <= 0:
                continue
            if str(t.get("ts", "")) < CUTOFF_TS:
                continue
            trades.append(t)
    return trades


def backfill():
    if os.path.exists(OUT_PATH):
        answer = input(f"{OUT_PATH} already exists. Overwrite? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    Q0, C0 = read_baseline()
    trades = load_post_cutoff_trades()
    print(f"\nFound {len(trades)} post-cutoff trades to replay.\n")

    overlay_qty  = D0
    overlay_cash = D0
    rebal_qty    = D0
    rebal_cash   = D0

    rows = []
    for t in trades:
        qty        = _dec(t.get("qty", 0))
        cum_quote  = _dec(t.get("cum_quote_qty", 0))
        price      = _dec(t.get("price", 0))
        side       = str(t.get("side", "")).upper()
        symbol     = str(t.get("symbol", ""))
        ts_raw     = str(t.get("ts", ""))
        reason     = str(t.get("reason", ""))
        is_rebal   = reason.startswith("rebalance_")
        trade_type = "rebalance" if is_rebal else "ladder"

        if side == "BUY":
            overlay_qty  += qty
            overlay_cash -= cum_quote
            if is_rebal:
                rebal_qty  += qty
                rebal_cash -= cum_quote
        else:
            overlay_qty  -= qty
            overlay_cash += cum_quote
            if is_rebal:
                rebal_qty  -= qty
                rebal_cash += cum_quote

        mark_price      = price
        overlay_pnl     = overlay_cash + overlay_qty  * mark_price
        rebal_pnl       = rebal_cash   + rebal_qty    * mark_price
        baseline_value  = C0 + Q0 * mark_price
        account_val_est = baseline_value + overlay_pnl

        # Convert ts to IST
        try:
            import datetime as _dt
            from zoneinfo import ZoneInfo
            dt = _dt.datetime.fromisoformat(ts_raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            ts_ist = dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")
        except Exception:
            ts_ist = ts_raw

        def r(v, d=3): return f"{float(v):.{d}f}"

        rows.append({
            "ts":                 ts_ist,
            "event":              "FILL",
            "trade_type":         trade_type,
            "symbol":             symbol,
            "side":               side,
            "qty":                r(qty, 6),
            "fill_price":         r(price, 3),
            "overlay_qty":        r(overlay_qty, 6),
            "overlay_cash":       r(overlay_cash, 3),
            "mark_price":         r(mark_price, 3),
            "overlay_pnl":        r(overlay_pnl, 3),
            "rebal_overlay_qty":  r(rebal_qty, 6),
            "rebal_overlay_cash": r(rebal_cash, 3),
            "rebal_pnl":          r(rebal_pnl, 3),
            "baseline_qty":       r(Q0, 6),
            "baseline_cash":      r(C0, 3),
            "baseline_value":     r(baseline_value, 3),
            "account_value_est":  r(account_val_est, 3),
        })

    os.makedirs(os.path.dirname(os.path.abspath(OUT_PATH)), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    print(f"Written {len(rows)} rows to {OUT_PATH}")
    print(f"\nFinal overlay state:")
    print(f"  overlay_qty  = {overlay_qty}")
    print(f"  overlay_cash = {overlay_cash}")
    last = rows[-1]
    print(f"  overlay_pnl (at last fill price) = {last['overlay_pnl']}")
    print(f"  account_value_est                = {last['account_value_est']}")


if __name__ == "__main__":
    backfill()
