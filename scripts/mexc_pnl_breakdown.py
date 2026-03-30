#!/usr/bin/env python3
"""
MEXC PnL Breakdown — reads mexc_snapshots.csv and produces a reconciling
period-by-period breakdown where:

    opening_portfolio + holding_pnl + cycle_pnl + rebal_pnl + carry = closing_portfolio

Columns:
  - period_start / period_end
  - open: portfolio, Q, Z (ETH price), cash
  - close: portfolio, Q, Z, cash
  - actual_delta     = close_portfolio - open_portfolio
  - holding_pnl      = open_Q × (close_Z - open_Z)   [ETH price change on held qty]
  - cycle_pnl        = realized ladder cycle pnl in period
  - rebal_pnl        = realized rebalancing pnl in period
  - carry            = actual_delta - holding_pnl - cycle_pnl - rebal_pnl
                       (value of net ETH qty change at close_Z, plus rounding)
  - check            = holding_pnl + cycle_pnl + rebal_pnl + carry  (should = actual_delta)

Usage:
    python3 scripts/mexc_pnl_breakdown.py \\
        --snapshots strategies/pct_ladder/state/mexc_snapshots.csv \\
        --out       strategies/pct_ladder/state/mexc_pnl_breakdown.csv
"""
from __future__ import annotations

import argparse
import csv
import os
from decimal import Decimal, ROUND_HALF_UP

D0 = Decimal("0")

HEADERS = [
    "period_start_ts",
    "period_end_ts",
    # Opening snapshot values
    "open_portfolio",
    "open_Q",
    "open_Z",
    "open_cash",
    # Closing snapshot values
    "close_portfolio",
    "close_Q",
    "close_Z",
    "close_cash",
    # Delta decomposition
    "actual_delta",
    "holding_pnl",      # open_Q × (close_Z - open_Z)
    "cycle_pnl",        # realized ladder cycle pnl
    "rebal_pnl",        # realized rebalancing pnl
    "carry",            # residual = actual_delta - holding - cycle - rebal
    "check",            # holding + cycle + rebal + carry (= actual_delta)
]


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


def generate(snapshots_path: str, out_path: str) -> None:
    if not os.path.exists(snapshots_path):
        print(f"ERROR: {snapshots_path} not found")
        return

    rows: list[dict] = []
    with open(snapshots_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if len(rows) < 2:
        print("Need at least 2 snapshot rows to compute a breakdown.")
        return

    breakdown: list[dict] = []

    for i in range(1, len(rows)):
        prev = rows[i - 1]
        curr = rows[i]

        open_portfolio  = _dec(prev["portfolio_value"])
        open_Q          = _dec(prev["Q_eth_holding"])
        open_Z          = _dec(prev["Z_eth_cmp"])
        open_cash       = _dec(prev["cash_usdc"])

        close_portfolio = _dec(curr["portfolio_value"])
        close_Q         = _dec(curr["Q_eth_holding"])
        close_Z         = _dec(curr["Z_eth_cmp"])
        close_cash      = _dec(curr["cash_usdc"])

        actual_delta    = close_portfolio - open_portfolio
        holding_pnl     = open_Q * (close_Z - open_Z)
        cycle_pnl       = _dec(curr["period_cycle_pnl"])
        rebal_pnl       = _dec(curr["period_rebal_pnl"])
        carry           = actual_delta - holding_pnl - cycle_pnl - rebal_pnl
        check           = holding_pnl + cycle_pnl + rebal_pnl + carry

        breakdown.append({
            "period_start_ts":  curr["period_start_ts"],
            "period_end_ts":    curr["period_end_ts"],
            "open_portfolio":   _r(open_portfolio, 2),
            "open_Q":           _r(open_Q, 5),
            "open_Z":           _r(open_Z, 4),
            "open_cash":        _r(open_cash, 2),
            "close_portfolio":  _r(close_portfolio, 2),
            "close_Q":          _r(close_Q, 5),
            "close_Z":          _r(close_Z, 4),
            "close_cash":       _r(close_cash, 2),
            "actual_delta":     _r(actual_delta, 2),
            "holding_pnl":      _r(holding_pnl, 2),
            "cycle_pnl":        _r(cycle_pnl, 2),
            "rebal_pnl":        _r(rebal_pnl, 2),
            "carry":            _r(carry, 2),
            "check":            _r(check, 2),
        })

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(breakdown)

    print(f"Written {len(breakdown)} rows to {out_path}")

    # Print recent rows summary
    recent = breakdown[-10:]
    print()
    print(f"{'Period End':<25} {'Open Port':>10} {'Close Port':>10} {'Delta':>8} {'Holding':>8} {'Cycle':>8} {'Rebal':>8} {'Carry':>8}")
    print("-" * 100)
    for r in recent:
        print(
            f"{r['period_end_ts']:<25} "
            f"{r['open_portfolio']:>10} "
            f"{r['close_portfolio']:>10} "
            f"{r['actual_delta']:>8} "
            f"{r['holding_pnl']:>8} "
            f"{r['cycle_pnl']:>8} "
            f"{r['rebal_pnl']:>8} "
            f"{r['carry']:>8}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots", required=True,
                    help="Path to mexc_snapshots.csv")
    ap.add_argument("--out", required=True,
                    help="Output path for pnl breakdown CSV")
    args = ap.parse_args()
    generate(args.snapshots, args.out)


if __name__ == "__main__":
    main()
