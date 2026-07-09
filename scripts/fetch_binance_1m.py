#!/usr/bin/env python3
"""
fetch_binance_1m.py — Download 1-minute OHLCV klines from Binance.

Data is saved to data/binance/<SYMBOL>_1m.parquet (incrementally updated).
Subsequent runs only download missing date ranges.

Usage:
    python scripts/fetch_binance_1m.py --symbol BTCUSDT --months 3
    python scripts/fetch_binance_1m.py --symbol BTCUSDT ETHUSDT --from 2026-01-01 --to 2026-06-20
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from intraday_research.binance import fetch_and_cache


def main():
    ap = argparse.ArgumentParser(description="Download 1-min Binance klines → data/binance/")
    ap.add_argument("--symbol", nargs="+", required=True, metavar="SYM",
                    help="Binance spot symbol(s) e.g. BTCUSDT ETHUSDT")
    ap.add_argument("--months", type=int, default=3,
                    help="Months of history to fetch (default: 3)")
    ap.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD (overrides --months)")
    ap.add_argument("--to",   dest="date_to",   help="End date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--out-dir", default="data/binance", help="Output directory")
    args = ap.parse_args()

    today     = date.today()
    yesterday = today - timedelta(days=1)
    end_date  = date.fromisoformat(args.date_to) if args.date_to else yesterday

    if args.date_from:
        start_date = date.fromisoformat(args.date_from)
    else:
        start_date = end_date.replace(day=1)
        for _ in range(args.months - 1):
            start_date = (start_date - timedelta(days=1)).replace(day=1)

    out_dir = REPO_ROOT / args.out_dir

    print(f"Symbols: {args.symbol}  {start_date} → {end_date}")
    print(f"Output:  {out_dir}\n")

    for sym in args.symbol:
        print(f"── {sym} ──")
        fetch_and_cache(sym.upper(), start_date, end_date, out_dir, verbose=True)
        print()


if __name__ == "__main__":
    main()
