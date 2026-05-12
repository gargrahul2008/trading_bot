#!/usr/bin/env python3
"""
fetch_binance_ticks.py — Download historical ETHUSDT aggTrades from Binance Data Vision
and save as daily parquet files in backtest/ticks/ (same format as mexc_tick_collector).

Source: https://data.binance.vision/data/spot/daily/aggTrades/ETHUSDT/

Usage:
    python3 scripts/fetch_binance_ticks.py --days 90
    python3 scripts/fetch_binance_ticks.py --from 2026-01-01 --to 2026-04-30
    python3 scripts/fetch_binance_ticks.py --days 30 --dir backtest/ticks
"""
from __future__ import annotations

import argparse
import io
import os
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

BASE_URL  = "https://data.binance.vision/data/spot/daily/aggTrades/ETHUSDT"
SYMBOL    = "ETHUSDT"
OUT_SYMBOL = "binance_ETHUSDT"   # prefix for output files


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fetch_day(day: date, out_dir: Path, session: requests.Session) -> bool:
    out_file = out_dir / f"{OUT_SYMBOL}_{day}.parquet"
    if out_file.exists():
        print(f"  {day}  already exists — skipping")
        return True

    url = f"{BASE_URL}/{SYMBOL}-aggTrades-{day}.zip"
    try:
        r = session.get(url, timeout=60)
        if r.status_code == 404:
            print(f"  {day}  not available yet")
            return False
        r.raise_for_status()
    except Exception as e:
        print(f"  {day}  download failed: {e}")
        return False

    # Parse zip → CSV
    # Binance aggTrades columns (8 cols as of 2026):
    # agg_id, price, qty, first_id, last_id, ts_us, is_buyer_maker, best_match
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                raw = pd.read_csv(f, header=None)
        n_cols = len(raw.columns)
        col_names = ["agg_id", "price", "qty", "first_id", "last_id", "ts_us", "is_buyer_maker"]
        if n_cols >= 8:
            col_names.append("best_match")
        raw.columns = col_names[:n_cols]
        df = raw[["ts_us", "price", "qty", "is_buyer_maker"]].copy()
    except Exception as e:
        print(f"  {day}  parse failed: {e}")
        return False

    # Match mexc_tick_collector format: ts(ms int), price(float), qty(float), is_buyer_maker(bool)
    # ts_us is in microseconds — convert to milliseconds
    df = df.rename(columns={"ts_us": "ts"})
    df["ts"]             = (df["ts"] // 1000).astype("int64")   # µs → ms
    df["price"]          = df["price"].astype("float64")
    df["qty"]            = df["qty"].astype("float64")
    df["is_buyer_maker"] = df["is_buyer_maker"].astype("bool")

    df.to_parquet(out_file, index=False)
    size_kb = out_file.stat().st_size / 1024
    print(f"  {day}  {len(df):>8,} ticks   {size_kb:>7.1f} KB   saved → {out_file.name}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30,
                    help="Number of past days to fetch (default: 30)")
    ap.add_argument("--from", dest="date_from", default=None,
                    help="Start date YYYY-MM-DD (overrides --days)")
    ap.add_argument("--to", dest="date_to", default=None,
                    help="End date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--dir", default="backtest/ticks",
                    help="Output directory (default: backtest/ticks)")
    args = ap.parse_args()

    out_dir = Path(args.dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    today     = date.today()
    yesterday = today - timedelta(days=1)

    end_date   = date.fromisoformat(args.date_to)   if args.date_to   else yesterday
    start_date = date.fromisoformat(args.date_from) if args.date_from else (end_date - timedelta(days=args.days - 1))

    days = list(date_range(start_date, end_date))
    print(f"Fetching {len(days)} days: {start_date} → {end_date}")
    print(f"Output dir: {out_dir.resolve()}")
    print()

    session = requests.Session()
    ok = skipped = failed = 0

    for day in days:
        result = fetch_day(day, out_dir, session)
        if result:
            ok += 1
        else:
            failed += 1
        time.sleep(0.3)   # be polite

    print()
    print(f"Done. {ok} fetched/cached, {failed} failed.")


if __name__ == "__main__":
    main()
