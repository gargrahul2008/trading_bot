#!/usr/bin/env python3
"""
MEXC tick data collector — runs as a standalone process, completely separate from the bot.

Polls /api/v3/aggTrades every POLL_INTERVAL seconds, deduplicates by timestamp,
and appends to daily parquet files in backtest/ticks/.

At current ETHUSDC volume (~2,600 trades/day), 1000 trades covers ~9 hours —
polling every 10s gives massive overlap so no trades are ever missed.

Storage: ~260 KB/day (tiny — only actual trades, no empty rows).

Usage:
    python3 scripts/mexc_tick_collector.py
    python3 scripts/mexc_tick_collector.py --symbol ETHUSDC --interval 10 --dir backtest/ticks
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://api.mexc.com/api/v3/aggTrades"


# ─────────────────────────────────────────────────────────────────────────────
# Fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_trades(symbol: str, limit: int = 1000) -> list:
    resp = requests.get(BASE_URL, params={"symbol": symbol, "limit": limit}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response: {data}")
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────

def tick_file(tick_dir: Path, symbol: str, ts_ms: int) -> Path:
    day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return tick_dir / f"mexc_{symbol}_{day}.parquet"


def load_last_ts(tick_dir: Path, symbol: str) -> int:
    """Return the highest timestamp already saved, or 0 if no files yet."""
    files = sorted(tick_dir.glob(f"mexc_{symbol}_*.parquet"))
    if not files:
        return 0
    try:
        df = pd.read_parquet(files[-1])
        return int(df["ts"].max())
    except Exception:
        return 0


def append_ticks(trades: list, symbol: str, tick_dir: Path, last_ts: int) -> tuple[int, int]:
    """
    Filter trades newer than last_ts, write to daily parquet files.
    Returns (new_last_ts, count_appended).
    """
    new = [t for t in trades if int(t["T"]) > last_ts]
    if not new:
        return last_ts, 0

    new.sort(key=lambda t: t["T"])

    df = pd.DataFrame({
        "ts":             [int(t["T"])    for t in new],
        "price":          [float(t["p"]) for t in new],
        "qty":            [float(t["q"]) for t in new],
        "is_buyer_maker": [bool(t["m"])  for t in new],
    })

    # Split by UTC day and append to each day's file
    df["_day"] = df["ts"].apply(
        lambda x: datetime.fromtimestamp(x / 1000, tz=timezone.utc).date()
    )
    for day, grp in df.groupby("_day"):
        grp = grp.drop(columns=["_day"]).reset_index(drop=True)
        fpath = tick_dir / f"mexc_{symbol}_{day}.parquet"
        if fpath.exists():
            existing = pd.read_parquet(fpath)
            grp = pd.concat([existing, grp], ignore_index=True)
            grp = grp.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
        grp.to_parquet(fpath, index=False)

    return int(df["ts"].max()), len(new)


# ─────────────────────────────────────────────────────────────────────────────
# Stats helper
# ─────────────────────────────────────────────────────────────────────────────

def print_stats(tick_dir: Path, symbol: str) -> None:
    files = sorted(tick_dir.glob(f"mexc_{symbol}_*.parquet"))
    if not files:
        print("  No tick files yet.")
        return
    total = 0
    for f in files:
        try:
            df = pd.read_parquet(f)
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name}: {len(df):,} trades  ({size_kb:.1f} KB)")
            total += len(df)
        except Exception as e:
            print(f"  {f.name}: error reading — {e}")
    print(f"  Total: {total:,} trades across {len(files)} day(s)")


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="MEXC tick data collector")
    parser.add_argument("--symbol",   default="ETHUSDC")
    parser.add_argument("--interval", type=int, default=10, help="Poll interval in seconds")
    parser.add_argument("--dir",      default="backtest/ticks", help="Directory to store tick parquet files")
    parser.add_argument("--stats",    action="store_true", help="Print stored tick stats and exit")
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent
    tick_dir  = repo_root / args.dir
    tick_dir.mkdir(parents=True, exist_ok=True)

    if args.stats:
        print(f"Tick data stats for {args.symbol}:")
        print_stats(tick_dir, args.symbol)
        return

    last_ts = load_last_ts(tick_dir, args.symbol)
    last_dt = (datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
               if last_ts else "none")

    print(f"[tick_collector] symbol={args.symbol}  poll={args.interval}s  dir={tick_dir}")
    print(f"[tick_collector] Resuming from last saved ts: {last_dt}")

    running = True

    def _stop(sig, frame):
        nonlocal running
        print("\n[tick_collector] Signal received — stopping cleanly.")
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    consecutive_errors = 0
    total_saved = 0

    while running:
        try:
            trades = fetch_trades(args.symbol)
            last_ts, saved = append_ticks(trades, args.symbol, tick_dir, last_ts)
            consecutive_errors = 0

            if saved > 0:
                total_saved += saved
                now  = datetime.now(timezone.utc).strftime("%H:%M:%S")
                last = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).strftime("%H:%M:%S")
                print(f"[tick_collector] {now} UTC  +{saved} trades  last={last} UTC  total={total_saved:,}")

        except Exception as e:
            consecutive_errors += 1
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[tick_collector] {now} UTC  ERROR #{consecutive_errors}: {e}", file=sys.stderr)
            if consecutive_errors >= 10:
                print("[tick_collector] 10 consecutive errors — sleeping 60s before retry", file=sys.stderr)
                time.sleep(60)
                consecutive_errors = 0

        time.sleep(args.interval)

    print(f"[tick_collector] Stopped. Total trades saved this session: {total_saved:,}")


if __name__ == "__main__":
    main()
