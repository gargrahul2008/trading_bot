#!/usr/bin/env python3
"""
Fetch ALL historical data for every stock that was ever in Nifty50 (2019-present).
Saves one master pickle that the backtest can use for any date range.

Daily data:  from 2017-01-01 (gives 2yr lookback for a 2019 sim start)
5-min data:  from 2019-01-01 (or earliest Fyers provides)

Usage:
    env/bin/python permanent_grid/fetch_master_data.py
    env/bin/python permanent_grid/fetch_master_data.py --resume
    env/bin/python permanent_grid/fetch_master_data.py --daily-only
    env/bin/python permanent_grid/fetch_master_data.py --min5-only
"""

import sys, os, json, pickle, time, argparse
sys.path.insert(0, "/root/trading_bot")

from datetime import datetime, timedelta
import pandas as pd
from common.broker.fyers_client import FyersClient
from permanent_grid.universe import MASTER_STOCKS, NIFTY_SYMBOL

DAILY_START = "2017-01-01"
MIN5_START = "2019-01-01"
END_DATE = "2026-06-08"

DAILY_CHUNK_DAYS = 365
MIN5_CHUNK_DAYS = 95

OUTPUT_DIR = "/root/trading_bot/permanent_grid/data"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "master_data.pkl")


def get_broker():
    with open("/root/trading_bot/fyers_auth.json") as f:
        auth = json.load(f)
    user = auth["users"]["user1"]
    return FyersClient(client_id=user["client_id"], access_token=user["access_token"])


def fetch_candles(broker, symbol: str, resolution: str,
                  start_str: str, end_str: str, chunk_days: int) -> pd.DataFrame:
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")

    all_candles = []
    chunk_start = start_dt

    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end_dt)
        data = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",
            "range_from": chunk_start.strftime("%Y-%m-%d"),
            "range_to": chunk_end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        }
        try:
            resp = broker.history(data)
            candles = resp.get("candles") or []
            if candles:
                all_candles.extend(candles)
        except Exception as e:
            print(f"  WARN {symbol} {chunk_start.date()}→{chunk_end.date()}: {e}")

        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(0.35)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df["date"] = df["datetime"].dt.normalize()
    return df


def save_master(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


def main():
    parser = argparse.ArgumentParser(description="Fetch master data for all Nifty50 stocks")
    parser.add_argument("--resume", action="store_true", help="Skip already-fetched symbols")
    parser.add_argument("--daily-only", action="store_true", help="Only fetch daily data")
    parser.add_argument("--min5-only", action="store_true", help="Only fetch 5-min data")
    parser.add_argument("--end", default=END_DATE, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    end_date = args.end
    fetch_daily = not args.min5_only
    fetch_min5 = not args.daily_only

    # Load existing data for resume
    master = {"daily": {}, "min5": {}, "nifty_daily": None, "nifty_5min": None,
              "daily_range": (DAILY_START, end_date),
              "min5_range": (MIN5_START, end_date)}

    if args.resume and os.path.exists(OUTPUT_PATH):
        print(f"Loading existing data from {OUTPUT_PATH}...")
        with open(OUTPUT_PATH, "rb") as f:
            master = pickle.load(f)
        print(f"  Daily: {len(master.get('daily', {}))} stocks")
        print(f"  5-min: {len(master.get('min5', {}))} stocks")

    broker = get_broker()
    all_syms = sorted(MASTER_STOCKS.keys())
    total = len(all_syms)

    print(f"\n{'='*70}")
    print(f"  Master Data Fetch: {total} stocks")
    print(f"  Daily: {DAILY_START} → {end_date}")
    print(f"  5-min: {MIN5_START} → {end_date}")
    print(f"{'='*70}\n")

    # ── Nifty50 index ──
    if fetch_daily and master.get("nifty_daily") is None:
        print(f"Fetching Nifty50 index (daily)...")
        df = fetch_candles(broker, NIFTY_SYMBOL, "D", DAILY_START, end_date, DAILY_CHUNK_DAYS)
        if not df.empty:
            master["nifty_daily"] = df
            print(f"  Nifty daily: {len(df)} bars, {df['date'].nunique()} days")
        else:
            print(f"  Nifty daily: NO DATA")
        time.sleep(0.5)

    if fetch_min5 and master.get("nifty_5min") is None:
        print(f"Fetching Nifty50 index (5-min)...")
        df = fetch_candles(broker, NIFTY_SYMBOL, "5", MIN5_START, end_date, MIN5_CHUNK_DAYS)
        if not df.empty:
            master["nifty_5min"] = df
            print(f"  Nifty 5-min: {len(df)} candles, {df['date'].nunique()} days")
        else:
            print(f"  Nifty 5-min: NO DATA")
        time.sleep(0.5)

    # ── Individual stocks ──
    fetched_daily = 0
    fetched_min5 = 0

    for i, sym in enumerate(all_syms, 1):
        info = MASTER_STOCKS[sym]
        fsym = info["fyers"]

        need_daily = fetch_daily and sym not in master.get("daily", {})
        need_min5 = fetch_min5 and sym not in master.get("min5", {})

        if not need_daily and not need_min5:
            continue

        print(f"[{i}/{total}] {sym} ({fsym})")

        if need_daily:
            df = fetch_candles(broker, fsym, "D", DAILY_START, end_date, DAILY_CHUNK_DAYS)
            if not df.empty:
                master["daily"][sym] = df
                print(f"    daily: {len(df)} bars, {df['date'].nunique()} days "
                      f"({df['date'].min().date()} → {df['date'].max().date()})")
                fetched_daily += 1
            else:
                print(f"    daily: NO DATA")
            time.sleep(0.4)

        if need_min5:
            df = fetch_candles(broker, fsym, "5", MIN5_START, end_date, MIN5_CHUNK_DAYS)
            if not df.empty:
                master["min5"][sym] = df
                days = df['date'].nunique()
                earliest = df['date'].min().date()
                print(f"    5-min: {len(df)} candles, {days} days "
                      f"({earliest} → {df['date'].max().date()})")
                fetched_min5 += 1
            else:
                print(f"    5-min: NO DATA (Fyers may not have intraday history this far back)")
            time.sleep(0.4)

        # Checkpoint every 5 stocks
        if (fetched_daily + fetched_min5) > 0 and i % 5 == 0:
            save_master(master, OUTPUT_PATH)
            print(f"    --- checkpoint: {len(master['daily'])} daily, {len(master['min5'])} min5 ---")

    # Final save
    master["daily_range"] = (DAILY_START, end_date)
    master["min5_range"] = (MIN5_START, end_date)
    master["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    save_master(master, OUTPUT_PATH)

    print(f"\n{'='*70}")
    print(f"  DONE — saved to {OUTPUT_PATH}")
    print(f"  Daily: {len(master['daily'])} stocks  (fetched {fetched_daily} new)")
    print(f"  5-min: {len(master['min5'])} stocks  (fetched {fetched_min5} new)")
    nifty_d = master.get('nifty_daily')
    nifty_5 = master.get('nifty_5min')
    if nifty_d is not None:
        print(f"  Nifty daily: {len(nifty_d)} bars")
    if nifty_5 is not None:
        print(f"  Nifty 5-min: {len(nifty_5)} candles")

    # Report stocks with no data
    no_daily = [s for s in all_syms if s not in master['daily']]
    no_min5 = [s for s in all_syms if s not in master['min5']]
    if no_daily:
        print(f"\n  No daily data: {', '.join(no_daily)}")
    if no_min5:
        print(f"  No 5-min data: {', '.join(no_min5)}")

    size_mb = os.path.getsize(OUTPUT_PATH) / 1024 / 1024
    print(f"\n  File size: {size_mb:.0f} MB")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
