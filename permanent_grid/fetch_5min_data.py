#!/usr/bin/env python3
"""
Fetch 5-minute candle data for all Nifty50 universe stocks + Nifty index.
Fyers API limits: max 100 calendar days per request for intraday resolutions.

Usage:
    env/bin/python permanent_grid/fetch_5min_data.py [--start 2023-01-01] [--end 2026-06-03]
"""

import sys, os, json, pickle, time, argparse
sys.path.insert(0, "/root/trading_bot")

from datetime import datetime, timedelta
import pandas as pd
from common.broker.fyers_client import FyersClient

CHUNK_DAYS = 95  # stay under Fyers' 100-day limit
NIFTY_SYMBOL = "NSE:NIFTY50-INDEX"
OUTPUT_DIR = "/root/trading_bot/permanent_grid/data"


def get_broker():
    with open("/root/trading_bot/fyers_auth.json") as f:
        auth = json.load(f)
    user = auth["users"]["user1"]
    return FyersClient(client_id=user["client_id"], access_token=user["access_token"])


def fetch_5min(broker, symbol: str, start_str: str, end_str: str) -> pd.DataFrame:
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")

    all_candles = []
    chunk_start = start_dt

    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_dt)
        data = {
            "symbol": symbol,
            "resolution": "5",
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
            print(f"    WARN: {symbol} {chunk_start.date()}→{chunk_end.date()}: {e}")

        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(0.35)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df["date"] = df["datetime"].dt.normalize()
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch 5-min data from Fyers")
    parser.add_argument("--start", default="2023-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-06-03", help="End date YYYY-MM-DD")
    parser.add_argument("--resume", action="store_true",
                        help="Resume: skip symbols already in output pkl")
    args = parser.parse_args()

    # Load universe from existing basket pkl
    basket_pkl = os.path.join(OUTPUT_DIR, "basket_3y.pkl")
    with open(basket_pkl, "rb") as f:
        basket_data = pickle.load(f)

    universe = basket_data.get("universe", [])
    basket = basket_data.get("basket", [])
    if not universe:
        print("ERROR: No universe data in basket pkl. Run select_basket_2023.py first.")
        return

    # Build full symbol list (universe + basket, deduplicated)
    all_stocks = {}
    for item in universe:
        all_stocks[item["symbol"]] = item["fyers_symbol"]
    for item in basket:
        all_stocks[item["symbol"]] = item["fyers_symbol"]

    broker = get_broker()
    output_path = os.path.join(OUTPUT_DIR, "universe_5min.pkl")

    # Resume support
    existing_data = {}
    if args.resume and os.path.exists(output_path):
        with open(output_path, "rb") as f:
            saved = pickle.load(f)
        existing_data = saved.get("data_5min", {})
        print(f"Resuming: {len(existing_data)} symbols already fetched")

    data_5min = dict(existing_data)
    total = len(all_stocks)
    fetched = 0
    skipped = 0

    # Fetch Nifty index first
    if "NIFTY50" not in data_5min:
        print(f"[0/{total}] Fetching Nifty50 index...")
        nifty_5min = fetch_5min(broker, NIFTY_SYMBOL, args.start, args.end)
        if not nifty_5min.empty:
            data_5min["NIFTY50"] = nifty_5min
            print(f"  Nifty50: {len(nifty_5min)} candles, "
                  f"{nifty_5min['date'].nunique()} trading days")
        else:
            print("  Nifty50: NO DATA")
        time.sleep(0.5)

    for i, (sym, fsym) in enumerate(sorted(all_stocks.items()), 1):
        if fsym in data_5min or sym in data_5min:
            skipped += 1
            continue

        print(f"[{i}/{total}] Fetching {sym} ({fsym})...")
        df = fetch_5min(broker, fsym, args.start, args.end)
        if not df.empty:
            data_5min[fsym] = df
            days = df['date'].nunique()
            print(f"  {sym}: {len(df)} candles, {days} trading days")
            fetched += 1
        else:
            print(f"  {sym}: NO DATA")

        # Save checkpoint every 10 stocks
        if fetched > 0 and fetched % 10 == 0:
            _save(output_path, data_5min, args.start, args.end)
            print(f"  --- checkpoint saved ({len(data_5min)} symbols) ---")

        time.sleep(0.5)

    _save(output_path, data_5min, args.start, args.end)
    print(f"\nDone. Fetched {fetched} new, skipped {skipped} existing.")
    print(f"Total: {len(data_5min)} symbols in {output_path}")


def _save(path, data_5min, start, end):
    result = {
        "data_5min": data_5min,
        "resolution": "5",
        "start_date": start,
        "end_date": end,
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(result, f)


if __name__ == "__main__":
    main()
