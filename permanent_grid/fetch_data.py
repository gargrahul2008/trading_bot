#!/usr/bin/env python3
"""Fetch 3 years of daily OHLCV for basket stocks + Nifty50 index via Fyers API."""

import sys, os, json, pickle, time
sys.path.insert(0, "/root/trading_bot")

from datetime import datetime, timedelta
import pandas as pd
from common.broker.fyers_client import FyersClient

BASKET = [
    {"symbol": "ETERNAL",    "fyers_symbol": "NSE:ETERNAL-EQ",    "sector": "Consumer Services"},
    {"symbol": "SHRIRAMFIN", "fyers_symbol": "NSE:SHRIRAMFIN-EQ", "sector": "Financial Services"},
    {"symbol": "ADANIENT",   "fyers_symbol": "NSE:ADANIENT-EQ",   "sector": "Metals & Mining"},
    {"symbol": "INFY",       "fyers_symbol": "NSE:INFY-EQ",       "sector": "Information Technology"},
    {"symbol": "ONGC",       "fyers_symbol": "NSE:ONGC-EQ",       "sector": "Oil Gas & Fuels"},
    {"symbol": "MAXHEALTH",  "fyers_symbol": "NSE:MAXHEALTH-EQ",  "sector": "Healthcare"},
    {"symbol": "BEL",        "fyers_symbol": "NSE:BEL-EQ",        "sector": "Capital Goods"},
    {"symbol": "TATACONSUM", "fyers_symbol": "NSE:TATACONSUM-EQ", "sector": "FMCG"},
    {"symbol": "EICHERMOT",  "fyers_symbol": "NSE:EICHERMOT-EQ",  "sector": "Automobile"},
    {"symbol": "ASIANPAINT", "fyers_symbol": "NSE:ASIANPAINT-EQ", "sector": "Consumer Durables"},
]

NIFTY_SYMBOL = "NSE:NIFTY50-INDEX"
FYERS_DAILY_MAX_DAYS = 365
END_DATE = "2026-06-03"
LOOKBACK_YEARS = 3
OUTPUT_PATH = "/root/trading_bot/permanent_grid/data/basket_3y.pkl"


def get_broker():
    with open("/root/trading_bot/fyers_auth.json") as f:
        auth = json.load(f)
    user = auth["users"]["user1"]
    return FyersClient(client_id=user["client_id"], access_token=user["access_token"])


def fetch_daily(broker, symbol: str, start_str: str, end_str: str) -> pd.DataFrame:
    """Fetch daily OHLCV, chunking by 365-day windows."""
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")

    all_candles = []
    chunk_end = end_dt
    chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_DAILY_MAX_DAYS))

    while chunk_start < chunk_end:
        data = {
            "symbol": symbol,
            "resolution": "D",
            "date_format": "1",
            "range_from": chunk_start.strftime("%Y-%m-%d"),
            "range_to": chunk_end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        }
        resp = broker.history(data)
        candles = resp.get("candles") or []
        if candles:
            all_candles.extend(candles)

        chunk_end = chunk_start - timedelta(days=1)
        chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_DAILY_MAX_DAYS))
        if chunk_start < chunk_end:
            time.sleep(0.3)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.normalize()
    return df


def main():
    broker = get_broker()
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=LOOKBACK_YEARS * 365 + 30)
    start_str = start_dt.strftime("%Y-%m-%d")

    print(f"Fetching {start_str} to {END_DATE} ({LOOKBACK_YEARS} years)")

    # Fetch Nifty50 index
    print(f"\nFetching Nifty50 index ({NIFTY_SYMBOL})...")
    nifty_df = fetch_daily(broker, NIFTY_SYMBOL, start_str, END_DATE)
    print(f"  Nifty50: {len(nifty_df)} days, {nifty_df['date'].min()} to {nifty_df['date'].max()}" if not nifty_df.empty else "  FAILED")
    time.sleep(0.5)

    # Fetch basket stocks
    daily_data = {}
    for item in BASKET:
        fsym = item["fyers_symbol"]
        print(f"Fetching {item['symbol']} ({fsym})...")
        df = fetch_daily(broker, fsym, start_str, END_DATE)
        if not df.empty:
            daily_data[fsym] = df
            print(f"  {item['symbol']}: {len(df)} days, {df['date'].min()} to {df['date'].max()}")
        else:
            print(f"  {item['symbol']}: NO DATA")
        time.sleep(0.5)

    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result = {
        "basket": BASKET,
        "daily_data": daily_data,
        "nifty_data": nifty_df,
        "fetch_date": END_DATE,
        "start_date": start_str,
    }
    with open(OUTPUT_PATH, "wb") as f:
        pickle.dump(result, f)

    print(f"\nSaved to {OUTPUT_PATH}")
    print(f"Stocks fetched: {len(daily_data)} / {len(BASKET)}")
    if not nifty_df.empty:
        print(f"Nifty50: {len(nifty_df)} days")


if __name__ == "__main__":
    main()
