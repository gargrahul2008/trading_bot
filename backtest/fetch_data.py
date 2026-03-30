"""
Fetch and cache historical OHLCV klines from MEXC public API.
No authentication required.
"""
from __future__ import annotations

import os
import sys
import time
import requests
import pandas as pd

MEXC_KLINES_URL    = "https://api.mexc.com/api/v3/klines"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

INTERVAL_MS = {
    "1m":   60_000,
    "5m":   300_000,
    "15m":  900_000,
    "30m":  1_800_000,
    "1h":   3_600_000,
    "4h":   14_400_000,
    "1d":   86_400_000,
}

KLINE_COLS = ["ts", "open", "high", "low", "close", "volume",
              "close_time", "quote_volume", "trades",
              "taker_buy_base", "taker_buy_quote", "ignore"]


def fetch_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    cache_dir: str = "backtest/cache",
    source: str = "binance",   # "binance" or "mexc"
) -> pd.DataFrame:
    """
    Download klines for `symbol` between start_ms and end_ms (epoch ms).
    Results are cached as parquet so subsequent calls are instant.
    Returns DataFrame with columns: ts, open, high, low, close (all float64).
    """
    url = BINANCE_KLINES_URL if source == "binance" else MEXC_KLINES_URL
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(
        cache_dir, f"{source}_{symbol}_{interval}_{start_ms}_{end_ms}.parquet"
    )

    if os.path.exists(cache_file):
        print(f"[fetch_data] Loading cached data from {cache_file}")
        df = pd.read_parquet(cache_file)
        return df

    step = INTERVAL_MS.get(interval)
    if step is None:
        raise ValueError(f"Unknown interval '{interval}'. Choose from: {list(INTERVAL_MS)}")

    total_candles_est = (end_ms - start_ms) // step
    print(f"[fetch_data] Fetching {symbol} {interval} from {source.upper()} "
          f"(~{total_candles_est:,} candles, ~{total_candles_est // 1000 + 1} requests)...")

    rows = []
    cur = start_ms
    req_count = 0

    while cur < end_ms:
        batch_end = min(cur + 1000 * step, end_ms)
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "endTime": batch_end,
            "limit": 1000,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[fetch_data] Request failed at {cur}: {e}. Retrying in 2s...")
            time.sleep(2)
            continue

        if not data:
            break

        rows.extend(data)
        cur = data[-1][0] + step
        req_count += 1

        if req_count % 50 == 0:
            pct = (cur - start_ms) / (end_ms - start_ms) * 100
            print(f"[fetch_data]   {pct:.1f}% — {len(rows):,} candles fetched...", flush=True)

        time.sleep(0.06)  # ~16 req/s, well under 20/s limit

    if not rows:
        raise RuntimeError(f"No data returned for {symbol} {interval} in the requested range.")

    df = pd.DataFrame(rows, columns=KLINE_COLS)
    df = df[["ts", "open", "high", "low", "close"]].copy()
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)

    df.to_parquet(cache_file)
    print(f"[fetch_data] Done — {len(df):,} candles. Cached to {cache_file}")
    return df


def dt_to_ms(dt_str: str) -> int:
    """Convert 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' to epoch milliseconds (UTC)."""
    ts = pd.Timestamp(dt_str, tz="UTC")
    return int(ts.timestamp() * 1000)
