"""Universe builder: fetch NSE stock listings and apply basic filters."""

import csv
import io
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
import pandas as pd

LOG = logging.getLogger("bzg.universe")

NSE_INDEX_URLS = {
    "nifty500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    "nifty200": "https://archives.nseindia.com/content/indices/ind_nifty200list.csv",
    "nifty100": "https://archives.nseindia.com/content/indices/ind_nifty100list.csv",
    "nifty50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
}

NSE_ALL_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/csv",
}


def fetch_index_constituents(source: str = "nifty500") -> List[Dict]:
    src = source.lower()
    if src == "nse_all":
        return _fetch_nse_all()

    url = NSE_INDEX_URLS.get(src)
    if not url:
        available = list(NSE_INDEX_URLS) + ["nse_all"]
        raise ValueError(f"Unknown universe source: {source}. Available: {available}")

    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    symbols = []
    for row in reader:
        nse_symbol = row.get("Symbol", "").strip()
        if not nse_symbol:
            continue
        symbols.append({
            "nse_symbol": nse_symbol,
            "fyers_symbol": f"NSE:{nse_symbol}-EQ",
            "company": row.get("Company Name", "").strip(),
            "industry": row.get("Industry", "").strip(),
            "series": row.get("Series", "EQ").strip(),
            "isin": row.get("ISIN Code", "").strip(),
        })
    LOG.info("Fetched %d symbols from %s", len(symbols), source)
    return symbols


def _fetch_nse_all() -> List[Dict]:
    resp = requests.get(NSE_ALL_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    symbols = []
    for row in reader:
        nse_symbol = row.get("SYMBOL", "").strip()
        series = row.get(" SERIES", row.get("SERIES", "")).strip()
        if not nse_symbol or series != "EQ":
            continue
        symbols.append({
            "nse_symbol": nse_symbol,
            "fyers_symbol": f"NSE:{nse_symbol}-EQ",
            "company": row.get("NAME OF COMPANY", "").strip(),
            "industry": "",
            "series": series,
            "isin": row.get(" ISIN NUMBER", row.get("ISIN NUMBER", "")).strip(),
        })
    LOG.info("Fetched %d EQ symbols from nse_all", len(symbols))
    return symbols


def apply_basic_filters(
    symbols: List[Dict],
    ohlcv_data: Dict[str, pd.DataFrame],
    config: Dict,
) -> List[Dict]:
    ucfg = config.get("universe", {})
    min_turnover = ucfg.get("min_avg_daily_turnover", 50_000_000)
    min_volume = ucfg.get("min_avg_daily_volume", 500_000)
    min_price = ucfg.get("min_price", 50)
    max_price = ucfg.get("max_price", 10000)
    lookback = config.get("range", {}).get("lookback_days", 60)

    filtered = []
    for s in symbols:
        fsym = s["fyers_symbol"]
        df = ohlcv_data.get(fsym)
        if df is None or len(df) < lookback * 0.6:
            continue

        recent = df.tail(lookback)
        avg_volume = recent["volume"].mean()
        avg_close = recent["close"].mean()
        avg_turnover = (recent["close"] * recent["volume"]).mean()
        last_close = recent["close"].iloc[-1]

        if avg_volume < min_volume:
            continue
        if avg_turnover < min_turnover:
            continue
        if last_close < min_price or last_close > max_price:
            continue

        s["avg_volume"] = round(avg_volume)
        s["avg_turnover"] = round(avg_turnover)
        s["last_close"] = round(last_close, 2)
        s["data_days"] = len(recent)
        filtered.append(s)

    LOG.info("After basic filters: %d / %d symbols pass", len(filtered), len(symbols))
    return filtered
