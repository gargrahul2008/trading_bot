"""Fetch historical OHLCV data from Fyers API with parquet caching."""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

LOG = logging.getLogger("bzg.data")

DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "strategy_data", "cache",
)


def _cache_key(symbol: str, resolution: str, start: str, end: str) -> str:
    safe_sym = symbol.replace(":", "_").replace("-", "_")
    return f"{safe_sym}_{resolution}_{start}_{end}.parquet"


def _load_cache(cache_dir: str, key: str) -> Optional[pd.DataFrame]:
    path = os.path.join(cache_dir, key)
    if os.path.exists(path):
        LOG.debug("Cache hit: %s", key)
        return pd.read_parquet(path)
    return None


def _save_cache(cache_dir: str, key: str, df: pd.DataFrame):
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, key)
    df.to_parquet(path, index=False)
    LOG.debug("Cached: %s (%d rows)", key, len(df))


def _is_cached(cache_dir: str, symbol: str, resolution: str, lookback_days: int,
               end_date: Optional[str] = None) -> bool:
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days + 10 if resolution == "D" else lookback_days + 3)
    key = _cache_key(symbol, resolution, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    return os.path.exists(os.path.join(cache_dir, key))


def fetch_daily_ohlcv(
    broker, symbol: str, lookback_days: int = 120, cache_dir: str = "",
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days + 10)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    cdir = cache_dir or DEFAULT_CACHE_DIR
    key = _cache_key(symbol, "D", start_str, end_str)
    cached = _load_cache(cdir, key)
    if cached is not None:
        cached["date"] = pd.to_datetime(cached["timestamp"], unit="s").dt.date
        return cached

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
    _save_cache(cdir, key, df)
    df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.date
    return df


def fetch_bulk_daily_ohlcv(
    broker,
    symbols: List[str],
    lookback_days: int = 120,
    throttle_seconds: float = 0.3,
    cache_dir: str = "",
    end_date: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    result = {}
    total = len(symbols)
    cache_hits = 0
    cdir = cache_dir or DEFAULT_CACHE_DIR
    for i, sym in enumerate(symbols):
        was_cached = _is_cached(cdir, sym, "D", lookback_days, end_date=end_date)
        try:
            df = fetch_daily_ohlcv(broker, sym, lookback_days, cache_dir=cache_dir, end_date=end_date)
            if not df.empty:
                result[sym] = df
                if was_cached:
                    cache_hits += 1
        except Exception as e:
            LOG.warning("Failed fetching %s: %s", sym, e)
        if throttle_seconds > 0 and i < total - 1 and not was_cached:
            time.sleep(throttle_seconds)
        if (i + 1) % 50 == 0:
            LOG.info("Fetched %d / %d symbols (cache hits: %d)", i + 1, total, cache_hits)
    LOG.info("Fetched OHLCV for %d / %d symbols (cache hits: %d)", len(result), total, cache_hits)
    return result


FYERS_DAILY_MAX_DAYS = 365
FYERS_INTRADAY_MAX_DAYS = 99


def fetch_intraday_ohlcv(
    broker, symbol: str, resolution: str = "15", lookback_days: int = 10,
    cache_dir: str = "", end_date: Optional[str] = None,
) -> pd.DataFrame:
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days + 3)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    cdir = cache_dir or DEFAULT_CACHE_DIR
    key = _cache_key(symbol, resolution, start_str, end_str)
    cached = _load_cache(cdir, key)
    if cached is not None:
        cached["datetime"] = pd.to_datetime(cached["timestamp"], unit="s")
        cached["bar_range"] = cached["high"] - cached["low"]
        return cached

    all_candles = []
    chunk_end = end_dt
    chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_INTRADAY_MAX_DAYS))

    while chunk_start < chunk_end:
        data = {
            "symbol": symbol,
            "resolution": resolution,
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
        chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_INTRADAY_MAX_DAYS))
        if chunk_start < chunk_end:
            time.sleep(0.3)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    _save_cache(cdir, key, df)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df["bar_range"] = df["high"] - df["low"]
    return df


def compute_avg_bar_range(df: pd.DataFrame) -> Optional[float]:
    if df.empty or "bar_range" not in df.columns:
        return None
    valid = df[df["bar_range"] > 0]["bar_range"]
    if len(valid) < 10:
        return None
    return float(valid.mean())


def fetch_bulk_intraday_bar_range(
    broker,
    symbols: List[str],
    resolutions: List[str] = None,
    lookback_days: int = 10,
    throttle_seconds: float = 0.3,
    cache_dir: str = "",
) -> Dict[str, Dict[str, Optional[float]]]:
    if resolutions is None:
        resolutions = ["15", "5"]

    result: Dict[str, Dict[str, Optional[float]]] = {}
    total = len(symbols)
    cdir = cache_dir or DEFAULT_CACHE_DIR

    for i, sym in enumerate(symbols):
        result[sym] = {}
        for res in resolutions:
            was_cached = _is_cached(cdir, sym, res, lookback_days)
            try:
                df = fetch_intraday_ohlcv(broker, sym, resolution=res,
                                          lookback_days=lookback_days, cache_dir=cache_dir)
                avg = compute_avg_bar_range(df)
                result[sym][res] = round(avg, 4) if avg else None
            except Exception as e:
                LOG.warning("Failed intraday %s/%smin: %s", sym, res, e)
                result[sym][res] = None
            if throttle_seconds > 0 and not was_cached:
                time.sleep(throttle_seconds)
        if (i + 1) % 50 == 0:
            LOG.info("Intraday bar range: %d / %d symbols", i + 1, total)

    fetched = sum(1 for v in result.values() if any(x is not None for x in v.values()))
    LOG.info("Intraday bar range: %d / %d symbols have data", fetched, total)
    return result


def fetch_bulk_intraday_ohlcv(
    broker,
    symbols: List[str],
    resolution: str = "15",
    lookback_days: int = 70,
    throttle_seconds: float = 0.3,
    cache_dir: str = "",
    end_date: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    result = {}
    total = len(symbols)
    cache_hits = 0
    cdir = cache_dir or DEFAULT_CACHE_DIR
    for i, sym in enumerate(symbols):
        was_cached = _is_cached(cdir, sym, resolution, lookback_days, end_date=end_date)
        try:
            df = fetch_intraday_ohlcv(broker, sym, resolution=resolution,
                                      lookback_days=lookback_days, cache_dir=cache_dir,
                                      end_date=end_date)
            if not df.empty:
                result[sym] = df
                if was_cached:
                    cache_hits += 1
        except Exception as e:
            LOG.warning("Failed intraday OHLCV %s/%smin: %s", sym, resolution, e)
        if throttle_seconds > 0 and i < total - 1 and not was_cached:
            time.sleep(throttle_seconds)
        if (i + 1) % 50 == 0:
            LOG.info("Intraday OHLCV (%smin): %d / %d symbols (cache: %d)", resolution, i + 1, total, cache_hits)
    LOG.info("Intraday OHLCV (%smin): %d / %d fetched (cache: %d)", resolution, len(result), total, cache_hits)
    return result


def get_latest_price(broker, symbol: str) -> Optional[float]:
    try:
        prices = broker.get_ltps([symbol])
        return float(prices.get(symbol, 0))
    except Exception as e:
        LOG.warning("Failed LTP for %s: %s", symbol, e)
        return None


def get_bulk_ltp(broker, symbols: List[str], batch_size: int = 50) -> Dict[str, float]:
    result = {}
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            prices = broker.get_ltps(batch)
            for sym, px in prices.items():
                result[sym] = float(px)
        except Exception as e:
            LOG.warning("LTP batch failed at offset %d: %s", i, e)
        if i + batch_size < len(symbols):
            time.sleep(0.2)
    return result
