"""
Binance 1-minute OHLCV downloader.

Downloads from Binance Data Vision monthly archives (fast bulk) and falls back
to the REST API for recent days not yet archived.  Saves an incrementally-updated
parquet file so repeated calls only download the missing date range.

Output schema (matches data/fyers/*.parquet):
    timestamp   datetime64[ns, UTC]  — convert to IST at load time if needed
    symbol      str                  — "BINANCE:BTCUSDT"
    open/high/low/close  float64
    volume      float64
    trade_date  str                  — "YYYY-MM-DD"
"""
from __future__ import annotations

import io
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

_DATA_VISION = "https://data.binance.vision/data/spot/monthly/klines"
_KLINES_API  = "https://api.binance.com/api/v3/klines"
_API_LIMIT   = 1000


def _ensure_requests():
    if not _HAS_REQUESTS:
        raise ImportError("pip install requests  — required for Binance data fetch")
    import requests
    return requests


def _month_range(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _to_df(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    ts = raw.iloc[:, 0].astype("int64")
    unit = "us" if ts.iloc[0] > 1_000_000_000_000_000 else "ms"
    df = pd.DataFrame({
        "timestamp":  pd.to_datetime(ts, unit=unit, utc=True),
        "symbol":     f"BINANCE:{symbol}",
        "open":       raw.iloc[:, 1].astype("float64"),
        "high":       raw.iloc[:, 2].astype("float64"),
        "low":        raw.iloc[:, 3].astype("float64"),
        "close":      raw.iloc[:, 4].astype("float64"),
        "volume":     raw.iloc[:, 5].astype("float64"),
    })
    df["trade_date"] = df["timestamp"].dt.date.astype(str)
    return df.reset_index(drop=True)


def _fetch_archive(symbol: str, year: int, month: int, session) -> pd.DataFrame | None:
    fname = f"{symbol}-1m-{year:04d}-{month:02d}.zip"
    url   = f"{_DATA_VISION}/{symbol}/1m/{fname}"
    r = session.get(url, timeout=120)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open(z.namelist()[0]) as f:
            raw = pd.read_csv(f, header=None)
    return _to_df(raw, symbol)


def _fetch_api(symbol: str, start: date, end: date, session) -> pd.DataFrame:
    start_ms = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
    end_ms   = int((pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000) - 1
    rows: list = []
    cur = start_ms
    while cur < end_ms:
        r = session.get(_KLINES_API, params=dict(
            symbol=symbol, interval="1m",
            startTime=cur, endTime=end_ms, limit=_API_LIMIT,
        ), timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        cur = int(batch[-1][0]) + 60_000
        if len(batch) < _API_LIMIT:
            break
        time.sleep(0.05)
    if not rows:
        return pd.DataFrame()
    return _to_df(pd.DataFrame(rows), symbol)


def fetch_and_cache(
    symbol: str,
    start: date,
    end: date,
    out_dir: Path,
    *,
    verbose: bool = True,
) -> Path:
    """
    Download missing 1-minute klines for `symbol` and save/update
    `out_dir/{symbol}_1m.parquet`.  Only missing date ranges are downloaded.

    Returns the path to the (updated) parquet file.
    """
    requests = _ensure_requests()
    symbol = symbol.upper()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{symbol}_1m.parquet"

    existing: pd.DataFrame | None = None
    cached_min: str | None = None
    cached_max: str | None = None
    if out_file.exists():
        existing = pd.read_parquet(out_file)
        if not existing.empty:
            cached_min = existing["trade_date"].min()
            cached_max = existing["trade_date"].max()
            if verbose:
                print(f"  {symbol}: cache {cached_min} → {cached_max} ({len(existing):,} bars)")

    session = requests.Session()
    session.headers["User-Agent"] = "intraday_research/binance.py"
    frames: list[pd.DataFrame] = []
    today = date.today()

    # archive cutoff: last day of previous month (archives lag ~2 weeks)
    prev_month_end = today.replace(day=1) - timedelta(days=1)

    for y, m in _month_range(start, min(end, prev_month_end)):
        ym = f"{y:04d}-{m:02d}"
        # Skip only months already inside the cached range [cached_min, cached_max].
        # Do NOT skip months before cached_min — those are the missing historical gap.
        if cached_min and cached_max and cached_min[:7] <= ym <= cached_max[:7]:
            continue
        if verbose:
            print(f"  {symbol}: archive {ym} ...", end=" ", flush=True)
        df = _fetch_archive(symbol, y, m, session)
        if df is None:
            if verbose:
                print("not published yet")
        else:
            frames.append(df)
            if verbose:
                print(f"{len(df):,} bars")
        time.sleep(0.3)

    api_start = max(start, prev_month_end + timedelta(days=1))
    api_end   = min(end, today - timedelta(days=1))
    if api_start <= api_end:
        if cached_max:
            adj = date.fromisoformat(cached_max) + timedelta(days=1)
            if adj > api_end:
                api_start = None  # type: ignore[assignment]
            else:
                api_start = adj
        if api_start is not None:
            if verbose:
                print(f"  {symbol}: API {api_start} → {api_end} ...", end=" ", flush=True)
            df = _fetch_api(symbol, api_start, api_end, session)
            if not df.empty:
                frames.append(df)
                if verbose:
                    print(f"{len(df):,} bars")
            else:
                if verbose:
                    print("no data")

    if not frames:
        return out_file   # nothing to update

    parts = ([existing] if existing is not None else []) + frames
    combined = (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    combined.to_parquet(out_file, index=False)
    if verbose:
        print(f"  {symbol}: saved {len(combined):,} bars → {out_file}")
    return out_file
