"""
MarketDataProvider — unified load-or-fetch interface for all asset types.

Usage (notebook):
    from intraday_research.provider import MarketDataProvider

    provider = MarketDataProvider(repo_root=REPO_ROOT)

    reliance = provider.load("RELIANCE", "equity",  "2026-04-01", "2026-06-08")
    nifty    = provider.load("NIFTY50",  "index",   "2026-04-01", "2026-06-08")
    btc      = provider.load("BTCUSDT",  "crypto",  "2026-04-01", "2026-06-08")

Returns a prepared DataFrame ready for FeatureEngine / IntradayBacktester:
  columns: timestamp (IST tz-aware), symbol, open, high, low, close, volume, trade_date

Auto-fetch behaviour:
  - equity / index  → checks data/fyers/{file}.parquet + .meta.json for coverage;
                       fetches missing date ranges from Fyers API if needed.
  - crypto          → checks data/binance/{SYM}_1m.parquet for coverage;
                       fetches missing date ranges from Binance (public API, no auth).

Fyers auth:
  - Only needed when equity/index data is NOT already cached for the requested range.
  - Reads credentials from fyers_auth_file (default "fyers_auth.json").
  - If the cached parquet already covers your dates, Fyers API is never called.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd

# ── special-case Fyers symbol mappings (everything else: NSE:{SYM}-EQ) ───────
_FYERS_SYMBOL_MAP: dict[str, str] = {
    "NIFTY50":   "NSE:NIFTY50-INDEX",
    "BAJAJ_AUTO": "NSE:BAJAJ-AUTO-EQ",
}

AssetType = Literal["equity", "index", "crypto"]


class MarketDataProvider:
    """
    Load market data from local cache, auto-fetching any missing date range.
    """

    def __init__(
        self,
        repo_root: Path | None = None,
        fyers_auth_file: str = "fyers_auth.json",
        fyers_user_key: str = "user1",
        verbose: bool = True,
    ) -> None:
        self.repo_root = repo_root or _find_repo_root()
        self.fyers_auth_file = fyers_auth_file
        self.fyers_user_key  = fyers_user_key
        self.verbose         = verbose

    # ── Public API ─────────────────────────────────────────────────────────────

    def load(
        self,
        symbol: str,
        asset_type: AssetType,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Return a prepared 1-minute OHLCV DataFrame for the given symbol and range.

        Fetches missing data automatically:
          - equity/index  → Fyers API  (requires fyers_auth.json when data is absent)
          - crypto        → Binance public API  (no auth needed)
        """
        start_d = date.fromisoformat(start)
        end_d   = date.fromisoformat(end)

        if asset_type == "crypto":
            return self._load_crypto(symbol.upper(), start_d, end_d)
        else:
            return self._load_fyers(symbol, asset_type, start_d, end_d)

    # ── Crypto (Binance) ───────────────────────────────────────────────────────

    def _load_crypto(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        from .binance import fetch_and_cache

        out_dir  = self.repo_root / "data" / "binance"
        out_file = out_dir / f"{symbol}_1m.parquet"

        need_fetch = True
        if out_file.exists():
            cached = pd.read_parquet(out_file, columns=["trade_date"])
            if not cached.empty:
                need_fetch = (
                    cached["trade_date"].min() > start.isoformat()
                    or cached["trade_date"].max() < end.isoformat()
                )

        if need_fetch:
            if self.verbose:
                print(f"[provider] {symbol} (crypto): fetching missing data from Binance …")
            fetch_and_cache(symbol, start, end, out_dir, verbose=self.verbose)

        df = pd.read_parquet(out_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        # Convert UTC → IST. Crypto trades 24/7 so skip the NSE session filter.
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")
        df["trade_date"] = df["timestamp"].dt.date.astype(str)
        df = df[(df["trade_date"] >= start.isoformat()) & (df["trade_date"] <= end.isoformat())]

        from .data import MarketDataLoader
        return MarketDataLoader().prepare(df.copy(), filter_session=False)

    # ── Equity / Index (Fyers) ─────────────────────────────────────────────────

    def _fyers_symbol(self, symbol: str, asset_type: AssetType) -> str:
        if symbol in _FYERS_SYMBOL_MAP:
            return _FYERS_SYMBOL_MAP[symbol]
        if asset_type == "index":
            return f"NSE:{symbol}-INDEX"
        return f"NSE:{symbol}-EQ"

    def _fyers_cache_file(self, fyers_sym: str) -> Path:
        from .fyers_cache import safe_symbol_filename
        return self.repo_root / "data" / "fyers" / f"{safe_symbol_filename(fyers_sym)}.parquet"

    def _fyers_meta_file(self, fyers_sym: str) -> Path:
        from .fyers_cache import safe_symbol_filename
        return self.repo_root / "data" / "fyers" / f"{safe_symbol_filename(fyers_sym)}.meta.json"

    def _fyers_covered(self, fyers_sym: str, start: date, end: date) -> bool:
        """Return True if the local cache fully covers [start, end]."""
        from .fyers_cache import load_fetch_metadata, subtract_covered_ranges
        meta = self._fyers_meta_file(fyers_sym)
        covered = load_fetch_metadata(meta)
        missing = subtract_covered_ranges(start, end, covered)
        return len(missing) == 0

    def _load_fyers(self, symbol: str, asset_type: AssetType, start: date, end: date) -> pd.DataFrame:
        fyers_sym  = self._fyers_symbol(symbol, asset_type)
        cache_file = self._fyers_cache_file(fyers_sym)

        if not self._fyers_covered(fyers_sym, start, end):
            if self.verbose:
                print(f"[provider] {symbol} ({asset_type}): fetching missing data from Fyers …")
            self._fyers_fetch(fyers_sym, start, end, cache_file)

        if not cache_file.exists():
            raise FileNotFoundError(
                f"No data found for {symbol} after fetch attempt.\n"
                f"Expected: {cache_file}"
            )

        df = pd.read_parquet(cache_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["trade_date"] = df["timestamp"].dt.date.astype(str)
        df = df[(df["trade_date"] >= start.isoformat()) & (df["trade_date"] <= end.isoformat())]

        from .data import MarketDataLoader
        return MarketDataLoader().prepare(df.copy())

    def _fyers_fetch(self, fyers_sym: str, start: date, end: date, cache_file: Path) -> None:
        """Fetch missing ranges from Fyers and merge into the cache parquet."""
        # Import lazily so the module doesn't require fyers-apiv3 unless actually fetching
        repo_root = self.repo_root
        _add_path(repo_root)

        try:
            from common.broker.auth_json import get_fyers_creds_from_json
            from common.broker.fyers_client import FyersClient
        except ImportError as e:
            raise ImportError(
                f"Fyers SDK not available: {e}\n"
                f"Install: pip install fyers-apiv3\n"
                f"Or pre-fetch data with: python scripts/fetch_fyers_intraday_data.py ..."
            ) from e

        from .fyers_cache import (
            load_cached_frame, load_fetch_metadata, merge_cached_frames,
            save_fetch_metadata, subtract_covered_ranges, symbol_cache_path,
            symbol_metadata_path,
        )

        auth_file = self.fyers_auth_file
        if not Path(auth_file).is_absolute():
            auth_file_path = repo_root / auth_file
            if auth_file_path.exists():
                auth_file = str(auth_file_path)

        try:
            client_id, access_token = get_fyers_creds_from_json(auth_file, user_key=self.fyers_user_key)
            client = FyersClient(client_id=client_id, access_token=access_token)
        except Exception as e:
            raise RuntimeError(
                f"Could not load Fyers credentials from {auth_file}: {e}\n"
                f"If your token is expired, run: python scripts/fyers_auto_auth.py "
                f"--auth-file {auth_file} --user-key {self.fyers_user_key}"
            ) from e

        out_dir = cache_file.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        meta_path = symbol_metadata_path(out_dir, fyers_sym)

        cached_frame   = load_cached_frame(cache_file)
        fetched_ranges = load_fetch_metadata(meta_path)
        missing_ranges = subtract_covered_ranges(start, end, fetched_ranges)

        # Import the fetch function from the script (avoids duplicating it)
        script_path = repo_root / "scripts" / "fetch_fyers_intraday_data.py"
        _add_path(repo_root / "scripts")
        from fetch_fyers_intraday_data import fetch_symbol_history

        for miss_start, miss_end in missing_ranges:
            if self.verbose:
                print(f"  Fyers {fyers_sym}: {miss_start} → {miss_end}")
            frame = fetch_symbol_history(
                client, symbol=fyers_sym,
                start=miss_start, end=miss_end,
                chunk_days=30, resolution="1",
            )
            cached_frame   = merge_cached_frames(cached_frame, frame)
            fetched_ranges.append((miss_start, miss_end))

        if not cached_frame.empty:
            cached_frame.to_parquet(cache_file, index=False)
        save_fetch_metadata(meta_path, fetched_ranges)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_repo_root(start: Path | None = None) -> Path:
    candidate = start or Path.cwd()
    for p in [candidate, *candidate.parents]:
        if (p / "src" / "intraday_research").exists():
            return p
    raise FileNotFoundError("Cannot find repo root (looking for src/intraday_research/)")


def _add_path(p: Path) -> None:
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
