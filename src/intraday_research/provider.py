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
        validate_missing_candles: bool = True,
        resolution: str = "1",
    ) -> pd.DataFrame:
        """
        Return a prepared OHLCV DataFrame for the given symbol and range.

        Fetches missing data automatically:
          - equity/index  → Fyers API  (requires fyers_auth.json when data is absent)
          - crypto        → Binance public API  (no auth needed)

        validate_missing_candles=False tolerates partial/special sessions (e.g.
        Diwali Muhurat) that legitimately lack the full 09:15–15:30 candle set.
        Only applies to resolution="1" (session-shaped 1-minute data).

        resolution: "1" (default) for 1-minute bars, cached under data/fyers/.
        "D" fetches Fyers' NATIVE daily candles directly instead — these use
        the exchange's official daily close (a VWAP of the last 30 minutes of
        trading), not the last-traded-price a 1-minute-bar resample would give
        you, and are cached separately under data/fyers_daily/ (a different
        cache namespace — daily and 1-minute rows must never be merged
        together). Ignored for crypto (always 1-minute; crypto has no
        exchange-official VWAP close convention to match).
        """
        start_d = date.fromisoformat(start)
        end_d   = date.fromisoformat(end)

        if asset_type == "crypto":
            return self._load_crypto(symbol.upper(), start_d, end_d)
        else:
            return self._load_fyers(symbol, asset_type, start_d, end_d, validate_missing_candles, resolution)

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

    def _fyers_cache_dir(self, resolution: str) -> Path:
        # Daily-native candles live in a SEPARATE namespace from 1-minute data
        # — different row shape (one bar/day vs one bar/minute), so they must
        # never be merged/deduped together.
        subdir = "fyers" if resolution == "1" else "fyers_daily"
        return self.repo_root / "data" / subdir

    def _fyers_cache_file(self, fyers_sym: str, resolution: str = "1") -> Path:
        from .fyers_cache import safe_symbol_filename
        return self._fyers_cache_dir(resolution) / f"{safe_symbol_filename(fyers_sym)}.parquet"

    def _fyers_meta_file(self, fyers_sym: str, resolution: str = "1") -> Path:
        from .fyers_cache import safe_symbol_filename
        return self._fyers_cache_dir(resolution) / f"{safe_symbol_filename(fyers_sym)}.meta.json"

    def _fyers_covered(self, fyers_sym: str, start: date, end: date, resolution: str = "1") -> bool:
        """Return True if the local cache fully covers [start, end]."""
        from .fyers_cache import load_fetch_metadata, subtract_covered_ranges
        meta = self._fyers_meta_file(fyers_sym, resolution)
        covered = load_fetch_metadata(meta)
        missing = subtract_covered_ranges(start, end, covered)
        return len(missing) == 0

    def _load_fyers(
        self, symbol: str, asset_type: AssetType, start: date, end: date,
        validate_missing_candles: bool = True, resolution: str = "1",
    ) -> pd.DataFrame:
        fyers_sym  = self._fyers_symbol(symbol, asset_type)

        from .fyers_cache import is_known_invalid_symbol, load_invalid_symbols
        data_dir = self.repo_root / "data"
        if is_known_invalid_symbol(data_dir, fyers_sym):
            reason = load_invalid_symbols(data_dir)[fyers_sym]
            raise RuntimeError(
                f"{fyers_sym} is on record as an invalid/delisted Fyers symbol "
                f"(marked {reason.get('marked_at')}: {reason.get('reason')}) — "
                f"skipping without a network call. Delete its entry in "
                f"{data_dir / 'fyers_invalid_symbols.json'} to force a re-check."
            )

        cache_file = self._fyers_cache_file(fyers_sym, resolution)

        if not self._fyers_covered(fyers_sym, start, end, resolution):
            if self.verbose:
                kind = "1-minute" if resolution == "1" else f"native {resolution}-resolution"
                print(f"[provider] {symbol} ({asset_type}): fetching missing {kind} data from Fyers …")
            self._fyers_fetch(fyers_sym, start, end, cache_file, resolution)

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
        # Daily-native bars aren't shaped like an intraday session (one row a
        # day, not 375 one-minute rows between 09:15-15:30) — the session-time
        # filter and missing-candle check only make sense for resolution="1",
        # same as _load_crypto already does for 24/7 data.
        is_intraday = resolution == "1"
        return MarketDataLoader().prepare(
            df.copy(), filter_session=is_intraday,
            validate_missing_candles=validate_missing_candles if is_intraday else False,
        )

    def _fyers_fetch(self, fyers_sym: str, start: date, end: date, cache_file: Path, resolution: str = "1") -> None:
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
            expected_weekday_count, gap_tolerance, get_confirmed_inactive_from, group_into_contiguous_ranges,
            load_cached_frame, load_fetch_metadata, merge_cached_frames, record_trailing_gap_observation,
            safe_cacheable_end, save_fetch_metadata, subtract_covered_ranges, symbol_cache_path, symbol_metadata_path,
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

        # A CONFIRMED-inactive symbol (see record_trailing_gap_observation —
        # requires multiple separate-day confirmations, never a single miss)
        # — cap the fetch so it never wastes a round-trip on a dead tail
        # again. Historical data up to that date is untouched.
        inactive_from = get_confirmed_inactive_from(self.repo_root / "data", fyers_sym)
        fetch_end = min(end, inactive_from - timedelta(days=1)) if inactive_from is not None else end
        missing_ranges = subtract_covered_ranges(start, fetch_end, fetched_ranges) if fetch_end >= start else []

        # Import the fetch helpers from the script (avoids duplicating them).
        # build_client / refresh_auth_via_script give this path the same
        # refresh-once-and-retry resilience fetch_fyers_intraday_data.py's own
        # CLI loop has (ensure_symbol_history) — without this, an expired
        # access_token just fails every chunk identically forever, since the
        # FyersClient built above is never rebuilt with a fresh token.
        script_path = repo_root / "scripts" / "fetch_fyers_intraday_data.py"
        _add_path(repo_root / "scripts")
        from fetch_fyers_intraday_data import (
            build_client, fetch_symbol_history, is_invalid_symbol_error, refresh_auth_via_script,
        )

        # Daily candles are tiny compared to 1-minute ones, so a much wider
        # chunk per request is safe and cuts the request count drastically.
        chunk_days = 30 if resolution == "1" else 365
        today = pd.Timestamp.now(tz="Asia/Kolkata").date()

        for miss_start, miss_end in missing_ranges:
            if self.verbose:
                print(f"  Fyers {fyers_sym}: {miss_start} → {miss_end}")

            # Drop any existing rows for the range we're about to (re-)fetch,
            # so fresh data always wins over whatever's already cached for
            # these dates. This matters once "today" can be legitimately
            # re-fetched more than once over the course of its own session
            # (see safe_cacheable_end below) — without this, merge_cached_frames'
            # de-dup keeps the FIRST (old/stale) row on a timestamp collision,
            # silently discarding the corrected data.
            if not cached_frame.empty:
                existing_dates = pd.to_datetime(cached_frame["timestamp"]).dt.date
                cached_frame = cached_frame[~((existing_dates >= miss_start) & (existing_dates <= miss_end))]

            try:
                frame = fetch_symbol_history(
                    client, symbol=fyers_sym,
                    start=miss_start, end=miss_end,
                    chunk_days=chunk_days, resolution=resolution,
                )
            except Exception:
                # NEVER trust "invalid symbol" on the first attempt alone — a
                # stale/expired auth session can make Fyers return that exact
                # same "-300 Invalid symbol provided" text for a perfectly
                # valid, actively-traded symbol (this is what silently
                # blacklisted 29 real stocks, including ZOMATO and TATAMOTORS,
                # in one bad batch on 2026-09-05 — every one already had a
                # working cached history, and all 29 were marked invalid at
                # the exact same timestamp, the signature of one broken
                # session, not 29 coincidental delistings). So ALWAYS refresh
                # and retry once on a confirmed-fresh session before treating
                # the error as meaningful either way — only a failure that
                # reproduces AFTER a fresh token is trustworthy enough to
                # permanently blacklist.
                if self.verbose:
                    print(f"  Fyers {fyers_sym}: fetch failed, refreshing auth and retrying once ...")
                refresh_auth_via_script(auth_file, user_key=self.fyers_user_key)
                client = build_client(auth_file, user_key=self.fyers_user_key)
                try:
                    frame = fetch_symbol_history(
                        client, symbol=fyers_sym,
                        start=miss_start, end=miss_end,
                        chunk_days=chunk_days, resolution=resolution,
                    )
                except Exception as e2:
                    if is_invalid_symbol_error(e2):
                        from .fyers_cache import record_invalid_symbol
                        record_invalid_symbol(self.repo_root / "data", fyers_sym, str(e2))
                    raise
            cached_frame = merge_cached_frames(cached_frame, frame)

            # Only trust the FULL requested range as "fetched" if Fyers
            # actually returned roughly that many trading days. A silent
            # empty/partial response (network hiccup, transient API error,
            # a dropped chunk) must NOT be recorded as fully covered — that
            # permanently poisons the cache, since every later run trusts
            # fetched_ranges and never retries it (this is exactly what
            # happened to LODHA's 44-SMA). Instead, record only the days we
            # actually received, so the rest naturally shows up as still
            # missing and gets retried on the next fetch.
            actual_dates = set(pd.to_datetime(frame["timestamp"]).dt.date) if not frame.empty else set()
            expected = expected_weekday_count(miss_start, miss_end)
            if len(actual_dates) >= expected - gap_tolerance(expected):
                # For anything OTHER than 1-minute resolution, "today" is
                # never safe to record as covered — a daily/weekly candle for
                # a session that may still be open keeps changing right up to
                # the close, so caching it as final would freeze a partial
                # snapshot forever (this is exactly what happened to INFY/ABB's
                # 44-SMA touch candles when a scan ran mid-session).
                safe_end = safe_cacheable_end(miss_end, resolution, today)
                if safe_end >= miss_start:
                    fetched_ranges.append((miss_start, safe_end))
            else:
                # Always surfaced (not gated on verbose) — this signals a real
                # data-integrity risk the caller should know about even in a
                # quiet notebook run.
                print(
                    f"  Fyers {fyers_sym}: {miss_start} → {miss_end} looked incomplete "
                    f"({len(actual_dates)}/{expected} expected trading days) — recording only "
                    f"what was actually received; the rest will be retried next fetch."
                )
                cacheable_dates = {d for d in actual_dates if resolution == "1" or d < today}
                for lo, hi in group_into_contiguous_ranges(cacheable_dates):
                    fetched_ranges.append((lo, hi))

                # Pre-listing gap: if the symbol has NO data anywhere before
                # its first received bar (merged cache included), the days
                # from this range's start up to that first bar are simply
                # before it existed on the exchange — a fact, not a fetch
                # failure. Without recording them they stay "missing" and
                # every run re-requests the same empty window (one wasted,
                # rate-limited API call per newly-listed symbol, forever).
                if not cached_frame.empty:
                    first_known = pd.to_datetime(cached_frame["timestamp"]).dt.date.min()
                    pre_end = min(first_known - timedelta(days=1), miss_end)
                    if first_known > miss_start and pre_end >= miss_start:
                        fetched_ranges.append((miss_start, pre_end))

                # Trailing-gap tracking: this chunk reaches the tail of what
                # we're fetching and came back with NOTHING at all, and
                # there's real prior coverage right before it (not just
                # "never fetched before") — a candidate for "this symbol has
                # stopped trading". One miss is never enough to act on; see
                # record_trailing_gap_observation's own docstring.
                if not actual_dates and miss_end >= fetch_end and miss_start > start:
                    last_good_date = miss_start - timedelta(days=1)
                    if record_trailing_gap_observation(
                        self.repo_root / "data", fyers_sym, last_good_date=last_good_date, checked_through=miss_end,
                    ):
                        print(
                            f"  Fyers {fyers_sym}: confirmed inactive from {last_good_date + timedelta(days=1)} "
                            f"(no new data on 2 separate days) — future fetches stop at that date."
                        )

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
