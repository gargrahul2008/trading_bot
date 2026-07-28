from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient
from intraday_research.data import MarketDataLoader
from intraday_research.fyers_cache import (
    load_cached_frame,
    load_fetch_metadata,
    merge_cached_frames,
    safe_symbol_filename,
    save_fetch_metadata,
    slice_frame_by_date,
    subtract_covered_ranges,
    symbol_cache_path,
    symbol_metadata_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch FYERS historical candles for research.")
    parser.add_argument("--auth-file", default="fyers_auth.json", help="Path to fyers_auth.json")
    parser.add_argument("--user-key", required=True, help="User key inside fyers_auth.json")
    parser.add_argument("--symbol", action="append", required=True, dest="symbols", help="FYERS symbol to fetch")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--output-dir", default="data/fyers", help="Directory to write output files")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet", help="Output file format")
    parser.add_argument(
        "--resolution",
        default="1",
        help="FYERS history resolution. Use 1/5/15 for intraday or D for daily positional research.",
    )
    parser.add_argument(
        "--refresh-auth-before-fetch",
        action="store_true",
        help="Refresh FYERS auth for the selected user before any history requests",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=30,
        help="Fetch date range in chunks to reduce API payload size",
    )
    parser.add_argument(
        "--skip-invalid-symbols",
        action="store_true",
        help="Continue fetching other symbols when FYERS rejects a symbol as invalid",
    )
    return parser.parse_args()


def daterange_chunks(start: dt.date, end: dt.date, chunk_days: int) -> Iterable[tuple[dt.date, dt.date]]:
    current = start
    step = max(int(chunk_days), 1)
    while current <= end:
        chunk_end = min(current + dt.timedelta(days=step - 1), end)
        yield current, chunk_end
        current = chunk_end + dt.timedelta(days=1)


# FYERS per-minute request cap returns {'code': 429, 'message': 'request limit
# reached'}; the client's internal retries max out at ~6s which never outlives
# it, so cool down for a full minute and continue where we were.
RATE_LIMIT_COOLDOWN_SECONDS = 61.0
RATE_LIMIT_MAX_COOLDOWNS = 15
REQUEST_THROTTLE_SECONDS = 0.25
_RATE_LIMIT_MARKERS = ("'code': 429", "request limit")


def _history_with_cooldown(client: FyersClient, payload: dict) -> dict:
    for _ in range(RATE_LIMIT_MAX_COOLDOWNS):
        try:
            return client.history(payload)
        except Exception as exc:
            message = str(exc)
            if not any(marker in message for marker in _RATE_LIMIT_MARKERS):
                raise
            print(f"FYERS rate limit hit; cooling down {RATE_LIMIT_COOLDOWN_SECONDS:.0f}s ...", flush=True)
            time.sleep(RATE_LIMIT_COOLDOWN_SECONDS)
    raise RuntimeError(f"still rate limited after {RATE_LIMIT_MAX_COOLDOWNS} cooldowns: {payload['symbol']}")


def fetch_symbol_history(
    client: FyersClient,
    *,
    symbol: str,
    start: dt.date,
    end: dt.date,
    chunk_days: int,
    resolution: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for range_from, range_to in daterange_chunks(start, end, chunk_days):
        response = _history_with_cooldown(
            client,
            {
                "symbol": symbol,
                "resolution": resolution,
                "date_format": "1",
                "range_from": range_from.isoformat(),
                "range_to": range_to.isoformat(),
                "cont_flag": "1",
            },
        )
        time.sleep(REQUEST_THROTTLE_SECONDS)
        candles = response.get("candles") or []
        if not candles:
            continue
        frame = pd.DataFrame(candles, columns=["epoch", "open", "high", "low", "close", "volume"])
        frame["timestamp"] = pd.to_datetime(frame["epoch"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        frame["symbol"] = symbol
        frames.append(frame[["timestamp", "symbol", "open", "high", "low", "close", "volume"]])

    if not frames:
        return pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_date"])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp", "symbol"]).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    return prepare_fetched_history(combined)


def prepare_fetched_history(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare fetched FYERS history for caching/export without enforcing full-session
    1-minute continuity. Some symbols/date ranges can contain incomplete vendor data;
    that should be handled later by research-time filtering rather than aborting fetch.
    """
    loader = MarketDataLoader()
    prepared = frame.copy()
    prepared["timestamp"] = loader._parse_timestamps(prepared["timestamp"])
    numeric_columns = ["open", "high", "low", "close", "volume"]
    prepared[numeric_columns] = prepared[numeric_columns].apply(pd.to_numeric, errors="raise")
    prepared = prepared.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    prepared["trade_date"] = prepared["timestamp"].dt.date
    loader._validate_duplicates(prepared)
    prepared = drop_invalid_ohlcv_rows(prepared)
    return prepared


def drop_invalid_ohlcv_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    invalid_price_mask = (
        frame[["open", "high", "low", "close"]].isna().any(axis=1)
        | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        | (frame["low"] > frame["high"])
        | (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
    )
    invalid_volume_mask = frame["volume"].isna() | (frame["volume"] < 0)
    invalid_mask = invalid_price_mask | invalid_volume_mask

    if not invalid_mask.any():
        return frame

    invalid_rows = frame.loc[
        invalid_mask,
        ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    ].head(10)
    print(f"dropping {int(invalid_mask.sum())} invalid OHLCV rows")
    print(invalid_rows.to_string(index=False))
    return frame.loc[~invalid_mask].reset_index(drop=True)


def write_output(frame: pd.DataFrame, *, output_dir: Path, fmt: str, symbol: str, start: dt.date, end: dt.date) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_symbol = safe_symbol_filename(symbol)
    suffix = "parquet" if fmt == "parquet" else "csv"
    path = output_dir / f"{safe_symbol}_{start.isoformat()}_{end.isoformat()}.{suffix}"
    if fmt == "parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)
    return path


def build_client(auth_file: str, *, user_key: str) -> FyersClient:
    client_id, access_token = get_fyers_creds_from_json(auth_file, user_key=user_key)
    return FyersClient(client_id=client_id, access_token=access_token)


def refresh_auth_via_script(auth_file: str, *, user_key: str) -> None:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "fyers_auto_auth.py"),
        "--auth-file",
        auth_file,
        "--user-key",
        user_key,
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def ensure_symbol_history(
    client: FyersClient,
    *,
    auth_file: str,
    user_key: str,
    symbol: str,
    start: dt.date,
    end: dt.date,
    output_dir: Path,
    chunk_days: int,
    export_format: str,
    resolution: str,
) -> tuple[pd.DataFrame, Path, FyersClient]:
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = symbol_cache_path(output_dir, symbol)
    meta_path = symbol_metadata_path(output_dir, symbol)

    cached_frame = load_cached_frame(cache_path)
    fetched_ranges = load_fetch_metadata(meta_path)
    missing_ranges = subtract_covered_ranges(start, end, fetched_ranges)

    refreshed_auth = False
    incoming_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        try:
            frame = fetch_symbol_history(
                client,
                symbol=symbol,
                start=missing_start,
                end=missing_end,
                chunk_days=chunk_days,
                resolution=resolution,
            )
        except Exception:
            if refreshed_auth:
                raise
            refresh_auth_via_script(auth_file, user_key=user_key)
            client = build_client(auth_file, user_key=user_key)
            refreshed_auth = True
            frame = fetch_symbol_history(
                client,
                symbol=symbol,
                start=missing_start,
                end=missing_end,
                chunk_days=chunk_days,
                resolution=resolution,
            )
        incoming_frames.append(frame)
        fetched_ranges.append((missing_start, missing_end))

    if incoming_frames:
        merged_frame = cached_frame
        for incoming in incoming_frames:
            merged_frame = merge_cached_frames(merged_frame, incoming)
        if not merged_frame.empty:
            merged_frame.to_parquet(cache_path, index=False)
        save_fetch_metadata(meta_path, fetched_ranges)
        cached_frame = merged_frame

    requested_frame = slice_frame_by_date(cached_frame, start=start, end=end)
    output_path = write_output(
        requested_frame,
        output_dir=output_dir,
        fmt=export_format,
        symbol=symbol,
        start=start,
        end=end,
    )
    return requested_frame, output_path, client


def is_invalid_symbol_error(exc: Exception) -> bool:
    message = str(exc)
    return "Invalid symbol provided" in message


def main() -> int:
    args = parse_args()
    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    if end < start:
        raise ValueError("end date must be on or after start date")

    if args.refresh_auth_before_fetch:
        refresh_auth_via_script(args.auth_file, user_key=args.user_key)

    client = build_client(args.auth_file, user_key=args.user_key)

    output_dir = Path(args.output_dir)
    written_paths: list[Path] = []
    skipped_symbols: list[tuple[str, str]] = []
    for symbol in args.symbols:
        try:
            frame, path, client = ensure_symbol_history(
                client,
                auth_file=args.auth_file,
                user_key=args.user_key,
                symbol=symbol,
                start=start,
                end=end,
                output_dir=output_dir,
                chunk_days=args.chunk_days,
                export_format=args.format,
                resolution=args.resolution,
            )
        except Exception as exc:
            if args.skip_invalid_symbols and is_invalid_symbol_error(exc):
                skipped_symbols.append((symbol, str(exc)))
                print(f"{symbol}: skipped invalid symbol ({exc})")
                continue
            raise
        written_paths.append(path)
        print(f"{symbol}: wrote {len(frame):,} rows to {path}")

    print("done")
    for path in written_paths:
        print(path)
    if skipped_symbols:
        print("skipped invalid symbols:")
        for symbol, reason in skipped_symbols:
            print(f"{symbol}: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
