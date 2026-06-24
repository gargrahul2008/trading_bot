from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace(":", "_").replace("-", "_")


def symbol_cache_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{safe_symbol_filename(symbol)}.parquet"


def symbol_metadata_path(output_dir: Path, symbol: str) -> Path:
    return output_dir / f"{safe_symbol_filename(symbol)}.meta.json"


def load_cached_frame(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])
    frame = pd.read_parquet(cache_path)
    if "timestamp" in frame.columns:
        frame = frame.copy()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return frame


def merge_cached_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        merged = incoming.copy()
    elif incoming.empty:
        merged = existing.copy()
    else:
        merged = pd.concat([existing, incoming], ignore_index=True)
    if merged.empty:
        return merged
    merged = merged.drop_duplicates(subset=["symbol", "timestamp"]).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    return merged


def slice_frame_by_date(frame: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    working = frame.copy()
    trade_dates = pd.to_datetime(working["timestamp"]).dt.date
    mask = (trade_dates >= start) & (trade_dates <= end)
    return working.loc[mask].reset_index(drop=True)


def normalize_ranges(ranges: list[tuple[date, date]]) -> list[tuple[date, date]]:
    if not ranges:
        return []
    merged: list[tuple[date, date]] = []
    for range_start, range_end in sorted(ranges):
        if not merged:
            merged.append((range_start, range_end))
            continue
        prev_start, prev_end = merged[-1]
        if range_start <= prev_end + timedelta(days=1):
            merged[-1] = (prev_start, max(prev_end, range_end))
        else:
            merged.append((range_start, range_end))
    return merged


def subtract_covered_ranges(
    requested_start: date,
    requested_end: date,
    covered_ranges: list[tuple[date, date]],
) -> list[tuple[date, date]]:
    if requested_end < requested_start:
        raise ValueError("requested_end must be on or after requested_start")

    missing: list[tuple[date, date]] = []
    cursor = requested_start
    for covered_start, covered_end in normalize_ranges(covered_ranges):
        if covered_end < cursor:
            continue
        if covered_start > requested_end:
            break
        if covered_start > cursor:
            missing.append((cursor, min(requested_end, covered_start - timedelta(days=1))))
        cursor = max(cursor, covered_end + timedelta(days=1))
        if cursor > requested_end:
            break
    if cursor <= requested_end:
        missing.append((cursor, requested_end))
    return missing


def load_fetch_metadata(meta_path: Path) -> list[tuple[date, date]]:
    if not meta_path.exists():
        return []
    payload = json.loads(meta_path.read_text())
    raw_ranges = payload.get("fetched_ranges", [])
    parsed: list[tuple[date, date]] = []
    for item in raw_ranges:
        parsed.append((date.fromisoformat(item["start"]), date.fromisoformat(item["end"])))
    return normalize_ranges(parsed)


def save_fetch_metadata(meta_path: Path, fetched_ranges: list[tuple[date, date]]) -> None:
    normalized = normalize_ranges(fetched_ranges)
    payload = {
        "fetched_ranges": [
            {"start": range_start.isoformat(), "end": range_end.isoformat()}
            for range_start, range_end in normalized
        ]
    }
    meta_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
