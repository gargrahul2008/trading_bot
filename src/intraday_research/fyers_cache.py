from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

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


def safe_cacheable_end(requested_end: date, resolution: str, today: date) -> date | None:
    """The latest date that's actually safe to permanently record as
    "fetched" for this resolution.

    A 1-minute candle is final the instant its own minute has passed, so
    resolution="1" is always fully safe to cache exactly as requested. A
    DAILY (or any other single-bar-per-session) candle is NOT final until
    that session's close — its OHLC keeps changing all day. Fetching
    "today" mid-session and permanently caching it as "fetched" freezes a
    partial snapshot forever (wrong close, a fraction of the day's real
    volume), since every later run trusts fetched_ranges and never
    re-fetches a date already marked covered.

    So for any non-"1" resolution, cap the cacheable end at `today - 1
    day` — "today" (by IST calendar date, whatever the actual time is) is
    never recorded as covered, so it keeps getting re-fetched (cheap: one
    small request) until the calendar date itself rolls over, at which
    point it becomes safely final. The caller is responsible for checking
    the returned date against its own requested start — if this capped end
    falls before that start, nothing in the range is safe to cache yet.
    """
    if resolution == "1" or requested_end < today:
        return requested_end
    return today - timedelta(days=1)


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


def expected_weekday_count(start: date, end: date) -> int:
    """Number of Mon-Fri calendar days in [start, end] — a cheap, calendar-free
    proxy for "how many trading days should roughly be in this range" (a few
    real market holidays will always fall short of this; see the tolerance in
    callers). Used to detect a fetch that silently came back empty/partial
    instead of blindly trusting whatever range was requested."""
    if end < start:
        return 0
    n_days = (end - start).days + 1
    full_weeks, remainder = divmod(n_days, 7)
    count = full_weeks * 5
    for i in range(remainder):
        if (start + timedelta(days=full_weeks * 7 + i)).weekday() < 5:
            count += 1
    return count


def gap_tolerance(expected_weekdays: int) -> int:
    """How many of `expected_weekday_count`'s Mon-Fri days may legitimately be
    absent before a fetch looks INCOMPLETE rather than just holiday-laden.
    India has ~12-16 NSE trading holidays a year (~1 per 15-16 trading days),
    so a flat small tolerance (fine for a short chunk) would spuriously flag
    every multi-month/multi-year fetch as broken. Scales with the requested
    span instead: ~1 in 15 expected weekdays, floor 2 (still catches a short
    real gap like a handful of missing days)."""
    return max(2, expected_weekdays // 15)


def group_into_contiguous_ranges(dates: set[date], *, bridge_days: int = 3) -> list[tuple[date, date]]:
    """Group a set of dates into contiguous [start, end] trading runs, treating
    a gap of up to `bridge_days` (default 3, so weekends don't split a run) as
    still-contiguous. A larger gap starts a new run."""
    ordered = sorted(dates)
    if not ordered:
        return []
    ranges = [[ordered[0], ordered[0]]]
    for d in ordered[1:]:
        if (d - ranges[-1][1]).days <= bridge_days:
            ranges[-1][1] = d
        else:
            ranges.append([d, d])
    return [(lo, hi) for lo, hi in ranges]


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


# ── Invalid/delisted-symbol registry ────────────────────────────────────────
# A symbol Fyers confirms doesn't exist ("-300 Invalid symbol provided") is
# invalid regardless of resolution (1-minute vs daily) — so this one small
# registry is shared across BOTH data/fyers/ and data/fyers_daily/, keyed by
# `data_dir` = the repo's data root (their common parent), not either
# resolution-specific subdir. Without it, every run re-discovers the same
# delisted/renamed tickers from scratch: ~4 retries x several seconds EACH,
# every single time, for every bad ticker in a universe scan.

def invalid_symbols_registry_path(data_dir: Path) -> Path:
    return data_dir / "fyers_invalid_symbols.json"


def load_invalid_symbols(data_dir: Path) -> dict[str, dict]:
    path = invalid_symbols_registry_path(data_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def is_known_invalid_symbol(data_dir: Path, fyers_symbol: str) -> bool:
    return fyers_symbol in load_invalid_symbols(data_dir)


def record_invalid_symbol(data_dir: Path, fyers_symbol: str, reason: str) -> None:
    """Persist a symbol Fyers has confirmed is invalid, so future runs skip
    it with zero network calls. Not auto-expired — if a symbol is later
    relisted under the same ticker (rare), delete its entry (or the whole
    file) to force a re-check."""
    path = invalid_symbols_registry_path(data_dir)
    registry = load_invalid_symbols(data_dir)
    registry[fyers_symbol] = {"reason": str(reason)[:300], "marked_at": date.today().isoformat()}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(registry, indent=2, sort_keys=True))


# ── Inactive-symbol tracking (confirmed-delisting fetch cap) ───────────────
# Distinct from the invalid-symbols registry above: a symbol here WAS valid
# and has real, already-cached history — it has just stopped producing NEW
# data past some date (delisted, suspended, merged into another listing).
# This needs a HIGHER bar than a single-retry confirmation: the SAME trailing
# gap must be independently observed on >= INACTIVE_CONFIRMATIONS_REQUIRED
# separate calendar days before being trusted enough to cap future fetch
# ranges — a single bad session/batch must never alone brand an actively-
# traded stock "inactive" (see the invalid-symbols registry's own 2026-09-05
# incident, where one broken session blacklisted 29 real stocks on a single
# observation — this mirrors that fix for a different failure mode: a
# silent empty response rather than a thrown error).

INACTIVE_MIN_GAP_TRADING_DAYS = 10
INACTIVE_CONFIRMATIONS_REQUIRED = 2


def inactive_symbols_registry_path(data_dir: Path) -> Path:
    return data_dir / "fyers_inactive_symbols.json"


def load_inactive_symbols(data_dir: Path) -> dict[str, dict]:
    path = inactive_symbols_registry_path(data_dir)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def get_confirmed_inactive_from(data_dir: Path, fyers_symbol: str) -> Optional[date]:
    """The confirmed inactive-from date for `fyers_symbol` — fetches should
    never request past `inactive_from - 1 day` — or None if not (yet)
    confirmed. A single observation is never enough; see the module note."""
    entry = load_inactive_symbols(data_dir).get(fyers_symbol)
    if not entry or not entry.get("confirmed"):
        return None
    return date.fromisoformat(entry["inactive_from"])


def record_trailing_gap_observation(
    data_dir: Path, fyers_symbol: str, *, last_good_date: date, checked_through: date,
) -> bool:
    """Record that, as of TODAY, `fyers_symbol` produced NO new data between
    `last_good_date` and `checked_through` despite an active fetch attempt
    covering that whole span. Ignored outright if that span is shorter than
    INACTIVE_MIN_GAP_TRADING_DAYS (an ordinary holiday cluster, not evidence
    of anything). Otherwise logs one observation for today's calendar date;
    only actually marks the symbol confirmed-inactive once the SAME gap
    (same `last_good_date`) has been observed on
    INACTIVE_CONFIRMATIONS_REQUIRED separate days — a later call with a
    NEWER `last_good_date` means the symbol produced fresh data since, so
    resets the count (it's alive). Returns True iff this call is what newly
    confirmed it."""
    if expected_weekday_count(last_good_date + timedelta(days=1), checked_through) < INACTIVE_MIN_GAP_TRADING_DAYS:
        return False

    path = inactive_symbols_registry_path(data_dir)
    registry = load_inactive_symbols(data_dir)
    today_str = date.today().isoformat()
    entry = registry.get(fyers_symbol)
    if not entry or entry.get("last_good_date") != last_good_date.isoformat() or entry.get("confirmed"):
        entry = {"last_good_date": last_good_date.isoformat(), "confirmed": False, "observed_on": []}

    if today_str not in entry["observed_on"]:
        entry["observed_on"].append(today_str)

    newly_confirmed = False
    if not entry["confirmed"] and len(entry["observed_on"]) >= INACTIVE_CONFIRMATIONS_REQUIRED:
        entry["confirmed"] = True
        entry["inactive_from"] = (last_good_date + timedelta(days=1)).isoformat()
        newly_confirmed = True

    registry[fyers_symbol] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(registry, indent=2, sort_keys=True))
    return newly_confirmed
