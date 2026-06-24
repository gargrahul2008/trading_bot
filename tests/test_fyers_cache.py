from __future__ import annotations

from datetime import date

import pandas as pd

from intraday_research.fyers_cache import (
    merge_cached_frames,
    normalize_ranges,
    slice_frame_by_date,
    subtract_covered_ranges,
)


def test_normalize_ranges_merges_overlapping_and_adjacent_ranges() -> None:
    ranges = [
        (date(2026, 4, 5), date(2026, 4, 7)),
        (date(2026, 4, 1), date(2026, 4, 3)),
        (date(2026, 4, 4), date(2026, 4, 4)),
    ]

    assert normalize_ranges(ranges) == [(date(2026, 4, 1), date(2026, 4, 7))]


def test_subtract_covered_ranges_returns_only_uncovered_intervals() -> None:
    covered = [
        (date(2026, 4, 1), date(2026, 4, 3)),
        (date(2026, 4, 6), date(2026, 4, 8)),
    ]

    missing = subtract_covered_ranges(date(2026, 4, 1), date(2026, 4, 10), covered)

    assert missing == [
        (date(2026, 4, 4), date(2026, 4, 5)),
        (date(2026, 4, 9), date(2026, 4, 10)),
    ]


def test_merge_cached_frames_deduplicates_and_sorts() -> None:
    existing = pd.DataFrame(
        [
            ["2026-04-01 09:15:00+05:30", "NSE:RELIANCE-EQ", 100.0, 101.0, 99.0, 100.5, 1000],
            ["2026-04-01 09:16:00+05:30", "NSE:RELIANCE-EQ", 100.5, 101.0, 100.0, 100.7, 900],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    incoming = pd.DataFrame(
        [
            ["2026-04-01 09:16:00+05:30", "NSE:RELIANCE-EQ", 100.5, 101.0, 100.0, 100.7, 900],
            ["2026-04-01 09:17:00+05:30", "NSE:RELIANCE-EQ", 100.7, 101.2, 100.4, 101.0, 1200],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    existing["timestamp"] = pd.to_datetime(existing["timestamp"])
    incoming["timestamp"] = pd.to_datetime(incoming["timestamp"])

    merged = merge_cached_frames(existing, incoming)

    assert len(merged) == 3
    assert list(merged["timestamp"]) == list(pd.to_datetime(
        [
            "2026-04-01 09:15:00+05:30",
            "2026-04-01 09:16:00+05:30",
            "2026-04-01 09:17:00+05:30",
        ]
    ))


def test_slice_frame_by_date_filters_requested_window() -> None:
    frame = pd.DataFrame(
        [
            ["2026-04-01 09:15:00+05:30", "NSE:RELIANCE-EQ", 100.0, 101.0, 99.0, 100.5, 1000],
            ["2026-04-02 09:15:00+05:30", "NSE:RELIANCE-EQ", 101.0, 102.0, 100.0, 101.5, 1100],
            ["2026-04-03 09:15:00+05:30", "NSE:RELIANCE-EQ", 102.0, 103.0, 101.0, 102.5, 1200],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])

    sliced = slice_frame_by_date(frame, start=date(2026, 4, 2), end=date(2026, 4, 3))

    assert len(sliced) == 2
    assert sliced.iloc[0]["timestamp"] == pd.Timestamp("2026-04-02 09:15:00+05:30")
    assert sliced.iloc[1]["timestamp"] == pd.Timestamp("2026-04-03 09:15:00+05:30")
