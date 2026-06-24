from __future__ import annotations

import pandas as pd

from intraday_research.features import FeatureEngine


def test_feature_engine_returns_clean_feature_frame() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 10.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 102.0, 100.0, 101.0, 20.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.0, 103.0, 100.0, 102.0, 30.0],
            ["2026-06-02 09:15:00+05:30", "RELIANCE", 103.0, 104.0, 102.0, 103.0, 10.0],
            ["2026-06-02 09:16:00+05:30", "RELIANCE", 103.0, 105.0, 102.0, 104.0, 20.0],
            ["2026-06-02 09:17:00+05:30", "RELIANCE", 104.0, 106.0, 103.0, 105.0, 10.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])

    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    assert list(featured.columns) == [
        "timestamp",
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_minute",
        "vwap",
        "atr",
        "ema_fast",
        "ema_slow",
        "trend_gap_atr",
        "opening_range_high",
        "opening_range_low",
        "prev_day_high",
        "prev_day_low",
        "prev_day_close",
    ]
    assert "typical_price" not in featured.columns
    assert "tpv" not in featured.columns
    assert "true_range" not in featured.columns


def test_feature_engine_computes_vwap_opening_range_and_previous_day_levels() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 10.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 102.0, 100.0, 101.0, 20.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.0, 103.0, 100.0, 102.0, 30.0],
            ["2026-06-02 09:15:00+05:30", "RELIANCE", 103.0, 104.0, 102.0, 103.0, 10.0],
            ["2026-06-02 09:16:00+05:30", "RELIANCE", 103.0, 105.0, 102.0, 104.0, 20.0],
            ["2026-06-02 09:17:00+05:30", "RELIANCE", 104.0, 106.0, 103.0, 105.0, 10.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])

    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    day_one = featured[featured["trade_date"] == pd.to_datetime("2026-06-01").date()].reset_index(drop=True)
    day_two = featured[featured["trade_date"] == pd.to_datetime("2026-06-02").date()].reset_index(drop=True)

    assert round(day_one.loc[0, "vwap"], 4) == 100.0
    assert round(day_one.loc[1, "vwap"], 4) == 100.6667
    assert round(day_one.loc[2, "vwap"], 4) == 101.1667
    assert day_one.loc[2, "ema_fast"] > day_one.loc[1, "ema_fast"]
    assert day_one.loc[2, "ema_slow"] > day_one.loc[1, "ema_slow"]
    assert day_one.loc[2, "opening_range_high"] == 102.0
    assert day_one.loc[2, "opening_range_low"] == 99.0
    assert day_two.loc[0, "prev_day_high"] == 103.0
    assert day_two.loc[0, "prev_day_low"] == 99.0
    assert day_two.loc[0, "prev_day_close"] == 102.0


def test_feature_engine_computes_atr_from_small_synthetic_series() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 101.0, 99.0, 100.0, 10.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 103.0, 99.0, 102.0, 10.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 102.0, 104.0, 101.0, 103.0, 10.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])

    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    assert round(featured.loc[0, "atr"], 4) == 2.0
    assert round(featured.loc[1, "atr"], 4) == 3.0
    assert round(featured.loc[2, "atr"], 4) == 3.0
