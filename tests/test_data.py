from __future__ import annotations

import pandas as pd
import pytest

from intraday_research.data import MarketDataLoader
from tests.helpers import make_full_session_frame


def test_prepare_requires_required_columns() -> None:
    frame = pd.DataFrame([["2026-06-01 09:15:00", "RELIANCE"]], columns=["timestamp", "symbol"])

    with pytest.raises(ValueError, match="Missing required columns"):
        MarketDataLoader().prepare(frame)


def test_prepare_converts_timezone_to_asia_kolkata() -> None:
    frame = make_full_session_frame("2026-06-01")
    frame["timestamp"] = pd.date_range(
        start="2026-06-01 03:45:00+00:00",
        end="2026-06-01 10:00:00+00:00",
        freq="1min",
    )

    prepared = MarketDataLoader().prepare(frame)

    assert str(prepared["timestamp"].dt.tz) == "Asia/Kolkata"
    assert prepared["timestamp"].iloc[0].hour == 9
    assert prepared["timestamp"].iloc[0].minute == 15


def test_prepare_rejects_invalid_timestamps() -> None:
    frame = make_full_session_frame("2026-06-01")
    frame.loc[0, "timestamp"] = "not-a-timestamp"

    with pytest.raises(ValueError, match="Invalid timestamp"):
        MarketDataLoader().prepare(frame)


def test_prepare_rejects_duplicate_candles() -> None:
    frame = make_full_session_frame("2026-06-01")
    duplicate = frame.iloc[[0]]
    frame = pd.concat([frame, duplicate], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate candles detected"):
        MarketDataLoader().prepare(frame)


def test_prepare_rejects_missing_session_candles() -> None:
    frame = make_full_session_frame("2026-06-01").drop(index=10).reset_index(drop=True)

    with pytest.raises(ValueError, match="Missing 1-minute candles detected"):
        MarketDataLoader().prepare(frame)


def test_prepare_rejects_invalid_ohlc_rows() -> None:
    frame = make_full_session_frame("2026-06-01")
    frame.loc[0, "high"] = frame.loc[0, "close"] - 1

    with pytest.raises(ValueError, match="Invalid OHLCV rows detected"):
        MarketDataLoader().prepare(frame)


def test_prepare_accepts_vendor_data_ending_at_1529() -> None:
    frame = make_full_session_frame("2026-06-01").iloc[:-1].reset_index(drop=True)

    prepared = MarketDataLoader().prepare(frame)

    assert not prepared.empty
    assert prepared["timestamp"].iloc[-1].hour == 15
    assert prepared["timestamp"].iloc[-1].minute == 29
