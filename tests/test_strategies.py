from __future__ import annotations

import pandas as pd
import pytest

from intraday_research.features import FeatureEngine
from intraday_research.strategies import (
    NiftyBOSFibScalpStrategy,
    OpeningRangeBreakoutStrategy,
    TrendPullbackStrategy,
    VWAPMeanReversionStrategy,
)


def test_opening_range_breakout_strategy_returns_signal_rows() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 1000.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 100.0, 103.0, 100.0, 103.0, 1000.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = OpeningRangeBreakoutStrategy(
        opening_range_minutes=2,
        breakout_buffer_atr=0.0,
        stop_atr_multiple=1.0,
        target_atr_multiple=2.0,
    ).generate_signals(featured)

    assert list(signals.columns) == [
        "timestamp",
        "symbol",
        "direction",
        "strength",
        "reason",
        "stop_loss",
        "target",
        "strategy_name",
    ]
    assert len(signals) == 1
    assert signals.iloc[0]["direction"] == "LONG"
    assert signals.iloc[0]["strategy_name"] == "opening_range_breakout"
    assert signals.iloc[0]["stop_loss"] < 103.0
    assert signals.iloc[0]["target"] > 103.0


def test_opening_range_breakout_strategy_applies_width_volume_and_side_limits() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 100.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 101.0, 99.0, 100.0, 100.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 100.0, 100.3, 100.0, 100.1, 10.0],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 100.1, 104.0, 100.1, 104.0, 500.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    filtered_signals = OpeningRangeBreakoutStrategy(
        opening_range_minutes=2,
        breakout_buffer_atr=0.0,
        stop_atr_multiple=1.0,
        target_atr_multiple=2.0,
        minimum_opening_range_width_atr=0.5,
        breakout_volume_ratio=2.0,
        max_breakouts_per_side_per_day=1,
    ).generate_signals(featured)

    assert len(filtered_signals) == 1
    assert filtered_signals.iloc[0]["timestamp"] == pd.Timestamp("2026-06-01 09:18:00+05:30")


def test_opening_range_breakout_strategy_can_require_trend_alignment() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 99.8, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 100.6, 99.9, 100.4, 1000.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 100.4, 101.4, 100.3, 101.3, 1200.0],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.3, 102.2, 101.2, 102.0, 1300.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2, ema_fast_span=2, ema_slow_span=3).transform(frame)

    signals = OpeningRangeBreakoutStrategy(
        opening_range_minutes=2,
        breakout_buffer_atr=0.0,
        stop_atr_multiple=1.0,
        target_atr_multiple=2.0,
        require_trend_alignment=True,
        minimum_trend_gap_atr=0.05,
        require_vwap_alignment=True,
    ).generate_signals(featured)

    assert len(signals) == 1
    assert signals.iloc[0]["direction"] == "LONG"


def test_vwap_mean_reversion_strategy_returns_signal_rows() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 100.0, 100.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 100.0, 94.0, 94.0, 1000.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 95.0, 99.0, 95.0, 99.0, 1000.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=2,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
    ).generate_signals(featured)

    assert list(signals.columns) == [
        "timestamp",
        "symbol",
        "direction",
        "strength",
        "reason",
        "stop_loss",
        "target",
        "strategy_name",
    ]
    assert len(signals) == 1
    assert signals.iloc[0]["direction"] == "LONG"
    assert signals.iloc[0]["strategy_name"] == "vwap_mean_reversion"
    assert signals.iloc[0]["reason"] == "reclaim_from_vwap_discount"


def test_vwap_mean_reversion_strategy_applies_reversal_midday_and_side_limits() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 100.0, 100.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 100.0, 94.0, 94.0, 1000.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 99.0, 99.0, 95.0, 95.0, 1000.0],
            ["2026-06-01 13:00:00+05:30", "NIFTY", 100.0, 100.0, 94.0, 94.0, 1000.0],
            ["2026-06-01 13:01:00+05:30", "NIFTY", 94.0, 99.0, 94.0, 99.0, 1000.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=2,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
        minimum_deviation_atr=0.2,
        require_reversal_confirmation=True,
        skip_midday_window_start=pd.Timestamp("12:30").time(),
        skip_midday_window_end=pd.Timestamp("13:30").time(),
        max_trades_per_side_per_day=1,
    ).generate_signals(featured)

    assert len(signals) == 0


def test_vwap_mean_reversion_strategy_requires_followthrough_and_quality_filters() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 100.0, 100.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 100.0, 94.0, 94.0, 1000.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 95.0, 99.0, 93.5, 98.5, 2500.0],
            ["2026-06-01 09:18:00+05:30", "NIFTY", 98.5, 100.0, 98.4, 99.8, 2200.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=2,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
        minimum_deviation_atr=0.2,
        require_reversal_confirmation=True,
        require_next_candle_followthrough=True,
        minimum_reversal_body_pct=0.5,
        minimum_rejection_wick_pct=0.05,
        minimum_volume_expansion_ratio=1.5,
        volume_expansion_lookback=2,
        max_trades_per_side_per_day=1,
    ).generate_signals(featured)

    assert len(signals) == 1
    assert signals.iloc[0]["timestamp"] == pd.Timestamp("2026-06-01 09:18:00+05:30")
    assert signals.iloc[0]["reason"] == "reclaim_from_vwap_discount_followthrough"


def test_vwap_mean_reversion_strategy_requires_regime_and_reclaim_filters() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 100.0, 100.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 100.2, 99.8, 100.1, 1000.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 100.1, 100.3, 95.0, 95.2, 1200.0],
            ["2026-06-01 09:18:00+05:30", "NIFTY", 95.4, 100.5, 95.0, 100.4, 2500.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=2,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
        minimum_deviation_atr=0.2,
        minimum_extreme_deviation_atr=0.4,
        require_reversal_confirmation=True,
        require_prior_candle_reclaim=True,
        require_opening_range_reclaim=True,
        maximum_vwap_slope_atr=0.2,
        vwap_slope_lookback=2,
    ).generate_signals(featured)

    assert len(signals) == 1
    assert signals.iloc[0]["direction"] == "LONG"
    assert signals.iloc[0]["timestamp"] == pd.Timestamp("2026-06-01 09:18:00+05:30")


def test_vwap_mean_reversion_strategy_blocks_trend_break_when_reclaim_filters_fail() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "NIFTY", 100.0, 100.0, 100.0, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "NIFTY", 100.0, 100.2, 99.8, 100.1, 1000.0],
            ["2026-06-01 09:17:00+05:30", "NIFTY", 100.1, 100.3, 95.0, 95.2, 1200.0],
            ["2026-06-01 09:18:00+05:30", "NIFTY", 95.4, 99.9, 95.0, 99.6, 2500.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=2,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
        minimum_deviation_atr=0.2,
        minimum_extreme_deviation_atr=0.4,
        require_reversal_confirmation=True,
        require_prior_candle_reclaim=True,
        require_opening_range_reclaim=True,
        maximum_vwap_slope_atr=0.1,
        vwap_slope_lookback=2,
    ).generate_signals(featured)

    assert signals.empty


def _bos_fib_bearish_frame() -> pd.DataFrame:
    """
    1-minute candles with a clear bearish micro-trend and BOS followed by a retracement.

    Structure:
      bar2  → pivot HIGH 100.2  (confirmed at bar4)
      bar6  → pivot LOW  97.8   (confirmed at bar8)
      bar11 → pivot HIGH 99.1   (LH, confirmed at bar13)  → bearish trend established
      bar14 → BOS: low=97.5 < pivot_low=97.8
              impulse_high=99.1, impulse_low=97.5, size=1.6, fib_618=98.49
      bar15-16 → consolidation above 97.5 (no extension, low stays > 97.5)
      bar17 → retrace: high=98.55 > fib_618=98.49 → SHORT signal fires (limit_618 mode)
    """
    rng = pd.date_range("2026-06-01 09:15", periods=25, freq="1min", tz="Asia/Kolkata")
    rows = [
        (99.5, 100.0, 99.0, 99.5),   # bar0
        (99.5, 100.0, 99.2, 99.8),   # bar1
        (99.8, 100.2, 99.5, 99.7),   # bar2 — pivot HIGH 100.2
        (99.7,  99.9, 99.2, 99.3),   # bar3
        (99.3,  99.5, 98.8, 98.9),   # bar4 — confirms HIGH@bar2
        (98.9,  99.1, 98.3, 98.4),   # bar5
        (98.4,  98.6, 97.8, 97.9),   # bar6 — pivot LOW 97.8
        (97.9,  98.3, 97.9, 98.1),   # bar7
        (98.1,  98.5, 97.9, 98.2),   # bar8 — confirms LOW@bar6
        (98.2,  98.8, 98.0, 98.6),   # bar9
        (98.6,  99.0, 98.4, 98.8),   # bar10
        (98.8,  99.1, 98.5, 98.7),   # bar11 — pivot HIGH 99.1 (LH < 100.2)
        (98.7,  98.9, 98.3, 98.4),   # bar12
        (98.4,  98.6, 97.9, 98.0),   # bar13 — confirms HIGH@bar11 → bearish trend
        (98.0,  98.2, 97.5, 97.6),   # bar14 — BOS: low 97.5 < pivot_low 97.8
        (97.6,  97.8, 97.6, 97.7),   # bar15 — consolidation, low 97.6 > 97.5 (no extension)
        (97.7,  97.9, 97.6, 97.8),   # bar16 — consolidation
        (98.55, 98.55, 97.5, 98.3),  # bar17 — retrace high 98.55 >= fib_618 98.49, bearish bar (C<O) ← LIMIT618 fires here
        (98.4,  98.6, 97.8, 97.9),   # bar18
        (97.9,  98.1, 97.3, 97.4),   # bar19
        (97.0,  97.3, 96.8, 97.0),   # bar20
        (97.0,  97.3, 96.8, 97.0),   # bar21
        (97.0,  97.3, 96.8, 97.0),   # bar22
        (97.0,  97.3, 96.8, 97.0),   # bar23
        (97.0,  97.3, 96.8, 97.0),   # bar24
    ]
    return pd.DataFrame(
        {
            "timestamp": rng,
            "symbol": "NIFTY",
            "trade_date": "2026-06-01",
            "open":  [r[0] for r in rows],
            "high":  [r[1] for r in rows],
            "low":   [r[2] for r in rows],
            "close": [r[3] for r in rows],
            "volume": [1000] * len(rows),
            "session_minute": list(range(len(rows))),
        }
    )


def _bos_fib_bullish_frame() -> pd.DataFrame:
    """
    1-minute candles with a clear bullish micro-trend and BOS followed by a pullback.

    Structure:
      bar2  → pivot LOW  95.0   (confirmed at bar4)
      bar6  → pivot HIGH 100.0  (confirmed at bar8)
      bar10 → pivot LOW  97.0   (HL > 95.0, confirmed at bar12) → bullish trend established
      bar13 → BOS: high=101.0 > pivot_high=100.0
              impulse_low=97.0, impulse_high=101.0, size=4.0
              fib_618 = 101.0 - 0.618*4.0 = 98.528
      bar16 → pullback: low=98.4 <= fib_618 98.528 → LONG signal fires (limit_618 mode)
    """
    rng = pd.date_range("2026-06-01 09:15", periods=25, freq="1min", tz="Asia/Kolkata")
    rows = [
        (96.0,  97.0, 95.5, 96.0),   # bar0
        (96.0,  96.5, 95.2, 95.5),   # bar1
        (95.5,  96.0, 95.0, 95.3),   # bar2 — pivot LOW 95.0
        (95.3,  96.0, 95.1, 95.8),   # bar3
        (95.8,  96.5, 95.4, 96.2),   # bar4 — confirms LOW@bar2
        (96.2,  97.5, 96.0, 97.2),   # bar5
        (97.2,  100.0, 97.0, 99.5),  # bar6 — pivot HIGH 100.0
        (99.5,  100.3, 98.8, 99.0),  # bar7
        (99.0,  99.5,  97.8, 98.0),  # bar8 — confirms HIGH@bar6
        (98.0,  98.5,  97.3, 97.5),  # bar9
        (97.5,  98.0,  97.0, 97.3),  # bar10 — pivot LOW 97.0 (HL > 95.0)
        (97.3,  97.8,  97.1, 97.6),  # bar11
        (97.6,  98.2,  97.4, 98.0),  # bar12 — confirms LOW@bar10 → bullish trend
        (98.0,  101.0, 97.9, 100.5), # bar13 — BOS: high 101.0 > pivot_high 100.0
        (100.5, 100.8, 99.5, 99.8),  # bar14 — pullback, high=100.8 <= 101.0 (no extension)
        (99.8,  100.0, 98.6, 98.8),  # bar15
        (98.4,  99.2,  98.4, 98.7),  # bar16 — pullback: low=98.4 <= fib_618=98.528, bullish bar (C>O) → LONG!
        (98.7,  99.5,  98.5, 99.3),  # bar17
        (99.3,  99.8,  99.0, 99.6),  # bar18
        (99.6, 100.2,  99.4, 100.0), # bar19
        (100.0, 100.5, 99.8, 100.3), # bar20
        (100.0, 100.5, 99.8, 100.3), # bar21
        (100.0, 100.5, 99.8, 100.3), # bar22
        (100.0, 100.5, 99.8, 100.3), # bar23
        (100.0, 100.5, 99.8, 100.3), # bar24
    ]
    return pd.DataFrame(
        {
            "timestamp": rng,
            "symbol": "NIFTY",
            "trade_date": "2026-06-01",
            "open":  [r[0] for r in rows],
            "high":  [r[1] for r in rows],
            "low":   [r[2] for r in rows],
            "close": [r[3] for r in rows],
            "volume": [1000] * len(rows),
            "session_minute": list(range(len(rows))),
        }
    )


def test_bos_fib_bearish_generates_short_signal_at_fib618() -> None:
    frame = _bos_fib_bearish_frame()
    signals = NiftyBOSFibScalpStrategy(pivot_lookback=2, entry_mode="limit_618").generate_signals(frame)

    assert len(signals) == 1
    sig = signals.iloc[0]
    assert sig["strategy_name"] == "NIFTY_1M_BOS_FIB_SCALP"
    assert sig["direction"] == "SHORT"
    # Stop above impulse_high (99.1) + buffer (2.0)
    assert sig["stop_loss"] == pytest.approx(99.1 + 2.0, abs=0.01)
    # Target at impulse_low (97.5)
    assert sig["target"] == pytest.approx(97.5, abs=0.01)
    assert "BEARISH" in sig["reason"] or "bos_fib_short" in sig["reason"]
    assert "fib618=98.49" in sig["reason"]


def test_bos_fib_bullish_generates_long_signal_at_fib618() -> None:
    frame = _bos_fib_bullish_frame()
    signals = NiftyBOSFibScalpStrategy(pivot_lookback=2, entry_mode="limit_618").generate_signals(frame)

    assert len(signals) == 1
    sig = signals.iloc[0]
    assert sig["direction"] == "LONG"
    # Stop below impulse_low (97.0) - buffer (2.0)
    assert sig["stop_loss"] == pytest.approx(97.0 - 2.0, abs=0.01)
    # Target at impulse_high (101.0)
    assert sig["target"] == pytest.approx(101.0, abs=0.01)
    assert "bos_fib_long" in sig["reason"]


def test_bos_fib_zone_touch_confirmation_fires_one_bar_later() -> None:
    frame = _bos_fib_bearish_frame()
    sig_limit = NiftyBOSFibScalpStrategy(pivot_lookback=2, entry_mode="limit_618").generate_signals(frame)
    sig_zone = NiftyBOSFibScalpStrategy(pivot_lookback=2, entry_mode="zone_touch_confirmation").generate_signals(frame)

    # zone_touch_confirmation must fire at least one bar after limit_618
    assert not sig_zone.empty
    assert sig_zone.iloc[0]["direction"] == "SHORT"
    ts_limit = pd.Timestamp(sig_limit.iloc[0]["timestamp"])
    ts_zone = pd.Timestamp(sig_zone.iloc[0]["timestamp"])
    assert ts_zone > ts_limit


def test_bos_fib_no_signal_without_trend_structure() -> None:
    # Only one day of random bars — no confirmed HH/HL or LH/LL pattern
    rng = pd.date_range("2026-06-01 09:15", periods=10, freq="1min", tz="Asia/Kolkata")
    frame = pd.DataFrame(
        {
            "timestamp": rng,
            "symbol": "NIFTY",
            "trade_date": "2026-06-01",
            "open":  [100.0] * 10,
            "high":  [100.5] * 10,
            "low":   [99.5] * 10,
            "close": [100.0] * 10,
            "volume": [1000] * 10,
            "session_minute": list(range(10)),
        }
    )
    signals = NiftyBOSFibScalpStrategy().generate_signals(frame)
    assert signals.empty


def test_vwap_mean_reversion_strategy_can_require_trend_alignment() -> None:
    frame = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.2, 99.9, 100.0, 1000.0],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.0, 101.1, 99.9, 101.0, 1000.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.0, 101.2, 98.8, 99.0, 1000.0],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 99.2, 101.6, 99.1, 101.4, 1400.0],
        ],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    featured = FeatureEngine(atr_window=3, opening_range_minutes=2, ema_fast_span=2, ema_slow_span=4).transform(frame)

    signals = VWAPMeanReversionStrategy(
        opening_range_minutes=3,
        reversion_band_atr=0.2,
        stop_atr_multiple=0.8,
        target_atr_multiple=1.2,
        minimum_deviation_atr=0.2,
        require_reversal_confirmation=True,
        require_trend_alignment=True,
        minimum_trend_gap_atr=0.05,
    ).generate_signals(featured)

    assert len(signals) == 1
    assert signals.iloc[0]["timestamp"] == pd.Timestamp("2026-06-01 09:18:00+05:30")
    assert signals.iloc[0]["direction"] == "LONG"


def test_trend_pullback_strategy_returns_reclaim_signal() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 100.15, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.7, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.1, 101.6, 101.8, 1300.0, 1, 100.8, 1.0, 101.4, 100.4, 1.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.6, 900.0, 2, 101.1, 1.0, 101.8, 100.7, 1.1],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.6, 101.9, 100.9, 101.4, 850.0, 3, 101.0, 1.0, 101.8, 100.8, 1.0],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.6, 102.4, 100.6, 102.2, 1800.0, 4, 101.4, 1.0, 101.8, 100.9, 0.9],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        pullback_touch_buffer_atr=0.1,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        stop_atr_multiple=0.1,
        target_atr_multiple=4.1,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_vwap_alignment=True,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
    ).generate_signals(featured)

    assert len(signals) == 1
    assert signals.iloc[0]["strategy_name"] == "trend_pullback"
    assert signals.iloc[0]["direction"] == "LONG"
    assert signals.iloc[0]["reason"] == "trend_pullback_long_reclaim"
    assert 0.0 <= signals.iloc[0]["strength"] <= 100.0


def test_trend_pullback_strategy_blocks_signal_when_regime_breaks() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 99.7, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.8, 0.1],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 100.6, 99.9, 100.1, 1000.0, 1, 100.1, 1.0, 100.0, 99.9, 0.05],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 100.1, 100.2, 99.8, 99.9, 1100.0, 2, 100.0, 1.0, 99.95, 99.98, -0.03],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 99.9, 100.2, 99.7, 100.0, 1200.0, 3, 100.0, 1.0, 99.98, 99.99, -0.01],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 100.0, 100.5, 99.9, 100.4, 1800.0, 4, 100.2, 1.0, 100.05, 100.02, 0.03],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_trend_gap_atr=0.5,
        require_vwap_alignment=True,
    ).generate_signals(featured)

    assert signals.empty


def test_trend_pullback_strategy_can_detect_earlier_rejection_entry() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.4, 100.15, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.8, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.2, 101.7, 101.8, 1200.0, 1, 100.8, 1.0, 101.5, 100.4, 1.1],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.4, 900.0, 2, 101.0, 1.0, 101.7, 100.8, 0.9],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.4, 101.8, 100.9, 101.1, 850.0, 3, 101.0, 1.0, 101.6, 100.9, 0.7],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.0, 101.25, 100.68, 101.2, 1700.0, 4, 101.0, 1.0, 101.4, 101.0, 0.5],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    default_signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_pullback_depth_atr=0.2,
        minimum_trend_gap_atr=0.01,
        minimum_reclaim_body_pct=0.3,
        minimum_pullback_bars=2,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.1,
        volume_expansion_lookback=2,
        require_vwap_alignment=False,
    ).generate_signals(featured)
    early_signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_pullback_depth_atr=0.2,
        minimum_trend_gap_atr=0.01,
        minimum_reclaim_body_pct=0.3,
        minimum_pullback_bars=2,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.1,
        volume_expansion_lookback=2,
        require_vwap_alignment=False,
        allow_intrabar_rejection_entry=True,
    ).generate_signals(featured)

    assert default_signals.empty
    assert len(early_signals) == 1
    assert early_signals.iloc[0]["timestamp"] == pd.Timestamp("2026-06-01 09:19:00+05:30")
    assert early_signals.iloc[0]["reason"] == "trend_pullback_long_rejection"


def test_trend_pullback_strategy_blocks_signal_when_structure_breaks() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 100.0, 100.3, 1000.0, 0, 100.0, 1.0, 100.0, 99.8, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.3, 102.0, 100.2, 101.8, 1200.0, 1, 100.8, 1.0, 101.6, 100.4, 1.2],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 101.9, 99.9, 101.4, 900.0, 2, 101.0, 1.0, 101.7, 100.7, 1.0],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.4, 101.7, 99.8, 101.2, 850.0, 3, 101.0, 1.0, 101.6, 100.9, 0.9],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.2, 102.4, 99.7, 102.1, 1800.0, 4, 101.2, 1.0, 101.7, 101.0, 0.7],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_pullback_depth_atr=0.1,
        minimum_pullback_bars=2,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        allow_intrabar_rejection_entry=True,
        require_vwap_alignment=False,
    ).generate_signals(featured)

    assert signals.empty


def test_trend_pullback_strategy_blocks_signal_when_reclaim_volume_fails() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 99.7, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.7, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.1, 100.1, 101.8, 1300.0, 1, 100.8, 1.0, 101.4, 100.4, 1.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.6, 900.0, 2, 101.1, 1.0, 101.8, 100.7, 1.1],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.6, 101.9, 100.9, 101.4, 850.0, 3, 101.0, 1.0, 101.8, 100.8, 1.0],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.4, 102.4, 101.0, 102.2, 900.0, 4, 101.4, 1.0, 101.8, 100.9, 0.9],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        require_vwap_alignment=True,
    ).generate_signals(featured)

    assert signals.empty


def test_trend_pullback_strategy_blocks_lagged_entry_when_move_already_consumed() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 99.7, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.7, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.1, 100.1, 101.8, 1300.0, 1, 100.8, 1.0, 101.4, 100.4, 1.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.6, 900.0, 2, 101.1, 1.0, 101.8, 100.7, 1.1],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.6, 101.9, 100.9, 101.4, 850.0, 3, 101.0, 1.0, 101.8, 100.8, 1.0],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.4, 103.2, 101.0, 103.0, 1800.0, 4, 101.4, 1.0, 101.8, 100.9, 0.9],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    signals = TrendPullbackStrategy(
        opening_range_minutes=2,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        maximum_consumed_target_pct=0.4,
        target_atr_multiple=1.5,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        require_vwap_alignment=True,
    ).generate_signals(featured)

    assert signals.empty


def test_trend_pullback_strategy_enforces_internal_minimum_signal_score() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 100.15, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.7, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.1, 101.6, 101.8, 1300.0, 1, 100.8, 1.0, 101.4, 100.4, 1.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.6, 900.0, 2, 101.1, 1.0, 101.8, 100.7, 1.1],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.6, 101.9, 100.9, 101.4, 850.0, 3, 101.0, 1.0, 101.8, 100.8, 1.0],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.6, 102.4, 100.6, 102.2, 1800.0, 4, 101.4, 1.0, 101.8, 100.9, 0.9],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    permissive = TrendPullbackStrategy(
        opening_range_minutes=2,
        pullback_touch_buffer_atr=0.1,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        target_atr_multiple=4.1,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_vwap_alignment=True,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        minimum_signal_score=0.0,
    ).generate_signals(featured)
    strict = TrendPullbackStrategy(
        opening_range_minutes=2,
        pullback_touch_buffer_atr=0.1,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        target_atr_multiple=4.1,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_vwap_alignment=True,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        minimum_signal_score=95.0,
    ).generate_signals(featured)

    assert len(permissive) == 1
    assert strict.empty


def test_trend_pullback_strategy_respects_max_trades_per_side_per_day() -> None:
    featured = pd.DataFrame(
        [
            ["2026-06-01 09:15:00+05:30", "RELIANCE", 100.0, 100.5, 100.15, 100.2, 1000.0, 0, 100.0, 1.0, 100.0, 99.7, 0.4],
            ["2026-06-01 09:16:00+05:30", "RELIANCE", 100.2, 102.1, 101.6, 101.8, 1300.0, 1, 100.8, 1.0, 101.4, 100.4, 1.0],
            ["2026-06-01 09:17:00+05:30", "RELIANCE", 101.8, 102.0, 101.0, 101.6, 900.0, 2, 101.1, 1.0, 101.8, 100.7, 1.1],
            ["2026-06-01 09:18:00+05:30", "RELIANCE", 101.6, 101.9, 100.9, 101.4, 850.0, 3, 101.0, 1.0, 101.8, 100.8, 1.0],
            ["2026-06-01 09:19:00+05:30", "RELIANCE", 101.6, 102.4, 100.6, 102.2, 1800.0, 4, 101.4, 1.0, 101.8, 100.9, 0.9],
        ],
        columns=[
            "timestamp", "symbol", "open", "high", "low", "close", "volume", "session_minute",
            "vwap", "atr", "ema_fast", "ema_slow", "trend_gap_atr",
        ],
    )
    featured["timestamp"] = pd.to_datetime(featured["timestamp"])
    featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

    no_signal = TrendPullbackStrategy(
        opening_range_minutes=2,
        pullback_touch_buffer_atr=0.1,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        target_atr_multiple=4.1,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_vwap_alignment=False,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        max_trades_per_side_per_day=0,
    ).generate_signals(featured)
    one_signal = TrendPullbackStrategy(
        opening_range_minutes=2,
        pullback_touch_buffer_atr=0.1,
        minimum_pullback_depth_atr=0.15,
        minimum_pullback_bars=2,
        target_atr_multiple=4.1,
        minimum_trend_gap_atr=0.05,
        minimum_reclaim_body_pct=0.3,
        minimum_close_location_pct=0.6,
        minimum_rejection_wick_to_body_ratio=1.5,
        minimum_volume_expansion_ratio=1.2,
        volume_expansion_lookback=2,
        swing_lookback=2,
        require_vwap_alignment=False,
        require_break_of_prior_candle=True,
        allow_intrabar_rejection_entry=True,
        max_trades_per_side_per_day=1,
    ).generate_signals(featured)

    assert no_signal.empty
    assert len(one_signal) == 1
