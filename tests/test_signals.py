"""Tests for signals.py — specifically the candle-strength trade-taking
filter (config.signal_candle.min_close_position / scanner.is_strong_directional_candle).

This filter is deliberately NOT part of touch detection: a touch whose own
candle is weak (e.g. a green inverted hammer — closes above its open, but
near the bar's own low under a long upper wick) is simply skipped as a trade
opportunity. The scanner's touch_count and sequence state are completely
unaffected — the next qualifying touch, whenever it happens, is judged fresh
on its own candle. See scanner.py's is_strong_directional_candle and
tests/test_scanner.py's unit tests for the shape check itself; this file
covers the wiring into candidate generation end-to-end.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sma44_level1_intraday.config.schema import MarketConfig, StrategyConfig, TradeFilterConfig
from sma44_level1_intraday.scanner import scan_history
from sma44_level1_intraday.signals import (
    _atr_reference,
    _buffer,
    _daily_volume_reference,
    _higher_timeframe_trend_class_reference,
    _ma200_reference,
    _resolve_pct_tolerance,
    _round_to_whole_number_if_near,
    _stop_reference,
    CANDIDATE_COLUMNS,
    generate_scan_candidates,
    higher_timeframes,
    pending_setup_status,
    is_rejected_by_daily_volume_filter,
    is_rejected_by_ma200_filter,
    is_rejected_by_trend_class_filter,
)

from tests.test_scanner import _PullbackSeries, _cfg, _warmup_bars

TZ = "Asia/Kolkata"


def _strategy_config(scanner_cfg, min_close_position: float) -> StrategyConfig:
    return StrategyConfig.from_dict({
        "timeframes": {"timeframe": "1D"},
        "scanner": {
            "sma_length": scanner_cfg.sma_length,
            "touch_tolerance_mode": scanner_cfg.touch_tolerance_mode,
            "touch_tolerance_pct": scanner_cfg.touch_tolerance_pct,
            "ma_slope_lookback": scanner_cfg.ma_slope_lookback,
            "min_ma_slope_pct": scanner_cfg.min_ma_slope_pct,
            "move_away_mode": scanner_cfg.move_away_mode,
            "move_away_pct": scanner_cfg.move_away_pct,
            "minimum_touch_number": scanner_cfg.minimum_touch_number,
        },
        "signal_candle": {"min_close_position": min_close_position},
    })


def test_weak_touch_is_skipped_but_touch_count_still_advances_for_the_next_one():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))

    idx_touch1 = len(s.closes)
    s.touch().away(3.0, 5)                 # anchor (touch #1 — never a candidate anyway)

    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 5)                 # touch #2 — will be shaped WEAK below

    idx_touch3 = len(s.closes)
    s.touch().away(3.0, 3)                 # touch #3 — will be shaped STRONG below

    close2, close3 = s.closes[idx_touch2], s.closes[idx_touch3]
    df = s.to_frame(wick_indices={
        # Weak: close sits near the bar's LOW under a long upper wick (a
        # green inverted hammer) — close > open still holds (open is fixed
        # at close - 0.01 by the builder), but close_position is near 0.
        idx_touch2: (close2 - 0.01, close2 + 5.0),
        # Strong: close sits near the bar's HIGH — close_position near 1.
        idx_touch3: (close3 - 2.0, close3 + 0.01),
    })
    df["symbol"] = "TEST"

    # The SCANNER's own view: touch counting is entirely unaffected by candle
    # shape — both touch #2 and #3 qualify as touches in the sequence.
    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    qualifying = events[events["touch_count"] >= 2].sort_values("touch_count")
    assert list(qualifying["touch_count"]) == [2, 3]

    # The CANDIDATE-GENERATION view: only touch #3 (the strong one) becomes a
    # tradeable candidate. Touch #2 (weak) produced no trade at all.
    config = _strategy_config(cfg, min_close_position=0.6)
    candidates = generate_scan_candidates(df, config)
    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["direction"] == "LONG"
    # Crucially, touch_count on the surviving candidate is still 3 (not
    # renumbered to 2) — proves the skipped weak touch wasn't erased from the
    # sequence, just excluded from trading.
    assert row["touch_count"] == 3
    assert row["touch_number"] == 3


def test_strong_touch_alone_produces_a_candidate():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 — shape it strong explicitly

    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 2.0, close2 + 0.01)})
    df["symbol"] = "TEST"

    config = _strategy_config(cfg, min_close_position=0.6)
    candidates = generate_scan_candidates(df, config)
    assert len(candidates) == 1
    assert candidates.iloc[0]["touch_count"] == 2


def test_all_weak_touches_produce_no_candidates_at_all():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 — shape it weak

    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 0.01, close2 + 5.0)})
    df["symbol"] = "TEST"

    config = _strategy_config(cfg, min_close_position=0.6)
    candidates = generate_scan_candidates(df, config)
    assert candidates.empty
    # But the touch itself still shows up in the scanner's own qualifying list.
    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] == 2).any()


def test_short_direction_mirrors_the_same_weak_strong_filtering():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "SHORT").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)                 # anchor

    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)                 # touch #2 — weak: close near the HIGH (long lower wick)

    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 5.0, close2 + 0.01)})
    df["symbol"] = "TEST"

    config = _strategy_config(cfg, min_close_position=0.6)
    candidates = generate_scan_candidates(df, config)
    assert candidates.empty

    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] == 2).any()   # still counted as a touch, just not traded


def test_min_close_position_is_configurable():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)

    close2 = s.closes[idx_touch2]
    # A moderately-placed close: close_position around 0.5.
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 1.0, close2 + 1.0)})
    df["symbol"] = "TEST"

    strict = generate_scan_candidates(df, _strategy_config(cfg, min_close_position=0.9))
    assert strict.empty

    lenient = generate_scan_candidates(df, _strategy_config(cfg, min_close_position=0.3))
    assert len(lenient) == 1


# ── stop lookback + 44-SMA floor, and the two target modes ─────────────────
# The stop isn't just the touch candle's own low (LONG) / high (SHORT) — it's
# the WIDEST of: that candle and stop_loss.stop_lookback_bars immediately
# before it, and the 44-SMA's own value at the touch (the stop must never
# sit INSIDE the support/resistance line itself). The target is either a
# FIXED distance off the signal candle's own range ("signal_candle_range",
# default — untouched by how wide the stop ends up being) or DYNAMICALLY
# tracks the actual stop distance ("dynamic_stop_multiple") — see
# config.target.target_mode.

def test_stop_reference_uses_the_lowest_low_across_the_lookback_window():
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=5, freq="1D", tz=TZ),
        "low": [100.0, 95.0, 98.0, 97.0, 99.0],
        "high": [110.0, 108.0, 109.0, 107.0, 111.0],
    })
    ref = _stop_reference(bars, lookback_bars=2)
    assert ref["_stop_low"].iloc[3] == 95.0   # window = rows 1..3 -> min(95, 98, 97)
    assert ref["_stop_low"].iloc[4] == 97.0   # window = rows 2..4 -> min(98, 97, 99)
    assert ref["_stop_high"].iloc[4] == 111.0  # window = rows 2..4 -> max(109, 107, 111)


def test_stop_reference_zero_lookback_is_just_the_bar_itself():
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="1D", tz=TZ),
        "low": [100.0, 90.0, 95.0],
        "high": [110.0, 120.0, 105.0],
    })
    ref = _stop_reference(bars, lookback_bars=0)
    assert list(ref["_stop_low"]) == [100.0, 90.0, 95.0]
    assert list(ref["_stop_high"]) == [110.0, 120.0, 105.0]


def _stop_target_config(cfg, *, stop_lookback_bars=2, risk_reward=1.0, target_mode="signal_candle_range") -> StrategyConfig:
    return StrategyConfig.from_dict({
        "timeframes": {"timeframe": "1D"},
        "scanner": {
            "sma_length": cfg.sma_length,
            "touch_tolerance_mode": cfg.touch_tolerance_mode, "touch_tolerance_pct": cfg.touch_tolerance_pct,
            "ma_slope_lookback": cfg.ma_slope_lookback, "min_ma_slope_pct": cfg.min_ma_slope_pct,
            "move_away_mode": cfg.move_away_mode, "move_away_pct": cfg.move_away_pct,
            "minimum_touch_number": cfg.minimum_touch_number,
        },
        "signal_candle": {"min_close_position": 0.6},
        "stop_loss": {"stop_lookback_bars": stop_lookback_bars},
        "target": {"risk_reward": risk_reward, "target_mode": target_mode},
        # Keep the new 200-SMA filter out of these tests' way — they're
        # about the stop/target mechanics, not that filter (see its own
        # dedicated tests below). A short warmup means it's simply NaN
        # (never rejects) for every touch in these short synthetic series.
    })


def test_generate_scan_candidates_stop_widens_to_a_lower_low_in_the_lookback_window():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor

    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2

    close2 = s.closes[idx_touch2]
    touch_low = close2 - 0.5
    lower_bar_idx = idx_touch2 - 1   # 1 bar before the touch — inside the 2-bar lookback
    # Lower than the touch's own low, but only slightly — enough to prove the
    # lookback kicked in without dragging this earlier bar's OWN low into the
    # 0.5% touch zone itself (it's still ~5% from the SMA before this nudge;
    # a small nudge keeps it there, unlike a large one which would make this
    # bar register as a touch of its own instead).
    lower_low = touch_low - 0.3

    df = s.to_frame(wick_indices={
        idx_touch2: (touch_low, close2 + 0.01),   # strong candle: close near the high
        lower_bar_idx: (lower_low, s.closes[lower_bar_idx] + 0.05),
    })
    df["symbol"] = "TEST"

    candidates = generate_scan_candidates(df, _stop_target_config(cfg))
    assert len(candidates) == 1
    row = candidates.iloc[0]
    # The stop reaches all the way to the lower low, not just the touch candle's own.
    assert row["stop_loss"] == pytest.approx(lower_low)
    assert row["stop_loss"] < row["signal_low"]
    # Default target_mode ("signal_candle_range"): a FIXED distance off the
    # signal candle's own range — untouched by how far the stop widened.
    assert row["target"] == pytest.approx(row["trigger_price"] + (row["signal_high"] - row["signal_low"]))


def test_generate_scan_candidates_signal_candle_range_target_ignores_the_entry_buffer():
    # Regression for BAJAJ-AUTO 2026-01-30: with an entry buffer active,
    # trigger_price sits ABOVE the signal candle's own high — but the target
    # must stay anchored to signal_high (not trigger_price), so the buffer
    # that shifts where you get IN doesn't also drag the target further away.
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor (touch #1)
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 -- qualifies (minimum_touch_number=2)
    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 0.5, close2 + 0.01)})  # strong candle
    df["symbol"] = "TEST"

    config = StrategyConfig.from_dict({
        "timeframes": {"timeframe": "1D"},
        "scanner": {
            "sma_length": cfg.sma_length,
            "touch_tolerance_mode": cfg.touch_tolerance_mode, "touch_tolerance_pct": cfg.touch_tolerance_pct,
            "ma_slope_lookback": cfg.ma_slope_lookback, "min_ma_slope_pct": cfg.min_ma_slope_pct,
            "move_away_mode": cfg.move_away_mode, "move_away_pct": cfg.move_away_pct,
            "minimum_touch_number": cfg.minimum_touch_number,
        },
        "signal_candle": {"min_close_position": 0.6},
        # round_to_whole_number off -- this test checks an exact computed
        # trigger_price value, which whole-number rounding would disturb.
        "entry": {"entry_buffer_type": "points", "entry_buffer_points": 2.0, "round_to_whole_number": False},
        "target": {"risk_reward": 1.0, "target_mode": "signal_candle_range"},
    })
    candidates = generate_scan_candidates(df, config)
    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["trigger_price"] == pytest.approx(row["signal_high"] + 2.0)
    # NOT trigger_price + range (which the 2.0-point buffer would otherwise leak into) —
    # anchored to signal_high itself.
    assert row["target"] == pytest.approx(row["signal_high"] + (row["signal_high"] - row["signal_low"]))
    assert row["target"] != pytest.approx(row["trigger_price"] + (row["signal_high"] - row["signal_low"]))


def test_generate_scan_candidates_dynamic_target_mode_tracks_the_widened_stop():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor

    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2

    close2 = s.closes[idx_touch2]
    touch_low = close2 - 0.5
    lower_bar_idx = idx_touch2 - 1
    lower_low = touch_low - 0.3

    df = s.to_frame(wick_indices={
        idx_touch2: (touch_low, close2 + 0.01),
        lower_bar_idx: (lower_low, s.closes[lower_bar_idx] + 0.05),
    })
    df["symbol"] = "TEST"

    candidates = generate_scan_candidates(df, _stop_target_config(cfg, target_mode="dynamic_stop_multiple"))
    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["stop_loss"] == pytest.approx(lower_low)
    risk = row["trigger_price"] - row["stop_loss"]
    assert row["target"] == pytest.approx(row["trigger_price"] + risk)


def test_generate_scan_candidates_stop_widens_to_the_44sma_when_touch_low_sits_above_it():
    # The touch's own low (and even its 2-bar lookback) can sit ABOVE the
    # 44-SMA — the tolerance zone allows a touch that doesn't actually reach
    # the SMA. The stop must still never sit inside that support line, so it
    # widens out to the SMA's own value.
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 — sits just inside the 0.5% zone, above the SMA

    close2 = s.closes[idx_touch2]
    # touch()'s own offset puts close2 ~0.016 above the SMA (see its
    # docstring) — a wick smaller than that keeps the low above the SMA too,
    # while still landing well inside the touch-zone tolerance. The high is
    # kept tight so close_position stays >= the strong-candle threshold.
    touch_low = close2 - 0.01
    df = s.to_frame(wick_indices={idx_touch2: (touch_low, close2 + 0.005)})
    df["symbol"] = "TEST"

    candidates = generate_scan_candidates(df, _stop_target_config(cfg))
    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert touch_low > row["sma_44"]  # the low itself never reached the SMA
    assert row["stop_loss"] == pytest.approx(row["sma_44"])
    assert row["stop_loss"] < touch_low


def test_generate_scan_candidates_stop_stays_at_touch_low_when_nothing_lower_nearby():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 — no manual wick shaping on nearby bars

    close2 = s.closes[idx_touch2]
    touch_low = close2 - 0.5
    df = s.to_frame(wick_indices={idx_touch2: (touch_low, close2 + 0.01)})
    df["symbol"] = "TEST"

    candidates = generate_scan_candidates(df, _stop_target_config(cfg))
    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["stop_loss"] == pytest.approx(row["signal_low"])


# ── trend-class / daily-volume hard filters ──────────────────────────────────
# Neither affects touch_count/qualification -- both are trade-taking filters,
# skipping a touch as a trade opportunity only.

def test_trend_class_filter_rejects_b_class_when_excluded():
    cfg = TradeFilterConfig(exclude_trend_class_b=True)
    assert is_rejected_by_trend_class_filter("LONG", "B", 0.1, cfg) is True


def test_trend_class_filter_keeps_b_class_when_not_excluded():
    cfg = TradeFilterConfig(exclude_trend_class_b=False)
    assert is_rejected_by_trend_class_filter("LONG", "B", 0.1, cfg) is False


def test_trend_class_filter_never_rejects_a_class():
    cfg = TradeFilterConfig(exclude_trend_class_b=True, min_trend_class_c_slope_pct=-1.5)
    assert is_rejected_by_trend_class_filter("LONG", "A", -50.0, cfg) is False


def test_trend_class_filter_c_class_long_respects_the_slope_floor():
    cfg = TradeFilterConfig(min_trend_class_c_slope_pct=-1.5)
    assert is_rejected_by_trend_class_filter("LONG", "C", -1.0, cfg) is False   # within floor
    assert is_rejected_by_trend_class_filter("LONG", "C", -1.5, cfg) is False  # exactly at floor
    assert is_rejected_by_trend_class_filter("LONG", "C", -2.0, cfg) is True   # worse than floor


def test_trend_class_filter_c_class_short_uses_the_mirrored_sign():
    # SHORT's adj_slope_pct = -raw, so a POSITIVE raw slope is what's against
    # a SHORT trade -- same -1.5 floor, opposite raw sign.
    cfg = TradeFilterConfig(min_trend_class_c_slope_pct=-1.5)
    assert is_rejected_by_trend_class_filter("SHORT", "C", 1.0, cfg) is False   # adj -1.0, within floor
    assert is_rejected_by_trend_class_filter("SHORT", "C", 2.0, cfg) is True    # adj -2.0, worse than floor


def test_trend_class_filter_c_class_nan_slope_never_rejects():
    cfg = TradeFilterConfig(min_trend_class_c_slope_pct=-1.5)
    assert is_rejected_by_trend_class_filter("LONG", "C", float("nan"), cfg) is False


def test_daily_volume_filter_rejects_below_floor():
    assert is_rejected_by_daily_volume_filter(50_000, min_daily_volume=100_000) is True


def test_daily_volume_filter_accepts_at_or_above_floor():
    assert is_rejected_by_daily_volume_filter(100_000, min_daily_volume=100_000) is False
    assert is_rejected_by_daily_volume_filter(150_000, min_daily_volume=100_000) is False


def test_daily_volume_filter_nan_never_rejects():
    assert is_rejected_by_daily_volume_filter(float("nan"), min_daily_volume=100_000) is False


# ── 200-SMA confluence filter (rule 4) ───────────────────────────────────────
# Only active when the 200-SMA sits on the far side of the 44-SMA from price
# (below it for LONG); when it does, a touch whose own low (LONG) / high
# (SHORT) is within nearby_tolerance_pct of the 200-SMA is rejected outright
# — the side that actually tested the level (same convention as the 44-SMA's
# own touch_tolerance_pct), not the close. Not just marked lower quality,
# never emitted as a candidate at all. touch_count/sequence are untouched.

def test_is_rejected_by_ma200_filter_long_true_when_below_44sma_and_nearby():
    # low is what's checked for LONG -- close here is deliberately far, to
    # prove it's not what's being compared.
    assert is_rejected_by_ma200_filter("LONG", sma_44=100.0, ma200=97.0, low=97.2, high=110.0, nearby_tolerance_pct=5.0) is True


def test_is_rejected_by_ma200_filter_long_false_when_below_44sma_but_far():
    assert is_rejected_by_ma200_filter("LONG", sma_44=100.0, ma200=50.0, low=97.2, high=110.0, nearby_tolerance_pct=5.0) is False


def test_is_rejected_by_ma200_filter_long_false_when_200ma_is_on_the_wrong_side():
    # 200-SMA ABOVE the 44-SMA for a LONG -- not the "normal" configuration
    # this filter targets -- never applies, regardless of distance.
    assert is_rejected_by_ma200_filter("LONG", sma_44=100.0, ma200=100.5, low=100.4, high=110.0, nearby_tolerance_pct=5.0) is False


def test_is_rejected_by_ma200_filter_uses_low_for_long_not_close():
    # The touch's low is within tolerance of the 200-SMA, but its close sits
    # well clear of it -- must still reject (this is the actual bug this
    # rewrite fixes: close alone missed real confluences like this).
    assert is_rejected_by_ma200_filter("LONG", sma_44=110.0, ma200=100.0, low=100.5, high=112.0, nearby_tolerance_pct=5.0) is True


def test_is_rejected_by_ma200_filter_short_mirrors_long_using_high():
    assert is_rejected_by_ma200_filter("SHORT", sma_44=100.0, ma200=103.0, low=90.0, high=102.8, nearby_tolerance_pct=5.0) is True
    assert is_rejected_by_ma200_filter("SHORT", sma_44=100.0, ma200=150.0, low=90.0, high=102.8, nearby_tolerance_pct=5.0) is False
    assert is_rejected_by_ma200_filter("SHORT", sma_44=100.0, ma200=99.5, low=90.0, high=102.8, nearby_tolerance_pct=5.0) is False


def test_is_rejected_by_ma200_filter_never_rejects_during_warmup_or_zero_ref_price():
    assert is_rejected_by_ma200_filter("LONG", sma_44=100.0, ma200=float("nan"), low=97.2, high=110.0, nearby_tolerance_pct=5.0) is False
    assert is_rejected_by_ma200_filter("LONG", sma_44=float("nan"), ma200=97.0, low=97.2, high=110.0, nearby_tolerance_pct=5.0) is False
    assert is_rejected_by_ma200_filter("LONG", sma_44=100.0, ma200=97.0, low=0.0, high=110.0, nearby_tolerance_pct=5.0) is False


# ── _resolve_pct_tolerance ─────────────────────────────────────────────────

def test_resolve_pct_tolerance_atr_mode_scales_with_atr_and_price():
    pct = _resolve_pct_tolerance("atr", tolerance_pct=5.0, atr_multiplier=1.5, close=100.0, atr_value=2.0)
    assert pct == pytest.approx(3.0)   # 1.5 * 2.0 / 100.0 * 100


def test_resolve_pct_tolerance_atr_mode_nan_when_atr_unavailable():
    assert np.isnan(_resolve_pct_tolerance("atr", 5.0, 1.5, close=100.0, atr_value=float("nan")))
    assert np.isnan(_resolve_pct_tolerance("atr", 5.0, 1.5, close=0.0, atr_value=2.0))


def test_resolve_pct_tolerance_percent_mode_ignores_atr():
    pct = _resolve_pct_tolerance("percent", tolerance_pct=5.0, atr_multiplier=1.5, close=200.0, atr_value=10.0)
    assert pct == pytest.approx(5.0)


def test_ma200_reference_is_a_backward_looking_rolling_mean_with_nan_warmup():
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=4, freq="1D", tz=TZ),
        "close": [10.0, 20.0, 30.0, 40.0],
    })
    ref = _ma200_reference(bars, ma_length=3)
    assert pd.isna(ref["_ma200"].iloc[0])
    assert pd.isna(ref["_ma200"].iloc[1])
    assert ref["_ma200"].iloc[2] == pytest.approx(20.0)   # mean(10,20,30)
    assert ref["_ma200"].iloc[3] == pytest.approx(30.0)   # mean(20,30,40)


# ── _daily_volume_reference ──────────────────────────────────────────────────

def test_daily_volume_reference_daily_bars_is_each_rows_own_volume():
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="1D", tz=TZ),
        "volume": [1000.0, 2000.0, 3000.0],
    })
    ref = _daily_volume_reference(bars)
    assert list(ref["_daily_volume"]) == [1000.0, 2000.0, 3000.0]


def test_daily_volume_reference_intraday_bars_sums_within_each_calendar_date():
    # Three 5min-style bars on day 1, two on day 2 -- daily_volume should be
    # the SAME total for every bar sharing a calendar date, timeframe-
    # independent (see _daily_volume_reference's own docstring).
    bars = pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2024-01-01 09:15", "2024-01-01 09:20", "2024-01-01 09:25",
            "2024-01-02 09:15", "2024-01-02 09:20",
        ]).tz_localize(TZ),
        "volume": [100.0, 200.0, 300.0, 50.0, 75.0],
    })
    ref = _daily_volume_reference(bars)
    assert list(ref["_daily_volume"]) == [600.0, 600.0, 600.0, 125.0, 125.0]


# ── higher_timeframes ─────────────────────────────────────────────────────────

def test_higher_timeframes_matches_every_documented_example():
    assert higher_timeframes("1D") == ["1W", "1M"]
    assert higher_timeframes("1W") == ["1M"]
    assert higher_timeframes("60min") == ["1D", "1W", "1M"]
    assert higher_timeframes("30min") == ["60min", "1D", "1W", "1M"]
    assert higher_timeframes("15min") == ["30min", "60min", "1D", "1W", "1M"]
    assert higher_timeframes("5min") == ["15min", "30min", "60min", "1D", "1W", "1M"]
    assert higher_timeframes("1M") == []                # already the top


def test_higher_timeframes_recognizes_aliases():
    assert higher_timeframes("daily") == ["1W", "1M"]
    assert higher_timeframes("weekly") == ["1M"]


def test_higher_timeframes_unrecognized_timeframe_returns_empty():
    assert higher_timeframes("1min") == []    # not on the standard ladder
    assert higher_timeframes("75min") == []


# ── _higher_timeframe_trend_class_reference ──────────────────────────────────

def test_higher_timeframe_trend_class_reference_never_uses_a_still_forming_period():
    # 4 full business weeks, closes rising steadily throughout. Verified by
    # hand against this exact function's real output before writing these
    # assertions (see the session's own "verify before trusting" practice):
    # week 1 has no prior week to reference at all (NaN); weeks 2-3 reference
    # a prior bar but don't yet have 2 full prior weeks for a real slope
    # (defaults "B" -- see _classify_trend's own NaN-slope default); week 4
    # is the first with enough history for a real "A" classification. The
    # KEY property under test either way: every row WITHIN a given week
    # shows the IDENTICAL value -- if the reference ever leaked that week's
    # OWN still-forming data, later days within a week would differ from
    # earlier ones as more of the week's (different) closes accumulated.
    idx = pd.bdate_range("2024-01-01", periods=20, tz=TZ)   # 4 full Mon-Fri weeks
    close = np.arange(20, dtype=float) + 100.0
    bars = pd.DataFrame({
        "timestamp": idx, "symbol": "TEST",
        "open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000.0,
    })
    market = MarketConfig(continuous_session=False)
    ref = _higher_timeframe_trend_class_reference(
        bars, "1D", market, sma_length=2, ma_slope_lookback=1, min_ma_slope_pct=0.0,
    )
    weeks = [ref.iloc[i:i + 5] for i in range(0, 20, 5)]
    for week in weeks:
        assert week["_weekly_class_long"].nunique(dropna=False) == 1   # constant within the week

    assert weeks[0]["_weekly_class_long"].isna().all()    # week 1: no prior week at all
    assert (weeks[1]["_weekly_class_long"] == "B").all()  # week 2: prior week alone, no slope yet
    assert (weeks[2]["_weekly_class_long"] == "B").all()  # week 3: still not enough prior history
    assert (weeks[3]["_weekly_class_long"] == "A").all()  # week 4: real, rising classification


# ── _round_to_whole_number_if_near ───────────────────────────────────────────

def test_round_to_whole_number_rounds_up_for_long_within_tolerance():
    trigger = np.array([1189.31])
    is_long = np.array([True])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(1190.0)


def test_round_to_whole_number_rounds_down_for_short_within_tolerance():
    trigger = np.array([1189.85])
    is_long = np.array([False])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(1189.0)


def test_round_to_whole_number_leaves_price_unchanged_outside_tolerance():
    # A cheap stock: 0.1% of 45.30 is ~0.045 -- the 0.7-point gap to 46 is
    # far outside that, so it must NOT round.
    trigger = np.array([45.30])
    is_long = np.array([True])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(45.30)


def test_round_to_whole_number_rounds_a_cheap_stock_only_when_genuinely_close():
    trigger = np.array([45.97])   # 0.03 from 46.0 -> 0.03/45.97% = 0.065% < 0.1% tolerance
    is_long = np.array([True])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(46.0)


def test_round_to_whole_number_already_whole_stays_whole():
    trigger = np.array([1200.0])
    is_long = np.array([True])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(1200.0)


def test_round_to_whole_number_vectorized_across_mixed_directions():
    trigger = np.array([1189.31, 1189.85])
    is_long = np.array([True, False])
    rounded = _round_to_whole_number_if_near(trigger, is_long, tolerance_pct=0.1)
    assert rounded[0] == pytest.approx(1190.0)
    assert rounded[1] == pytest.approx(1189.0)


# ── entry/SL buffer (points / pct / atr) ─────────────────────────────────────

def test_buffer_points_mode_ignores_close_and_atr():
    close = pd.Series([50.0, 200.0])
    assert list(_buffer(close, "points", points=1.5, pct=0.0)) == [1.5, 1.5]


def test_buffer_pct_mode_scales_with_close():
    close = pd.Series([50.0, 200.0])
    assert list(_buffer(close, "pct", points=0.0, pct=2.0)) == pytest.approx([1.0, 4.0])


def test_buffer_atr_mode_scales_with_atr_not_close():
    close = pd.Series([50.0, 200.0])
    atr = pd.Series([4.0, 4.0])  # same ATR despite very different close -> same buffer
    assert list(_buffer(close, "atr", points=0.0, pct=0.0, atr=atr, atr_multiplier=0.25)) == pytest.approx([1.0, 1.0])


def test_buffer_atr_mode_falls_back_to_zero_during_atr_warmup():
    # NaN ATR (not enough warmup) -> zero buffer, never a blocker — same
    # fail-open convention as the 200-SMA filter.
    close = pd.Series([50.0, 50.0])
    atr = pd.Series([float("nan"), 4.0])
    assert list(_buffer(close, "atr", points=0.0, pct=0.0, atr=atr, atr_multiplier=0.25)) == [0.0, 1.0]


def test_atr_reference_is_a_backward_looking_rolling_mean_of_true_range():
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=4, freq="1D", tz=TZ),
        "high": [10.0, 12.0, 13.0, 11.0],
        "low": [8.0, 9.0, 10.0, 9.0],
        "close": [9.0, 11.0, 12.0, 10.0],
    })
    # True Range per bar: [2.0 (no prev close), 3.0, 3.0, 3.0]
    ref = _atr_reference(bars, atr_length=2)
    assert pd.isna(ref["_atr"].iloc[0])
    assert ref["_atr"].iloc[1] == pytest.approx(2.5)   # mean(2.0, 3.0)
    assert ref["_atr"].iloc[2] == pytest.approx(3.0)   # mean(3.0, 3.0)
    assert ref["_atr"].iloc[3] == pytest.approx(3.0)   # mean(3.0, 3.0)


def _ma200_config(cfg, *, ma_length=3, nearby_tolerance_pct=5.0) -> StrategyConfig:
    return StrategyConfig.from_dict({
        "timeframes": {"timeframe": "1D"},
        "scanner": {
            "sma_length": cfg.sma_length,
            "touch_tolerance_mode": cfg.touch_tolerance_mode, "touch_tolerance_pct": cfg.touch_tolerance_pct,
            "ma_slope_lookback": cfg.ma_slope_lookback, "min_ma_slope_pct": cfg.min_ma_slope_pct,
            "move_away_mode": cfg.move_away_mode, "move_away_pct": cfg.move_away_pct,
            "minimum_touch_number": cfg.minimum_touch_number,
        },
        "signal_candle": {"min_close_position": 0.6},
        # Pinned to "percent" -- this helper's own tests exercise exact
        # nearby_tolerance_pct values (including a near-zero one), which
        # "atr" mode (the schema's own default) would simply ignore.
        "ma200_filter": {
            "ma_length": ma_length, "nearby_tolerance_mode": "percent",
            "nearby_tolerance_pct": nearby_tolerance_pct,
        },
    })


def test_generate_scan_candidates_is_rejected_when_below_min_entry_price():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG", start=10.0).warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 -- shape it strong
    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 0.5, close2 + 0.01)})
    df["symbol"] = "TEST"
    # trigger_price lands at ~16.14 here (a low-priced series) -- isolate the
    # min_entry_price check by neutralising the OTHER new trade filters
    # (trend_class), same reasoning as _strategy_config's own notes.
    base_overrides = {
        "timeframes": {"timeframe": "1D"},
        "scanner": {
            "sma_length": cfg.sma_length, "touch_tolerance_mode": cfg.touch_tolerance_mode,
            "touch_tolerance_pct": cfg.touch_tolerance_pct, "ma_slope_lookback": cfg.ma_slope_lookback,
            "min_ma_slope_pct": cfg.min_ma_slope_pct, "move_away_mode": cfg.move_away_mode,
            "move_away_pct": cfg.move_away_pct, "minimum_touch_number": cfg.minimum_touch_number,
        },
        "signal_candle": {"min_close_position": 0.6},
        "trade_filters": {"exclude_trend_class_b": False, "min_trend_class_c_slope_pct": -100.0},
    }

    rejected = generate_scan_candidates(df, StrategyConfig.from_dict({
        **base_overrides, "trade_filters": {**base_overrides["trade_filters"], "min_entry_price": 20.0},
    }))
    assert rejected.empty

    accepted = generate_scan_candidates(df, StrategyConfig.from_dict({
        **base_overrides, "trade_filters": {**base_overrides["trade_filters"], "min_entry_price": 5.0},
    }))
    assert len(accepted) == 1
    assert accepted.iloc[0]["trigger_price"] >= 5.0

    # touch_count itself is identical either way -- this is a trade-taking
    # filter, not touch detection.
    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] == 2).any()


def test_generate_scan_candidates_is_rejected_when_ma200_confluence_is_within_tolerance():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)   # touch #2 — shape it strong

    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 0.5, close2 + 0.01)})
    df["symbol"] = "TEST"

    # A ma_length=6 window in this synthetic series sits ~1.2% below the
    # 44-SMA at the touch -- below it (the condition this filter requires)
    # and within a generous 5% tolerance.
    rejected = generate_scan_candidates(df, _ma200_config(cfg, ma_length=6, nearby_tolerance_pct=5.0))
    assert rejected.empty

    # The identical touch, but with a tolerance too tight for anything to
    # count as "nearby" -> the filter never applies, candidate comes through.
    accepted = generate_scan_candidates(df, _ma200_config(cfg, ma_length=6, nearby_tolerance_pct=0.0001))
    assert len(accepted) == 1

    # touch_count itself is identical either way -- this is a trade-taking
    # filter, not touch detection.
    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] == 2).any()


# ── pending_setup_status (live-check actionability) ──────────────────────────

def _sig(direction="LONG", trigger=101.0, signal_low=99.0, signal_high=101.0):
    return {"direction": direction, "trigger_price": trigger, "signal_low": signal_low, "signal_high": signal_high}


def _bars_after(rows):
    return pd.DataFrame(rows, columns=["open", "high", "low", "close"])


def test_pending_setup_status_actionable_when_no_bars_yet():
    assert pending_setup_status(_sig(), _bars_after([]), setup_expiry_bars=1) == "actionable"


def test_pending_setup_status_expired_after_waiting_without_trigger():
    # 100.5 high never reaches trigger 101, low 99.5 never breaks signal_low 99
    assert pending_setup_status(_sig(), _bars_after([(100, 100.5, 99.5, 100.2)]), setup_expiry_bars=1) == "expired"


def test_pending_setup_status_still_actionable_within_expiry_window():
    assert pending_setup_status(_sig(), _bars_after([(100, 100.5, 99.5, 100.2)]), setup_expiry_bars=3) == "actionable"


def test_pending_setup_status_triggered():
    # bearish bar (high visited first) that trades through 101 -> triggered
    assert pending_setup_status(_sig(), _bars_after([(101.5, 102, 100.5, 100.8)]), setup_expiry_bars=3) == "triggered"


def test_pending_setup_status_invalidated_by_breaking_signal_low():
    assert pending_setup_status(_sig(), _bars_after([(100, 100.4, 98.5, 98.9)]), setup_expiry_bars=3) == "invalidated"


# ── multiple signal MAs ───────────────────────────────────────────────────────

def _multi_cfg(lengths=None) -> StrategyConfig:
    scanner = {"sma_length": 44}
    if lengths is not None:
        scanner["signal_ma_lengths"] = lengths
    return StrategyConfig.from_dict({"scanner": scanner})


def _fake_candidates(rows):
    df = pd.DataFrame(rows, columns=["timestamp", "direction", "signal_ma", "ma_length"])
    for col in CANDIDATE_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
    return df


def test_signal_ma_lengths_default_is_just_sma_length():
    assert _multi_cfg().scanner.ma_lengths() == [44]
    assert _multi_cfg([50, 30, 44, 30]).scanner.ma_lengths() == [30, 44, 50]


def test_multi_ma_checks_every_other_ma_plus_the_200_filter_length(monkeypatch):
    import sma44_level1_intraday.signals as sig
    seen = {}

    def fake(bars, config, reference):
        seen[config.scanner.sma_length] = reference
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    monkeypatch.setattr(sig, "_generate_candidates_for_ma", fake)
    bars = pd.DataFrame({"timestamp": [pd.Timestamp("2026-01-01")], "symbol": ["T"]})
    cfg = _multi_cfg([30, 44, 50, 200])
    generate_scan_candidates(bars, cfg)
    assert seen == {30: [44, 50, 200], 44: [30, 50, 200], 50: [30, 44, 200], 200: [30, 44, 50]}

    seen.clear()
    generate_scan_candidates(bars, _multi_cfg())   # default: single MA checked against the 200 only
    assert seen == {44: [200]}


def test_multi_ma_keeps_only_the_farthest_ma_per_bar_and_direction(monkeypatch):
    import sma44_level1_intraday.signals as sig
    t = pd.Timestamp("2026-01-05")
    per_ma = {
        30: [(t, "LONG", 105.0, 30), (t, "SHORT", 95.0, 30)],
        44: [(t, "LONG", 103.0, 44), (t, "SHORT", 97.0, 44)],
        50: [(t, "LONG", 101.0, 50), (t, "SHORT", 99.0, 50)],
    }
    monkeypatch.setattr(
        sig, "_generate_candidates_for_ma",
        lambda bars, config, reference: _fake_candidates(per_ma[config.scanner.sma_length]),
    )
    bars = pd.DataFrame({"timestamp": [t], "symbol": ["T"], "close": [100.0]})
    out = generate_scan_candidates(bars, _multi_cfg([30, 44, 50]))
    assert len(out) == 2
    long_row = out[out["direction"] == "LONG"].iloc[0]
    short_row = out[out["direction"] == "SHORT"].iloc[0]
    assert long_row["ma_length"] == 50      # lowest MA = farthest below price
    assert short_row["ma_length"] == 50     # highest MA value (99) = farthest above price


def test_ma_alignment_letters_count_mas_not_agreeing_with_the_trade():
    from sma44_level1_intraday.signals import ma_alignment_letters
    out = ma_alignment_letters(
        ["LONG", "LONG", "LONG", "SHORT", "SHORT"],
        rising=[4, 3, 0, 0, 4], falling=[0, 1, 4, 4, 0], total=[4, 4, 4, 4, 4],
    )
    assert list(out) == ["A", "B", "E", "A", "E"]


def test_ma_alignment_reference_counts_rising_and_falling_mas():
    from sma44_level1_intraday.signals import _ma_alignment_reference
    n = 60
    bars = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=n, freq="D"),
        "close": np.linspace(100, 160, n),   # steady uptrend: every MA rising
    })
    ref = _ma_alignment_reference(bars, _multi_cfg([5, 10, 20]).scanner)
    last = ref.iloc[-1]
    assert (last["_ma_rising"], last["_ma_falling"], last["_ma_total"]) == (3, 0, 3)
    # early bars: MAs still warming up count as neither
    assert ref.iloc[0]["_ma_rising"] == 0 and ref.iloc[0]["_ma_falling"] == 0


def test_merged_ma_gap_waives_a_close_reference_ma_and_records_it():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    idx_touch2 = len(s.closes)
    s.touch().away(3.0, 3)
    close2 = s.closes[idx_touch2]
    df = s.to_frame(wick_indices={idx_touch2: (close2 - 0.5, close2 + 0.01)})
    df["symbol"] = "TEST"

    def config(gap):
        c = _ma200_config(cfg, ma_length=6, nearby_tolerance_pct=5.0)
        c.ma200_filter.merged_ma_gap_atr_multiplier = gap
        return c

    assert generate_scan_candidates(df, config(0.0)).empty          # off: rejected as before
    waived = generate_scan_candidates(df, config(1000.0))            # huge gap allowance: waived
    assert len(waived) == 1
    row = waived.iloc[0]
    assert bool(row["confluence_gap_skipped"]) is True
    assert row["confluence_gap_ma"] == "6"
    assert row["confluence_gap_atr"] >= 0


def test_merged_ma_gap_rejects_negative_values():
    with pytest.raises(ValueError):
        StrategyConfig.from_dict({"ma200_filter": {"merged_ma_gap_atr_multiplier": -0.1}})
