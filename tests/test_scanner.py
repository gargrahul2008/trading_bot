"""Tests for the new 44-SMA uptrend/downtrend-pullback scanner (scanner.py).

This is the module that replaced the old Level 1/2/3 "44 MA filter" entirely
— see scanner.py's module docstring for the state machine it implements.

Series are built with `_PullbackSeries`, a small deterministic bar-builder:
rather than hand-picking OHLC values and hoping they land in the touch zone,
it solves for the exact close that makes the rolling SMA equal that bar's own
close (a fixed point of the rolling-mean formula: close_i = S / (length - 1),
where S is the sum of the preceding length-1 closes) and offsets it by a tiny
amount — so every "touch" bar is a real, arithmetically exact touch of
whatever `sma_length`/`ma_slope_lookback` the test configures, no matter the
timeframe or how many bars separate the touches.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sma44_level1_intraday.config.schema import ScannerConfig
from sma44_level1_intraday.scanner import (
    calculate_indicators,
    close_position,
    detect_bullish_reclaim,
    has_moved_away,
    is_in_touch_zone,
    is_sma_rising,
    is_strong_directional_candle,
    process_touch_state,
    scan_history,
    scan_symbol,
)

TZ = "Asia/Kolkata"


class _PullbackSeries:
    """Builds a deterministic OHLC series for one direction ("LONG" = rising
    SMA / pullback from above; "SHORT" = falling SMA / rally from below)."""

    def __init__(self, sma_length: int, direction: str = "LONG", warmup_step: float = 1.0, start: float = 100.0):
        self.length = sma_length
        self.sign = 1.0 if direction == "LONG" else -1.0
        self.closes: list[float] = [start + i * warmup_step * self.sign for i in range(sma_length - 1)]

    def warmup(self, n: int, step: float = 1.0) -> "_PullbackSeries":
        last = self.closes[-1] if self.closes else 100.0
        for i in range(1, n + 1):
            self.closes.append(last + i * step * self.sign)
        return self

    def touch(self, offset: float = 0.02) -> "_PullbackSeries":
        """Append a bar whose close is an exact fixed point of the rolling SMA
        (== the SMA itself), nudged `offset` in the trend's favour."""
        window_sum = sum(self.closes[-(self.length - 1):])
        fixed_point = window_sum / (self.length - 1)
        self.closes.append(fixed_point + offset * self.sign)
        return self

    def away(self, pct: float, n: int) -> "_PullbackSeries":
        """Move price away from the SMA by compounding `pct`% per bar, `n` bars."""
        last = self.closes[-1]
        factor = 1 + (pct / 100.0) * self.sign
        for _ in range(n):
            last *= factor
            self.closes.append(last)
        return self

    def away_hold(self, pct_above_sma: float, n: int) -> "_PullbackSeries":
        """Append `n` bars each freshly recomputed to sit `pct_above_sma`% away
        from the (updating) SMA — unlike compounding `.away()`, this holds a
        constant distance indefinitely instead of asymptoting back toward the
        SMA's own tracking rate, so it can span an arbitrary number of bars."""
        for _ in range(n):
            window_sum = sum(self.closes[-(self.length - 1):])
            fixed_point = window_sum / (self.length - 1)
            self.closes.append(fixed_point * (1 + (pct_above_sma / 100.0) * self.sign))
        return self

    def stay_near_last_touch(self, n: int, offset: float = 0.02) -> "_PullbackSeries":
        """Append `n` more bars that each re-touch the (updating) SMA — price
        never actually leaves the zone, so this is ONE touch cluster, not n."""
        for _ in range(n):
            self.touch(offset=offset)
        return self

    def breach(self, mult: float = 0.5) -> "_PullbackSeries":
        """A violent bar that closes far through the SMA (invalidates the sequence)."""
        last = self.closes[-1]
        target = last * (mult if self.sign > 0 else (1 / mult))
        self.closes.append(target)
        return self

    def to_frame(self, wick: float = 0.05, wick_indices: dict[int, tuple[float, float]] | None = None) -> pd.DataFrame:
        n = len(self.closes)
        idx = pd.date_range("2024-01-01", periods=n, freq="1D", tz=TZ)
        close = np.array(self.closes, dtype=float)
        low = close - wick
        high = close + wick
        open_ = close - 0.01 * self.sign
        if wick_indices:
            for i, (lo, hi) in wick_indices.items():
                low[i] = lo
                high[i] = hi
        volume = np.full(n, 100_000.0)
        return pd.DataFrame({"timestamp": idx, "open": open_, "high": high, "low": low, "close": close, "volume": volume})


def _cfg(**over) -> ScannerConfig:
    # touch_tolerance_mode/move_away_mode pinned to "percent" here -- these
    # tests' hand-computed distances/wick shaping are all percent-based math;
    # the schema's own default is "atr" (see ScannerConfig docstring), but
    # that's a production default, not what these specific tests verify.
    base = dict(touch_tolerance_mode="percent", touch_tolerance_pct=0.5,
                move_away_mode="percent", move_away_pct=2.0,
                ma_slope_lookback=3, min_ma_slope_pct=0.0,
                minimum_touch_number=2, sma_length=5)
    base.update(over)
    return ScannerConfig(**base)


def _warmup_bars(cfg: ScannerConfig) -> int:
    """Bars needed before the first touch so BOTH the SMA and its slope are
    already valid at that bar (see the module docstring's fixed-point note)."""
    return cfg.sma_length - 1 + cfg.ma_slope_lookback


# ── ScannerConfig validation ─────────────────────────────────────────────────

def test_scanner_config_defaults_match_spec():
    cfg = ScannerConfig()
    assert cfg.sma_length == 44
    assert cfg.touch_tolerance_mode == "atr"
    assert cfg.touch_tolerance_pct == 0.5
    assert cfg.touch_tolerance_atr_multiplier == 0.2
    assert cfg.ma_slope_lookback == 5
    assert cfg.min_ma_slope_pct == 0.0
    assert cfg.move_away_mode == "atr"
    assert cfg.move_away_pct == 2.0
    assert cfg.atr_length == 14
    assert cfg.move_away_atr_multiplier == 0.6
    assert cfg.minimum_touch_number == 2
    assert cfg.sequence_reset_mode == "atr"
    assert cfg.sequence_reset_pct == 2.0
    assert cfg.sequence_reset_atr_multiplier == 0.6
    assert cfg.sequence_reset_consecutive_bars == 2
    assert cfg.emit_once_per_touch_event is True
    assert cfg.use_only_completed_candles is True


@pytest.mark.parametrize("bad_kwargs", [
    dict(sma_length=1),
    dict(touch_tolerance_mode="bogus"),
    dict(touch_tolerance_pct=-1),
    dict(touch_tolerance_atr_multiplier=-1),
    dict(ma_slope_lookback=0),
    dict(move_away_mode="bogus"),
    dict(move_away_pct=-1),
    dict(atr_length=0),
    dict(move_away_atr_multiplier=-1),
    dict(minimum_touch_number=0),
    dict(sequence_reset_mode="bogus"),
    dict(sequence_reset_pct=-1),
    dict(sequence_reset_atr_multiplier=-1),
    dict(sequence_reset_consecutive_bars=0),
    dict(setup_expiry_bars=-1),
    dict(allow_long=False, allow_short=False),
])
def test_scanner_config_rejects_invalid_settings(bad_kwargs):
    with pytest.raises(ValueError):
        ScannerConfig(**bad_kwargs)


# ── indicator / building-block unit tests ────────────────────────────────────

def test_calculate_indicators_is_a_plain_backward_looking_sma_and_does_not_mutate_input():
    df = pd.DataFrame({"close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
    original = df.copy()
    out = calculate_indicators(df, sma_length=3)
    assert list(df.columns) == list(original.columns)          # input untouched
    assert "sma_44" not in df.columns
    assert np.isnan(out["sma_44"].iloc[0]) and np.isnan(out["sma_44"].iloc[1])
    assert out["sma_44"].iloc[2] == pytest.approx((1 + 2 + 3) / 3)
    assert out["sma_44"].iloc[5] == pytest.approx((4 + 5 + 6) / 3)


def test_is_sma_rising_and_is_in_touch_zone_building_blocks():
    assert is_sma_rising(0.5, min_ma_slope_pct=0.0, monotonic=True) is True
    assert is_sma_rising(0.0, min_ma_slope_pct=0.0, monotonic=True) is False
    assert is_sma_rising(float("nan"), min_ma_slope_pct=0.0, monotonic=True) is False
    # A comfortably positive NET slope over the window is no longer enough on
    # its own — every bar in the window must individually have moved the SMA
    # in the trend's favour (see process_touch_state's is_monotonic).
    assert is_sma_rising(0.5, min_ma_slope_pct=0.0, monotonic=False) is False
    assert is_in_touch_zone(0.3, touch_tolerance_pct=0.5) is True    # short of the SMA, within tolerance
    assert is_in_touch_zone(0.6, touch_tolerance_pct=0.5) is False   # short of the SMA, beyond tolerance
    assert is_in_touch_zone(-1.0, touch_tolerance_pct=0.5) is True   # pierced through -> always in zone


def test_trend_class_labels_a_to_b_to_c_without_ever_resetting_the_sequence():
    """Rule 5 (revised): the SMA's own slope no longer gates anything — it
    only labels each bar "A" (cleanly rising: net slope positive AND every
    one of the last ma_slope_lookback bars individually rose), "C" (cleanly
    falling: net slope clearly negative, the mirror threshold), or "B"
    (anything in between — flattish/wobbling). A single continuous rising
    -> flat -> falling series proves the label tracks the SMA's regime
    while touch_count/active state stay completely untouched by it."""
    rising = [100.0 + i for i in range(20)]
    flat = [rising[-1]] * 8
    falling = [flat[-1] - i for i in range(1, 15)]
    closes = rising + flat + falling
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    opens = [c - 0.1 for c in closes]
    idx = pd.date_range("2024-01-01", periods=len(closes), freq="1D", tz=TZ)
    df = pd.DataFrame({"timestamp": idx, "open": opens, "high": highs, "low": lows, "close": closes})

    cfg = ScannerConfig(sma_length=5, ma_slope_lookback=3, min_ma_slope_pct=0.0, atr_length=3, minimum_touch_number=1)
    ann, _events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")

    assert list(ann["trend_class"].iloc[15:23]) == ["A"] * 8
    assert list(ann["trend_class"].iloc[24:28]) == ["B"] * 4
    assert list(ann["trend_class"].iloc[29:35]) == ["C"] * 6


def test_touch_tolerance_atr_mode_recognizes_a_touch_percent_mode_misses():
    """A wide-but-steady gap from the SMA (here: always 1 price unit, ~0.9-1%
    of price) sits outside a tight 0.5% percent tolerance but inside 1.0x a
    real (non-trivial) ATR — exactly the daily-vs-5-min mismatch that
    motivated touch_tolerance_mode='atr' (see ScannerConfig's docstring:
    0.6% is ~0.15-0.2x a typical DAILY bar's ATR but ~2.5-3.5x a typical
    5-min bar's ATR)."""
    n = 20
    closes = [100.0 + i for i in range(n)]
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]   # low always sits SMA+1 -- short of it by a
                                      # steady 1 price unit (~0.9-1%): outside a
                                      # 0.5% percent tolerance, inside 1.0x ATR(=2.0)
    opens = [c - 0.5 for c in closes]
    idx = pd.date_range("2024-01-01", periods=n, freq="1D", tz=TZ)
    df = pd.DataFrame({"timestamp": idx, "open": opens, "high": highs, "low": lows, "close": closes})

    cfg_pct = ScannerConfig(sma_length=5, ma_slope_lookback=3, min_ma_slope_pct=0.0, atr_length=3,
                             touch_tolerance_mode="percent", touch_tolerance_pct=0.5, minimum_touch_number=2)
    cfg_atr = ScannerConfig(sma_length=5, ma_slope_lookback=3, min_ma_slope_pct=0.0, atr_length=3,
                             touch_tolerance_mode="atr", touch_tolerance_atr_multiplier=1.0, minimum_touch_number=2)

    ind = calculate_indicators(df, 5)
    ann_pct, _ = process_touch_state(ind, cfg_pct, "LONG")
    ann_atr, _ = process_touch_state(ind, cfg_atr, "LONG")

    # Percent mode: this steady 1-unit gap never qualifies as a touch anywhere
    # in the series -- no sequence ever anchors.
    assert (ann_pct["touch_count"] == 0).all()
    # ATR mode: once ATR itself warms up, the SAME gap DOES qualify -- an
    # anchor forms (touch_count becomes 1) purely because it's within 1.0x ATR.
    assert (ann_atr["touch_count"] > 0).any()


def test_has_moved_away_percent_and_atr_modes():
    pct_cfg = _cfg(move_away_mode="percent", move_away_pct=2.0)
    assert has_moved_away(2.5, 0.0, None, pct_cfg) is True
    assert has_moved_away(1.0, 0.0, None, pct_cfg) is False
    atr_cfg = _cfg(move_away_mode="atr", move_away_atr_multiplier=1.5)
    assert has_moved_away(0.0, 3.0, atr_value=2.0, config=atr_cfg) is True     # 3.0 >= 1.5*2.0
    assert has_moved_away(0.0, 1.0, atr_value=2.0, config=atr_cfg) is False    # 1.0 < 3.0
    assert has_moved_away(0.0, 3.0, atr_value=None, config=atr_cfg) is False   # no ATR yet -> never armed


def test_detect_bullish_reclaim():
    assert detect_bullish_reclaim(prev_close=99.0, prev_sma=100.0, close=101.0, sma=100.0) is True
    assert detect_bullish_reclaim(prev_close=101.0, prev_sma=100.0, close=101.0, sma=100.0) is False  # already above
    assert detect_bullish_reclaim(prev_close=99.0, prev_sma=100.0, close=99.5, sma=100.0) is False    # still below


# ── single-bar candle strength (trade-taking filter, not touch detection) ───

def test_close_position_at_high_low_and_midpoint():
    assert close_position(open_=99, high=110, low=100, close=110) == pytest.approx(1.0)
    assert close_position(open_=99, high=110, low=100, close=100) == pytest.approx(0.0)
    assert close_position(open_=99, high=110, low=100, close=105) == pytest.approx(0.5)


def test_close_position_zero_range_is_nan():
    assert np.isnan(close_position(open_=100, high=100, low=100, close=100))


def test_is_strong_directional_candle_strong_bullish_passes():
    # close > open AND close sits at 90% of the range -> comfortably strong.
    assert is_strong_directional_candle("LONG", open_=100, high=110, low=99, close=109.9, min_close_position=0.6) is True


def test_is_strong_directional_candle_green_inverted_hammer_fails():
    # The exact case that motivated this rule: close > open (technically
    # "green"), but the close sits near the bar's LOW under a long upper wick
    # showing the rally got rejected — must NOT count as a strong bullish bar.
    assert is_strong_directional_candle("LONG", open_=100.0, high=110, low=99.9, close=100.1, min_close_position=0.6) is False


def test_is_strong_directional_candle_rejects_a_red_candle_for_long():
    # close < open outright -> never strong-bullish regardless of close_position.
    assert is_strong_directional_candle("LONG", open_=105, high=110, low=99, close=101, min_close_position=0.6) is False


def test_is_strong_directional_candle_strong_bearish_passes():
    assert is_strong_directional_candle("SHORT", open_=100, high=101, low=90, close=90.1, min_close_position=0.6) is True


def test_is_strong_directional_candle_red_with_long_lower_wick_fails():
    # Mirror of the inverted-hammer case: close < open, but close sits near
    # the bar's HIGH under a long lower wick (rejection of the lows) — must
    # NOT count as a strong bearish bar.
    assert is_strong_directional_candle("SHORT", open_=100.0, high=100.1, low=90, close=99.9, min_close_position=0.6) is False


def test_is_strong_directional_candle_zero_range_is_never_strong():
    assert is_strong_directional_candle("LONG", open_=100, high=100, low=100, close=100, min_close_position=0.6) is False


# ── hammer exception ─────────────────────────────────────────────────────────

def test_is_strong_directional_candle_hammer_exception_off_by_default():
    # TBOTEK 2026-09-16: closed red on a LONG touch (1631.4 < 1643.7) with a
    # long lower wick — a hammer, but the exception is opt-in (default False
    # on the bare function call, matching every pre-existing call site).
    assert is_strong_directional_candle(
        "LONG", open_=1643.7, high=1647.7, low=1600.0, close=1631.4, min_close_position=0.6,
    ) is False


def test_is_strong_directional_candle_hammer_exception_rejects_tbotek_under_the_default_body_limit():
    # Same TBOTEK bar: wick (8%) and close_position (0.658) both still clear
    # their own thresholds, but its 12.3-pt body is 26% of the 47.7-pt range
    # -- over the default hammer_max_body_ratio (0.20) -- not a "proper"
    # small-bodied hammer, just a red candle with a decent-but-not-tiny body.
    assert is_strong_directional_candle(
        "LONG", open_=1643.7, high=1647.7, low=1600.0, close=1631.4, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15, hammer_max_body_ratio=0.20,
    ) is False
    # A looser body budget (big enough for TBOTEK's 26%) rescues it again --
    # confirms hammer_max_body_ratio is actually driving the outcome, not
    # some other check.
    assert is_strong_directional_candle(
        "LONG", open_=1643.7, high=1647.7, low=1600.0, close=1631.4, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15, hammer_max_body_ratio=0.30,
    ) is True


def test_is_strong_directional_candle_hammer_exception_rescues_a_genuinely_small_bodied_hammer():
    # A "proper" hammer: tiny body (0.5 of a 12-pt range, ~4%), small upper
    # wick (8%), strong close_position (0.875) -- passes every threshold,
    # including the body-size one TBOTEK above no longer clears.
    assert is_strong_directional_candle(
        "LONG", open_=101.0, high=102.0, low=90.0, close=100.5, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15, hammer_max_body_ratio=0.20,
    ) is True


def test_is_strong_directional_candle_hammer_exception_rejects_a_fat_body_even_with_good_wick_and_close():
    # Isolates the body check: wick (4%) and close_position (0.71) both
    # comfortably clear their own thresholds, but the body alone (25% of
    # range) exceeds hammer_max_body_ratio (0.20) -- not a small-bodied
    # candle, so the hammer exception must not rescue it.
    assert is_strong_directional_candle(
        "LONG", open_=100.0, high=101.0, low=76.0, close=93.75, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15, hammer_max_body_ratio=0.20,
    ) is False


def test_is_strong_directional_candle_hammer_exception_rescues_a_short_shooting_star():
    # Mirror case: closed GREEN on a SHORT touch, but a long UPPER wick and
    # almost no lower wick (2.0 pts, 11% of range) is a shooting star.
    assert is_strong_directional_candle(
        "SHORT", open_=100.0, high=115.9, low=98.0, close=101.9, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15,
    ) is True


def test_is_strong_directional_candle_hammer_exception_rejects_a_wide_opposite_wick():
    # close (107) <= open (108) -> wrong side, close_position 0.7 clears
    # min_close_position -> but the 2.0-pt upper wick is 20% of the 10-pt
    # range, over the 15% max_opposite_wick_ratio budget -> not a clean
    # enough hammer, exception should not rescue it.
    assert is_strong_directional_candle(
        "LONG", open_=108.0, high=110.0, low=100.0, close=107.0, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15,
    ) is False


def test_is_strong_directional_candle_hammer_exception_rejects_weak_close_position():
    # Upper wick (10%) clears hammer_max_opposite_wick_ratio, but
    # close_position (0.55) falls short of min_close_position (0.6) -> the
    # close itself still needs to land near the top, a tight opposite wick
    # alone isn't enough.
    assert is_strong_directional_candle(
        "LONG", open_=99.0, high=100.0, low=90.0, close=95.5, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15,
    ) is False


def test_is_strong_directional_candle_hammer_exception_does_not_apply_to_a_weak_green_candle():
    # The ORIGINAL green-inverted-hammer case (close > open, but close sits
    # near the LOW) must stay excluded even with the exception turned on —
    # it's on the RIGHT side of its own open, so _is_hammer_exception must
    # not treat it as eligible just because the plain check failed.
    assert is_strong_directional_candle(
        "LONG", open_=100.0, high=110.0, low=99.9, close=100.1, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_opposite_wick_ratio=0.15,
    ) is False


# ── skip_close_position_check_for_same_color_candle ─────────────────────────

def test_skip_close_position_check_rescues_a_weak_green_candle_for_long():
    # The exact green-inverted-hammer fixture above, which the DEFAULT rule
    # rejects -- with the flag on, ANY green candle is strong for a LONG,
    # regardless of where it closed in its own range.
    assert is_strong_directional_candle(
        "LONG", open_=100.0, high=110, low=99.9, close=100.1, min_close_position=0.6,
        skip_close_position_check_for_same_color_candle=True,
    ) is True


def test_skip_close_position_check_rescues_a_weak_red_candle_for_short():
    assert is_strong_directional_candle(
        "SHORT", open_=100.0, high=100.1, low=90, close=99.9, min_close_position=0.6,
        skip_close_position_check_for_same_color_candle=True,
    ) is True


def test_skip_close_position_check_still_rejects_the_opposite_color_candle():
    # The flag only ever relaxes the SAME-colour case -- a red candle on a
    # LONG touch is still never "strong" just because the flag is on.
    assert is_strong_directional_candle(
        "LONG", open_=105, high=110, low=99, close=101, min_close_position=0.6,
        skip_close_position_check_for_same_color_candle=True,
    ) is False


def test_skip_close_position_check_does_not_relax_the_hammer_exception_path():
    # The hammer exception (opposite-colour candle) keeps its OWN
    # min_close_position / wick-ratio checks even with the flag on -- it only
    # ever relaxes the plain SAME-colour check, never the hammer path.
    # A fat-bodied red candle with a weak close position on a LONG touch:
    # fails the hammer exception's own body-ratio check regardless of the flag.
    assert is_strong_directional_candle(
        "LONG", open_=100.0, high=110.0, low=90.0, close=95.0, min_close_position=0.6,
        allow_hammer_exception=True, hammer_max_body_ratio=0.30,
        skip_close_position_check_for_same_color_candle=True,
    ) is False


# ── Valid Scenario 1: second touch ───────────────────────────────────────────

def test_valid_scenario_second_touch_is_recognized():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch()            # first interaction (anchor) — NOT itself a qualifying result
    s.away(3.0, 5)        # moves meaningfully above the SMA
    s.touch()            # SECOND touch — must qualify
    s.away(3.0, 3)

    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    qualifying = events[events["touch_count"] >= cfg.minimum_touch_number]
    assert len(qualifying) == 1
    row = qualifying.iloc[0]
    assert row["touch_count"] == 2
    assert row["touch_number"] == 2
    assert row["is_new_event"] and row["direction"] == "LONG"
    assert pd.notna(row["previous_touch_time"]) and pd.notna(row["first_interaction_time"])


# ── Valid Scenario 2: third-or-later touch ───────────────────────────────────

def test_valid_scenario_third_touch_is_recognized_with_correct_touch_number():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)     # anchor
    s.touch().away(3.0, 5)     # 2nd touch
    s.touch().away(3.0, 3)     # 3rd touch

    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    qualifying = events[events["touch_count"] >= cfg.minimum_touch_number].sort_values("touch_count")
    assert list(qualifying["touch_count"]) == [2, 3]
    # No candle ever closed below the SMA and the SMA never stopped rising.
    frame = s.to_frame()
    ann, _ = process_touch_state(calculate_indicators(frame, cfg.sma_length), cfg, "LONG")
    assert not (ann["reject_reason"] == "close_breached_sma").any()
    assert not (ann["reject_reason"] == "sma_not_rising").any()


# ── Valid Scenario 3: wick below the SMA, close holds above ─────────────────

def test_valid_scenario_wick_below_sma_still_counts_as_a_valid_touch():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch()  # this bar becomes the 2nd touch; give it a wick well below the SMA
    s.away(3.0, 3)
    df = s.to_frame(wick_indices={len(s.closes) - 4: (s.closes[-4] - 5.0, s.closes[-4] + 0.05)})

    result = scan_symbol(df, cfg, symbol="TEST", timeframe="1D")
    # (this bar isn't the latest bar, so check via the full event list instead)
    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    qualifying = events[events["touch_count"] >= 2]
    assert len(qualifying) == 1
    assert bool(qualifying.iloc[0]["wicked_through_sma"]) is True
    assert qualifying.iloc[0]["close"] > qualifying.iloc[0]["sma_44"]   # close still held above


# ── require_ma_within_signal_range: a gap clean past the SMA is not a touch ──

def test_gap_up_entirely_above_sma_is_not_a_short_touch():
    """NSE:NIFTY50-INDEX 2026-09-18 09:15 5min: the prior session closed well
    below the SMA, and the next session's opening candle gapped up so hard
    that even its LOW sat above the SMA -- the tolerance formula alone
    (which also has to accept a genuine deep break-through as a touch)
    can't tell that apart from a real approach, and called it a SHORT
    touch. require_ma_within_signal_range adds the floor: the SMA must
    actually be inside (or crossed by) the candle's own range."""
    cfg = _cfg(sma_length=44)
    s = _PullbackSeries(cfg.sma_length, "SHORT").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor + armed, price approaching from below as SHORT expects

    df = s.to_frame()
    close_before_gap = df["close"].iloc[-1]
    sma_before_gap = calculate_indicators(df, cfg.sma_length)["sma_44"].iloc[-1]
    # A gap-up bar whose entire range (even its low) sits comfortably above
    # the SMA -- exactly the NIFTY50 shape (SMA ~57pts below the high, ~13pts
    # below even the low).
    gap_low = sma_before_gap + 5.0
    gap_high = gap_low + 20.0
    gap_row = pd.DataFrame([{
        "timestamp": df["timestamp"].iloc[-1] + pd.Timedelta(days=1),
        "open": gap_low + 15.0, "high": gap_high, "low": gap_low,
        "close": gap_low + 5.0, "volume": 100_000.0,
    }])
    df = pd.concat([df, gap_row], ignore_index=True)
    assert df["low"].iloc[-1] > sma_before_gap   # confirms the gap: even the low cleared the SMA

    ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "SHORT")
    gap_bar = ann.iloc[-1]
    assert gap_bar["low"] > gap_bar["sma_44"]
    assert not any(e.qualifies and e.current_touch_time == df["timestamp"].iloc[-1] for e in events)


def test_require_ma_within_signal_range_false_restores_old_gap_through_behaviour():
    cfg = _cfg(sma_length=44, require_ma_within_signal_range=False)
    s = _PullbackSeries(cfg.sma_length, "SHORT").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)

    df = s.to_frame()
    sma_before_gap = calculate_indicators(df, cfg.sma_length)["sma_44"].iloc[-1]
    gap_low = sma_before_gap + 5.0
    gap_row = pd.DataFrame([{
        "timestamp": df["timestamp"].iloc[-1] + pd.Timedelta(days=1),
        "open": gap_low + 15.0, "high": gap_low + 20.0, "low": gap_low,
        "close": gap_low + 5.0, "volume": 100_000.0,
    }])
    df = pd.concat([df, gap_row], ignore_index=True)

    _ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "SHORT")
    assert any(e.qualifies and e.current_touch_time == df["timestamp"].iloc[-1] for e in events)


# ── Invalid Scenario 1: a SUSTAINED closing breach resets the sequence ──────

def test_invalid_scenario_sustained_closing_breach_resets_the_sequence():
    """Two consecutive bars, each closing beyond sequence_reset_pct on the
    wrong side of the SMA, resets the whole active sequence (Rule 6,
    revised) — mirrors NSE:PRIVISCL-EQ's genuine 2025-08/09 breakdown
    (15+ consecutive days beyond -5%)."""
    cfg = _cfg(sequence_reset_mode="percent", sequence_reset_pct=2.0, sequence_reset_consecutive_bars=2)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)                     # anchor + armed
    s.breach(mult=0.93).breach(mult=0.93)       # two consecutive deep (~2-8%) dips

    df = s.to_frame()
    ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    assert (ann["reject_reason"] == "sustained_close_breach").any()
    # Neither breach bar qualifies as a touch: .breach() (unlike .touch()'s
    # deliberately wide wick) gives each bar only its default TINY wick
    # (close +/- 0.05), so a bar whose CLOSE has dropped ~7% below the SMA
    # has its HIGH well below the SMA too -- require_ma_within_signal_range
    # (see ScannerConfig) means the SMA was never actually inside this
    # candle's own range, so it's not a touch at all, just a violent bar
    # that helps trigger the sustained-breach reset below. Contrast with the
    # "wick below SMA" scenario above, whose wick is explicitly engineered
    # to still reach the SMA while its close holds above it.
    qualifying = [e for e in events if e.qualifies]
    assert qualifying == []
    assert (ann["reject_reason"] == "sustained_close_breach").sum() >= 1


def test_valid_scenario_a_single_deep_breach_bar_does_not_reset_the_sequence():
    """A lone bar clearing sequence_reset_pct does NOT reset anything by
    itself — only a SUSTAINED (2+ consecutive) breach does. Mirrors
    NSE:PRIVISCL-EQ 2026-06-09/10, where only one of the two dip days
    actually cleared the depth threshold, so the sequence stayed intact
    through 2026-06-11's bounce."""
    cfg = _cfg(sequence_reset_mode="percent", sequence_reset_pct=2.0, sequence_reset_consecutive_bars=2)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)     # anchor + armed
    s.breach(mult=0.93)        # ONE deep (~7%) dip only

    df = s.to_frame()
    ann, _events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    assert not (ann["reject_reason"] == "sustained_close_breach").any()


# ── Invalid Scenario 2: approach from below is not a valid pullback ─────────

def test_invalid_scenario_move_from_below_only_reanchors_never_counts_as_second_touch():
    # sequence_reset_mode pinned to "percent" -- this short synthetic series
    # doesn't have enough bars for ATR (atr_length=14) to warm up by the
    # breach, so the schema's own "atr" default would never trigger a reset
    # here at all (see ScannerConfig.sequence_reset_mode's fail-open NaN
    # handling). A SUSTAINED (2-consecutive-bar) breach is required now --
    # see test_invalid_scenario_sustained_closing_breach_resets_the_sequence
    # -- a single breach bar, however violent, no longer resets by itself
    # (Rule 5's slope check used to catch that case as a side effect; it no
    # longer gates anything at all, see Rule 5's revised docstring).
    cfg = _cfg(sequence_reset_mode="percent", sequence_reset_pct=2.0, sequence_reset_consecutive_bars=2)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)          # anchor (touch_count=1), armed
    s.breach(mult=0.7).breach(mult=0.7)   # two consecutive deep dips -> sustained breach, reset
    # price now rebuilds from BELOW and reclaims — this is a fresh FIRST interaction.
    s.touch()

    df = s.to_frame()
    ann, _events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    # The final (rebuild-from-below) bar only RE-ANCHORS (touch_count=1,
    # is_new_event=True) -- it never jumps straight to touch_count=2 on its
    # own.
    last = ann.iloc[-1]
    assert last["touch_count"] == 1
    assert bool(last["is_new_event"]) is True


# ── Invalid Scenario 3: falling/flat SMA rejects a long pullback ────────────

def test_invalid_scenario_flat_sma_never_anchors_a_long_sequence():
    cfg = _cfg(min_ma_slope_pct=0.0)
    # Perfectly flat closes -> slope is exactly 0, which fails the STRICT ">" rising test.
    n = _warmup_bars(cfg) + 10
    idx = pd.date_range("2024-01-01", periods=n, freq="1D", tz=TZ)
    close = np.full(n, 100.0)
    df = pd.DataFrame({"timestamp": idx, "open": close, "high": close + 0.05, "low": close - 0.05, "close": close})

    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert events.empty
    ann, _ = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    valid_rows = ann[ann["sma_44"].notna()]
    assert (valid_rows["reject_reason"] == "no_active_sequence_and_not_a_valid_first_interaction").all()


# ── Invalid Scenario 4: consecutive near-SMA candles are ONE touch cluster ──

def test_invalid_scenario_consecutive_touch_candles_count_as_one_cluster():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch()
    s.stay_near_last_touch(3)   # several more candles hovering at the SAME level — never moved away

    df = s.to_frame()
    ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    assert list(ann["touch_count"].iloc[-4:]) == [1, 1, 1, 1]   # never increments
    assert ann["is_new_event"].iloc[-3:].sum() == 0             # only the original anchor bar was "new"
    assert not any(e.qualifies for e in events)


def test_prolonged_hover_survives_a_slope_wobble_but_gets_labelled_trend_class_b():
    # A tight hover right at the fixed point asymptotically flattens the
    # SMA's own bar-to-bar change — repeated for long enough, that change
    # actually wobbles negative for a bar (the rolling average's oldest,
    # larger value rolling out gets replaced by a smaller new one), which
    # fails the "every bar in the window individually rising" test (Rule 5).
    # Revised behaviour: this no longer resets the sequence (Rule 5 only
    # labels trend_class now, see scanner.py's module docstring) — the
    # touch cluster survives the wobble untouched; the wobbling bar just
    # gets classified "B" (flattish) instead of "A" (cleanly rising).
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch()
    s.stay_near_last_touch(4)

    df = s.to_frame()
    ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    assert list(ann["touch_count"].iloc[-5:]) == [1, 1, 1, 1, 1]
    assert ann["reject_reason"].iloc[-1] == "same_existing_touch_cluster"
    assert ann["trend_class"].iloc[-1] == "B"


# ── Invalid Scenario 5: move-away threshold never reached -> no new touch ───

def test_invalid_scenario_insufficient_move_away_does_not_count_a_new_touch():
    cfg = _cfg(move_away_pct=5.0)   # require a big move away
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch()
    s.away(1.0, 3)     # only ~3% away — short of the 5% requirement
    s.touch()          # returns to the zone — should NOT count as touch #2

    df = s.to_frame()
    ann, events = process_touch_state(calculate_indicators(df, cfg.sma_length), cfg, "LONG")
    assert not any(e.qualifies for e in events)
    assert (ann["reject_reason"] == "no_meaningful_move_away_since_previous_touch").any()


def test_move_away_percent_threshold_is_exactly_respected():
    cfg = _cfg(move_away_pct=2.0)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(2.5, 3)   # clears the 2.0% bar comfortably
    s.touch()
    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] == 2).any()


# ── SHORT mirror ──────────────────────────────────────────────────────────

def test_short_direction_mirrors_long_for_a_falling_sma():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "SHORT").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)   # anchor: rallies down away from the SMA (i.e. falls further)
    s.touch().away(3.0, 3)   # 2nd touch: rallies back UP into the falling SMA from below

    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    qualifying = events[(events["touch_count"] >= 2) & (events["direction"] == "SHORT")]
    assert len(qualifying) == 1
    row = qualifying.iloc[0]
    assert row["close"] < row["sma_44"]           # close held BELOW the (falling) SMA
    assert row["ma_slope_pct"] < 0                 # SMA genuinely falling


def test_allow_long_and_allow_short_gate_which_directions_run():
    cfg_long_only = _cfg(allow_long=True, allow_short=False)
    s = _PullbackSeries(cfg_long_only.sma_length, "SHORT").warmup(_warmup_bars(cfg_long_only) - (cfg_long_only.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch().away(3.0, 3)
    events = scan_history(s.to_frame(), cfg_long_only, symbol="TEST", timeframe="1D")
    assert events.empty  # the SHORT pattern is real, but allow_short=False suppresses it


# ── Dynamic-duration: pattern length must not be a hardcoded lookback ───────

def test_dynamic_duration_short_and_long_patterns_both_recognized():
    cfg = _cfg()

    fast = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    fast.touch().away(3.0, 3)
    fast.touch().away(3.0, 3)
    fast_events = scan_history(fast.to_frame(), cfg, symbol="FAST", timeframe="1D")
    assert len(fast.closes) < 40

    slow = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    slow.touch().away_hold(5.0, 30)   # holds well clear of the SMA for far more bars than `fast`
    slow.touch().away_hold(5.0, 20)
    slow_events = scan_history(slow.to_frame(), cfg, symbol="SLOW", timeframe="1D")
    assert len(slow.closes) > 50

    assert (fast_events["touch_count"] == 2).any()
    assert (slow_events["touch_count"] == 2).any()
    # No strategy-level lookback was configured differently between the two —
    # both used the exact same ScannerConfig instance.


# ── Multi-timeframe: identical state logic regardless of the timeframe label ─

def test_same_state_logic_regardless_of_timeframe_label():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch().away(3.0, 3)
    df = s.to_frame()

    results = {}
    for tf_label in ["1min", "5min", "15min", "60min", "1D", "1W", "monthly"]:
        events = scan_history(df, cfg, symbol="TEST", timeframe=tf_label)
        results[tf_label] = list(events["touch_count"].sort_values())

    first = next(iter(results.values()))
    assert all(v == first for v in results.values())   # identical qualification regardless of label
    assert results["1D"] == [2]
    assert set(results.keys()) == {"1min", "5min", "15min", "60min", "1D", "1W", "monthly"}
    # and the label itself is carried through verbatim into the output
    events_daily = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events_daily["timeframe"] == "1D").all()


# ── Incomplete-candle handling ────────────────────────────────────────────

def test_incomplete_last_candle_is_excluded_by_default():
    cfg = _cfg()   # use_only_completed_candles=True by default
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch()   # this WOULD be the qualifying 2nd touch...
    df = s.to_frame()
    df["is_complete"] = True
    df.loc[df.index[-1], "is_complete"] = False   # ...but it is still forming

    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert not (events["touch_count"] >= 2).any()   # dropped before it could ever qualify

    result = scan_symbol(df, cfg, symbol="TEST", timeframe="1D")
    assert result["LONG"] is None


def test_completed_candle_flag_true_still_qualifies():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch()
    df = s.to_frame()
    df["is_complete"] = True   # every bar, including the last, is explicitly completed

    events = scan_history(df, cfg, symbol="TEST", timeframe="1D")
    assert (events["touch_count"] >= 2).any()


# ── scan_symbol: "current scanner result" ────────────────────────────────────

def test_scan_symbol_returns_none_when_not_yet_qualifying():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch()   # only the FIRST interaction so far
    result = scan_symbol(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    assert result["LONG"] is None


def test_scan_symbol_returns_the_touch_event_when_the_latest_bar_qualifies():
    cfg = _cfg()
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch()   # the latest bar IS the qualifying 2nd touch
    result = scan_symbol(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    assert result["LONG"] is not None
    assert result["LONG"].touch_count == 2
    assert result["LONG"].symbol == "TEST" and result["LONG"].timeframe == "1D"


def test_scan_history_and_scan_symbol_return_empty_shapes_for_empty_input():
    cfg = _cfg()
    empty = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])
    events = scan_history(empty, cfg, symbol="TEST", timeframe="1D")
    assert events.empty
    result = scan_symbol(empty, cfg, symbol="TEST", timeframe="1D")
    assert result == {"LONG": None, "SHORT": None}


# ── Rule 9 (revised): later in-zone bars of a qualified cluster are signals too ──

def test_in_cluster_bars_after_a_qualified_touch_are_emitted_as_signal_candidates():
    cfg = _cfg(emit_signal_bars_within_cluster=True)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)      # anchor
    s.touch()                   # touch #2 -> qualifies (new event)
    s.stay_near_last_touch(1)   # next bar still inside the same zone
    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    long_events = events[events["direction"] == "LONG"].sort_values("timestamp")
    assert list(long_events["touch_count"]) == [2, 2]              # touch_count never increments
    assert list(long_events["is_new_event"]) == [True, False]      # second one is the in-cluster signal bar
    assert long_events.iloc[1]["previous_touch_time"] == long_events.iloc[0]["current_touch_time"]


def test_in_cluster_signal_bars_can_be_switched_off():
    cfg = _cfg(emit_signal_bars_within_cluster=False)
    s = _PullbackSeries(cfg.sma_length, "LONG").warmup(_warmup_bars(cfg) - (cfg.sma_length - 1))
    s.touch().away(3.0, 5)
    s.touch()
    s.stay_near_last_touch(1)
    events = scan_history(s.to_frame(), cfg, symbol="TEST", timeframe="1D")
    assert list(events[events["direction"] == "LONG"]["is_new_event"]) == [True]
