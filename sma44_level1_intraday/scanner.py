"""44-SMA uptrend-pullback scanner (replaces the old Level 1/2/3 "44 MA filter").

Finds symbols pulling back toward a 44-period SMA for the SECOND OR LATER
time within the same uninterrupted touch sequence — the pattern a
discretionary trader means by "buy the pullback to the 44 MA," as distinct
from the FIRST time price ever reaches that MA (which is just the anchor,
not yet a proven, repeated pullback). The SMA's own slope no longer gates
whether a touch is recognized at all (see Rule 5 below) — it's instead
attached to every touch as a trend_class label ("A" = cleanly rising, "B" =
flattish, "C" = falling), so a discretionary trader (or a later filtering
pass) can weight an "A-class" pullback-in-an-uptrend differently from a
"C-class" counter-trend bounce, without the scanner silently discarding the
touch outright.

Timeframe-independent: every function here operates on whatever OHLC bars it
is given (1min, 15min, daily, weekly, ...). There is no timeframe-specific
branching anywhere in this module — see ScannerConfig / calculate_indicators.

State machine (see process_touch_state)
----------------------------------------
    NO_SEQUENCE
        -> an "anchor" interaction (reclaim, touch, or simply starting near a
           rising/falling MA) starts a new sequence at touch_count = 1.
    ANCHORED (an active sequence exists)
        while price stays inside the SMA zone: same TOUCH CLUSTER, touch_count
           does not increment (Rule 9).
        once price leaves the zone and moves away far enough: ARMED for the
           next touch (Rule 10) — tracked as `armed`, not a separate state name,
           to avoid an explosion of (armed x in_cluster) state combinations.
        if ARMED and price re-enters the zone with a valid close: a NEW touch
           is counted (touch_count += 1) and, once touch_count reaches
           minimum_touch_number, this becomes a qualifying scan result.
        a SUSTAINED close breach — `sequence_reset_consecutive_bars` or more
           consecutive closes each clearing `sequence_reset_pct`/
           `sequence_reset_atr_multiplier` x ATR beyond the SMA (Rule 6) —
           resets straight back to NO_SEQUENCE. A brief, shallow dip below
           the SMA does NOT reset anything by itself; only a genuinely broken
           trend does. The whole sequence is discarded and a fresh anchor
           must form before anything can qualify again. The SMA's own slope
           (Rule 5) does NOT reset a sequence either — see trend_class above;
           a sequence anchored while the MA was flat/falling can keep
           accumulating touches even if the MA never turns cleanly up.

Direction symmetry
-------------------
The spec above is written for an uptrend/long pullback. A falling-SMA "rally
into resistance from below" short setup is the exact mirror and is implemented
here via one shared, direction-parametrised implementation (see the `sign`
trick in `process_touch_state`) rather than duplicated code — this satisfies
the "identical state logic, no direction-specific branching" spirit the spec
applies to timeframes. Set allow_short=False to disable it if you only want
the long-only pattern exactly as specified.

No lookahead
------------
Every array used by the state machine is computed once, up front, from
already-known values only:
  * sma_44[i]            — trailing rolling mean, uses bars <= i.
  * ma_slope_pct[i]       — compares sma_44[i] to sma_44[i - lookback]; both
                            already known at bar i.
  * ATR[i] (move_away_mode="atr") — trailing, via exits.compute_atr.
The sequential loop itself only ever reads state carried forward from bars
< i, and reads bar i's own OHLC to decide what happens to bar i (never a
bar j > i). Nothing here is smoothed, centered, or shifted backward in time.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

import numpy as np
import pandas as pd

from .config.schema import ScannerConfig
from .exits import compute_atr

# A tiny, PURELY numerical-noise guard — never a real trading tolerance. Real
# tolerances (touch zone, move-away) are the configurable pct fields below.
_EPS_PCT = 1e-9

Direction = Literal["LONG", "SHORT"]


# ── Output row ─────────────────────────────────────────────────────────────

@dataclass
class TouchEvent:
    """One row of scanner output — a bar where `direction`'s pullback pattern
    is (or, if touch_number < minimum_touch_number, is not yet) qualifying."""
    symbol: str
    timeframe: str
    direction: Direction
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    sma_44: float
    close_distance_pct: float
    low_distance_pct: float
    high_distance_pct: float
    ma_slope_pct: float
    trend_class: str              # "A" (rising) / "B" (flattish) / "C" (falling) — see
                                   # process_touch_state's Rule 5 comment; informational
                                   # only, never affects touch_count/qualification.
    touch_count: int              # touches in the active sequence as of this bar
    touch_number: int             # == touch_count (kept as a distinct field: the
                                   # "current touch number" the spec asks for)
    first_interaction_time: Optional[pd.Timestamp]
    previous_touch_time: Optional[pd.Timestamp]
    current_touch_time: Optional[pd.Timestamp]
    bars_since_previous_touch: Optional[int]
    max_distance_reached_pct: Optional[float]
    wicked_through_sma: bool      # low<sma (long) / high>sma (short) intrabar
    is_new_event: bool            # a NEW touch cluster started on this bar
    qualifies: bool               # touch_count >= minimum_touch_number
    reject_reason: Optional[str]  # populated only for non-qualifying rows (debug)


# ── Indicators (pure, backward-looking only) ────────────────────────────────

def calculate_indicators(df: pd.DataFrame, sma_length: int) -> pd.DataFrame:
    """Attach `sma_44` (a plain rolling SMA of `close`, length `sma_length`) to
    a COPY of df. Does not mutate the input. Value at bar i uses only bars <= i."""
    out = df.copy()
    out["sma_44"] = out["close"].rolling(window=sma_length, min_periods=sma_length).mean()
    return out


def _select_completed_candles(df: pd.DataFrame, use_only_completed_candles: bool) -> pd.DataFrame:
    """Rule: never use a still-forming candle. If the caller has explicitly
    flagged completeness via an `is_complete` column, drop incomplete rows
    before anything else can see them. Otherwise the input is trusted as-is."""
    if use_only_completed_candles and "is_complete" in df.columns:
        return df[df["is_complete"].astype(bool)].drop(columns=["is_complete"]).reset_index(drop=True)
    if "is_complete" in df.columns:
        return df.drop(columns=["is_complete"]).reset_index(drop=True)
    return df.reset_index(drop=True)


# ── Rule-level building blocks (kept as small, independently testable units) ─

def is_sma_rising(slope_pct: float, min_ma_slope_pct: float, monotonic: bool) -> bool:
    """Rule 5. `slope_pct` must already be direction-adjusted (positive means
    'trending in this direction's favour') — see the `sign` transform in
    `_scan_one_direction`. NaN (not enough history yet) is never rising.

    BOTH conditions must hold: a decent NET rise over the whole
    `ma_slope_lookback` window (`slope_pct > min_ma_slope_pct`) is no longer
    enough on its own — `monotonic` (precomputed once per bar in
    process_touch_state) additionally requires the SMA to have moved in the
    trend's favour on EVERY single one of those bars individually, not just
    on net. A window that's mostly risen but just ticked down on the most
    recent bar no longer counts as genuinely rising, even if the net change
    over the window is still comfortably positive."""
    if slope_pct is None or (isinstance(slope_pct, float) and np.isnan(slope_pct)):
        return False
    return slope_pct > min_ma_slope_pct and monotonic


def is_in_touch_zone(touch_distance_pct: float, touch_tolerance_pct: float) -> bool:
    """Rule 3. `touch_distance_pct` must already be direction-adjusted (the
    low's distance from the SMA for a long, the high's for a short, positive
    meaning 'beyond the SMA on the trend side'). Zone = at/through the SMA, or
    short of it by no more than the configured tolerance."""
    if touch_distance_pct is None or (isinstance(touch_distance_pct, float) and np.isnan(touch_distance_pct)):
        return False
    return touch_distance_pct <= touch_tolerance_pct


def has_moved_away(
    adj_close_distance_pct: float,
    adj_close_diff_price: float,
    atr_value: float,
    config: ScannerConfig,
) -> bool:
    """Rule 10. Direction-adjusted inputs (positive = further into trend
    territory). Percent mode compares the normalized distance; ATR mode
    compares the raw price gap to a multiple of ATR."""
    if config.move_away_mode == "atr":
        if atr_value is None or (isinstance(atr_value, float) and np.isnan(atr_value)):
            return False
        return adj_close_diff_price >= config.move_away_atr_multiplier * atr_value
    if adj_close_distance_pct is None or (isinstance(adj_close_distance_pct, float) and np.isnan(adj_close_distance_pct)):
        return False
    return adj_close_distance_pct >= config.move_away_pct


def detect_bullish_reclaim(prev_close: float, prev_sma: float, close: float, sma: float) -> bool:
    """Rule "Bullish Reclaim as the First Interaction" (long side, as literally
    specified): the previous completed candle closed at/below its SMA and this
    one closes above its SMA. May only ever anchor a NEW sequence (touch_count
    = 1) — never counts as a second-or-later pullback on its own."""
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in (prev_close, prev_sma, close, sma)):
        return False
    return prev_close <= prev_sma + abs(prev_sma) * _EPS_PCT / 100 and close > sma


def _detect_reclaim_adjusted(prev_adj_close_dist: float, adj_close_dist: float) -> bool:
    """Direction-symmetric form of detect_bullish_reclaim, operating on the
    same sign-adjusted distances the rest of the state machine uses (bearish
    reclaim for shorts is the mirror image, not a second implementation)."""
    if np.isnan(prev_adj_close_dist) or np.isnan(adj_close_dist):
        return False
    return prev_adj_close_dist <= _EPS_PCT and adj_close_dist > _EPS_PCT


# ── Single-bar candle strength (trade-taking filter, not part of touch
# detection — see signals.py) ────────────────────────────────────────────────

def close_position(open_: float, high: float, low: float, close: float) -> float:
    """Where the close sits within the bar's own range: 0.0 = at the low,
    1.0 = at the high. NaN for a zero-range bar (high == low), which can
    never count as a strong directional candle either way."""
    range_ = high - low
    if range_ <= 0:
        return float("nan")
    return (close - low) / range_


def _is_hammer_exception(
    direction: Direction, open_: float, high: float, low: float, close: float,
    pos: float, min_close_position: float, max_opposite_wick_ratio: float, max_body_ratio: float,
) -> bool:
    """A candle that closed on the "wrong" side of its own open (red on a
    LONG touch / green on a SHORT touch) can still count as strong if it's a
    genuine hammer / shooting-star rejection: almost no wick on the
    unfavourable side, a SMALL body (a "proper" hammer, not just a merely
    long favourable-side wick with a fat body eating up the rest of the
    range), and a close that still lands within `min_close_position` of the
    favourable extreme — e.g. TBOTEK 2026-09-16: open 1643.70, high 1647.70,
    low 1600.00, close 1631.40 — closed red (close < open) but the mere
    4.0-point upper wick (8% of the 47.7-point range), with the close itself
    66% of the way up the range, passes the wick/close checks — its 12.3-pt
    body is 26% of range, comfortably under the default 30% `max_body_ratio`
    (a stricter threshold, e.g. 20%, would flip a borderline case like this
    one back out).

    Note there's no separate "lower wick is long enough" check on the
    favourable side: whenever the candle closed on the wrong side of its own
    open, the favourable-side wick (low-to-body for LONG, body-to-high for
    SHORT) is bounded by the close itself, so its ratio to the full range is
    IDENTICAL to `pos` — `min_close_position` already covers it; a separate
    wick-ratio threshold there would just be comparing the same number twice.
    """
    if direction == "LONG":
        if close > open_:
            return False  # not on the "wrong" side; not this exception's job
        upper_wick = high - open_  # body_high == open_ here (close <= open_)
        body = open_ - close
    else:
        if close < open_:
            return False
        upper_wick = high - close  # body_high == close here (close >= open_)
        body = close - open_
    range_ = high - low
    if range_ <= 0:
        return False
    if body / range_ > max_body_ratio:
        return False
    if direction == "LONG":
        return upper_wick / range_ <= max_opposite_wick_ratio and pos >= min_close_position
    lower_wick = open_ - low  # body_low == open_ here (close >= open_)
    return lower_wick / range_ <= max_opposite_wick_ratio and pos <= (1.0 - min_close_position)


def is_strong_directional_candle(
    direction: Direction, open_: float, high: float, low: float, close: float,
    min_close_position: float,
    allow_hammer_exception: bool = False,
    hammer_max_opposite_wick_ratio: float = 0.15,
    hammer_max_body_ratio: float = 0.30,
    skip_close_position_check_for_same_color_candle: bool = False,
) -> bool:
    """A single-bar 'strong' bullish (LONG) / bearish (SHORT) candle. True if
    EITHER:
      - the plain directional check: closes on the right side of its own open
        AND (unless skip_close_position_check_for_same_color_candle) closes
        within `min_close_position` of its own high (LONG) / low (SHORT) —
        the close-position check is what disqualifies a
        technically-green-but-weak candle, e.g. a green inverted hammer:
        close > open, but the close still sits near the bar's LOW under a
        long upper wick showing the rally got rejected. `close > open` alone
        can't tell that apart from a candle that closed strongly near its
        high; close_position can. With the flag set, a same-colour candle
        (green for LONG / red for SHORT) is ALWAYS strong regardless of
        where it closed in its own range — the position/wick checks then
        only ever apply below, to the OPPOSITE-colour hammer case. OR
      - (if `allow_hammer_exception`) the hammer exception — see
        _is_hammer_exception: a candle on the "wrong" side of its own open
        that's still a genuine rejection hammer/shooting-star, with a
        SMALL body (<= hammer_max_body_ratio of its own range) — a "proper"
        hammer, not just a fat-bodied candle with a long-ish wick. This
        path's own min_close_position check is NEVER skipped by the flag
        above — it only ever relaxes the SAME-colour case.
    """
    pos = close_position(open_, high, low, close)
    if np.isnan(pos):
        return False
    if direction == "LONG":
        plain = (close > open_) if skip_close_position_check_for_same_color_candle else (close > open_ and pos >= min_close_position)
    else:
        plain = (close < open_) if skip_close_position_check_for_same_color_candle else (close < open_ and pos <= (1.0 - min_close_position))
    if plain or not allow_hammer_exception:
        return plain
    return _is_hammer_exception(
        direction, open_, high, low, close, pos,
        min_close_position, hammer_max_opposite_wick_ratio, hammer_max_body_ratio,
    )


# ── The state machine ────────────────────────────────────────────────────────

def process_touch_state(
    df: pd.DataFrame,
    config: ScannerConfig,
    direction: Direction,
    *,
    symbol: str = "",
    timeframe: str = "",
) -> tuple[pd.DataFrame, list[TouchEvent]]:
    """Walk `df` (one symbol, already sorted by timestamp, already completed
    candles only, carrying open/high/low/close/timestamp) bar by bar and
    return:
      1. a debug-annotated copy of df (one row per input bar: touch_count,
         armed, in_cluster, is_new_event, reject_reason, ...),
      2. the list of TouchEvent rows for every bar that belonged to an active
         sequence (qualifying or not — callers filter on `.qualifies` for
         production use; the full list exists so debug mode can show WHY a
         bar was rejected).

    Long and short share this one implementation. `sign` (+1 long, -1 short)
    turns every "beyond the SMA in the trend's favour" comparison into the
    same `adjusted_distance > 0` form for both directions — see the module
    docstring. This is the only place direction enters the calculation.
    """
    n = len(df)
    if n == 0:
        return df.copy(), []

    sign = 1.0 if direction == "LONG" else -1.0
    lookback = config.ma_slope_lookback

    close = df["close"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    open_ = df["open"].to_numpy(dtype=float)
    sma = df["sma_44"].to_numpy(dtype=float)
    timestamps = df["timestamp"].to_numpy()

    with np.errstate(invalid="ignore", divide="ignore"):
        close_distance_pct = (close - sma) / sma * 100.0
        low_distance_pct = (low - sma) / sma * 100.0
        high_distance_pct = (high - sma) / sma * 100.0

        sma_shifted = np.full(n, np.nan)
        sma_shifted[lookback:] = sma[:-lookback]
        raw_slope_pct = (sma - sma_shifted) / sma_shifted * 100.0

    adj_slope_pct = sign * raw_slope_pct
    adj_close_distance = sign * close_distance_pct
    touch_distance_pct = low_distance_pct if direction == "LONG" else high_distance_pct
    adj_touch_distance = sign * touch_distance_pct
    adj_close_diff_price = sign * (close - sma)
    touch_diff_price = (low - sma) if direction == "LONG" else (high - sma)
    adj_touch_diff_price = sign * touch_diff_price

    # Rule 5's OTHER half (see is_sma_rising): the SMA must have moved in the
    # trend's favour on EVERY one of the last `lookback` bars individually,
    # not just on net over the window — a rolling min of the (direction-
    # adjusted) bar-to-bar SMA deltas that's still positive means all of them
    # were. NaN (not enough of the window yet) is never monotonic.
    sma_bar_diff = np.diff(sma, prepend=np.nan)
    adj_sma_bar_diff = sign * sma_bar_diff
    with np.errstate(invalid="ignore"):
        rolling_min_diff = (
            pd.Series(adj_sma_bar_diff).rolling(window=lookback, min_periods=lookback).min().to_numpy()
        )
    is_monotonic = rolling_min_diff > 0

    atr = None
    if config.move_away_mode == "atr" or config.sequence_reset_mode == "atr" or config.touch_tolerance_mode == "atr":
        atr = compute_atr(df, config.atr_length).to_numpy(dtype=float)

    prev_adj_close_distance = np.full(n, np.nan)
    prev_adj_close_distance[1:] = adj_close_distance[:-1]

    wicked_through = (low_distance_pct < -_EPS_PCT) if direction == "LONG" else (high_distance_pct > _EPS_PCT)

    # Rule 6 (revised): a bar counts toward a SUSTAINED breach only if it
    # clears the configured depth on the wrong side of the SMA — a shallow
    # dip never counts, no matter how many bars it lasts. NaN ATR (warmup)
    # never counts as a breach — comparisons against NaN are False already.
    with np.errstate(invalid="ignore"):
        if config.sequence_reset_mode == "atr":
            deep_breach = adj_close_diff_price <= -(config.sequence_reset_atr_multiplier * atr)
        else:
            deep_breach = adj_close_distance <= -config.sequence_reset_pct

    # Rule 3, precomputed for the whole series: ATR mode compares the raw
    # price gap to touch_tolerance_atr_multiplier x ATR instead of a flat
    # percent — see ScannerConfig.touch_tolerance_mode for why (a flat
    # percent tuned against daily bars is wildly too loose on 5-min bars).
    # NaN ATR (warmup) never counts as "in zone" here either.
    with np.errstate(invalid="ignore"):
        if config.touch_tolerance_mode == "atr":
            in_touch_zone_arr = adj_touch_diff_price <= (config.touch_tolerance_atr_multiplier * atr)
        else:
            in_touch_zone_arr = adj_touch_distance <= config.touch_tolerance_pct

    # Extra floor (see ScannerConfig.require_ma_within_signal_range): the
    # tolerance check above alone can be trivially satisfied by a candle
    # that GAPPED clean past the SMA (most common on indices/intraday
    # timeframes) without the candle's own range ever being near the MA --
    # it exists to also accept a genuine deep break-through as a touch, and
    # can't tell that apart from an opening gap. Require the SMA to actually
    # sit inside, or be crossed by, the candle's own high-low range.
    if config.require_ma_within_signal_range:
        ma_within_range = (high >= sma) if direction == "LONG" else (low <= sma)
        in_touch_zone_arr = in_touch_zone_arr & ma_within_range

    # Rule 5 (revised): the SMA's own slope no longer gates touch
    # recognition at all — a touch is a touch regardless of what the MA is
    # doing. Instead it's classified into one of three trend regimes, purely
    # informational (like setup_quality — never affects touch_count/
    # qualification): "A" = genuinely rising (the OLD Rule 5 gate: net slope
    # positive AND every one of the last ma_slope_lookback bars individually
    # moved the SMA in the trend's favour), "C" = genuinely falling (net
    # slope clearly negative, the mirror threshold), "B" = anything in
    # between — a flattish/wobbling MA that's neither cleanly rising nor
    # cleanly falling.
    slope_ok_arr = (adj_slope_pct > config.min_ma_slope_pct) & is_monotonic
    with np.errstate(invalid="ignore"):
        falling_arr = adj_slope_pct < -config.min_ma_slope_pct
    trend_class_arr = np.where(slope_ok_arr, "A", np.where(falling_arr, "C", "B"))

    # ── per-bar debug/annotation output arrays ──
    out_touch_count = np.zeros(n, dtype=int)
    out_is_new_event = np.zeros(n, dtype=bool)
    out_armed = np.zeros(n, dtype=bool)
    out_in_cluster = np.zeros(n, dtype=bool)
    out_reject_reason: list[Optional[str]] = [None] * n

    events: list[TouchEvent] = []

    active = False
    touch_count = 0
    armed = False
    in_cluster = False
    first_interaction_time = None
    previous_touch_time = None
    current_touch_time = None
    max_distance_reached = None  # tracked while ANCHORED, between touches
    deep_breach_streak = 0       # consecutive bars clearing the Rule 6 depth threshold

    def _reset() -> None:
        nonlocal active, touch_count, armed, in_cluster, deep_breach_streak
        nonlocal first_interaction_time, previous_touch_time, current_touch_time, max_distance_reached
        active = False
        touch_count = 0
        armed = False
        in_cluster = False
        first_interaction_time = None
        previous_touch_time = None
        current_touch_time = None
        max_distance_reached = None
        deep_breach_streak = 0

    for i in range(n):
        if np.isnan(sma[i]):
            out_reject_reason[i] = "sma_unavailable"
            continue

        # Rule 6 (revised): only a SUSTAINED breach invalidates the active
        # sequence — `sequence_reset_consecutive_bars` or more consecutive
        # bars, each individually clearing the configured depth on the wrong
        # side of the SMA (see ScannerConfig.sequence_reset_*). A shallow or
        # one-off dip is ordinary pullback noise, not a broken trend, and
        # leaves touch_count/in_cluster/armed untouched.
        if active:
            deep_breach_streak = deep_breach_streak + 1 if deep_breach[i] else 0
            if deep_breach_streak >= config.sequence_reset_consecutive_bars:
                _reset()
                out_reject_reason[i] = "sustained_close_breach"
                continue

        if not active:
            # Anchor a new sequence: reclaim, a touch that closes beyond the
            # SMA, or simply starting near the SMA and then closing beyond it
            # — any of these are valid FIRST interactions (Rule 8). The SMA's
            # own slope no longer gates this (see trend_class below) — a
            # touch is a touch regardless of whether the MA happens to be
            # cleanly rising, flat, or falling at that moment.
            reclaim = _detect_reclaim_adjusted(prev_adj_close_distance[i], adj_close_distance[i])
            beyond_sma = adj_close_distance[i] > _EPS_PCT
            in_zone = bool(in_touch_zone_arr[i])
            if beyond_sma and (in_zone or reclaim):
                active = True
                touch_count = 1
                in_cluster = True
                armed = False
                first_interaction_time = timestamps[i]
                previous_touch_time = None
                current_touch_time = timestamps[i]
                max_distance_reached = adj_close_distance[i]
                out_touch_count[i] = 1
                out_is_new_event[i] = True
                out_in_cluster[i] = True
            else:
                out_reject_reason[i] = "no_active_sequence_and_not_a_valid_first_interaction"
            continue

        # active sequence, SMA valid, no breach this bar.
        if max_distance_reached is None or adj_close_distance[i] > max_distance_reached:
            max_distance_reached = adj_close_distance[i]

        in_zone = bool(in_touch_zone_arr[i])

        if in_zone:
            if in_cluster:
                out_touch_count[i] = touch_count
                out_in_cluster[i] = True
                out_reject_reason[i] = "same_existing_touch_cluster"
                if config.emit_signal_bars_within_cluster and touch_count >= config.minimum_touch_number:
                    # Rule 9 (revised): a later bar of an already-qualified
                    # cluster is still a candidate signal bar in its own
                    # right (touch_count unchanged) — see ScannerConfig.
                    cluster_idx = int(np.searchsorted(timestamps, current_touch_time))
                    events.append(TouchEvent(
                        symbol=symbol, timeframe=timeframe, direction=direction,
                        timestamp=pd.Timestamp(timestamps[i]),
                        open=float(open_[i]), high=float(high[i]), low=float(low[i]), close=float(close[i]),
                        sma_44=float(sma[i]),
                        close_distance_pct=float(close_distance_pct[i]),
                        low_distance_pct=float(low_distance_pct[i]),
                        high_distance_pct=float(high_distance_pct[i]),
                        ma_slope_pct=float(raw_slope_pct[i]) if not np.isnan(raw_slope_pct[i]) else float("nan"),
                        trend_class=str(trend_class_arr[i]),
                        touch_count=touch_count, touch_number=touch_count,
                        first_interaction_time=pd.Timestamp(first_interaction_time) if first_interaction_time is not None else None,
                        previous_touch_time=pd.Timestamp(current_touch_time),
                        current_touch_time=pd.Timestamp(timestamps[i]),
                        bars_since_previous_touch=i - cluster_idx,
                        max_distance_reached_pct=float(max_distance_reached) if max_distance_reached is not None else None,
                        wicked_through_sma=bool(wicked_through[i]),
                        is_new_event=False,
                        qualifies=True,
                        reject_reason=None,
                    ))
            elif armed:
                # Rule 4/8/9/10: a NEW touch — the system was armed (moved away
                # meaningfully since the previous touch) and has now returned.
                touch_count += 1
                previous_touch_time = current_touch_time
                current_touch_time = timestamps[i]
                in_cluster = True
                armed = False
                out_touch_count[i] = touch_count
                out_is_new_event[i] = True
                out_in_cluster[i] = True
                bars_since_previous = None
                if previous_touch_time is not None:
                    prev_idx = np.searchsorted(timestamps, previous_touch_time)
                    bars_since_previous = i - int(prev_idx)
                qualifies = touch_count >= config.minimum_touch_number
                events.append(TouchEvent(
                    symbol=symbol, timeframe=timeframe, direction=direction,
                    timestamp=pd.Timestamp(timestamps[i]),
                    open=float(open_[i]), high=float(high[i]), low=float(low[i]), close=float(close[i]),
                    sma_44=float(sma[i]),
                    close_distance_pct=float(close_distance_pct[i]),
                    low_distance_pct=float(low_distance_pct[i]),
                    high_distance_pct=float(high_distance_pct[i]),
                    ma_slope_pct=float(raw_slope_pct[i]) if not np.isnan(raw_slope_pct[i]) else float("nan"),
                    trend_class=str(trend_class_arr[i]),
                    touch_count=touch_count, touch_number=touch_count,
                    first_interaction_time=pd.Timestamp(first_interaction_time) if first_interaction_time is not None else None,
                    previous_touch_time=pd.Timestamp(previous_touch_time) if previous_touch_time is not None else None,
                    current_touch_time=pd.Timestamp(current_touch_time),
                    bars_since_previous_touch=bars_since_previous,
                    max_distance_reached_pct=float(max_distance_reached) if max_distance_reached is not None else None,
                    wicked_through_sma=bool(wicked_through[i]),
                    is_new_event=True,
                    qualifies=qualifies,
                    reject_reason=None if qualifies else f"touch_number_{touch_count}_below_minimum_{config.minimum_touch_number}",
                ))
                max_distance_reached = adj_close_distance[i]  # reset the between-touch tracker
            else:
                # Back in the zone, but never moved away enough since the
                # anchor/last touch — Rule 10: do not count a new touch.
                in_cluster = True
                out_touch_count[i] = touch_count
                out_in_cluster[i] = True
                out_reject_reason[i] = "no_meaningful_move_away_since_previous_touch"
        else:
            in_cluster = False
            if not armed:
                moved_away = has_moved_away(
                    adj_close_distance[i], adj_close_diff_price[i],
                    atr[i] if atr is not None else None, config,
                )
                if moved_away:
                    armed = True
            out_touch_count[i] = touch_count
            out_armed[i] = armed
            if not armed:
                out_reject_reason[i] = "moved_away_but_not_yet_far_enough" if adj_close_distance[i] > _EPS_PCT else None

    annotated = df.copy()
    annotated["direction"] = direction
    annotated["sma_44"] = sma
    annotated["close_distance_pct"] = close_distance_pct
    annotated["low_distance_pct"] = low_distance_pct
    annotated["high_distance_pct"] = high_distance_pct
    annotated["ma_slope_pct"] = raw_slope_pct
    annotated["trend_class"] = trend_class_arr
    annotated["touch_count"] = out_touch_count
    annotated["is_new_event"] = out_is_new_event
    annotated["armed"] = out_armed
    annotated["in_touch_cluster"] = out_in_cluster
    annotated["reject_reason"] = out_reject_reason
    return annotated, events


# ── Public scanning entry points ────────────────────────────────────────────

def scan_history(
    df: pd.DataFrame,
    config: ScannerConfig,
    *,
    symbol: str = "",
    timeframe: str = "",
) -> pd.DataFrame:
    """Run the full state machine over `df`'s entire history (both directions,
    per allow_long/allow_short) and return every QUALIFYING TouchEvent
    (touch_count >= minimum_touch_number) as a DataFrame — one row per touch
    event by default, or one row per bar-while-still-in-that-cluster if
    emit_once_per_touch_event is False. Empty DataFrame (with the right
    columns) if nothing qualifies or df is empty."""
    columns = list(TouchEvent.__dataclass_fields__.keys())
    if df.empty:
        return pd.DataFrame(columns=columns)

    working = _select_completed_candles(df, config.use_only_completed_candles)
    working = working.sort_values("timestamp").reset_index(drop=True)
    working = calculate_indicators(working, config.sma_length)

    all_events: list[TouchEvent] = []
    for direction, allowed in (("LONG", config.allow_long), ("SHORT", config.allow_short)):
        if not allowed:
            continue
        _annotated, events = process_touch_state(working, config, direction, symbol=symbol, timeframe=timeframe)
        all_events.extend(e for e in events if e.qualifies)

    if not config.emit_once_per_touch_event:
        # "Live scanner" style is not meaningful for a historical DataFrame of
        # discrete touch events (there is no continuous bar stream to repeat
        # across here) — scan_history always emits one row per touch event;
        # callers wanting per-bar cluster visibility should use scan_symbol
        # (the latest-bar view) on a rolling window instead.
        pass

    if not all_events:
        return pd.DataFrame(columns=columns)
    rows = [{k: getattr(e, k) for k in columns} for e in all_events]
    return pd.DataFrame(rows, columns=columns).sort_values("timestamp").reset_index(drop=True)


def scan_symbol(
    df: pd.DataFrame,
    config: ScannerConfig,
    *,
    symbol: str = "",
    timeframe: str = "",
    debug: bool = False,
) -> dict[Direction, Optional[TouchEvent]]:
    """"Current scanner result": does the LATEST completed candle in `df`
    qualify right now, for each enabled direction? Returns {direction: None}
    when it does not qualify (or that direction is disabled); with debug=True
    the rejected bar's reason is still inspectable via process_touch_state
    directly.
    """
    result: dict[Direction, Optional[TouchEvent]] = {}
    if df.empty:
        return {"LONG": None, "SHORT": None}

    working = _select_completed_candles(df, config.use_only_completed_candles)
    working = working.sort_values("timestamp").reset_index(drop=True)
    working = calculate_indicators(working, config.sma_length)

    for direction, allowed in (("LONG", config.allow_long), ("SHORT", config.allow_short)):
        if not allowed:
            result[direction] = None
            continue
        annotated, events = process_touch_state(working, config, direction, symbol=symbol, timeframe=timeframe)
        last = annotated.iloc[-1]
        qualifies = bool(last["touch_count"] >= config.minimum_touch_number) and bool(
            last["in_touch_cluster"]
        )
        if not qualifies:
            result[direction] = None
            continue
        # Find the event describing the cluster the latest bar belongs to —
        # the most recent event with is_new_event True at/before the last bar.
        matching = [e for e in events if e.qualifies and e.current_touch_time <= working["timestamp"].iloc[-1]]
        result[direction] = matching[-1] if matching else None

    return result
