from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import time
from pathlib import Path
from typing import Any, List, Literal, Optional, get_type_hints

DEFAULT_CONFIG_PATH = Path(__file__).with_name("default_config.json")


def _parse_time(value: str) -> time:
    hour, minute = value.split(":")
    return time(hour=int(hour), minute=int(minute))


@dataclass
class MarketConfig:
    timezone: str = "Asia/Kolkata"
    market_start_time: time = field(default_factory=lambda: time(9, 15))
    market_end_time: time = field(default_factory=lambda: time(15, 30))
    no_new_entry_after: time = field(default_factory=lambda: time(15, 10))
    force_exit_time: time = field(default_factory=lambda: time(15, 15))
    # True for 24/7 instruments (crypto): market_start_time / no_new_entry_after /
    # force_exit_time are ignored entirely — positions exit only on SL/target.
    continuous_session: bool = False


@dataclass
class TimeframesConfig:
    # The scanner is timeframe-independent (see scanner.py): whatever OHLC
    # bars are resampled to this timeframe is what it operates on — "1min",
    # "5min", "15min", "60min", "1D"/"daily", "1W"/"weekly", etc.
    timeframe: str = "15min"


@dataclass
class ScannerConfig:
    """44-SMA uptrend/downtrend-pullback scanner (see scanner.py for the state
    machine that consumes this). Finds symbols in an established trend that
    are pulling back to a sloping 44-SMA for the SECOND OR LATER time in the
    same uninterrupted trend — not merely touching it for the first time."""

    # Rule 1: the moving average itself — SMA only, length configurable.
    sma_length: int = 44

    # Rule 3: touch-zone tolerance. The low (long) / high (short) may sit up to
    # this far on the FAR side of the SMA and still count as "in zone" — either
    # touch_tolerance_atr_multiplier x ATR ("atr", default — timeframe-
    # independent, the same setting works on daily or 5-min bars with no
    # re-tuning) or a flat percent of price ("percent" — simpler, but a value
    # tuned on one timeframe is wrong on another: 0.6% (the value this was
    # originally tuned against on DAILY bars) is ~0.15-0.2x a typical daily
    # bar's ATR but ~2.5-3.5x a typical 5-min bar's ATR, measured on
    # NSE:PRIVISCL-EQ/NSE:TATASTEEL-EQ). 0.2x ATR is a rough cross-symbol
    # estimate matched to that same 0.6% on daily bars, not a proper
    # calibration in its own right.
    touch_tolerance_mode: Literal["percent", "atr"] = "atr"
    touch_tolerance_pct: float = 0.5
    touch_tolerance_atr_multiplier: float = 0.2

    # A touch-zone check based purely on distance-vs-tolerance can be
    # trivially satisfied by a candle that GAPPED clean past the SMA (an
    # overnight/opening gap, most common on indices and intraday timeframes)
    # without any part of the candle's own range ever being near the MA --
    # see NSE:NIFTY50-INDEX 2026-09-18 09:15 5min: the SMA sat ~57 points
    # below the candle's high and ~13 points below even its LOW, yet the
    # tolerance formula alone (which also has to accept a genuine deep
    # break-through as a valid touch) called it a SHORT touch. This adds a
    # floor: the SMA must actually sit inside, or be crossed by, the
    # candle's own high-low range -- high >= sma for LONG, low <= sma for
    # SHORT -- not just be somewhere outside a candle the tolerance math
    # happens to accept. True (default) = the fix; False = old behaviour.
    require_ma_within_signal_range: bool = True

    # Rule 5: rising/falling SMA test, normalized by price so it is comparable
    # across symbols/timeframes. rising (long) needs slope_pct > min_ma_slope_pct;
    # falling (short) needs slope_pct < -min_ma_slope_pct.
    ma_slope_lookback: int = 5
    min_ma_slope_pct: float = 0.0

    # Rule 10: how far price must move away from the SMA after a touch before
    # the NEXT touch is allowed to count. Defaults to "atr" (timeframe-
    # independent); 0.6x is what the original 2.0% (tuned on daily bars)
    # works out to, on average, against NSE:PRIVISCL-EQ/NSE:TATASTEEL-EQ
    # daily ATR — a rough cross-symbol estimate, not a proper calibration,
    # same caveat as touch_tolerance_atr_multiplier above.
    move_away_mode: Literal["percent", "atr"] = "atr"
    move_away_pct: float = 2.0
    atr_length: int = 14
    move_away_atr_multiplier: float = 0.6

    # Rule 8: a scan result requires at least this many touches in the active
    # sequence (2 = "second or later", the spec's own framing).
    minimum_touch_number: int = 2

    # Rule 6: a brief close below the SMA (long) / above it (short) no longer
    # nukes the whole active sequence by itself — only a SUSTAINED breach does:
    # `sequence_reset_consecutive_bars` or more CONSECUTIVE closes, each beyond
    # `sequence_reset_pct` (percent mode) / `sequence_reset_atr_multiplier` x
    # ATR (atr mode) on the wrong side of the SMA. A single bad day, or even
    # two days that don't individually clear the depth threshold, is treated
    # as ordinary pullback noise within an otherwise-intact uptrend — not
    # proof the trend itself broke. ATR mode is the default so the same
    # setting scales sensibly whether this runs on daily or 5-min bars (same
    # rationale as move_away_mode/move_away_atr_multiplier above). See NSE:
    # PRIVISCL-EQ 2026-06-09/10 (2 days, -0.7%/-2.2%, ~0.2x/0.7x ATR — does
    # NOT reset: the deep-breach day count never reaches 2) vs 2025-08/09
    # (15+ consecutive days beyond -5%, several ATR deep — resets repeatedly).
    sequence_reset_mode: Literal["percent", "atr"] = "atr"
    sequence_reset_pct: float = 2.0
    sequence_reset_atr_multiplier: float = 0.6
    sequence_reset_consecutive_bars: int = 2

    # Emit one candidate per touch CLUSTER (Rule "Current Scanner Result") —
    # True: only the bar where a new cluster first qualifies. False: every bar
    # the symbol remains inside a qualifying cluster (live-scanner style).
    emit_once_per_touch_event: bool = True

    # Rule 9 (revised): once a touch cluster has qualified (touch_count >=
    # minimum_touch_number), every LATER in-zone bar of that same cluster is
    # ALSO emitted as its own signal candidate (is_new_event=False, same
    # touch_count) instead of being silently swallowed — the trade-taking
    # candle-strength filter downstream then keeps only the strong ones. Fixes
    # the pattern where a red touch bar is followed, still inside the zone, by
    # the green bar that's the real entry signal (see IDFCFIRSTB 2026-01-14,
    # PRIVISCL 2026-04-13, DYCL 2026-09-23). Never changes touch_count itself.
    emit_signal_bars_within_cluster: bool = False

    # Which moving averages generate signals. None (default) = just
    # `sma_length` (44), exactly the original single-MA behaviour. A list such
    # as [30, 44, 50, 200] runs the SAME touch/filter rules once per length
    # and, when several MAs signal on the same bar, keeps only the one
    # farthest from price (see signals.generate_scan_candidates). Each signal
    # MA is also checked against every OTHER length in this list (plus
    # ma200_filter.ma_length) by the confluence filter.
    signal_ma_lengths: Optional[List[int]] = None

    def ma_lengths(self) -> List[int]:
        lengths = self.signal_ma_lengths if self.signal_ma_lengths else [self.sma_length]
        return sorted({int(n) for n in lengths})

    # Rule "Use only fully completed candles": if the input carries an
    # `is_complete` column, rows flagged False are dropped before any
    # processing (can never be touched, counted, or feed the MA/slope). If no
    # such column is present, the caller is trusted to have supplied only
    # completed candles already.
    use_only_completed_candles: bool = True

    allow_long: bool = True
    allow_short: bool = True

    # How many bars a qualifying touch event may wait, once armed as a pending
    # order, for its trigger price to actually be hit before expiring. This is
    # order-execution plumbing (consumed by backtest/engine.py), not part of
    # the SMA detection logic itself.
    setup_expiry_bars: int = 3

    def __post_init__(self) -> None:
        if self.sma_length < 2:
            raise ValueError(f"sma_length must be >= 2, got {self.sma_length}")
        if self.signal_ma_lengths is not None and any(int(n) < 2 for n in self.signal_ma_lengths):
            raise ValueError(f"signal_ma_lengths must all be >= 2, got {self.signal_ma_lengths}")
        if self.touch_tolerance_mode not in ("percent", "atr"):
            raise ValueError(f'touch_tolerance_mode must be "percent" or "atr", got {self.touch_tolerance_mode!r}')
        if self.touch_tolerance_pct < 0:
            raise ValueError(f"touch_tolerance_pct must be >= 0, got {self.touch_tolerance_pct}")
        if self.touch_tolerance_atr_multiplier < 0:
            raise ValueError(
                f"touch_tolerance_atr_multiplier must be >= 0, got {self.touch_tolerance_atr_multiplier}"
            )
        if self.ma_slope_lookback < 1:
            raise ValueError(f"ma_slope_lookback must be >= 1, got {self.ma_slope_lookback}")
        if self.move_away_mode not in ("percent", "atr"):
            raise ValueError(f'move_away_mode must be "percent" or "atr", got {self.move_away_mode!r}')
        if self.move_away_pct < 0:
            raise ValueError(f"move_away_pct must be >= 0, got {self.move_away_pct}")
        if self.atr_length < 1:
            raise ValueError(f"atr_length must be >= 1, got {self.atr_length}")
        if self.move_away_atr_multiplier < 0:
            raise ValueError(f"move_away_atr_multiplier must be >= 0, got {self.move_away_atr_multiplier}")
        if self.minimum_touch_number < 1:
            raise ValueError(f"minimum_touch_number must be >= 1, got {self.minimum_touch_number}")
        if self.sequence_reset_mode not in ("percent", "atr"):
            raise ValueError(f'sequence_reset_mode must be "percent" or "atr", got {self.sequence_reset_mode!r}')
        if self.sequence_reset_pct < 0:
            raise ValueError(f"sequence_reset_pct must be >= 0, got {self.sequence_reset_pct}")
        if self.sequence_reset_atr_multiplier < 0:
            raise ValueError(f"sequence_reset_atr_multiplier must be >= 0, got {self.sequence_reset_atr_multiplier}")
        if self.sequence_reset_consecutive_bars < 1:
            raise ValueError(
                f"sequence_reset_consecutive_bars must be >= 1, got {self.sequence_reset_consecutive_bars}"
            )
        if self.setup_expiry_bars < 0:
            raise ValueError(f"setup_expiry_bars must be >= 0, got {self.setup_expiry_bars}")
        if not self.allow_long and not self.allow_short:
            raise ValueError("at least one of allow_long / allow_short must be True")


@dataclass
class SignalCandleConfig:
    """Trade-taking filter applied to a touch's own candle — NOT part of touch
    detection/counting (see scanner.py; touch_count is entirely unaffected by
    this). A touch that otherwise qualifies still only becomes a trade
    candidate if its own candle is a genuinely strong directional bar, via
    EITHER of two independent checks:
      - the plain directional check: closes beyond its own open AND within
        `min_close_position` of its own high (long) / low (short) — excludes
        a technically-green-but-weak candle like an inverted hammer, OR
      - the hammer exception: the candle closed on the WRONG side of its own
        open (red on a LONG touch / green on a SHORT touch) but still shows a
        genuine rejection — almost no wick on the unfavourable side
        (<= `hammer_max_opposite_wick_ratio` of the bar's own high-low
        range), a SMALL body (<= `hammer_max_body_ratio` of the range — a
        "proper" hammer, not just a fat-bodied candle with a long-ish wick),
        and the close still lands within `min_close_position` of the
        favourable extreme (there's no separate "favourable wick is long
        enough" threshold — once the candle's on the wrong side, that wick's
        ratio to the full range is mathematically identical to close
        position, so `min_close_position` already covers it). A classic
        hammer/shooting-star: price pushed hard against the touch intraday
        and got rejected, even though the open-to-close print alone reads as
        the "wrong" colour.
    A touch whose candle fails BOTH is simply skipped as a trade opportunity;
    the sequence and touch count carry on unaffected, and the NEXT qualifying
    touch is judged fresh on its own candle."""
    min_close_position: float = 0.6
    allow_hammer_exception: bool = True
    hammer_max_opposite_wick_ratio: float = 0.15
    hammer_max_body_ratio: float = 0.30

    # False (default: the ORIGINAL rule above -- a same-colour candle still
    # needs its close within min_close_position of its own extreme).
    # True: skip that close-position check entirely for a same-colour candle
    # (green on a LONG touch / red on a SHORT touch) -- ANY green candle is
    # "strong" for a LONG regardless of where it closed in its own range.
    # min_close_position (and the wick/body ratios) then only ever apply to
    # the OPPOSITE-colour hammer-exception case -- i.e. "only check
    # strength when we're looking for a bullish/bearish hammer on the
    # 'wrong'-coloured candle", not on an already-favourable-coloured one.
    skip_close_position_check_for_same_color_candle: bool = False

    def __post_init__(self) -> None:
        if not 0.0 <= self.min_close_position <= 1.0:
            raise ValueError(f"min_close_position must be within [0, 1], got {self.min_close_position}")
        if not 0.0 <= self.hammer_max_opposite_wick_ratio <= 1.0:
            raise ValueError(
                f"hammer_max_opposite_wick_ratio must be within [0, 1], got {self.hammer_max_opposite_wick_ratio}"
            )
        if not 0.0 <= self.hammer_max_body_ratio <= 1.0:
            raise ValueError(
                f"hammer_max_body_ratio must be within [0, 1], got {self.hammer_max_body_ratio}"
            )


@dataclass
class Ma200FilterConfig:
    """Trade-taking filter applied against a longer 200-SMA — like
    SignalCandleConfig, NOT part of touch detection/counting; a touch that
    otherwise qualifies is simply skipped as a trade opportunity, sequence
    and touch_count entirely unaffected.

    Only ever active when the 200-SMA sits on the FAR side of the 44-SMA
    from price — below it for LONG, above it for SHORT (the "normal"
    configuration where the 200-SMA is a deeper support/resistance behind
    the 44-SMA). When that holds, a touch is rejected outright if the
    200-SMA is within `nearby_tolerance_pct` of the touch candle's own low
    (LONG) / high (SHORT) — the side that actually tested the level, same
    convention as the 44-SMA's own touch_tolerance_pct, not the close (which
    can sit well clear of the 200-SMA even on a bar whose wick reached right
    into it) — a real 200-SMA confluence sitting right on top of the setup,
    not just generally in the neighbourhood. If the 200-SMA is on the near
    side (or not close enough), this is a no-op and the touch is judged
    exactly as it would be without this filter at all.

    nearby_tolerance_mode: "atr" (default, timeframe-independent) — 1.5x is
    what the original 5.0% (tuned on daily bars) works out to, on average,
    against NSE:PRIVISCL-EQ/NSE:TATASTEEL-EQ daily ATR, a rough cross-symbol
    estimate, not a proper calibration; "percent" is the simpler but
    timeframe-sensitive alternative."""
    ma_length: int = 200
    nearby_tolerance_mode: Literal["percent", "atr"] = "atr"
    nearby_tolerance_pct: float = 5.0
    nearby_tolerance_atr_multiplier: float = 1.5

    # 0 (default) = off, exactly the original behaviour. > 0: a reference MA
    # that WOULD reject the touch is waived when it sits within
    # merged_ma_gap_atr_multiplier x ATR of the SIGNAL MA itself — the two
    # MAs are practically one support/resistance band, not a competing
    # second level. Which MAs were waived is recorded on each candidate
    # (confluence_gap_skipped / _ma / _atr).
    merged_ma_gap_atr_multiplier: float = 0.0

    def __post_init__(self) -> None:
        if self.merged_ma_gap_atr_multiplier < 0:
            raise ValueError(
                f"merged_ma_gap_atr_multiplier must be >= 0, got {self.merged_ma_gap_atr_multiplier}"
            )
        if self.ma_length < 2:
            raise ValueError(f"ma_length must be >= 2, got {self.ma_length}")
        if self.nearby_tolerance_mode not in ("percent", "atr"):
            raise ValueError(
                f'nearby_tolerance_mode must be "percent" or "atr", got {self.nearby_tolerance_mode!r}'
            )
        if self.nearby_tolerance_pct <= 0:
            raise ValueError(f"nearby_tolerance_pct must be > 0, got {self.nearby_tolerance_pct}")
        if self.nearby_tolerance_atr_multiplier <= 0:
            raise ValueError(
                f"nearby_tolerance_atr_multiplier must be > 0, got {self.nearby_tolerance_atr_multiplier}"
            )


@dataclass
class TradeFilterConfig:
    """Hard trade-taking filters — like SignalCandleConfig/Ma200FilterConfig,
    NOT part of touch detection/counting; each independently culls a touch
    from becoming a trade candidate, never affects touch_count/qualifies.

    exclude_trend_class_b: drop every "B" (flattish-MA) touch outright —
    only "A" (cleanly rising/falling with the trend) and "C" (cleanly
    against it) are ever considered.

    min_trend_class_c_slope_pct: for touches that ARE "C" (the MA moving
    against the trade), a floor on how far against it the MA may be —
    measured the same direction-adjusted way trend_class itself is (positive
    = favourable), so -1.5 means "no worse than 1.5% against the trade,
    net over ma_slope_lookback bars". Only ever checked on "C" touches;
    ignored for "A"/"B".

    min_daily_volume / min_entry_price: plain liquidity/price floors —
    below either, the setup is skipped regardless of how good it otherwise
    looks. min_entry_price is checked against the computed trigger_price
    (not the touch candle's raw close), since that's the price you'd
    actually be entering at."""
    exclude_trend_class_b: bool = True
    min_trend_class_c_slope_pct: float = -1.5
    min_daily_volume: float = 100_000.0
    min_entry_price: float = 15.0

    def __post_init__(self) -> None:
        if self.min_daily_volume < 0:
            raise ValueError(f"min_daily_volume must be >= 0, got {self.min_daily_volume}")
        if self.min_entry_price < 0:
            raise ValueError(f"min_entry_price must be >= 0, got {self.min_entry_price}")


@dataclass
class SetupQualityConfig:
    """Setup-quality checks layered on top of an already-qualifying touch —
    NOT part of touch detection/counting (see scanner.py) and NOT a
    trade-taking gate like SignalCandleConfig either. These only ever
    describe/label a touch (`setup_quality` = BEST / OK / BAD, plus the
    individual flags it's built from); nothing here changes touch_count,
    qualifies, or whether signals.py generates a trade candidate."""

    # Rule 1/2: bars since the previous touch ("swing length").
    swing_best_min_bars: int = 4
    swing_best_max_bars: int = 6      # 4-6 bars is unconditionally BEST.
    swing_short_max_bars: int = 3     # <= this many bars is a short swing —
                                       # suspect for actually being a range
                                       # (see ongoing_range_* below).
    swing_bad_min_bars: int = 16      # >= this many bars is unconditionally BAD.

    # Rule 4: double bottom/top — an earlier touch (any sequence) whose
    # low (long) / high (short) sits within this tolerance of the current
    # touch's. "atr" (default, timeframe-independent) — 0.15x is what the
    # original 0.5% (tuned on daily bars) works out to, on average, against
    # NSE:PRIVISCL-EQ/NSE:TATASTEEL-EQ daily ATR, a rough cross-symbol
    # estimate, not a proper calibration; "percent" is the simpler but
    # timeframe-sensitive alternative.
    double_bottom_tolerance_mode: Literal["percent", "atr"] = "atr"
    double_bottom_tolerance_pct: float = 0.5
    double_bottom_tolerance_atr_multiplier: float = 0.15

    # Rule 5: old resistance (long) / support (short) flip. A single matching
    # pivot isn't enough evidence a level is a genuine, established
    # resistance/support LINE (see IDBI 2026-01-12: one match turned out to
    # be this very sequence's own anchor, and the touch's own low dipped
    # BELOW several of the "resistance" levels it supposedly retested from
    # above — never actually held as support) — require at least
    # resistance_flip_min_touches separate confirmed swing pivots, each
    # within resistance_flip_tolerance_pct of the current touch's price AND
    # within resistance_flip_lookback_bars bars of it (a fixed rolling
    # window, not the whole unbounded history — an old level needs to have
    # been retested recently enough to still be structurally relevant).
    # Same percent/atr mode choice as double_bottom_tolerance_mode above,
    # same rough 0.15x cross-symbol ATR estimate for the 0.5% it replaces.
    resistance_flip_tolerance_mode: Literal["percent", "atr"] = "atr"
    resistance_flip_tolerance_pct: float = 0.5
    resistance_flip_tolerance_atr_multiplier: float = 0.15
    resistance_flip_swing_lookback: int = 3
    resistance_flip_lookback_bars: int = 200
    resistance_flip_min_touches: int = 2

    # Rule 6: walking backward from the touch, an ONGOING (contiguous, never
    # broken by a genuine breakout) trading range at least ongoing_range_min_bars
    # bars long counts as a live consolidation the touch is sitting inside,
    # rather than a real pullback. The range's own max width is measured
    # either as a pct of the touch's own price, or (default) in ATR
    # multiples — ATR mode travels correctly across timeframes (1min / 5min /
    # 15min / daily / ...) with no re-tuning, since it's computed from
    # whatever bars are actually being scanned; percent mode is a simpler but
    # timeframe-sensitive alternative (a "tight" range in % terms means very
    # different things on a daily vs. a 5min chart).
    ongoing_range_width_mode: Literal["percent", "atr"] = "atr"
    ongoing_range_max_width_pct: float = 5.0
    ongoing_range_max_width_atr_multiplier: float = 1.8
    ongoing_range_min_bars: int = 4

    # A SECOND, independent way to catch a range the walk above misses: the
    # CLOSING price (ignoring each bar's own intraday wick) over this many
    # bars ending at the touch, checked against the SAME width budget as
    # above. Catches a genuine multi-week consolidation where one bar in the
    # window (maybe the touch bar itself) just happens to have an unusually
    # wide wick that would otherwise bust the bar-by-bar walk at a single
    # step (see PNBHOUSING). Either check passing is enough — see
    # setup_quality.is_range_bound.
    close_range_lookback_bars: int = 10

    # Rule 7: a violent single-bar pullback — an absolute BAD override (see
    # classify_setup_quality), regardless of everything else. A single bar
    # within the pullback (touch bar itself excluded) that closed, gap PLUS
    # same-day follow-through combined, at least this many multiples of ATR
    # below (LONG) / above (SHORT) the prior close is a panicked gap-and-sell
    # day, not an orderly pullback — TATASTEEL 2026-05-20 (1.29x) / IDBI
    # 2026-01-12 (1.14x) vs. JINDALSAW/EMBDL/ACE's gentler pullbacks
    # (0.73x / 0.60x / 0.47x) calibrated this default. See
    # setup_quality.max_pullback_decline_atr_ratio / has_violent_pullback_bar.
    violent_pullback_max_decline_atr_multiplier: float = 1.0
    # The window this walks is primarily previous_touch_time -> current_touch_time
    # (the scanner's own swing leg) — but padded further backward when that
    # gives fewer than this many bars to judge (a quick 2nd touch shortly
    # after the 1st shouldn't starve the sample — see TATASTEEL, whose
    # previous touch was only 1 bar before it).
    violent_pullback_min_bars: int = 3

    # Rule 7b: a SECOND, independent way a pullback can be violent — not one
    # sharp bar, but a genuine SUSTAINED decline over the last
    # violent_pullback_recent_bars bars (touch bar excluded) that no single
    # bar is sharp enough to trip the check above on its own — KTKBANK
    # 2026-09-16 (1.90x) / SAGILITY 2026-09-11 (1.88x) / JINDALSAW 2026-08-14
    # (1.92x) vs. ACE 2026-09-11 (0.15x) / ASKAUTOLTD 2026-09-16 (0.98x)
    # calibrated these defaults. Either this OR the single-bar check firing
    # is enough — see setup_quality.recent_peak_decline_atr_ratio /
    # has_sustained_recent_decline, and classify_setup_quality.
    violent_pullback_recent_bars: int = 5
    violent_pullback_recent_decline_atr_multiplier: float = 1.3

    def __post_init__(self) -> None:
        if self.close_range_lookback_bars < 1:
            raise ValueError(f"close_range_lookback_bars must be >= 1, got {self.close_range_lookback_bars}")
        if self.violent_pullback_max_decline_atr_multiplier <= 0:
            raise ValueError(
                "violent_pullback_max_decline_atr_multiplier must be > 0, got "
                f"{self.violent_pullback_max_decline_atr_multiplier}"
            )
        if self.violent_pullback_min_bars < 1:
            raise ValueError(f"violent_pullback_min_bars must be >= 1, got {self.violent_pullback_min_bars}")
        if self.violent_pullback_recent_bars < 1:
            raise ValueError(f"violent_pullback_recent_bars must be >= 1, got {self.violent_pullback_recent_bars}")
        if self.violent_pullback_recent_decline_atr_multiplier <= 0:
            raise ValueError(
                "violent_pullback_recent_decline_atr_multiplier must be > 0, got "
                f"{self.violent_pullback_recent_decline_atr_multiplier}"
            )
        if self.swing_short_max_bars < 0:
            raise ValueError(f"swing_short_max_bars must be >= 0, got {self.swing_short_max_bars}")
        if not (self.swing_short_max_bars < self.swing_best_min_bars <= self.swing_best_max_bars < self.swing_bad_min_bars):
            raise ValueError(
                "expected swing_short_max_bars < swing_best_min_bars <= swing_best_max_bars < "
                f"swing_bad_min_bars, got {self.swing_short_max_bars}, {self.swing_best_min_bars}, "
                f"{self.swing_best_max_bars}, {self.swing_bad_min_bars}"
            )
        if self.double_bottom_tolerance_mode not in ("percent", "atr"):
            raise ValueError(
                f'double_bottom_tolerance_mode must be "percent" or "atr", got {self.double_bottom_tolerance_mode!r}'
            )
        if self.double_bottom_tolerance_pct < 0:
            raise ValueError(f"double_bottom_tolerance_pct must be >= 0, got {self.double_bottom_tolerance_pct}")
        if self.double_bottom_tolerance_atr_multiplier < 0:
            raise ValueError(
                "double_bottom_tolerance_atr_multiplier must be >= 0, "
                f"got {self.double_bottom_tolerance_atr_multiplier}"
            )
        if self.resistance_flip_tolerance_mode not in ("percent", "atr"):
            raise ValueError(
                f'resistance_flip_tolerance_mode must be "percent" or "atr", got {self.resistance_flip_tolerance_mode!r}'
            )
        if self.resistance_flip_tolerance_pct < 0:
            raise ValueError(f"resistance_flip_tolerance_pct must be >= 0, got {self.resistance_flip_tolerance_pct}")
        if self.resistance_flip_tolerance_atr_multiplier < 0:
            raise ValueError(
                "resistance_flip_tolerance_atr_multiplier must be >= 0, "
                f"got {self.resistance_flip_tolerance_atr_multiplier}"
            )
        if self.resistance_flip_swing_lookback < 1:
            raise ValueError(f"resistance_flip_swing_lookback must be >= 1, got {self.resistance_flip_swing_lookback}")
        if self.resistance_flip_lookback_bars < 1:
            raise ValueError(f"resistance_flip_lookback_bars must be >= 1, got {self.resistance_flip_lookback_bars}")
        if self.resistance_flip_min_touches < 1:
            raise ValueError(f"resistance_flip_min_touches must be >= 1, got {self.resistance_flip_min_touches}")
        if self.ongoing_range_width_mode not in ("percent", "atr"):
            raise ValueError(f'ongoing_range_width_mode must be "percent" or "atr", got {self.ongoing_range_width_mode!r}')
        if self.ongoing_range_max_width_pct <= 0:
            raise ValueError(f"ongoing_range_max_width_pct must be > 0, got {self.ongoing_range_max_width_pct}")
        if self.ongoing_range_max_width_atr_multiplier <= 0:
            raise ValueError(f"ongoing_range_max_width_atr_multiplier must be > 0, got {self.ongoing_range_max_width_atr_multiplier}")
        if self.ongoing_range_min_bars < 1:
            raise ValueError(f"ongoing_range_min_bars must be >= 1, got {self.ongoing_range_min_bars}")


@dataclass
class EntryConfig:
    # "atr" scales the buffer with the stock's own recent volatility
    # (scanner.atr_length's ATR * entry_buffer_atr_multiplier) instead of a
    # fixed points/pct offset — the same false-breakout margin means
    # something very different on a calm large-cap vs. a choppy microcap, so
    # a fixed number under- or over-shoots one of them. NaN ATR (warmup not
    # complete yet) falls back to a zero buffer — see signals.py's own
    # handling — never blocks a trade outright, same fail-open convention as
    # the 200-SMA filter.
    entry_buffer_type: Literal["points", "pct", "atr"] = "points"
    entry_buffer_points: float = 0.0
    entry_buffer_pct: float = 0.0
    entry_buffer_atr_multiplier: float = 0.1

    # Round the final trigger price to the nearest whole rupee when it's
    # already close to one, on the SAFE side only — up (ceil) for LONG, down
    # (floor) for SHORT, so rounding only ever makes the trigger a little
    # harder to hit, never easier. "Close" is judged as a % of the price
    # itself (round_to_whole_number_tolerance_pct), not a fixed rupee gap —
    # scales sensibly across very differently priced stocks (e.g. 1189.31 ->
    # 1190 for a ~1189 stock at 0.1%, since the ~0.7pt gap is well inside
    # 0.1% of ~1189; a 45-rupee stock only rounds when within a few paise).
    round_to_whole_number: bool = True
    round_to_whole_number_tolerance_pct: float = 0.1

    def __post_init__(self) -> None:
        if self.entry_buffer_atr_multiplier < 0:
            raise ValueError(
                f"entry_buffer_atr_multiplier must be >= 0, got {self.entry_buffer_atr_multiplier}"
            )
        if self.round_to_whole_number_tolerance_pct < 0:
            raise ValueError(
                f"round_to_whole_number_tolerance_pct must be >= 0, got {self.round_to_whole_number_tolerance_pct}"
            )


@dataclass
class StopLossConfig:
    # See EntryConfig.entry_buffer_type for what "atr" mode means and why.
    sl_buffer_type: Literal["points", "pct", "atr"] = "points"
    sl_buffer_points: float = 0.0
    sl_buffer_pct: float = 0.0
    sl_buffer_atr_multiplier: float = 0.1

    # The stop isn't just the signal candle's own low (LONG) / high (SHORT) —
    # it's the WIDEST (furthest from entry) of: the lowest low / highest high
    # across the signal candle AND stop_lookback_bars immediately before it,
    # and the 44-SMA's own value at the touch (the stop must never sit
    # INSIDE the support/resistance line itself — see signals.py). This level
    # is used once a position is actually OPEN; the PRE-ENTRY invalidation
    # check for a still-pending setup deliberately uses a narrower level
    # instead — the signal candle's own opposite extreme, not this one (see
    # backtest/engine.py's _resolve_pending_trigger).
    stop_lookback_bars: int = 2

    def __post_init__(self) -> None:
        if self.stop_lookback_bars < 0:
            raise ValueError(f"stop_lookback_bars must be >= 0, got {self.stop_lookback_bars}")
        if self.sl_buffer_atr_multiplier < 0:
            raise ValueError(f"sl_buffer_atr_multiplier must be >= 0, got {self.sl_buffer_atr_multiplier}")


@dataclass
class TargetConfig:
    risk_reward: float = 2.0

    # "signal_candle_range" (default): target = trigger +/- risk_reward *
    #   (signal candle's own high - low) — a FIXED distance, independent of
    #   whatever the stop ends up being (it can widen a lot once the 44-SMA
    #   floor above kicks in; the target deliberately does not follow it).
    # "dynamic_stop_multiple": target = trigger +/- risk_reward * (trigger -
    #   stop) — the earlier behaviour, where the target tracks the actual
    #   (possibly widened) stop distance. Kept as an option, not the default.
    target_mode: Literal["signal_candle_range", "dynamic_stop_multiple"] = "signal_candle_range"

    def __post_init__(self) -> None:
        if self.target_mode not in ("signal_candle_range", "dynamic_stop_multiple"):
            raise ValueError(f'target_mode must be "signal_candle_range" or "dynamic_stop_multiple", got {self.target_mode!r}')


@dataclass
class TradeLimitsConfig:
    max_trades_per_symbol_per_day: Optional[int] = 10
    max_total_trades_per_day: Optional[int] = None
    max_open_positions: Optional[int] = None          # None = unlimited
    one_active_position_per_symbol: bool = True

    # How many setups may sit ARMED (signalled, waiting for their trigger price)
    # at once for a single symbol. While a setup is pending, any further signal on
    # that symbol is dropped. Default 1 = the original behaviour; raise it (or set
    # None for unlimited) to stop discarding overlapping setups.
    max_pending_setups_per_symbol: Optional[int] = 1


@dataclass
class RiskConfig:
    initial_capital: float = 1_000_000.0
    position_sizing_mode: Literal["fixed_qty", "risk_per_trade", "capital_based"] = "fixed_qty"
    fixed_qty: float = 1.0
    # risk_per_trade sizing: qty = risk_amount / abs(trigger_price - stop_loss),
    # where risk_amount is risk_per_trade_amount if set, else current_equity *
    # risk_per_trade_pct/100 (current_equity = initial_capital + realized P&L
    # so far — sizing compounds as the backtest's equity moves) — same
    # fixed-amount-overrides-percentage convention as capital_based below.
    # risk_per_trade_amount is what "trade a fixed ₹X of risk per trade,
    # regardless of total capital" means: e.g. initial_capital=2,500,000 with
    # risk_per_trade_amount=25,000 risks the same ₹25,000 on every trade,
    # sized wider or narrower purely by how far the stop sits from entry.
    risk_per_trade_pct: float = 0.25
    risk_per_trade_amount: Optional[float] = None
    # capital_based sizing: qty = capital_amount / trigger_price, where capital_amount
    # is capital_per_trade_amount if set, else current_equity * capital_per_trade_pct/100.
    capital_per_trade_pct: float = 10.0
    capital_per_trade_amount: Optional[float] = None
    max_daily_loss_pct: float = 1.0
    close_positions_on_daily_loss_hit: bool = False


@dataclass
class ExecutionConfig:
    # "use_ohlc_path" infers which of SL/target was hit first from the bar's
    # own shape (the same bullish/bearish-implies-low/high-first convention
    # backtest/engine.py's _resolve_pending_trigger already uses for the
    # pre-entry trigger/invalidation ambiguity) — a bar's actual intrabar
    # path is unknowable without tick data, but the bar's own shape is a
    # reasonable approximation, and it's the default here for the same
    # reason it's used there: a flat "sl_first"/"target_first" convention
    # ignores information the bar itself already gives you.
    same_candle_priority: Literal["sl_first", "target_first", "use_ohlc_path"] = "use_ohlc_path"
    allow_entry_and_exit_same_candle: bool = True

    # Candle-trailing stop: after entry, trail the stop to each completed candle's
    # extreme in the trade's favor — a LONG's stop trails UP to the previous
    # candle's low (exit when a candle's low breaks below it); a SHORT's stop
    # trails DOWN to the previous candle's high (exit when a candle's high breaks
    # above it). The stop is always the PREVIOUS completed candle's extreme (a
    # one-bar delay, so no lookahead) and only ever ratchets in the favorable
    # direction. When enabled, the fixed risk_reward target is IGNORED — the trade
    # runs on the trailing stop alone (plus force-exit / daily-loss). exit_reason
    # is "trail_stop". Default off (fixed stop + 2R target as before).
    # Legacy alias — equivalent to exit_mode="candle_trail".
    use_candle_trailing_stop: bool = False

    # ── Exit / trade-management mode ─────────────────────────────────────────
    #   "fixed"          initial stop + fixed risk_reward target (original behaviour)
    #   "candle_trail"   trail to each prior candle's extreme (see above)
    #   "atr_chandelier" ATR Chandelier trailing stop that only ARMS after the trade
    #                    has moved trail_after_R in your favour. Until then the trade
    #                    is protected by the original signal stop (so losers still
    #                    exit at the initial stop). Designed to let winners run
    #                    instead of capping them at a fixed 1:2 / 1:3.
    # exit_mode wins when set; otherwise use_candle_trailing_stop is honoured.
    exit_mode: Literal["fixed", "candle_trail", "atr_chandelier"] = "fixed"

    # ── ATR Chandelier trailing-stop parameters (exit_mode="atr_chandelier") ──
    # The initial stop comes from the signal (entry-timeframe candle), and defines 1R:
    #   long:  R = entry_price - initial_stop      short: R = initial_stop - entry_price
    atr_length: int = 14                  # ATR period (entry timeframe)
    atr_multiplier: float = 2.5           # chandelier width, in ATRs, from the run-up extreme
    swing_lookback: int = 5               # confirmed swing pivot = extreme vs N bars each side
    use_structure_filter: bool = True     # also pull the stop to the last confirmed swing
    structure_buffer_points: float = 0.0  # small buffer beyond that swing
    trail_after_R: float = 1.0            # arm trailing only once price reaches this many R
    exit_on_close: bool = True            # True: exit only if the CLOSE breaches the stop.
                                          # False: exit on any intrabar touch of the stop.
    use_breakeven_after_activation: bool = True   # on arming, pull the stop to at least breakeven
    breakeven_buffer_points: float = 0.0  # breakeven +/- this buffer

    # Optional fixed take-profit. OFF by default — the whole point is to let the
    # trailing stop capture larger moves rather than cap them at a fixed R multiple.
    use_fixed_take_profit: bool = False
    fixed_rr_target: float = 2.0


@dataclass
class CostsConfig:
    brokerage_per_order: float = 0.0
    brokerage_pct: float = 0.0
    slippage_points: float = 0.0
    slippage_pct: float = 0.0
    taxes_pct: float = 0.0


@dataclass
class StrategyConfig:
    strategy_name: str = "44_SMA_UPTREND_PULLBACK_SCANNER"
    market: MarketConfig = field(default_factory=MarketConfig)
    timeframes: TimeframesConfig = field(default_factory=TimeframesConfig)
    scanner: ScannerConfig = field(default_factory=ScannerConfig)
    signal_candle: SignalCandleConfig = field(default_factory=SignalCandleConfig)
    ma200_filter: Ma200FilterConfig = field(default_factory=Ma200FilterConfig)
    trade_filters: TradeFilterConfig = field(default_factory=TradeFilterConfig)
    setup_quality: SetupQualityConfig = field(default_factory=SetupQualityConfig)
    entry: EntryConfig = field(default_factory=EntryConfig)
    stop_loss: StopLossConfig = field(default_factory=StopLossConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    trade_limits: TradeLimitsConfig = field(default_factory=TradeLimitsConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    costs: CostsConfig = field(default_factory=CostsConfig)

    @staticmethod
    def from_dict(overrides: dict[str, Any] | None) -> "StrategyConfig":
        overrides = overrides or {}
        return _build_dataclass(StrategyConfig, overrides)

    @staticmethod
    def from_json(path: str | Path) -> "StrategyConfig":
        with open(path, "r") as fh:
            data = json.load(fh)
        return StrategyConfig.from_dict(data)

    @staticmethod
    def default() -> "StrategyConfig":
        return StrategyConfig.from_json(DEFAULT_CONFIG_PATH)


def _build_dataclass(cls: type, overrides: dict[str, Any]) -> Any:
    """Build a (possibly nested) dataclass from a dict, merging with field defaults
    so a partial JSON config only needs to specify the keys it wants to change."""
    hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for f in fields(cls):
        if f.name not in overrides:
            continue
        raw_value = overrides[f.name]
        field_type = hints.get(f.name)
        if raw_value is None:
            kwargs[f.name] = None
        elif isinstance(field_type, type) and is_dataclass(field_type):
            kwargs[f.name] = _build_dataclass(field_type, raw_value)
        elif field_type is time and isinstance(raw_value, str):
            kwargs[f.name] = _parse_time(raw_value)
        else:
            kwargs[f.name] = raw_value
    return cls(**kwargs)
