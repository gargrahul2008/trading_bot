"""Setup-quality checks for the 44-SMA scanner — describe how GOOD an
already-qualifying touch's pullback structure looks, on top of (never as
part of) touch detection/counting.

Orthogonal to everything else in this package, the same way
`is_strong_directional_candle` (scanner.py) is orthogonal to touch counting
and `generate_scan_candidates` (signals.py) is orthogonal to it too:
- scanner.py decides WHETHER a bar is the Nth touch of a sequence.
- signals.py decides WHETHER that touch's own candle is worth trading.
- this module decides HOW GOOD that touch's pullback looks, using only
  information already available strictly before/at the touch itself.

None of the functions here ever change touch_count, qualifies, or whether a
candidate gets generated — they only ever attach descriptive columns
(`setup_quality` = "BEST" / "OK" / "BAD", plus the individual flags it is
built from) for a human (or a later, explicitly separate filter) to use.

Six checks feed `classify_setup_quality`: three of them (violent_pullback,
range_bound, double_bottom) are absolute overrides, the rest are scored
together — see that function's own docstring for the exact combination
logic. A SEVENTH, pullback_bullish_count, is computed and exposed as a
column but no longer feeds the verdict at all (see its own docstring for
why — is_violent_pullback replaced it there):
  1/2. swing_bars           — bars since the previous touch (already on every
                               TouchEvent as `bars_since_previous_touch`).
                               4-6 bars is one positive signal; >=16 bars is
                               one negative signal.
  3. pullback_bullish_count — INFORMATIONAL ONLY (see is_violent_pullback,
                               #7, for the check that replaced this in
                               scoring) — how many bars between the swing
                               extreme (the away-leg's peak/trough) and this
                               touch closed green (LONG) / red (SHORT). Used
                               to double as a proxy for "steep, one-directional
                               fall," but a slow multi-day all-red grind
                               (JINDALSAW, EMBDL) and a violent one-bar
                               gap-crash (TATASTEEL) both read as 0 here —
                               color count alone can't tell them apart.
  4. is_double_bottom        — an earlier swing low (LONG) / swing high
                               (SHORT) ANYWHERE in the symbol's history — a
                               genuine local price extremum, NOT restricted to
                               a bar that happened to also be a counted touch
                               of the SMA (only the CURRENT side needs to be
                               an actual touch; the earlier one can sit
                               anywhere relative to the MA), AND never
                               breached (closed past) in between — a level
                               already broken and revisited isn't a genuine
                               double bottom/top, just a coincidental
                               price-level revisit. Checks both the low/high
                               AND the close of each side against
                               double_bottom_tolerance_pct — a "bottom" as a
                               trader reads a chart isn't only the exact wick.
  5. is_resistance_flip      — within the last resistance_flip_lookback_bars
                               bars before this touch, at least
                               resistance_flip_min_touches separate confirmed
                               swing highs (LONG) / lows (SHORT) — checking
                               EITHER the pivot's own wick OR its close, not
                               the wick alone — within resistance_flip_tolerance_pct
                               of this touch candle's own TEST ZONE (see
                               touch_test_zone — the wick-plus-body side that
                               actually tested the level, e.g. [low, open]
                               for a bullish LONG touch, not just the exact
                               low) — a genuine, multiple-times-confirmed
                               resistance/support LINE now being retested
                               from the other side, not just one coincidental
                               pivot (which can be this very sequence's own
                               anchor, or a level the touch's own wick dipped
                               straight through rather than actually held
                               above — see IDBI 2026-01-12).
  6. is_range_bound          — has the CLOSING price over the last
                               close_range_lookback_bars bars (touch bar
                               inclusive, fixed window, wicks ignored) stayed
                               within a tight budget? A touch sitting inside
                               a live consolidation — price chopping sideways
                               while the MA slowly drifts into it — is not a
                               real pullback. Deliberately close-only: a
                               wick-by-wick contiguous walk was tried and
                               dropped (it let a touch bar that's itself a
                               wide-range breakout candle still count as
                               "still in the range" just because it — plus a
                               handful of bars before it — squeezed under the
                               width budget; see THYROCARE/CUB 2026-08-31).
                               "Tight" is measured in ATR multiples by default
                               (ongoing_range_width_mode="atr") so it travels
                               correctly across timeframes without re-tuning —
                               a fixed pct is available as a simpler but
                               timeframe-sensitive alternative.
  7. is_violent_pullback     — did any SINGLE bar within the pullback (the
                               touch/signal bar itself excluded — it's
                               frequently a strong reversal candle BY
                               CONSTRUCTION, not part of what fell into it)
                               CLOSE at least violent_pullback_max_decline_atr_multiplier
                               multiples of ATR below (LONG) / above (SHORT)
                               the PRIOR close? Close-to-close, not wick-based
                               — a bar that gapped down then recovered to
                               close back near its high (a hammer, not a
                               breakdown) must not count (see ACE
                               2026-09-11). Window: primarily
                               previous_touch_time -> the touch (the same
                               leg swing_bars already describes), padded
                               further back only if that gives fewer than
                               violent_pullback_min_bars bars to judge (see
                               TATASTEEL 2026-05-20, whose previous touch was
                               only 1 bar earlier). A candidate bar only
                               counts if its own decline was never
                               RECOVERED (price never closed back past it)
                               before the touch — a long previous_touch_time
                               window can span many quiet weeks, and without
                               this a single ordinary rough day buried in
                               otherwise-choppy, range-bound action gets
                               mistaken for a real fall (see ASKAUTOLTD
                               2026-09-16: Aug 7 alone crossed the threshold,
                               but price fully recovered above it by Aug 31 —
                               three weeks of chop, not a sustained decline).
                               A slow, multi-day all-red grind (low
                               pullback_bullish_count alone can't tell this
                               apart) is a fundamentally different, healthier
                               pullback than a single panicked gap-and-sell
                               day that sticks — see TATASTEEL/IDBI (single
                               bar) vs. ACE/ASKAUTOLTD (neither check fires).
                               A SECOND, independent check (either firing is
                               enough) catches a genuine SUSTAINED decline
                               that's spread across several moderate days,
                               none sharp enough on its own to trip the
                               single-bar check — see
                               recent_peak_decline_atr_ratio /
                               has_sustained_recent_decline (KTKBANK
                               2026-09-16 / SAGILITY 2026-09-11 / JINDALSAW
                               2026-08-14). An ABSOLUTE override to BAD (see
                               classify_setup_quality), checked before even
                               double_bottom.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from .config.schema import ScannerConfig, SetupQualityConfig
from .exits import compute_atr
from .scanner import Direction, _select_completed_candles, calculate_indicators

SETUP_QUALITY_COLUMNS = [
    "swing_bars", "pullback_bullish_count",
    "is_double_bottom", "is_resistance_flip", "is_range_bound", "is_violent_pullback",
    "setup_quality",
]


# ── small numpy-array helpers (mirrors the searchsorted style already used in
# scanner.py's process_touch_state for bars_since_previous_touch) ────────────

def _bar_index(timestamps: pd.DatetimeIndex, t: Optional[pd.Timestamp]) -> Optional[int]:
    """Exact index of timestamp `t` within `timestamps` (already sorted), or
    None if it isn't present. Uses pandas' own searchsorted (not a raw numpy
    datetime64 comparison) so tz-aware timestamps compare correctly."""
    if t is None:
        return None
    idx = int(timestamps.searchsorted(t))
    if idx < len(timestamps) and timestamps[idx] == t:
        return idx
    return None


def find_swing_extreme_index(
    close: np.ndarray, sma: np.ndarray, timestamps: pd.DatetimeIndex,
    direction: Direction, window_start_time: pd.Timestamp, current_touch_time: pd.Timestamp,
) -> Optional[int]:
    """Index of the away-leg's extreme bar (swing high for LONG, swing low
    for SHORT) strictly between `window_start_time` and the touch — the bar
    where price sat furthest from the SMA in the trend's favour. None if
    there are no bars strictly between them (back-to-back touches).

    `window_start_time` should be the sequence's `first_interaction_time`,
    not just the immediately preceding touch — when two touches land close
    together (a quick re-test shortly after the first), scoping to only the
    last inter-touch leg badly undercounts the real pullback a chart reader
    would see (see TATASTEEL 2026-05-20: touch #2 landed just 2 bars after
    touch #1, but the actual swing high was 4+ days earlier)."""
    lo = _bar_index(timestamps, window_start_time)
    hi = _bar_index(timestamps, current_touch_time)
    if lo is None or hi is None or hi - lo <= 1:
        return None
    sign = 1.0 if direction == "LONG" else -1.0
    window = sign * (close[lo + 1:hi] - sma[lo + 1:hi])
    return lo + 1 + int(np.nanargmax(window))


def pullback_bullish_count(
    open_: np.ndarray, close: np.ndarray, timestamps: pd.DatetimeIndex,
    swing_extreme_index: Optional[int], current_touch_time: pd.Timestamp,
) -> int:
    """Bars strictly between the swing extreme and this touch that closed in
    the bullish (green) direction — LOW count / all-red means a steep,
    one-directional fall; a higher count means an orderly, two-way pullback."""
    if swing_extreme_index is None:
        return 0
    hi = _bar_index(timestamps, current_touch_time)
    if hi is None or hi - swing_extreme_index <= 1:
        return 0
    window_open = open_[swing_extreme_index + 1:hi]
    window_close = close[swing_extreme_index + 1:hi]
    return int(np.sum(window_close > window_open))


def max_pullback_decline_atr_ratio(
    close: np.ndarray, atr: np.ndarray, timestamps: pd.DatetimeIndex,
    previous_touch_time: pd.Timestamp, current_touch_time: pd.Timestamp,
    direction: Direction, min_bars: int,
) -> float:
    """The worst SINGLE bar's CLOSE-to-close decline within the pullback,
    expressed as a multiple of ATR so it means the same thing across
    different stocks/price levels.

    Two deliberate design choices, both from real false positives:
      - Uses prior_close - close (LONG) / close - prior_close (SHORT), NOT
        the bar's own low/high. A bar that gapped down and then recovered to
        close back near its high (a hammer/reversal, not a breakdown) must
        NOT count as violent — close-to-close naturally nets the gap against
        any same-day recovery, the same way a hammer's own close_position
        already tells a strong candle apart from a weak one elsewhere in
        this codebase (see ACE 2026-09-11, whose low-based reading alone
        wrongly flagged what was actually a bullish reversal candle).
      - The SIGNAL/touch bar itself is EXCLUDED — it's evaluating whether
        the PULLBACK INTO the touch was violent, not the touch's own candle
        (which is frequently a strong reversal by construction — that's what
        makes it a touch worth trading in the first place).

    Window: primarily `previous_touch_time` to `current_touch_time` (the
    same leg the scanner's own bars_since_previous_touch/swing_bars already
    describes) — but if that gives fewer than `min_bars` bars to actually
    judge (a quick second touch shortly after the first, e.g. TATASTEEL
    2026-05-20's previous touch was only 1 bar earlier), pad further
    backward, past previous_touch_time if needed, until there's a
    reasonable sample. No fixed anchor works for every sequence shape — a
    short window undercounts a genuine violent leg; a window fixed all the
    way back to first_interaction_time overcounts by pulling in unrelated
    history from an entirely different leg of a long-running sequence (see
    ACE 2026-09-11: its sequence started 3 months earlier).

    A THIRD filter, on top of the window: a candidate bar's decline only
    counts if price never closed back above (LONG) / below (SHORT) that
    bar's own close at any point between it and the touch — i.e. the damage
    was never undone. Without this, a long, genuinely quiet-but-choppy
    window (previous_touch_time can be many weeks back if price simply
    didn't touch the SMA again for a while) will almost always contain SOME
    one-off rough day purely from ordinary variation — see ASKAUTOLTD
    2026-09-16: previous_touch_time was 53 bars back, and Aug 7 alone
    crossed the threshold (1.38x), but price fully recovered above that
    close by Aug 31 — three weeks of range/chop, not a sustained fall. A
    stale, since-recovered bar is not evidence of a currently-relevant
    violent pullback, no matter how sharp it looked in isolation.

    NaN if there's no usable window at all, or ATR is unavailable for every
    bar in it."""
    touch_idx = _bar_index(timestamps, current_touch_time)
    prev_idx = _bar_index(timestamps, previous_touch_time)
    if touch_idx is None or prev_idx is None:
        return float("nan")
    end_idx = touch_idx - 1  # exclude the touch/signal bar itself
    start_idx = prev_idx
    if end_idx - start_idx < min_bars:
        start_idx = max(0, end_idx - min_bars)
    if end_idx <= start_idx:
        return float("nan")
    worst = float("-inf")
    for i in range(start_idx + 1, end_idx + 1):
        a = atr[i]
        if np.isnan(a) or a <= 0:
            continue
        prev_close = close[i - 1]
        decline = (prev_close - close[i]) if direction == "LONG" else (close[i] - prev_close)
        if decline <= 0:
            continue
        recovered = (
            np.any(close[i + 1:touch_idx] > close[i]) if direction == "LONG"
            else np.any(close[i + 1:touch_idx] < close[i])
        )
        if recovered:
            continue
        worst = max(worst, decline / a)
    return worst if worst != float("-inf") else float("nan")


def has_violent_pullback_bar(
    close: np.ndarray, atr: np.ndarray, timestamps: pd.DatetimeIndex,
    previous_touch_time: pd.Timestamp, current_touch_time: pd.Timestamp,
    direction: Direction, min_bars: int, max_decline_atr_multiplier: float,
) -> bool:
    """True if max_pullback_decline_atr_ratio clears `max_decline_atr_multiplier`
    — some bar within the pullback (touch bar itself excluded, and never
    since recovered — see max_pullback_decline_atr_ratio) closed that many
    multiples of ATR below (LONG) / above (SHORT) the prior close. NaN
    (no measurable bars) is never violent — same fail-open convention used
    elsewhere for insufficient warmup."""
    ratio = max_pullback_decline_atr_ratio(
        close, atr, timestamps, previous_touch_time, current_touch_time, direction, min_bars,
    )
    return bool(not np.isnan(ratio) and ratio >= max_decline_atr_multiplier)


def recent_peak_decline_atr_ratio(
    close: np.ndarray, atr: np.ndarray, timestamps: pd.DatetimeIndex,
    current_touch_time: pd.Timestamp, direction: Direction, recent_bars: int,
) -> float:
    """A SECOND, independent way a pullback can be violent — not one sharp
    bar, but a genuine SUSTAINED decline over the last `recent_bars` bars
    (touch bar excluded) that no single bar is sharp enough to trip
    max_pullback_decline_atr_ratio on its own (see KTKBANK 2026-09-16 /
    SAGILITY 2026-09-11: several moderate down days in a row, none violent
    individually, but the cumulative slide is real).

    The reference point is whichever close in that window sat FURTHEST in
    the trend's favour (highest close for LONG, lowest for SHORT) — NOT
    necessarily the window's own first bar, since price can still be RISING
    at the very start of a short recent window before the real decline
    begins (see JINDALSAW 2026-08-14: within its own last 5 bars, price rose
    Aug 7->10 before falling Aug 10->13 — anchoring to Aug 7's close alone
    would understate the real fall by folding in that unrelated rise).

    Expressed as a multiple of the window's own average ATR. NaN if there's
    no usable window, or ATR is unavailable throughout it."""
    touch_idx = _bar_index(timestamps, current_touch_time)
    if touch_idx is None:
        return float("nan")
    end_idx = touch_idx - 1  # exclude the touch/signal bar itself
    start_idx = max(0, end_idx - recent_bars + 1)
    if end_idx < start_idx:
        return float("nan")
    window = close[start_idx:end_idx + 1]
    if direction == "LONG":
        decline = np.max(window) - close[end_idx]
    else:
        decline = close[end_idx] - np.min(window)
    atr_window = atr[start_idx:end_idx + 1]
    if np.all(np.isnan(atr_window)):
        return float("nan")
    avg_atr = np.nanmean(atr_window)
    if np.isnan(avg_atr) or avg_atr <= 0:
        return float("nan")
    return decline / avg_atr


def has_sustained_recent_decline(
    close: np.ndarray, atr: np.ndarray, timestamps: pd.DatetimeIndex,
    current_touch_time: pd.Timestamp, direction: Direction,
    recent_bars: int, max_decline_atr_multiplier: float,
) -> bool:
    """True if recent_peak_decline_atr_ratio clears `max_decline_atr_multiplier`
    — a sustained slide over the recent window, not one bad bar. NaN is
    never violent (fail-open, same convention as elsewhere)."""
    ratio = recent_peak_decline_atr_ratio(
        close, atr, timestamps, current_touch_time, direction, recent_bars,
    )
    return bool(not np.isnan(ratio) and ratio >= max_decline_atr_multiplier)


def find_confirmed_swing_pivots(
    prices: np.ndarray, close: np.ndarray, timestamps: pd.DatetimeIndex, lookback: int, kind: str,
) -> list[tuple[pd.Timestamp, float, float]]:
    """Confirmed swing pivots (a bar that is the max/min of itself and
    `lookback` bars on each side) over the WHOLE series — 'confirmed' the
    same way exits.compute_confirmed_swings defines it, just returned as the
    full list of pivot points rather than a per-bar rolled-forward value,
    since is_resistance_flip / is_double_bottom need to check every prior
    pivot, not only the most recent one. Each entry also carries that same
    bar's own CLOSE alongside the extremum price (`prices`) — is_double_bottom
    checks both, not just the wick."""
    n = len(prices)
    pivots: list[tuple[pd.Timestamp, float, float]] = []
    for i in range(lookback, n - lookback):
        window = prices[i - lookback:i + lookback + 1]
        value = prices[i]
        if kind == "high" and value >= np.nanmax(window):
            pivots.append((pd.Timestamp(timestamps[i]), float(value), float(close[i])))
        elif kind == "low" and value <= np.nanmin(window):
            pivots.append((pd.Timestamp(timestamps[i]), float(value), float(close[i])))
    return pivots


def _has_intervening_close_breach(
    close: np.ndarray, timestamps: pd.DatetimeIndex,
    pivot_time: pd.Timestamp, current_touch_time: pd.Timestamp,
    level: float, direction: Direction,
) -> bool:
    """True if any bar's CLOSE strictly between `pivot_time` and
    `current_touch_time` breached `level` — closed below it for a LONG
    double bottom, above it for a SHORT double top. A level that's already
    been broken in between isn't a valid '1st bottom/top' for the CURRENT
    touch to be forming a genuine double against — it wasn't actually held
    as support/resistance the whole time, just revisited afterward."""
    lo = _bar_index(timestamps, pivot_time)
    hi = _bar_index(timestamps, current_touch_time)
    if lo is None or hi is None or hi - lo <= 1:
        return False
    window = close[lo + 1:hi]
    if direction == "LONG":
        return bool(np.any(window < level))
    return bool(np.any(window > level))


def is_double_bottom(
    pivot_points: list[tuple[pd.Timestamp, float, float]],
    current_touch_time: pd.Timestamp, current_extreme: float, current_close: float, tolerance_pct: float,
    *, close: np.ndarray, timestamps: pd.DatetimeIndex, direction: Direction,
    first_interaction_time: Optional[pd.Timestamp] = None,
) -> bool:
    """Any EARLIER swing low (LONG) / swing high (SHORT) — a genuine local
    price extremum from BEFORE the current sequence even started (strictly
    before `first_interaction_time`, when given — a pivot from inside the
    CURRENT active sequence, e.g. this touch's own immediately-preceding
    touch, is trivially close in price just because it's part of the same
    uninterrupted pullback a few bars ago; that's not a genuine double
    bottom/top, it's the same leg), NOT restricted to bars that happened to
    also be a counted touch of the SMA. Only the CURRENT side needs to
    satisfy the scanner's own touch definition (guaranteed by construction,
    since this is always called on an already-qualifying touch) — the
    earlier bottom/top can sit anywhere relative to the MA, PROVIDED it was
    never breached in between (see _has_intervening_close_breach) — a level
    that's already been broken and revisited isn't a genuine double
    bottom/top, just a coincidental price-level revisit.

    Checks BOTH the low/high AND the close of each side against each other
    (4 comparisons per candidate pivot): a "bottom" as a trader reads a chart
    isn't only the exact wick extreme — e.g. today's CLOSE landing right on
    an old candle's LOW is just as much the same level as wick-to-wick."""
    current_prices = [p for p in (current_extreme, current_close) if p != 0]
    if not current_prices:
        return False
    cutoff = first_interaction_time if first_interaction_time is not None else current_touch_time
    for t, prior_extreme, prior_close in pivot_points:
        if t >= cutoff:
            continue
        matched_level = None
        for prior_price in (prior_extreme, prior_close):
            for cur in current_prices:
                if abs(prior_price - cur) / abs(cur) * 100.0 <= tolerance_pct:
                    matched_level = prior_price
                    break
            if matched_level is not None:
                break
        if matched_level is None:
            continue
        if _has_intervening_close_breach(close, timestamps, t, current_touch_time, matched_level, direction):
            continue
        return True
    return False


def touch_test_zone(direction: Direction, open_: float, high: float, low: float, close: float) -> tuple[float, float]:
    """The touch candle's own 'test zone' — the wick-plus-body side that
    actually tested the level, as a [low, high] price range rather than a
    single point: the two LOWEST of the four OHLC values for LONG (always
    includes the low, plus whichever of open/close is smaller), the two
    HIGHEST for SHORT. A bullish LONG touch candle that dipped to its low
    and closed back up near its high tested the level anywhere between that
    low and its open — not only the exact wick."""
    if direction == "LONG":
        return low, min(open_, close)
    return max(open_, close), high


def is_resistance_flip(
    pivots: list[tuple[pd.Timestamp, float, float]],
    current_touch_time: pd.Timestamp, zone_low: float, zone_high: float, tolerance_pct: float,
    *, timestamps: pd.DatetimeIndex, lookback_bars: int, min_touches: int,
) -> bool:
    """Is this touch retesting a genuine, multiple-times-confirmed resistance
    LINE (LONG) / support line (SHORT) — not just one coincidental pivot?
    A single old high/low isn't enough evidence on its own (see IDBI
    2026-01-12: one matching pivot turned out to be this very sequence's own
    anchor, and the touch's own low dipped BELOW several "resistance" levels
    it was supposedly retesting from above — never actually held as
    support). Requires at least `min_touches` separate confirmed pivots,
    each within `tolerance_pct` of the touch candle's own test zone (see
    touch_test_zone) AND within `lookback_bars` bars of the touch — a
    level only counts as an established line if it's been tested more than
    once, recently enough (a fixed rolling window, not the whole unbounded
    history) to still be structurally relevant. `tolerance_pct` buffers
    OUTWARD from the zone's own edges (a pivot doesn't need to fall strictly
    inside it). Checks BOTH the pivot's own extreme (the wick — `price`)
    AND its close against the zone, matching either counts (see IDBI
    2025-10-31 / 2025-11-20: confirmed swing-high pivots whose WICK shot
    well past the zone but whose CLOSE sat right on it — a chart reader
    still reads that as the level being respected, so a wick-only check
    misses genuine touches the way is_double_bottom's low-only check used
    to before it started cross-checking close too)."""
    if zone_low <= 0 or zone_high <= 0:
        return False
    lo_bound = zone_low * (1 - tolerance_pct / 100.0)
    hi_bound = zone_high * (1 + tolerance_pct / 100.0)
    touch_idx = _bar_index(timestamps, current_touch_time)
    if touch_idx is None:
        return False
    earliest_idx = max(0, touch_idx - lookback_bars)
    touches = 0
    for t, price, pivot_close in pivots:
        if t >= current_touch_time:
            continue
        in_zone = (lo_bound <= price <= hi_bound) or (lo_bound <= pivot_close <= hi_bound)
        if not in_zone:
            continue
        piv_idx = _bar_index(timestamps, t)
        if piv_idx is None or piv_idx < earliest_idx:
            continue
        touches += 1
        if touches >= min_touches:
            return True
    return False


def ongoing_range_bar_count(high: np.ndarray, low: np.ndarray, touch_index: int, max_width_points: float) -> int:
    """Starting at `touch_index`, expand a [range_low, range_high] window one
    bar at a time going BACKWARD (each bar's own high/low), for as long as
    the window's width (in raw price points) stays within `max_width_points`
    — a single fixed reference resolved once by the caller (see
    resolve_ongoing_range_max_width_points), not recomputed as the window
    expands. Stops the instant the next (older) bar would push the width
    past that — i.e. the last genuine breakout, which the walk can never
    cross — so the range is always CONTIGUOUS back from the touch, never
    "was in range, broke out, re-entered a new range". Returns the bar count
    (including the touch bar itself) making up this ongoing range."""
    if touch_index < 0 or touch_index >= len(high):
        return 0
    range_high = high[touch_index]
    range_low = low[touch_index]
    count = 1
    i = touch_index - 1
    while i >= 0:
        candidate_high = max(range_high, high[i])
        candidate_low = min(range_low, low[i])
        if candidate_high - candidate_low > max_width_points:
            break
        range_high, range_low = candidate_high, candidate_low
        count += 1
        i -= 1
    return count


def close_range_width(close: np.ndarray, touch_index: int, lookback_bars: int) -> tuple[float, int]:
    """The aggregate CLOSING-price range (max close - min close) over the
    FIXED window of `lookback_bars` bars ending at `touch_index` (touch bar
    inclusive) — deliberately ignores each bar's own intraday high/low wick,
    so one unusually volatile day (a wide wick, but an orderly close) can't
    by itself disqualify an otherwise-tight consolidation the way
    ongoing_range_bar_count's bar-by-bar wick walk can (see PNBHOUSING:
    2026-09-10 had a single 53-point-range day that alone busted the wick
    walk, even though the closes over the surrounding weeks stayed tight).
    Returns (width, bars_used) — bars_used < lookback_bars means there
    wasn't enough history yet, which the caller should treat as inconclusive."""
    if touch_index < 0 or touch_index >= len(close):
        return float("nan"), 0
    start = max(0, touch_index - lookback_bars + 1)
    window = close[start:touch_index + 1]
    return float(np.max(window) - np.min(window)), len(window)


def is_tight_close_range(close: np.ndarray, touch_index: Optional[int], lookback_bars: int, max_width_points: Optional[float]) -> bool:
    """Is the CLOSING price over the last `lookback_bars` bars (touch bar
    inclusive) within `max_width_points` of each other? See close_range_width
    for why this is a useful companion to ongoing_range_bar_count rather
    than a replacement — the two catch different failure modes."""
    if touch_index is None or max_width_points is None:
        return False
    width, bars_used = close_range_width(close, touch_index, lookback_bars)
    if bars_used < lookback_bars:
        return False
    return width <= max_width_points


def resolve_ongoing_range_max_width_points(
    mode: str, close_at_touch: float, atr_at_touch: Optional[float],
    max_width_pct: float, max_width_atr_multiplier: float,
) -> Optional[float]:
    """The fixed width budget (raw price points) ongoing_range_bar_count
    walks within, resolved once per touch. ATR mode (default) scales with
    whatever the bars' own recent volatility is — so it means the same thing
    on a 1min, 15min, or daily chart with no re-tuning; percent mode is a
    simpler but timeframe-sensitive alternative. None if ATR mode is
    selected but ATR isn't available yet (insufficient warmup)."""
    if mode == "atr":
        if atr_at_touch is None or np.isnan(atr_at_touch):
            return None
        return atr_at_touch * max_width_atr_multiplier
    return close_at_touch * (max_width_pct / 100.0)


def resolve_tolerance_pct(
    mode: str, close_at_touch: float, atr_at_touch: Optional[float],
    tolerance_pct: float, atr_multiplier: float,
) -> float:
    """Convert a percent/atr tolerance choice into a single effective
    percent-of-price value, resolved once per touch — lets an ATR-mode
    tolerance plug into is_double_bottom/is_resistance_flip's existing
    percent-based comparisons unchanged. NaN ATR (warmup) resolves to NaN,
    which always fails a "<=" comparison — fail-open, same convention as
    resolve_ongoing_range_max_width_points above."""
    if mode == "atr":
        if atr_at_touch is None or np.isnan(atr_at_touch) or close_at_touch == 0:
            return float("nan")
        return atr_at_touch * atr_multiplier / abs(close_at_touch) * 100.0
    return tolerance_pct


def is_range_bound(
    close: np.ndarray, current_touch_index: Optional[int],
    max_width_points: Optional[float], close_range_lookback_bars: int,
) -> bool:
    """Is this touch sitting inside an ongoing tight consolidation? Delegates
    entirely to the CLOSING-price fixed window check (is_tight_close_range) —
    the wick-by-wick walk (ongoing_range_bar_count) was dropped from this
    decision: it let a touch whose own candle is a wide-range breakout bar
    still count as "still in the range" just because a handful of bars right
    before it (plus that same breakout bar's own wick) happened to squeeze
    under the ATR budget (see THYROCARE/CUB 2026-08-31 — both passed the wick
    walk at/near the bare minimum bar count while the close-only view over the
    same stretch told a different story). ongoing_range_bar_count remains
    available as a standalone helper; it's just no longer part of this
    verdict."""
    return is_tight_close_range(close, current_touch_index, close_range_lookback_bars, max_width_points)


def classify_setup_quality(
    swing_bars: Optional[int],
    double_bottom: bool, resistance_flip: bool, range_bound: bool, violent_pullback: bool,
    config: SetupQualityConfig,
) -> str:
    """Combine the individual checks into one BEST / OK / BAD verdict: three
    absolute overrides checked first, then SCORE the remaining signals for
    everything else.

    Overrides (checked in this order — the first one that applies wins,
    regardless of anything else):
      1. violent_pullback    -> BAD, always. A single panicked gap-and-plunge
         bar (see has_violent_pullback_bar) disqualifies the setup outright —
         checked BEFORE double_bottom (next) on purpose: a genuinely violent
         fall shouldn't get rescued into BEST just because it also happens to
         land on an old double-bottom level (see TATASTEEL 2026-05-20, which
         is both).
      2. range_bound         -> OK, always. A live consolidation is a caution
         flag nothing else can lift above OK OR drop below — not even a
         double bottom (checked next): the touch is still just sitting
         inside chop regardless of what price level that chop is at.
      3. double_bottom        -> BEST, always. Strong enough structural
         evidence on its own, regardless of how the other signals land —
         including a swing length that would otherwise be a strong negative.

    Otherwise (none of the above apply — note that means violent_pullback is
    already known False here), score the 3 remaining signals:

    POSITIVE (one point each): swing length in the ideal band
    (swing_best_min_bars..swing_best_max_bars); NOT violent_pullback (already
    guaranteed true by this point, since a True would've returned BAD above —
    kept explicit here, not hardcoded, so BAD stays reserved for a genuinely
    violent fall rather than sneaking back in from a bar-COLOR-only check
    like the old pullback_bullish_count>0 signal this replaced: a slow,
    entirely-red-but-orderly multi-day grind — JINDALSAW, EMBDL — was scoring
    the same BAD as a violent one-bar crash, which the color count alone
    can't tell apart); resistance_flip.
    NEGATIVE (one point): swing far too long (>= swing_bad_min_bars) — no
    longer an automatic override, just one vote enough positives can outweigh.

      BAD  — zero of the 3 positives. Unreachable via scoring alone now that
             one of the three is always-true here — BAD is effectively
             reserved for the violent_pullback override above; scoring alone
             floors out at OK.
      BEST — all 3 positives present AND the swing isn't too long.
      OK   — everything in between.

    `swing_bars is None` (the sequence's very first touch has no prior swing
    to measure) simply can't satisfy the swing-length positive or the
    too-long negative — scored on the remaining two checks alone."""
    if violent_pullback:
        return "BAD"
    if range_bound:
        return "OK"
    if double_bottom:
        return "BEST"

    swing_ideal = swing_bars is not None and config.swing_best_min_bars <= swing_bars <= config.swing_best_max_bars
    swing_too_long = swing_bars is not None and swing_bars >= config.swing_bad_min_bars

    positives = sum([swing_ideal, not violent_pullback, resistance_flip])

    if positives == 0:
        return "BAD"
    if positives == 3 and not swing_too_long:
        return "BEST"
    return "OK"


# ── orchestration: enrich scan_history's qualifying touches ─────────────────

def enrich_touches_with_setup_quality(
    bars: pd.DataFrame,
    touches: pd.DataFrame,
    scanner_config: ScannerConfig,
    setup_quality_config: SetupQualityConfig,
) -> pd.DataFrame:
    """`bars` = the same raw OHLC bars given to scan_history for this symbol
    (one symbol, all directions). `touches` = scan_history's qualifying
    touches for this symbol. Returns `touches` with SETUP_QUALITY_COLUMNS
    attached, in the same row order.

    Recomputes indicators on the raw `bars` (a cheap second pass — no need to
    re-run the touch-counting state machine itself) so it has the SMA/ATR and
    swing-pivot structure the swing-extreme / pivot / range-bound checks need
    — none of which scan_history's qualifying-touches-only output carries.
    """
    if touches.empty:
        return touches.assign(**{col: pd.Series(dtype=object) for col in SETUP_QUALITY_COLUMNS})

    working = _select_completed_candles(bars, scanner_config.use_only_completed_candles)
    working = working.sort_values("timestamp").reset_index(drop=True)
    working = calculate_indicators(working, scanner_config.sma_length)

    timestamps = pd.DatetimeIndex(working["timestamp"])
    open_ = working["open"].to_numpy(dtype=float)
    high = working["high"].to_numpy(dtype=float)
    low = working["low"].to_numpy(dtype=float)
    close = working["close"].to_numpy(dtype=float)
    sma = working["sma_44"].to_numpy(dtype=float)
    atr = compute_atr(working, scanner_config.atr_length).to_numpy(dtype=float)

    lookback = setup_quality_config.resistance_flip_swing_lookback
    results: dict[tuple[str, pd.Timestamp], dict] = {}

    for direction in ("LONG", "SHORT"):
        if direction not in set(touches["direction"]):
            continue
        # Resistance/support-flip pivots: the OPPOSITE extremum to this
        # touch's own side (old highs for a LONG touch, old lows for SHORT).
        resistance_pivots = find_confirmed_swing_pivots(
            high if direction == "LONG" else low, close, timestamps, lookback,
            kind="high" if direction == "LONG" else "low",
        )
        # Double-bottom/top pivots: the SAME extremum as this touch's own
        # side (prior swing LOWS for a LONG touch, prior swing HIGHS for
        # SHORT) — genuine local price structure, not restricted to bars
        # that happened to also be a counted touch of the SMA.
        bottom_pivots = find_confirmed_swing_pivots(
            low if direction == "LONG" else high, close, timestamps, lookback,
            kind="low" if direction == "LONG" else "high",
        )

        for _, row in touches[touches["direction"] == direction].iterrows():
            current_touch_time = row["current_touch_time"]
            previous_touch_time = row["previous_touch_time"]
            first_interaction_time = row["first_interaction_time"]
            raw_swing_bars = row["bars_since_previous_touch"]
            swing_bars = None if pd.isna(raw_swing_bars) else int(raw_swing_bars)
            current_extreme = float(row["low"] if direction == "LONG" else row["high"])
            current_close = float(row["close"])

            swing_extreme_idx = find_swing_extreme_index(
                close, sma, timestamps, direction, first_interaction_time, current_touch_time,
            )
            current_idx = _bar_index(timestamps, current_touch_time)
            atr_at_touch = atr[current_idx] if current_idx is not None else float("nan")
            bullish_count = pullback_bullish_count(open_, close, timestamps, swing_extreme_idx, current_touch_time)
            dbl_bottom_tolerance_pct = resolve_tolerance_pct(
                setup_quality_config.double_bottom_tolerance_mode, current_close, atr_at_touch,
                setup_quality_config.double_bottom_tolerance_pct,
                setup_quality_config.double_bottom_tolerance_atr_multiplier,
            )
            dbl_bottom = is_double_bottom(
                bottom_pivots, current_touch_time, current_extreme, current_close,
                dbl_bottom_tolerance_pct,
                close=close, timestamps=timestamps, direction=direction,
                first_interaction_time=first_interaction_time,
            )
            zone_low, zone_high = touch_test_zone(direction, float(row["open"]), float(row["high"]), float(row["low"]), current_close)
            res_flip_tolerance_pct = resolve_tolerance_pct(
                setup_quality_config.resistance_flip_tolerance_mode, current_close, atr_at_touch,
                setup_quality_config.resistance_flip_tolerance_pct,
                setup_quality_config.resistance_flip_tolerance_atr_multiplier,
            )
            res_flip = is_resistance_flip(
                resistance_pivots, current_touch_time, zone_low, zone_high,
                res_flip_tolerance_pct,
                timestamps=timestamps,
                lookback_bars=setup_quality_config.resistance_flip_lookback_bars,
                min_touches=setup_quality_config.resistance_flip_min_touches,
            )
            max_width_points = None
            if current_idx is not None:
                max_width_points = resolve_ongoing_range_max_width_points(
                    setup_quality_config.ongoing_range_width_mode, close[current_idx], atr[current_idx],
                    setup_quality_config.ongoing_range_max_width_pct, setup_quality_config.ongoing_range_max_width_atr_multiplier,
                )
            range_bound = is_range_bound(
                close, current_idx, max_width_points, setup_quality_config.close_range_lookback_bars,
            )
            violent_pullback = has_violent_pullback_bar(
                close, atr, timestamps, previous_touch_time, current_touch_time, direction,
                setup_quality_config.violent_pullback_min_bars,
                setup_quality_config.violent_pullback_max_decline_atr_multiplier,
            ) or has_sustained_recent_decline(
                close, atr, timestamps, current_touch_time, direction,
                setup_quality_config.violent_pullback_recent_bars,
                setup_quality_config.violent_pullback_recent_decline_atr_multiplier,
            )
            quality = classify_setup_quality(
                swing_bars, dbl_bottom, res_flip, range_bound, violent_pullback, setup_quality_config,
            )

            results[(direction, current_touch_time)] = {
                "swing_bars": swing_bars,
                "pullback_bullish_count": bullish_count,
                "is_double_bottom": dbl_bottom,
                "is_resistance_flip": res_flip,
                "is_range_bound": range_bound,
                "is_violent_pullback": violent_pullback,
                "setup_quality": quality,
            }

    out = touches.copy()
    for col in SETUP_QUALITY_COLUMNS:
        out[col] = [
            results.get((d, t), {}).get(col)
            for d, t in zip(out["direction"], out["current_touch_time"])
        ]
    return out
