"""Candidate-signal generation for the 44-SMA uptrend-pullback scanner strategy.

Thin adapter only: scanner.py owns all pattern detection (finding qualifying
touch events) and knows nothing about prices/orders. This module's only job is
to turn each qualifying TouchEvent into a tradeable candidate row — a
breakout-continuation trigger/stop/target computed from the touch candle's own
OHLC, using the existing (unchanged) EntryConfig/StopLossConfig/TargetConfig —
so the backtest engine can consume it exactly as before. It carries no notion
of "is a setup already pending" or "is a position already open" — that state
belongs to the backtest engine (backtest/engine.py).
"""
from __future__ import annotations

import numpy as np
import dataclasses

import pandas as pd

from .config.schema import MarketConfig, StrategyConfig, TradeFilterConfig
from .exits import compute_atr
from .resample import is_daily_timeframe, is_monthly_timeframe, is_weekly_timeframe, resample_ohlc
from .backtest.engine import ScannerBacktester
from .scanner import is_strong_directional_candle, scan_history

CANDIDATE_COLUMNS = [
    "timestamp", "symbol", "direction", "timeframe",
    "sma_44", "close_distance_pct", "low_distance_pct", "high_distance_pct", "ma_slope_pct", "trend_class",
    # Higher-timeframe trend_class context (see higher_timeframes) -- always
    # present for a stable schema, but only ever populated for timeframes
    # ABOVE whatever `timeframe` this run actually used; the rest stay None.
    "min5_trend_class", "min15_trend_class", "min30_trend_class", "hourly_trend_class",
    "daily_trend_class", "weekly_trend_class", "monthly_trend_class",
    "touch_count", "touch_number",
    "first_interaction_time", "previous_touch_time", "current_touch_time",
    "bars_since_previous_touch", "max_distance_reached_pct", "wicked_through_sma", "is_new_event",
    "signal_open", "signal_high", "signal_low", "signal_close",
    "trigger_price", "stop_loss", "target", "daily_volume",
    "ma_length", "signal_ma", "ma_alignment",
    "confluence_gap_skipped", "confluence_gap_ma", "confluence_gap_atr",
]


def _buffer(
    close: pd.Series, buffer_type: str, points: float, pct: float,
    atr: "pd.Series | None" = None, atr_multiplier: float = 0.0,
) -> pd.Series:
    if buffer_type == "atr":
        # NaN ATR (still in warmup) -> zero buffer, never a blocker — same
        # fail-open convention as the 200-SMA filter (see
        # is_rejected_by_ma200_filter's own docstring).
        return (atr * atr_multiplier).fillna(0.0)
    if buffer_type == "pct":
        return close * (pct / 100.0)
    return pd.Series(points, index=close.index)


def _resolve_pct_tolerance(
    mode: str, tolerance_pct: float, atr_multiplier: float, close: float, atr_value: float,
) -> float:
    """Convert a config's percent/atr tolerance choice into a single
    effective percent-of-price value, computed once per touch from THAT
    touch's own close/ATR — lets ATR-mode tolerances plug into an existing
    percent-based comparison (e.g. is_rejected_by_ma200_filter's own
    nearby_tolerance_pct) unchanged. NaN ATR (warmup) or a zero close
    resolves to NaN, which always fails a "<=" comparison — fail-open, same
    convention as every other trade-taking filter here."""
    if mode == "atr":
        if pd.isna(atr_value) or close == 0:
            return float("nan")
        return atr_value * atr_multiplier / abs(close) * 100.0
    return tolerance_pct


def _round_to_whole_number_if_near(
    trigger_price: np.ndarray, is_long: np.ndarray, tolerance_pct: float,
) -> np.ndarray:
    """Round the trigger to the nearest whole rupee, but only on the SAFE
    side — up (ceil) for LONG, down (floor) for SHORT — and only when that
    whole number is within `tolerance_pct` of the raw price. Rounding this
    way can only ever make the trigger a little harder to hit, never easier
    (e.g. 1189.31 -> 1190 for a LONG, not down to 1189). `tolerance_pct` is
    relative to price, so it scales across very differently priced stocks —
    at 0.1%, a ~1189-rupee stock rounds whenever within ~1.2 points of a
    whole number (in practice: almost always, since the whole-number grid
    itself is only 1 point wide there), while a ~45-rupee stock only rounds
    within a few paise."""
    candidate = np.where(is_long, np.ceil(trigger_price), np.floor(trigger_price))
    diff_pct = np.abs(candidate - trigger_price) / np.abs(trigger_price) * 100.0
    return np.where(diff_pct <= tolerance_pct, candidate, trigger_price)


def _atr_reference(bars: pd.DataFrame, atr_length: int) -> pd.DataFrame:
    """Backward-looking ATR, one row per bar in `bars`, keyed by timestamp —
    same merge-by-timestamp convention as _stop_reference/_ma200_reference,
    for the entry/SL "atr" buffer mode (see EntryConfig.entry_buffer_type)."""
    ordered = bars.sort_values("timestamp")
    return pd.DataFrame({
        "timestamp": ordered["timestamp"],
        "_atr": compute_atr(ordered, atr_length).to_numpy(),
    })


def _daily_volume_reference(bars: pd.DataFrame) -> pd.DataFrame:
    """Total DAILY volume, one row per bar in `bars`, keyed by timestamp --
    always the FULL day's volume regardless of what timeframe `bars` itself
    is resampled to. For a daily-timeframe scan, each row already IS one
    calendar day, so this is trivially that row's own volume; for an
    intraday scan (5min/15min/...), every bar sharing that calendar date is
    summed together (resampling always SUMS volume across intervals, never
    averages, so this reconstructs the true daily total without needing a
    separate native-daily fetch). A still-forming session's bars-so-far only
    sum to a partial day, same as any live "volume today" figure would."""
    ordered = bars.sort_values("timestamp")
    trade_date = ordered["timestamp"].dt.date
    daily_totals = ordered.groupby(trade_date)["volume"].transform("sum")
    return pd.DataFrame({
        "timestamp": ordered["timestamp"],
        "_daily_volume": daily_totals.to_numpy(),
    })


# ── Higher-timeframe trend-class context ─────────────────────────────────────
# A separate concern from everything above: not a trade-taking FILTER, purely
# an informational overlay showing what the 44-SMA is doing on the timeframes
# ABOVE whatever the scan itself runs on (e.g. a daily scan also shows the
# weekly/monthly trend_class) — never affects touch_count or candidate
# qualification, exactly like trend_class itself on the base timeframe.

_TIMEFRAME_LADDER = ["5min", "15min", "30min", "60min", "1D", "1W", "1M"]
_TIMEFRAME_COLUMN_PREFIX = {
    "5min": "min5", "15min": "min15", "30min": "min30", "60min": "hourly",
    "1D": "daily", "1W": "weekly", "1M": "monthly",
}


def _canonical_timeframe(timeframe: str) -> "str | None":
    """Map any recognized spelling onto its rung in _TIMEFRAME_LADDER --
    None for anything not on the standard ladder (e.g. '1min', '75min')."""
    if is_monthly_timeframe(timeframe):
        return "1M"
    if is_weekly_timeframe(timeframe):
        return "1W"
    if is_daily_timeframe(timeframe):
        return "1D"
    try:
        td = pd.Timedelta(timeframe)
    except ValueError:
        return None
    for label in ("5min", "15min", "30min", "60min"):
        if td == pd.Timedelta(label):
            return label
    return None


def higher_timeframes(base_timeframe: str) -> list[str]:
    """Every standard timeframe strictly ABOVE `base_timeframe` in the fixed
    5min/15min/30min/60min/1D/1W/1M ladder — e.g. '1D' -> ['1W', '1M'];
    '60min' -> ['1D', '1W', '1M']. Empty if base_timeframe isn't on the
    ladder at all, or is already at the top ('1M')."""
    canon = _canonical_timeframe(base_timeframe)
    if canon is None:
        return []
    idx = _TIMEFRAME_LADDER.index(canon)
    return _TIMEFRAME_LADDER[idx + 1:]


def _classify_trend(
    close: np.ndarray, sma_length: int, ma_slope_lookback: int, min_ma_slope_pct: float, sign: float,
) -> np.ndarray:
    """trend_class ("A"/"B"/"C") for a single direction (sign=+1.0 LONG,
    -1.0 SHORT) — replicates scanner.py's own Rule-5 classification exactly
    (see process_touch_state's trend_class computation), for an arbitrary
    close series rather than the base-timeframe one scanner.py itself
    works from. Kept as a separate, deliberately duplicated formula rather
    than importing scanner.py internals — this is the same small, stable
    piece of math computed on a DIFFERENT (resampled) series, not a
    variation on the rule itself."""
    sma = pd.Series(close).rolling(window=sma_length, min_periods=sma_length).mean().to_numpy()
    n = len(close)
    with np.errstate(invalid="ignore", divide="ignore"):
        sma_shifted = np.full(n, np.nan)
        sma_shifted[ma_slope_lookback:] = sma[:-ma_slope_lookback]
        raw_slope_pct = (sma - sma_shifted) / sma_shifted * 100.0
    adj_slope_pct = sign * raw_slope_pct
    sma_bar_diff = np.diff(sma, prepend=np.nan)
    adj_sma_bar_diff = sign * sma_bar_diff
    with np.errstate(invalid="ignore"):
        rolling_min_diff = (
            pd.Series(adj_sma_bar_diff).rolling(window=ma_slope_lookback, min_periods=ma_slope_lookback)
            .min().to_numpy()
        )
    is_monotonic = rolling_min_diff > 0
    slope_ok = (adj_slope_pct > min_ma_slope_pct) & is_monotonic
    with np.errstate(invalid="ignore"):
        falling = adj_slope_pct < -min_ma_slope_pct
    return np.where(slope_ok, "A", np.where(falling, "C", "B"))


def _higher_timeframe_trend_class_reference(
    bars: pd.DataFrame, base_timeframe: str, market: MarketConfig,
    sma_length: int, ma_slope_lookback: int, min_ma_slope_pct: float,
) -> pd.DataFrame:
    """For each timeframe ABOVE `base_timeframe` (see higher_timeframes),
    the LAST FULLY COMPLETED period's trend_class as of each bar in `bars`
    — computed separately per direction (LONG/SHORT need opposite signs,
    see _classify_trend) and aligned via an as-of (backward) merge on
    resample.py's own bar_close_time, so a still-forming higher-timeframe
    period is NEVER used (no lookahead): a Wednesday daily bar gets last
    week's finished trend_class, not this week's still-forming one. One row
    per bar in `bars`, keyed by timestamp, with two columns per higher
    timeframe (e.g. "_weekly_class_long"/"_weekly_class_short") — the
    caller picks whichever matches each touch's own direction."""
    ordered = bars.sort_values("timestamp").reset_index(drop=True)
    out = pd.DataFrame({"timestamp": ordered["timestamp"]})
    for htf in higher_timeframes(base_timeframe):
        prefix = _TIMEFRAME_COLUMN_PREFIX[htf]
        long_col, short_col = f"_{prefix}_class_long", f"_{prefix}_class_short"
        resampled = resample_ohlc(
            ordered, htf, continuous_session=market.continuous_session,
            session_start=market.market_start_time, session_end=market.market_end_time,
        )
        if resampled.empty:
            out[long_col] = None
            out[short_col] = None
            continue
        close = resampled["close"].to_numpy(dtype=float)
        htf_ref = pd.DataFrame({
            "bar_close_time": resampled["bar_close_time"],
            long_col: _classify_trend(close, sma_length, ma_slope_lookback, min_ma_slope_pct, sign=1.0),
            short_col: _classify_trend(close, sma_length, ma_slope_lookback, min_ma_slope_pct, sign=-1.0),
        }).sort_values("bar_close_time")
        merged = pd.merge_asof(
            ordered[["timestamp"]], htf_ref, left_on="timestamp", right_on="bar_close_time", direction="backward",
        )
        out[long_col] = merged[long_col].to_numpy()
        out[short_col] = merged[short_col].to_numpy()
    return out


def _stop_reference(bars: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    """For every bar, the lowest low / highest high across that bar AND the
    `lookback_bars` immediately before it — the wider (further) of the touch
    candle's own extreme and a nearby swing that reaches past it. Purely
    backward-looking (a trailing rolling window anchored at each row), so no
    lookahead. One row per bar in `bars`, keyed by timestamp for the caller
    to merge onto its (sparser) touches frame."""
    ordered = bars.sort_values("timestamp")
    window = lookback_bars + 1
    return pd.DataFrame({
        "timestamp": ordered["timestamp"],
        "_stop_low": ordered["low"].rolling(window=window, min_periods=1).min(),
        "_stop_high": ordered["high"].rolling(window=window, min_periods=1).max(),
    })


def _ma200_reference(bars: pd.DataFrame, ma_length: int) -> pd.DataFrame:
    """Backward-looking rolling mean of close over `ma_length` bars — the
    longer SMA config.ma200_filter needs (see is_rejected_by_ma200_filter).
    NaN until ma_length bars have accumulated (no lookahead; a touch that
    lands during that warmup never gets rejected by the filter below — see
    its own NaN handling)."""
    ordered = bars.sort_values("timestamp")
    return pd.DataFrame({
        "timestamp": ordered["timestamp"],
        "_ma200": ordered["close"].rolling(window=ma_length, min_periods=ma_length).mean(),
    })


def is_rejected_by_ma200_filter(
    direction: str, sma_44: float, ma200: float, low: float, high: float, nearby_tolerance_pct: float,
) -> bool:
    """See Ma200FilterConfig's own docstring for the full rule. Only ever
    active when the 200-SMA sits on the far side of the 44-SMA from price —
    below it for LONG, above it for SHORT; when that holds, rejects the
    touch outright if the 200-SMA is within `nearby_tolerance_pct` of the
    touch candle's own low (LONG) / high (SHORT) — the side that actually
    tested the level, same convention as the 44-SMA's own touch_tolerance_pct
    check, not the close (which can sit well clear of the 200-SMA even on a
    bar whose wick reached right into it). NaN ma200/sma_44 (not enough
    warmup yet) never rejects — the filter is simply not applicable."""
    ref_price = low if direction == "LONG" else high
    if pd.isna(ma200) or pd.isna(sma_44) or ref_price == 0:
        return False
    applies = ma200 < sma_44 if direction == "LONG" else ma200 > sma_44
    if not applies:
        return False
    return abs(ma200 - ref_price) / abs(ref_price) * 100.0 <= nearby_tolerance_pct


def is_rejected_by_trend_class_filter(
    direction: str, trend_class: str, ma_slope_pct: float, config: TradeFilterConfig,
) -> bool:
    """See TradeFilterConfig's own docstring for the full rule. "B" touches
    are dropped outright when exclude_trend_class_b; "C" touches additionally
    need their direction-adjusted slope (positive = favourable, matching
    scanner.py's own sign convention) to be no worse than
    min_trend_class_c_slope_pct. "A" touches are never rejected here. NaN
    ma_slope_pct (warmup) never rejects a "C" touch — fail-open, same
    convention as every other trade-taking filter here (though in practice
    a touch can't be labelled "C" at all before the slope is available)."""
    if trend_class == "B" and config.exclude_trend_class_b:
        return True
    if trend_class == "C" and not pd.isna(ma_slope_pct):
        sign = 1.0 if direction == "LONG" else -1.0
        adj_slope_pct = sign * ma_slope_pct
        if adj_slope_pct < config.min_trend_class_c_slope_pct:
            return True
    return False


def is_rejected_by_daily_volume_filter(daily_volume: float, min_daily_volume: float) -> bool:
    """NaN daily_volume (missing data) never rejects — fail-open, same
    convention as every other trade-taking filter here."""
    if pd.isna(daily_volume):
        return False
    return daily_volume < min_daily_volume


def pending_setup_status(candidate: "pd.Series | dict", bars_after: pd.DataFrame, setup_expiry_bars: int) -> str:
    """Would this candidate's setup still be a live, un-entered order as of
    the LAST bar in `bars_after` (the bars strictly AFTER its signal bar)?
    Replays the backtest engine's own pending-setup lifecycle bar by bar —
    literally its _resolve_pending_trigger — so "actionable" here can never
    disagree with what the backtest would do: "triggered" (entry already
    filled/passed), "invalidated" (signal candle's opposite extreme broken
    before entry), "expired" (waited setup_expiry_bars without triggering),
    or "actionable" (still waiting, entry not yet hit)."""
    sig = dict(candidate)
    waited = 0
    for _, rec in bars_after.iterrows():
        waited += 1
        triggered, invalidated = ScannerBacktester._resolve_pending_trigger(sig, rec)
        if triggered:
            return "triggered"
        if invalidated:
            return "invalidated"
        if waited >= setup_expiry_bars:
            return "expired"
    return "actionable"


def _ma_alignment_reference(bars: pd.DataFrame, scanner_cfg) -> pd.DataFrame:
    """Per bar: how many of the configured signal MAs (scanner.ma_lengths())
    are rising / falling over `ma_slope_lookback` bars (net slope beyond
    +/- min_ma_slope_pct; a still-warming-up MA counts as neither)."""
    ordered = bars.sort_values("timestamp")
    close = ordered["close"]
    lookback = scanner_cfg.ma_slope_lookback
    lengths = scanner_cfg.ma_lengths()
    rising = np.zeros(len(ordered), dtype=int)
    falling = np.zeros(len(ordered), dtype=int)
    for n in lengths:
        sma = close.rolling(window=n, min_periods=n).mean()
        slope_pct = ((sma / sma.shift(lookback)) - 1.0) * 100.0
        rising += (slope_pct > scanner_cfg.min_ma_slope_pct).to_numpy()
        falling += (slope_pct < -scanner_cfg.min_ma_slope_pct).to_numpy()
    return pd.DataFrame({
        "timestamp": ordered["timestamp"].to_numpy(),
        "_ma_rising": rising, "_ma_falling": falling, "_ma_total": len(lengths),
    })


def ma_alignment_letters(direction, rising, falling, total) -> np.ndarray:
    """'A' when every MA agrees with the trade (rising for LONG, falling for
    SHORT), then one letter later per MA that does not: B = 1 disagrees,
    C = 2, D = 3, ... (grows with the number of MAs)."""
    direction = np.asarray(direction)
    agree = np.where(direction == "LONG", np.asarray(rising), np.asarray(falling))
    not_agreeing = np.asarray(total) - agree
    return np.array([chr(ord("A") + int(k)) for k in not_agreeing], dtype=object)


def generate_scan_candidates(bars: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    """Candidates from every configured signal MA (scanner.signal_ma_lengths;
    default just sma_length). Each MA runs the full single-MA pipeline below
    as its own signal MA, confluence-checked against every OTHER MA in the
    set (and ma200_filter.ma_length). If several MAs yield a candidate on the
    same bar and direction, only the one FARTHEST from price survives — the
    lowest MA for LONG, the highest for SHORT. `ma_length` / `signal_ma`
    columns say which MA (and its value) a candidate came from."""
    lengths = config.scanner.ma_lengths()
    frames = []
    for n in lengths:
        cfg_n = dataclasses.replace(config, scanner=dataclasses.replace(config.scanner, sma_length=n))
        reference = sorted(({*lengths, config.ma200_filter.ma_length}) - {n})
        frame = _generate_candidates_for_ma(bars, cfg_n, reference)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)
    out = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
    if len(frames) > 1:
        far_key = np.where(out["direction"] == "LONG", out["signal_ma"], -out["signal_ma"])
        out = (
            out.assign(_far=far_key)
            .sort_values(["timestamp", "direction", "_far"], kind="stable")
            .drop_duplicates(["timestamp", "direction"], keep="first")
            .drop(columns="_far")
        )
    out = out.sort_values("timestamp", kind="stable").reset_index(drop=True)
    align = _ma_alignment_reference(bars, config.scanner)
    out = out.merge(align, on="timestamp", how="left")
    out["ma_alignment"] = ma_alignment_letters(
        out["direction"], out["_ma_rising"].fillna(0), out["_ma_falling"].fillna(0), out["_ma_total"].fillna(len(lengths)),
    )
    return out[CANDIDATE_COLUMNS]


def _generate_candidates_for_ma(
    bars: pd.DataFrame, config: StrategyConfig, reference_ma_lengths: "list[int]",
) -> pd.DataFrame:
    """`bars` must carry timestamp/open/high/low/close (and, for a multi-symbol
    caller, `symbol`) for ONE symbol, already resampled to
    config.timeframes.timeframe and sorted by timestamp. Returns one row per
    qualifying touch event (touch_count >= scanner.minimum_touch_number),
    with trigger/stop/target computed as:
      LONG:  trigger = touch high + entry buffer
             stop    = the WIDEST (furthest from entry) of: the lowest low
                       across the touch candle and stop_loss.stop_lookback_bars
                       bars before it, and the 44-SMA's own value at the
                       touch (the stop must never sit INSIDE the support
                       line itself) — minus SL buffer
      SHORT: stop    = mirror (highest high / 44-SMA), plus SL buffer
             (entry/SL buffer: fixed points, a flat % of close, or
             atr_multiplier * ATR — see EntryConfig.entry_buffer_type; the
             ATR mode is a false-breakout margin that scales with the
             stock's own recent volatility instead of a one-size-fits-all
             fixed number)
      target = EITHER signal high (LONG) / low (SHORT) + risk_reward *
               (signal candle's own high - low) ["signal_candle_range",
               default — anchored to the signal candle's own extreme, NOT
               trigger_price, so an entry buffer shifts the entry without
               also dragging the target away from it] OR trigger +/-
               risk_reward * (trigger - stop) ["dynamic_stop_multiple" —
               target tracks the actual stop distance, trigger-anchored by
               design here] — see config.target.target_mode.
    i.e. "wait for price to break back in the trend's favour past the touch
    candle's own extreme before committing, but protect the stop against a
    nearby swing (or the support/resistance line itself) the touch candle
    alone doesn't capture" — this is order-execution convention, unrelated
    to the scanner's own pattern-qualification logic. This stop value is
    used once a position is actually OPEN; backtest/engine.py's PRE-ENTRY
    invalidation check (still a pending setup) deliberately uses a
    DIFFERENT, narrower level instead — the signal candle's own opposite
    extreme (signal_low for LONG, signal_high for SHORT), not this widened
    stop — see its _resolve_pending_trigger for why.

    A touch that qualifies (per the scanner's own touch-counting rules) still
    only becomes a candidate here if:
      - its own candle is a strong directional bar (see
        scanner.is_strong_directional_candle / config.signal_candle),
      - it doesn't fail the 200-SMA confluence filter (see
        is_rejected_by_ma200_filter / config.ma200_filter),
      - it isn't "B"-class (flattish MA), and if it's "C"-class (MA against
        the trade) the slope isn't worse than a configured floor (see
        is_rejected_by_trend_class_filter / config.trade_filters),
      - the day's volume clears a configured floor (see
        is_rejected_by_daily_volume_filter / config.trade_filters), and
      - the computed trigger_price clears a configured floor (see
        config.trade_filters.min_entry_price).
    All of these are trade-taking filters, not part of touch detection: a touch
    that fails any of them is simply skipped as a trade opportunity, while
    the scanner's touch_count/sequence carries on completely unaffected (the
    next qualifying touch, whenever it happens, is judged fresh on its own
    candle).
    """
    if bars.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    symbol = bars["symbol"].iloc[0] if "symbol" in bars.columns else ""
    touches = scan_history(bars, config.scanner, symbol=symbol, timeframe=config.timeframes.timeframe)
    if touches.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    signal_candle_cfg = config.signal_candle
    is_strong = touches.apply(
        lambda row: is_strong_directional_candle(
            row["direction"], row["open"], row["high"], row["low"], row["close"],
            signal_candle_cfg.min_close_position,
            signal_candle_cfg.allow_hammer_exception,
            signal_candle_cfg.hammer_max_opposite_wick_ratio,
            signal_candle_cfg.hammer_max_body_ratio,
        ),
        axis=1,
    )
    touches = touches[is_strong].reset_index(drop=True)
    if touches.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    stop_ref = _stop_reference(bars, config.stop_loss.stop_lookback_bars)
    touches = touches.merge(stop_ref, on="timestamp", how="left")
    ref_cols = []
    for n in reference_ma_lengths:
        ref = _ma200_reference(bars, n).rename(columns={"_ma200": f"_ref_ma_{n}"})
        touches = touches.merge(ref, on="timestamp", how="left")
        ref_cols.append(f"_ref_ma_{n}")
    volume_ref = _daily_volume_reference(bars)
    touches = touches.merge(volume_ref, on="timestamp", how="left")
    htf_ref = _higher_timeframe_trend_class_reference(
        bars, config.timeframes.timeframe, config.market,
        config.scanner.sma_length, config.scanner.ma_slope_lookback, config.scanner.min_ma_slope_pct,
    )
    touches = touches.merge(htf_ref, on="timestamp", how="left")
    gap_mult = config.ma200_filter.merged_ma_gap_atr_multiplier
    needs_atr = (
        "atr" in (config.entry.entry_buffer_type, config.stop_loss.sl_buffer_type)
        or config.ma200_filter.nearby_tolerance_mode == "atr"
        or gap_mult > 0
    )
    if needs_atr:
        atr_ref = _atr_reference(bars, config.scanner.atr_length)
        touches = touches.merge(atr_ref, on="timestamp", how="left")

    def _confluence_check(row):
        """(rejected, waived reference-MA lengths, smallest waived gap in ATR)."""
        atr = row["_atr"] if needs_atr else float("nan")
        tol = _resolve_pct_tolerance(
            config.ma200_filter.nearby_tolerance_mode, config.ma200_filter.nearby_tolerance_pct,
            config.ma200_filter.nearby_tolerance_atr_multiplier, row["close"], atr,
        )
        waived, gaps = [], []
        for n, col in zip(reference_ma_lengths, ref_cols):
            if not is_rejected_by_ma200_filter(
                row["direction"], row["sma_44"], row[col], row["low"], row["high"], tol,
            ):
                continue
            if gap_mult > 0 and pd.notna(atr) and atr > 0:
                gap_atr = abs(row["sma_44"] - row[col]) / atr
                if gap_atr < gap_mult:
                    waived.append(str(n))
                    gaps.append(gap_atr)
                    continue
            return True, "", float("nan")
        return False, ",".join(waived), (min(gaps) if gaps else float("nan"))

    checks = touches.apply(_confluence_check, axis=1)
    rejected_by_ma200 = checks.apply(lambda t: t[0])
    touches["_cg_ma"] = checks.apply(lambda t: t[1])
    touches["_cg_atr"] = checks.apply(lambda t: t[2])
    touches = touches[~rejected_by_ma200].reset_index(drop=True)
    if touches.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    rejected_by_trend_class = touches.apply(
        lambda row: is_rejected_by_trend_class_filter(
            row["direction"], row["trend_class"], row["ma_slope_pct"], config.trade_filters,
        ),
        axis=1,
    )
    touches = touches[~rejected_by_trend_class].reset_index(drop=True)
    if touches.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    rejected_by_volume = touches["_daily_volume"].apply(
        lambda v: is_rejected_by_daily_volume_filter(v, config.trade_filters.min_daily_volume)
    )
    touches = touches[~rejected_by_volume].reset_index(drop=True)
    if touches.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    is_long = (touches["direction"] == "LONG").to_numpy()
    close = touches["close"]

    atr = touches["_atr"] if needs_atr else None
    entry_buf = _buffer(
        close, config.entry.entry_buffer_type,
        config.entry.entry_buffer_points, config.entry.entry_buffer_pct,
        atr, config.entry.entry_buffer_atr_multiplier,
    )
    sl_buf = _buffer(
        close, config.stop_loss.sl_buffer_type,
        config.stop_loss.sl_buffer_points, config.stop_loss.sl_buffer_pct,
        atr, config.stop_loss.sl_buffer_atr_multiplier,
    )

    trigger_price = np.where(is_long, touches["high"] + entry_buf, touches["low"] - entry_buf)
    if config.entry.round_to_whole_number:
        trigger_price = _round_to_whole_number_if_near(
            trigger_price, is_long, config.entry.round_to_whole_number_tolerance_pct,
        )
    # The stop must never sit inside the 44-SMA support/resistance line
    # itself — widen the lookback-based stop out to the SMA when needed.
    base_stop = np.where(
        is_long,
        np.minimum(touches["_stop_low"], touches["sma_44"]),
        np.maximum(touches["_stop_high"], touches["sma_44"]),
    )
    stop_loss = np.where(is_long, base_stop - sl_buf, base_stop + sl_buf)
    risk = np.where(is_long, trigger_price - stop_loss, stop_loss - trigger_price)

    rr = config.target.risk_reward
    if config.target.target_mode == "dynamic_stop_multiple":
        target = np.where(is_long, trigger_price + rr * risk, trigger_price - rr * risk)
    else:
        # Anchored to the signal candle's OWN extreme, not trigger_price —
        # so an entry buffer (see EntryConfig.entry_buffer_type) shifts where
        # you get IN without also dragging the target away from it. Whatever
        # the trigger ends up being, the target is always signal high (LONG)
        # / low (SHORT) + risk_reward * the signal candle's own range.
        signal_high = touches["high"].to_numpy(dtype=float)
        signal_low = touches["low"].to_numpy(dtype=float)
        range_ = signal_high - signal_low
        target = np.where(is_long, signal_high + rr * range_, signal_low - rr * range_)

    out = touches.copy()
    out["signal_open"] = touches["open"]
    out["signal_high"] = touches["high"]
    out["signal_low"] = touches["low"]
    out["signal_close"] = touches["close"]
    out["trigger_price"] = trigger_price
    out["stop_loss"] = stop_loss
    out["target"] = target
    out["daily_volume"] = touches["_daily_volume"]
    out["ma_length"] = config.scanner.sma_length
    out["confluence_gap_skipped"] = touches["_cg_ma"] != ""
    out["confluence_gap_ma"] = touches["_cg_ma"].replace("", None)
    out["confluence_gap_atr"] = touches["_cg_atr"]
    out["signal_ma"] = touches["sma_44"]
    for prefix in _TIMEFRAME_COLUMN_PREFIX.values():
        long_col, short_col = f"_{prefix}_class_long", f"_{prefix}_class_short"
        if long_col in touches.columns:
            out[f"{prefix}_trend_class"] = np.where(is_long, touches[long_col], touches[short_col])
        else:
            out[f"{prefix}_trend_class"] = None
    out["_risk"] = risk

    # Entry-price floor checked against trigger_price (the actual entry
    # level), not the touch candle's raw close — see TradeFilterConfig.
    out = out[
        (out["_risk"] > 0) & (out["trigger_price"] >= config.trade_filters.min_entry_price)
    ].drop(columns=["_risk"]).reset_index(drop=True)
    if out.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)
    return out[[c for c in CANDIDATE_COLUMNS if c != "ma_alignment"]]
