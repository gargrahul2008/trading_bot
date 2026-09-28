"""ATR Chandelier trailing stop (volatility + structure based trade management).

Self-contained and reusable: it knows nothing about signal generation. Given a
trade's direction / entry / initial stop, it manages the stop bar-by-bar for both
longs and shorts.

Design, and why it cannot look ahead or repaint
-----------------------------------------------
Each bar is processed in a strict two-phase order:

  1. ``check_exit(state, bar)`` — evaluates the bar against ``state.stop``, which
     was computed *entirely from bars strictly before this one*. Nothing about the
     current bar can move the stop that the current bar is judged against.
  2. ``update(state, bar)`` — only then folds this bar into the run-up extreme,
     tests activation, and recomputes the stop **for the next bar**.

So the stop is always "as of the previous close" and never repaints. Swing pivots
are likewise only used once *confirmed*: a swing low at bar ``i`` needs
``swing_lookback`` bars on each side, so it first becomes usable at bar
``i + swing_lookback`` (see :func:`compute_confirmed_swings`).

Behaviour
---------
* Trailing does **not** arm at entry. Until price has run ``trail_after_R`` in the
  trade's favour, the position is protected by the original signal stop — so a
  losing trade still exits at its initial stop, exactly as before.
* On arming, the stop optionally jumps to breakeven.
* Once armed the stop is the Chandelier: ``run_up_extreme -/+ atr_multiplier * ATR``,
  optionally tightened to the last confirmed swing low/high.
* The stop **only ever ratchets in the trade's favour** (up for longs, down for
  shorts) — enforced with max()/min() against the previous stop.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from .config.schema import ExecutionConfig


def resolve_exit_mode(cfg: ExecutionConfig) -> str:
    """The effective exit mode. `exit_mode` wins when set; otherwise the legacy
    `use_candle_trailing_stop` flag is honoured, so old configs keep working."""
    if cfg.exit_mode != "fixed":
        return cfg.exit_mode
    if cfg.use_candle_trailing_stop:
        return "candle_trail"
    return "fixed"


# ── Indicator helpers (both are strictly backward-looking) ────────────────────

def compute_atr(frame: pd.DataFrame, length: int) -> pd.Series:
    """Wilder-style ATR as a simple rolling mean of True Range. Value at bar j
    uses only bars <= j."""
    prev_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window=max(int(length), 1), min_periods=max(int(length), 1)).mean()


def compute_confirmed_swings(frame: pd.DataFrame, lookback: int) -> tuple[np.ndarray, np.ndarray]:
    """For every bar, the price of the most recent **confirmed** swing low / swing high.

    A swing low at bar ``i`` requires ``lookback`` bars on each side to be higher,
    so it cannot be known until bar ``i + lookback``. We therefore only publish a
    pivot from bar ``i + lookback`` onward — never earlier. Returns two float
    arrays (NaN until the first confirmed pivot).
    """
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    n = max(int(lookback), 1)
    m = len(frame)

    swing_low = np.zeros(m, dtype=bool)
    swing_high = np.zeros(m, dtype=bool)
    for i in range(n, m - n):
        if low[i] <= low[i - n:i].min() and low[i] <= low[i + 1:i + n + 1].min():
            swing_low[i] = True
        if high[i] >= high[i - n:i].max() and high[i] >= high[i + 1:i + n + 1].max():
            swing_high[i] = True

    recent_low = np.full(m, np.nan)
    recent_high = np.full(m, np.nan)
    last_low = np.nan
    last_high = np.nan
    for j in range(m):
        i = j - n  # the pivot that becomes confirmed exactly at bar j
        if i >= 0:
            if swing_low[i]:
                last_low = low[i]
            if swing_high[i]:
                last_high = high[i]
        recent_low[j] = last_low
        recent_high[j] = last_high
    return recent_low, recent_high


# ── Per-trade state ──────────────────────────────────────────────────────────

@dataclass
class ChandelierState:
    direction: str            # "LONG" | "SHORT"
    entry_price: float
    initial_stop: float
    r_value: float            # 1R, in price points (always > 0)
    stop: float               # the stop the NEXT bar will be judged against
    highest_high: float       # run-up extreme since entry (longs)
    lowest_low: float         # run-down extreme since entry (shorts)
    activated: bool = False
    activated_at: Optional[pd.Timestamp] = None
    breakeven_applied: bool = False


class ChandelierTrailingStop:
    """Stateless w.r.t. trades — hold one instance and pass it a per-trade
    :class:`ChandelierState`. Works identically for longs and shorts."""

    def __init__(self, cfg: ExecutionConfig):
        self.cfg = cfg

    # ── lifecycle ────────────────────────────────────────────────────────────

    def open(self, direction: str, entry_price: float, initial_stop: float) -> ChandelierState:
        """Start managing a trade. The initial stop (from the existing signal
        logic) defines 1R and is the stop until trailing arms."""
        if direction == "LONG":
            r_value = entry_price - initial_stop
        else:
            r_value = initial_stop - entry_price
        if r_value <= 0:
            raise ValueError(f"initial stop must be on the losing side of entry (R={r_value})")
        return ChandelierState(
            direction=direction, entry_price=entry_price, initial_stop=initial_stop,
            r_value=r_value, stop=initial_stop,
            highest_high=entry_price, lowest_low=entry_price,
        )

    # ── phase 1: judge this bar against the stop set by earlier bars ─────────

    def check_exit(self, state: ChandelierState, bar: dict) -> Optional[tuple[float, str]]:
        """Return (exit_price, reason) if this bar breaches the current stop.
        Uses ``state.stop`` only — never this bar's own high/low to move it."""
        long = state.direction == "LONG"
        cfg = self.cfg
        reason = "trail_stop" if state.activated else "sl"

        if cfg.exit_on_close:
            # Only a CLOSE beyond the stop exits; intrabar spikes are ignored.
            breached = (bar["close"] < state.stop) if long else (bar["close"] > state.stop)
            if breached:
                return float(bar["close"]), reason
        else:
            # Any touch of the stop exits, filled at the stop level.
            breached = (bar["low"] <= state.stop) if long else (bar["high"] >= state.stop)
            if breached:
                return float(state.stop), reason
        return None

    def check_take_profit(self, state: ChandelierState, bar: dict) -> Optional[tuple[float, str]]:
        """Optional fixed R-multiple take-profit (off by default)."""
        cfg = self.cfg
        if not cfg.use_fixed_take_profit:
            return None
        long = state.direction == "LONG"
        if long:
            tp = state.entry_price + state.r_value * cfg.fixed_rr_target
            if bar["high"] >= tp:
                return float(tp), "target"
        else:
            tp = state.entry_price - state.r_value * cfg.fixed_rr_target
            if bar["low"] <= tp:
                return float(tp), "target"
        return None

    # ── phase 2: fold this bar in and set the stop for the NEXT bar ──────────

    def update(self, state: ChandelierState, bar: dict) -> None:
        """Advance the run-up extreme, arm trailing once the trade is
        ``trail_after_R`` in profit, then ratchet the Chandelier stop."""
        cfg = self.cfg
        long = state.direction == "LONG"

        # Run-up extreme since entry (the Chandelier hangs off this).
        state.highest_high = max(state.highest_high, float(bar["high"]))
        state.lowest_low = min(state.lowest_low, float(bar["low"]))

        # Arm only once price has actually run trail_after_R in our favour.
        if not state.activated:
            trigger = state.r_value * cfg.trail_after_R
            reached = (
                float(bar["high"]) >= state.entry_price + trigger if long
                else float(bar["low"]) <= state.entry_price - trigger
            )
            if not reached:
                return  # still on the initial stop — nothing to trail yet
            state.activated = True
            state.activated_at = bar.get("timestamp")
            if cfg.use_breakeven_after_activation:
                buf = cfg.breakeven_buffer_points
                be = state.entry_price + buf if long else state.entry_price - buf
                state.stop = max(state.stop, be) if long else min(state.stop, be)
                state.breakeven_applied = True

        # ── Chandelier: hang the stop off the run-up extreme by N ATRs ───────
        atr = bar.get("atr")
        candidates = [state.stop]  # ratchet: never worse than the current stop
        if atr is not None and not pd.isna(atr):
            atr_stop = (
                state.highest_high - cfg.atr_multiplier * float(atr) if long
                else state.lowest_low + cfg.atr_multiplier * float(atr)
            )
            candidates.append(atr_stop)

        # ── Structure: also pull to the last CONFIRMED swing, if enabled ─────
        if cfg.use_structure_filter:
            swing = bar.get("recent_swing_low") if long else bar.get("recent_swing_high")
            if swing is not None and not pd.isna(swing):
                structure_stop = (
                    float(swing) - cfg.structure_buffer_points if long
                    else float(swing) + cfg.structure_buffer_points
                )
                candidates.append(structure_stop)

        # Only ever move in the trade's favour.
        state.stop = max(candidates) if long else min(candidates)
