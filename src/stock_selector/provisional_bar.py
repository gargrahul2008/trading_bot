"""Provisional current-session daily bar and pre-close strength indicators.

The provisional bar is built ONLY from data through the selection cutoff and
is appended to completed history for "current-time" indicator calculations in
PRE_CLOSE_SNAPSHOT mode. Nothing after the cutoff may enter it.
"""
from __future__ import annotations

import math
from datetime import date

import pandas as pd

from .models import PreCloseSnapshot

NAN = float('nan')


def snapshot_to_provisional_bar(snapshot: PreCloseSnapshot) -> dict[str, float]:
    """Open/high/low/close/volume of the current session up to the cutoff."""
    return {
        'open': float(snapshot.session_open),
        'high': float(snapshot.high_to_cutoff),
        'low': float(snapshot.low_to_cutoff),
        'close': float(snapshot.last_price),
        'volume': float(snapshot.cumulative_volume),
    }


def append_provisional_bar(
    completed: pd.DataFrame, bar: dict[str, float], as_of_date: date,
) -> pd.DataFrame:
    """Completed daily history plus the provisional current-session row."""
    row = {'date': pd.Timestamp(as_of_date), **bar}
    return pd.concat([completed, pd.DataFrame([row])], ignore_index=True)


def close_location_value(bar: dict[str, float]) -> float:
    """(close - low) / (high - low); 0.5 for a zero-range session."""
    span = bar['high'] - bar['low']
    if span > 0:
        return (bar['close'] - bar['low']) / span
    return 0.5


def compute_preclose_metrics(
    bar: dict[str, float],
    previous_close: float,
    vwap: float | None,
    bid_price: float | None = None,
    ask_price: float | None = None,
) -> dict[str, float]:
    """Current-day strength metrics from the provisional bar (spec section 12)."""
    last_price = bar['close']
    day_return = last_price / previous_close - 1.0 if previous_close > 0 else NAN
    opening_gap = bar['open'] / previous_close - 1.0 if previous_close > 0 else NAN
    vwap_distance = last_price / vwap - 1.0 if vwap is not None and vwap > 0 else NAN
    drawdown = last_price / bar['high'] - 1.0 if bar['high'] > 0 else NAN
    if bid_price is not None and ask_price is not None and bid_price > 0:
        spread = ask_price / bid_price - 1.0
    else:
        spread = NAN
    return {
        'day_return': day_return,
        'opening_gap': opening_gap,
        'close_location_value': close_location_value(bar),
        'vwap_distance': vwap_distance,
        'drawdown_from_intraday_high': drawdown,
        'bid_ask_spread_percent': spread,
    }


def same_time_volume_ratio(
    current_cumulative_volume: float, historical_same_time_volumes: pd.Series | None,
) -> float:
    """Current cumulative volume at the cutoff vs the historical median
    cumulative volume AT THE SAME cutoff time. NaN when history is missing —
    never silently compare against full-day volume."""
    if historical_same_time_volumes is None or len(historical_same_time_volumes) == 0:
        return NAN
    median = float(historical_same_time_volumes.median())
    if not math.isfinite(median) or median <= 0:
        return NAN
    return current_cumulative_volume / median
