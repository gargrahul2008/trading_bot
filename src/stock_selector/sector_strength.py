"""Sector trend and sector relative strength (spec section 11).

When sector data is unavailable nothing is invented: availability is flagged
False, sector points are zero, and the stock is only rejected when
``require_sector_bullish`` is enabled.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .config import SelectorConfig
from .daily_indicators import sma

NAN = float('nan')

UNAVAILABLE_SECTOR_METRICS = {
    'sector_data_available': False,
    'sector_close': NAN,
    'sector_sma_50': NAN,
    'sector_sma_50_rising': False,
    'sector_return_63': NAN,
    'sector_excess_return_63': NAN,
    'sector_trend_bullish': False,
    'sector_rs_bullish': False,
}


def compute_sector_strength(
    sector_closes_full: np.ndarray | None,
    benchmark_return_63: float,
    config: SelectorConfig,
) -> dict[str, object]:
    """Sector metrics from the sector-index close series (ending at the
    selection timestamp). Returns the "unavailable" block when data is
    missing or too short."""
    if sector_closes_full is None:
        return dict(UNAVAILABLE_SECTOR_METRICS)
    closes = pd.Series(np.asarray(sector_closes_full, dtype=float))
    needed = config.sma_medium_period + config.sector_sma_slope_lookback
    if len(closes) < max(needed, config.rs_medium_period + 1):
        return dict(UNAVAILABLE_SECTOR_METRICS)

    sma_series = sma(closes, config.sma_medium_period)
    sector_close = float(closes.iloc[-1])
    sector_sma_50 = float(sma_series.iloc[-1])
    then = sma_series.iloc[-1 - config.sector_sma_slope_lookback]
    sma_rising = bool(pd.notna(then) and sma_series.iloc[-1] > then)

    base = closes.iloc[-1 - config.rs_medium_period]
    sector_return_63 = float(closes.iloc[-1] / base - 1.0) if base > 0 else NAN
    if math.isfinite(sector_return_63) and math.isfinite(benchmark_return_63):
        sector_excess = sector_return_63 - benchmark_return_63
    else:
        sector_excess = NAN

    return {
        'sector_data_available': True,
        'sector_close': sector_close,
        'sector_sma_50': sector_sma_50,
        'sector_sma_50_rising': sma_rising,
        'sector_return_63': sector_return_63,
        'sector_excess_return_63': sector_excess,
        'sector_trend_bullish': bool(sector_close > sector_sma_50 and sma_rising),
        'sector_rs_bullish': bool(math.isfinite(sector_excess) and sector_excess > 0),
    }
