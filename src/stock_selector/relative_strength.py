"""Benchmark-relative strength and cross-sectional percentile ranks
(spec section 10).

Percentiles are computed across the point-in-time valid universe at one
selection timestamp — never across future universe members.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .config import SelectorConfig

NAN = float('nan')
MIN_VOLATILITY_FLOOR = 0.05

# rs metric column -> percentile column
PERCENTILE_MAP = {
    'excess_return_20': 'rs_20_percentile',
    'excess_return_63': 'rs_63_percentile',
    'excess_return_126': 'rs_126_percentile',
    'rs_volatility_adjusted': 'rs_composite_percentile',
}


def _trailing_return(closes: np.ndarray, sessions: int) -> float:
    if len(closes) <= sessions:
        return NAN
    then = closes[-1 - sessions]
    if not math.isfinite(then) or then <= 0:
        return NAN
    return float(closes[-1] / then - 1.0)


def compute_benchmark_returns(
    benchmark_closes_full: np.ndarray, config: SelectorConfig,
) -> dict[str, float]:
    """Benchmark trailing returns; ``benchmark_closes_full`` ends at the
    benchmark's value at the selection timestamp."""
    return {
        'benchmark_return_20': _trailing_return(benchmark_closes_full, config.rs_short_period),
        'benchmark_return_63': _trailing_return(benchmark_closes_full, config.rs_medium_period),
        'benchmark_return_126': _trailing_return(benchmark_closes_full, config.rs_long_period),
    }


def compute_stock_relative_strength(
    stock_closes_full: np.ndarray,
    benchmark_returns: dict[str, float],
    config: SelectorConfig,
) -> dict[str, float]:
    """Excess returns, 63-session volatility, raw and vol-adjusted RS."""
    stock_return_20 = _trailing_return(stock_closes_full, config.rs_short_period)
    stock_return_63 = _trailing_return(stock_closes_full, config.rs_medium_period)
    stock_return_126 = _trailing_return(stock_closes_full, config.rs_long_period)

    excess_20 = stock_return_20 - benchmark_returns['benchmark_return_20']
    excess_63 = stock_return_63 - benchmark_returns['benchmark_return_63']
    excess_126 = stock_return_126 - benchmark_returns['benchmark_return_126']

    window = config.rs_medium_period
    if len(stock_closes_full) > window:
        tail = np.asarray(stock_closes_full[-(window + 1):], dtype=float)
        daily_returns = tail[1:] / tail[:-1] - 1.0
        volatility_63 = float(np.std(daily_returns, ddof=1) * math.sqrt(252.0))
    else:
        volatility_63 = NAN

    if all(math.isfinite(v) for v in (excess_20, excess_63, excess_126)):
        rs_raw = 0.40 * excess_20 + 0.40 * excess_63 + 0.20 * excess_126
    else:
        rs_raw = NAN
    if math.isfinite(rs_raw) and math.isfinite(volatility_63):
        rs_vol_adj = rs_raw / max(volatility_63, MIN_VOLATILITY_FLOOR)
    else:
        rs_vol_adj = NAN

    return {
        'excess_return_20': excess_20,
        'excess_return_63': excess_63,
        'excess_return_126': excess_126,
        'volatility_63': volatility_63,
        'rs_raw': rs_raw,
        'rs_volatility_adjusted': rs_vol_adj,
    }


def assign_cross_sectional_percentiles(rows: list[dict]) -> None:
    """Mutate ``rows`` in place, adding percentile ranks over the valid pool.

    A row enters the pool only when its data is valid and its vol-adjusted RS
    is finite (i.e. the stock existed with enough history at this timestamp).
    Rows outside the pool receive NaN percentiles.
    """
    for row in rows:
        for pct_col in PERCENTILE_MAP.values():
            row[pct_col] = NAN

    pool = [
        r for r in rows
        if r.get('data_valid', False)
        and isinstance(r.get('rs_volatility_adjusted'), float)
        and math.isfinite(r['rs_volatility_adjusted'])
    ]
    if not pool:
        return
    for metric_col, pct_col in PERCENTILE_MAP.items():
        values = pd.Series([r.get(metric_col, NAN) for r in pool], dtype=float)
        percentiles = values.rank(pct=True, method='average') * 100.0
        for row, pct, value in zip(pool, percentiles, values):
            row[pct_col] = float(pct) if pd.notna(value) else NAN
