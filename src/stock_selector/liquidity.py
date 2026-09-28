"""Turnover and execution-liquidity conditions (spec section 13).

The turnover requirement is sized to the largest order the mature strategy
will place (``expected_max_stock_order_value``), not the first entry unit.
"""
from __future__ import annotations

import math

import pandas as pd

from .config import SelectorConfig

NAN = float('nan')


def compute_liquidity(completed_daily: pd.DataFrame, config: SelectorConfig) -> dict[str, object]:
    """Median 20-session traded value versus the required turnover."""
    required = config.required_turnover
    if completed_daily is None or len(completed_daily) < config.turnover_lookback:
        return {
            'median_traded_value_20': NAN,
            'required_turnover': required,
            'liquidity_pass': False,
        }
    tail = completed_daily.tail(config.turnover_lookback)
    traded_value = tail['close'] * tail['volume']
    median_value = float(traded_value.median())
    return {
        'median_traded_value_20': median_value,
        'required_turnover': required,
        'liquidity_pass': bool(math.isfinite(median_value) and median_value >= required),
    }


def spread_pass(bid_ask_spread_percent: float, config: SelectorConfig) -> bool | None:
    """True/False when spread data exists; None when unavailable."""
    if bid_ask_spread_percent is None or not math.isfinite(bid_ask_spread_percent):
        return None
    return bid_ask_spread_percent <= config.max_bid_ask_spread_percent
