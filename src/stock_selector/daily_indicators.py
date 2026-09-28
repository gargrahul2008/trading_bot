"""Daily indicators (spec section 8).

All functions receive an explicit frame: ``full`` = completed sessions plus
the current bar (provisional in PRE_CLOSE_SNAPSHOT mode, the last completed
session in PREVIOUS_COMPLETED_DAY mode). The "previous 20-day high" always
excludes the current bar; the 52-week high includes the current bar's high
up to the cutoff.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .config import SelectorConfig

NAN = float('nan')


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()


def wilder_atr(df: pd.DataFrame, period: int) -> pd.Series:
    """ATR with Wilder smoothing (ewm alpha = 1/period)."""
    prev_close = df['close'].shift(1)
    tr = pd.concat(
        [
            df['high'] - df['low'],
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def _last(series: pd.Series) -> float:
    value = series.iloc[-1] if len(series) else NAN
    return float(value) if pd.notna(value) else NAN


def _rising(series: pd.Series, lookback: int) -> bool:
    """value today > value ``lookback`` sessions ago (False on missing data)."""
    if len(series) <= lookback:
        return False
    now, then = series.iloc[-1], series.iloc[-1 - lookback]
    if pd.isna(now) or pd.isna(then):
        return False
    return bool(now > then)


def _trailing_return(closes: pd.Series, sessions: int) -> float:
    if len(closes) <= sessions:
        return NAN
    now, then = closes.iloc[-1], closes.iloc[-1 - sessions]
    if pd.isna(now) or pd.isna(then) or then <= 0:
        return NAN
    return float(now / then - 1.0)


def compute_daily_indicators(full: pd.DataFrame, config: SelectorConfig) -> dict[str, object]:
    """All daily indicators for one stock.

    ``full`` must contain the current bar as its last row.
    """
    closes = full['close']
    last_price = _last(closes)

    ema_short = ema(closes, config.ema_short_period)
    sma_medium = sma(closes, config.sma_medium_period)
    sma_long = sma(closes, config.sma_long_period)

    atr_series = wilder_atr(full, config.atr_period)
    atr_value = _last(atr_series)
    atr_percent = atr_value / last_price if math.isfinite(atr_value) and last_price > 0 else NAN

    ema_value = _last(ema_short)
    if math.isfinite(atr_value) and atr_value > 0 and math.isfinite(ema_value):
        extension = (last_price - ema_value) / atr_value
    else:
        extension = NAN

    completed_highs = full['high'].iloc[:-1]
    if len(completed_highs) >= config.recent_high_lookback:
        previous_recent_high = float(completed_highs.tail(config.recent_high_lookback).max())
    else:
        previous_recent_high = NAN
    breakout_ratio = (
        last_price / previous_recent_high
        if math.isfinite(previous_recent_high) and previous_recent_high > 0 else NAN
    )

    # 52-week high: previous (year_high_lookback - 1) completed sessions PLUS
    # the current session's high up to the cutoff.
    year_window = full['high'].tail(config.year_high_lookback)
    high_52_week = float(year_window.max()) if len(year_window) else NAN
    proximity = (
        last_price / high_52_week
        if math.isfinite(high_52_week) and high_52_week > 0 else NAN
    )

    return {
        'last_price': last_price,
        'ema_20': ema_value,
        'sma_50': _last(sma_medium),
        'sma_200': _last(sma_long),
        'ema_20_rising': _rising(ema_short, config.ema_slope_lookback),
        'sma_50_rising': _rising(sma_medium, config.sma_50_slope_lookback),
        'sma_200_rising': _rising(sma_long, config.sma_200_slope_lookback),
        'atr_14': atr_value,
        'atr_percent': atr_percent,
        'extension_from_ema20_atr': extension,
        'return_5d': _trailing_return(closes, 5),
        'return_20d': _trailing_return(closes, 20),
        'return_63d': _trailing_return(closes, 63),
        'return_126d': _trailing_return(closes, 126),
        'previous_20_day_high': previous_recent_high,
        'breakout_ratio': breakout_ratio,
        'high_52_week': high_52_week,
        'high_52_week_proximity': proximity,
    }
