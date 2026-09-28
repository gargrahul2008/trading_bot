"""Weekly trend indicators (spec section 9).

Weekly bars are built from COMPLETED daily sessions only, and the current
incomplete week is always excluded: a weekly bar labelled with a Friday on or
after the as-of date cannot be complete at the selection cutoff, so it is
dropped.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from .config import SelectorConfig
from .daily_indicators import ema, sma, _last, _rising

NAN = float('nan')


def build_completed_weekly_bars(completed_daily: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    """Weekly OHLCV bars from completed sessions, incomplete week excluded."""
    if completed_daily is None or completed_daily.empty:
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    df = completed_daily.copy()
    df.index = pd.DatetimeIndex(pd.to_datetime(df['date']))
    weekly = df.resample('W-FRI').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    ).dropna(subset=['close'])
    # a week labelled Friday >= as_of_date still had sessions to run at the cutoff
    return weekly.loc[weekly.index.date < as_of_date]


def compute_weekly_indicators(
    completed_daily: pd.DataFrame, as_of_date: date, config: SelectorConfig,
) -> dict[str, object]:
    weekly = build_completed_weekly_bars(completed_daily, as_of_date)
    closes = weekly['close']

    weekly_ema = ema(closes, config.weekly_ema_period) if len(closes) else pd.Series(dtype=float)
    weekly_sma = sma(closes, config.weekly_sma_period) if len(closes) else pd.Series(dtype=float)

    weekly_close = _last(closes) if len(closes) else NAN
    ema_value = _last(weekly_ema) if len(weekly_ema) else NAN
    sma_value = _last(weekly_sma) if len(weekly_sma) else NAN
    ema_rising = _rising(weekly_ema, config.weekly_slope_lookback)
    sma_rising = _rising(weekly_sma, config.weekly_slope_lookback)

    alignment = (
        pd.notna(weekly_close) and pd.notna(ema_value) and pd.notna(sma_value)
        and weekly_close > ema_value and ema_value > sma_value and ema_rising
    )
    structure = (
        pd.notna(weekly_close) and pd.notna(sma_value)
        and weekly_close > sma_value and sma_rising
    )

    return {
        'weekly_bars': int(len(weekly)),
        'weekly_close': weekly_close,
        'weekly_ema_10': ema_value,
        'weekly_sma_30': sma_value,
        'weekly_ema_10_rising': bool(ema_rising),
        'weekly_sma_30_rising': bool(sma_rising),
        'weekly_bullish_alignment': bool(alignment),
        'weekly_structure_ok': bool(structure),
    }
