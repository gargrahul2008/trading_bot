"""Bullish score 0-100 from five sub-scores (spec sections 17-22).

Sub-score maxima: trend 25, relative strength 25, momentum/price-position 20,
pre-close confirmation 20, risk & sector quality 10. Missing inputs (NaN)
never score points and never raise.
"""
from __future__ import annotations

import math

from .config import SelectorConfig

NAN = float('nan')

MAX_TREND_SCORE = 25.0
MAX_RS_SCORE = 25.0
MAX_MOMENTUM_SCORE = 20.0
MAX_PRECLOSE_SCORE = 20.0
MAX_RISK_SECTOR_SCORE = 10.0


def _num(value: object) -> float:
    """Coerce to float; anything non-finite becomes NaN."""
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return NAN
    return result if math.isfinite(result) else NAN


def _gt(a: object, b: object) -> bool:
    a, b = _num(a), _num(b)
    return math.isfinite(a) and math.isfinite(b) and a > b


def linear_points(value: float, low: float, high: float, maximum_points: float) -> float:
    """0 at/below ``low``, ``maximum_points`` at/above ``high``, linear between."""
    value = _num(value)
    if not math.isfinite(value) or value <= low:
        return 0.0
    if value >= high:
        return maximum_points
    return ((value - low) / (high - low)) * maximum_points


def compute_trend_score(row: dict, config: SelectorConfig) -> float:
    points = 0.0
    last_price, ema_20 = row.get('last_price'), row.get('ema_20')
    sma_50, sma_200 = row.get('sma_50'), row.get('sma_200')

    if _gt(last_price, ema_20) and _gt(ema_20, sma_50) and _gt(sma_50, sma_200):
        points += 8.0
    elif _gt(last_price, ema_20) and _gt(sma_50, sma_200):
        points += 4.0

    if row.get('ema_20_rising'):
        points += 4.0
    if row.get('sma_50_rising'):
        points += 4.0
    if row.get('sma_200_rising'):
        points += 4.0

    if row.get('weekly_bullish_alignment'):
        points += 5.0
    elif _gt(row.get('weekly_close'), row.get('weekly_sma_30')) and row.get('weekly_sma_30_rising'):
        points += 2.5

    return min(points, MAX_TREND_SCORE)


def compute_relative_strength_score(row: dict, config: SelectorConfig) -> float:
    points = (
        linear_points(row.get('rs_20_percentile', NAN), 50.0, 100.0, 8.0)
        + linear_points(row.get('rs_63_percentile', NAN), 50.0, 100.0, 10.0)
        + linear_points(row.get('rs_126_percentile', NAN), 50.0, 100.0, 7.0)
    )
    return min(points, MAX_RS_SCORE)


def compute_momentum_score(row: dict, config: SelectorConfig) -> float:
    points = linear_points(
        row.get('high_52_week_proximity', NAN),
        config.score_high_proximity_low, config.score_high_proximity_high, 6.0,
    )
    points += linear_points(
        row.get('breakout_ratio', NAN),
        config.score_breakout_low, config.score_breakout_high, 8.0,
    )
    for key in ('return_5d', 'return_20d', 'return_63d'):
        if _gt(row.get(key), 0.0):
            points += 2.0
    return min(points, MAX_MOMENTUM_SCORE)


def compute_preclose_score(row: dict, config: SelectorConfig) -> float:
    points = linear_points(
        row.get('close_location_value', NAN),
        config.min_close_location_value, 1.0, 6.0,
    )
    points += min(
        linear_points(
            row.get('same_time_volume_ratio', NAN),
            config.min_same_time_volume_ratio, config.score_volume_ratio_high, 5.0,
        ),
        5.0,
    )
    vwap_distance = _num(row.get('vwap_distance'))
    if math.isfinite(vwap_distance) and vwap_distance > 0.0:
        points += linear_points(vwap_distance, 0.0, config.score_vwap_distance_high, 4.0)

    day_return = _num(row.get('day_return'))
    if math.isfinite(day_return):
        if 0.005 <= day_return <= 0.030:
            points += 5.0
        elif 0.0025 <= day_return < 0.005:
            points += 3.0
        elif 0.030 < day_return <= 0.045:
            points += 3.0
    return min(points, MAX_PRECLOSE_SCORE)


def compute_risk_sector_score(row: dict, config: SelectorConfig) -> float:
    points = 0.0
    extension = _num(row.get('extension_from_ema20_atr'))
    if math.isfinite(extension):
        if extension <= config.score_extension_full_points:
            points += 3.0
        elif extension <= config.max_extension_from_ema20_atr:
            points += 1.5

    atr_percent = _num(row.get('atr_percent'))
    if math.isfinite(atr_percent):
        if config.score_atr_quality_low <= atr_percent <= config.score_atr_quality_high:
            points += 2.0
        elif config.min_atr_percent <= atr_percent <= config.max_atr_percent:
            points += 1.0

    opening_gap = _num(row.get('opening_gap'))
    if math.isfinite(opening_gap):
        if abs(opening_gap) <= config.score_gap_full_points:
            points += 2.0
        elif abs(opening_gap) <= config.score_gap_half_points:
            points += 1.0

    if row.get('sector_data_available'):
        if row.get('sector_trend_bullish'):
            points += 1.5
        if row.get('sector_rs_bullish'):
            points += 1.5

    return min(points, MAX_RISK_SECTOR_SCORE)


def compute_scores(row: dict, config: SelectorConfig) -> dict[str, float]:
    """All five sub-scores plus the clipped 0-100 total."""
    trend = compute_trend_score(row, config)
    rs = compute_relative_strength_score(row, config)
    momentum = compute_momentum_score(row, config)
    preclose = compute_preclose_score(row, config)
    risk_sector = compute_risk_sector_score(row, config)
    total = trend + rs + momentum + preclose + risk_sector
    return {
        'trend_score': trend,
        'relative_strength_score': rs,
        'momentum_score': momentum,
        'preclose_score': preclose,
        'risk_sector_score': risk_sector,
        'bullish_score': max(0.0, min(100.0, total)),
    }
