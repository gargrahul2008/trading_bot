"""Range detection and validation for bottom-zone grid strategy."""

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import pandas as pd

LOG = logging.getLogger("bzg.range")


@dataclass
class RangeInfo:
    low: float = 0
    high: float = 0
    width_points: float = 0
    width_pct: float = 0
    support_touches: int = 0
    resistance_touches: int = 0
    containment_pct: float = 0
    valid: bool = False
    in_watch_zone: bool = False
    reject_reason: Optional[str] = None
    reject_status: Optional[str] = None


def calculate_range_percentile(
    df: pd.DataFrame,
    lookback_days: int = 60,
    pct_low: float = 5,
    pct_high: float = 95,
) -> tuple:
    recent = df.tail(lookback_days)
    if len(recent) < lookback_days * 0.6:
        return None, None
    range_low = float(np.percentile(recent["low"].values, pct_low))
    range_high = float(np.percentile(recent["high"].values, pct_high))
    return range_low, range_high


def count_zone_touches(
    series: pd.Series,
    level: float,
    tolerance_pct: float = 0.5,
) -> int:
    band = level * tolerance_pct / 100
    touches = ((series >= level - band) & (series <= level + band)).sum()
    return int(touches)


def calculate_containment(
    df: pd.DataFrame,
    range_low: float,
    range_high: float,
) -> float:
    contained = ((df["low"] >= range_low * 0.99) & (df["high"] <= range_high * 1.01)).sum()
    return float(contained / len(df) * 100) if len(df) > 0 else 0


def validate_range(df: pd.DataFrame, config: Dict) -> RangeInfo:
    rcfg = config.get("range", {})
    lookback = rcfg.get("lookback_days", 60)
    pct_low = rcfg.get("percentile_low", 5)
    pct_high = rcfg.get("percentile_high", 95)
    min_width = rcfg.get("min_range_width_pct", 1.0)
    max_width = rcfg.get("max_range_width_pct", 20.0)
    watch_max_width = rcfg.get("range_watch_max_width_pct", 25.0)
    min_support = rcfg.get("min_support_touches", 2)
    min_resistance = rcfg.get("min_resistance_touches", 2)
    min_containment = rcfg.get("min_price_containment_pct", 80)

    recent = df.tail(lookback)
    if len(recent) < lookback * 0.6:
        return RangeInfo(reject_reason=f"insufficient data: {len(recent)} days",
                         reject_status="DATA_FAIL")

    range_low, range_high = calculate_range_percentile(df, lookback, pct_low, pct_high)
    if range_low is None or range_high is None or range_low >= range_high:
        return RangeInfo(reject_reason="invalid range bounds",
                         reject_status="DATA_FAIL")

    width_points = range_high - range_low
    width_pct = (width_points / range_low) * 100

    base = RangeInfo(
        low=round(range_low, 2),
        high=round(range_high, 2),
        width_points=round(width_points, 2),
        width_pct=round(width_pct, 2),
    )

    # Width checks — hard fail, watch zone, or pass
    if width_pct < min_width:
        base.reject_reason = f"range width {width_pct:.2f}% < {min_width}%"
        base.reject_status = "RANGE_TOO_NARROW"
        return base

    if width_pct > watch_max_width:
        base.reject_reason = f"range width {width_pct:.2f}% > {watch_max_width}%"
        base.reject_status = "RANGE_TOO_WIDE"
        return base

    # Support/resistance touches
    support_touches = count_zone_touches(recent["low"], range_low, tolerance_pct=1.0)
    resistance_touches = count_zone_touches(recent["high"], range_high, tolerance_pct=1.0)
    base.support_touches = support_touches
    base.resistance_touches = resistance_touches

    if support_touches < min_support:
        base.reject_reason = f"support touches {support_touches} < {min_support}"
        base.reject_status = "INVALID_RANGE_SUPPORT_TOUCHES"
        return base

    if resistance_touches < min_resistance:
        base.reject_reason = f"resistance touches {resistance_touches} < {min_resistance}"
        base.reject_status = "INVALID_RANGE_RESISTANCE_TOUCHES"
        return base

    # Containment
    containment = calculate_containment(recent, range_low, range_high)
    base.containment_pct = round(containment, 1)

    if containment < min_containment:
        base.reject_reason = f"containment {containment:.1f}% < {min_containment}%"
        base.reject_status = "INVALID_RANGE_CONTAINMENT"
        return base

    # Soft range width — valid for tracking but not trading
    if width_pct > max_width:
        base.valid = False
        base.in_watch_zone = True
        base.reject_reason = f"range width {width_pct:.2f}% > {max_width}% (watch zone)"
        base.reject_status = "RANGE_WATCH"
        return base

    # All checks passed
    base.valid = True
    return base
