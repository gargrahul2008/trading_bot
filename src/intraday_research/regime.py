from __future__ import annotations

import pandas as pd


def build_trend_regime_filters(
    frame: pd.DataFrame,
    *,
    minimum_trend_gap_atr: float = 0.0,
    require_price_above_vwap_for_long: bool = False,
    require_price_below_vwap_for_short: bool = False,
) -> tuple[pd.Series, pd.Series]:
    """Return long/short regime masks from shared EMA and VWAP features."""

    minimum_gap = abs(float(minimum_trend_gap_atr))
    long_filter = (frame["ema_fast"] > frame["ema_slow"]) & (frame["trend_gap_atr"] >= minimum_gap)
    short_filter = (frame["ema_fast"] < frame["ema_slow"]) & (frame["trend_gap_atr"] <= -minimum_gap)

    if require_price_above_vwap_for_long:
        long_filter &= frame["close"] >= frame["vwap"]
    if require_price_below_vwap_for_short:
        short_filter &= frame["close"] <= frame["vwap"]

    return long_filter.fillna(False), short_filter.fillna(False)
