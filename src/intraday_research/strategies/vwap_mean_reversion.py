from __future__ import annotations

from dataclasses import dataclass
from datetime import time

import pandas as pd

from ..regime import build_trend_regime_filters
from ..strategy import SIGNAL_COLUMNS, Strategy


@dataclass
class VWAPMeanReversionStrategy(Strategy):
    name: str = "vwap_mean_reversion"
    opening_range_minutes: int = 15
    reversion_band_atr: float = 0.5
    stop_atr_multiple: float = 0.8
    target_atr_multiple: float = 1.2
    minimum_deviation_atr: float = 0.0
    require_reversal_confirmation: bool = False
    require_next_candle_followthrough: bool = False
    minimum_reversal_body_pct: float = 0.0
    minimum_rejection_wick_pct: float = 0.0
    minimum_volume_expansion_ratio: float = 0.0
    volume_expansion_lookback: int = 5
    minimum_extreme_deviation_atr: float = 0.0
    require_prior_candle_reclaim: bool = False
    require_opening_range_reclaim: bool = False
    maximum_vwap_slope_atr: float | None = None
    vwap_slope_lookback: int = 3
    skip_midday_window_start: time | None = None
    skip_midday_window_end: time | None = None
    max_trades_per_side_per_day: int | None = None
    require_trend_alignment: bool = False
    minimum_trend_gap_atr: float = 0.0

    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        signal_rows: list[dict[str, object]] = []
        for _, group in frame.groupby(["symbol", "trade_date"], sort=False):
            group = group.copy()
            upper_band = group["vwap"] + (group["atr"] * self.reversion_band_atr)
            lower_band = group["vwap"] - (group["atr"] * self.reversion_band_atr)
            minimum_lower_deviation = group["vwap"] - (group["atr"] * self.minimum_deviation_atr)
            minimum_upper_deviation = group["vwap"] + (group["atr"] * self.minimum_deviation_atr)
            prior_close = group["close"].shift(1)
            prior_high = group["high"].shift(1)
            prior_low = group["low"].shift(1)
            candle_range = (group["high"] - group["low"]).replace(0, 1e-6)
            candle_body = (group["close"] - group["open"]).abs()
            body_pct = candle_body / candle_range
            lower_wick_pct = (group[["open", "close"]].min(axis=1) - group["low"]) / candle_range
            upper_wick_pct = (group["high"] - group[["open", "close"]].max(axis=1)) / candle_range
            maximum_lower_deviation = group["vwap"] - (group["atr"] * self.minimum_extreme_deviation_atr)
            maximum_upper_deviation = group["vwap"] + (group["atr"] * self.minimum_extreme_deviation_atr)
            volume_baseline = (
                group["volume"].shift(1).rolling(self.volume_expansion_lookback, min_periods=1).mean()
            )
            volume_filter = (
                group["volume"] >= (volume_baseline * self.minimum_volume_expansion_ratio)
                if self.minimum_volume_expansion_ratio > 0
                else pd.Series(True, index=group.index)
            )
            vwap_slope_filter = pd.Series(True, index=group.index)
            if self.maximum_vwap_slope_atr is not None:
                slope_lookback = max(self.vwap_slope_lookback, 1)
                vwap_slope = (
                    group["vwap"] - group["vwap"].shift(slope_lookback)
                ).abs() / max(slope_lookback, 1)
                normalized_slope = vwap_slope / group["atr"].replace(0, 1e-6)
                vwap_slope_filter &= normalized_slope <= self.maximum_vwap_slope_atr
            reversal_filter_long = pd.Series(True, index=group.index)
            reversal_filter_short = pd.Series(True, index=group.index)
            if self.require_reversal_confirmation:
                reversal_filter_long &= (group["close"] > group["open"]) & (group["close"] > prior_close)
                reversal_filter_short &= (group["close"] < group["open"]) & (group["close"] < prior_close)
            if self.minimum_reversal_body_pct > 0:
                reversal_filter_long &= body_pct >= self.minimum_reversal_body_pct
                reversal_filter_short &= body_pct >= self.minimum_reversal_body_pct
            if self.minimum_rejection_wick_pct > 0:
                reversal_filter_long &= lower_wick_pct >= self.minimum_rejection_wick_pct
                reversal_filter_short &= upper_wick_pct >= self.minimum_rejection_wick_pct
            if self.require_prior_candle_reclaim:
                reversal_filter_long &= group["close"] > prior_high
                reversal_filter_short &= group["close"] < prior_low
            if self.require_opening_range_reclaim:
                reversal_filter_long &= group["close"] >= group["opening_range_low"]
                reversal_filter_short &= group["close"] <= group["opening_range_high"]
            reversal_filter_long &= volume_filter
            reversal_filter_short &= volume_filter
            midday_filter = self._outside_midday_window(group["timestamp"])
            long_regime_filter = pd.Series(True, index=group.index)
            short_regime_filter = pd.Series(True, index=group.index)
            if self.require_trend_alignment:
                long_regime_filter, short_regime_filter = build_trend_regime_filters(
                    group,
                    minimum_trend_gap_atr=self.minimum_trend_gap_atr,
                    require_price_above_vwap_for_long=False,
                    require_price_below_vwap_for_short=False,
                )

            long_candidates = (
                (group["session_minute"] >= self.opening_range_minutes)
                & (prior_close < lower_band)
                & (prior_close <= minimum_lower_deviation)
                & (group["low"] <= maximum_lower_deviation)
                & (group["close"] >= lower_band)
                & reversal_filter_long
                & vwap_slope_filter
                & midday_filter
                & long_regime_filter
            )
            short_candidates = (
                (group["session_minute"] >= self.opening_range_minutes)
                & (prior_close > upper_band)
                & (prior_close >= minimum_upper_deviation)
                & (group["high"] >= maximum_upper_deviation)
                & (group["close"] <= upper_band)
                & reversal_filter_short
                & vwap_slope_filter
                & midday_filter
                & short_regime_filter
            )

            if self.require_next_candle_followthrough:
                long_followthrough = (
                    long_candidates.shift(1, fill_value=False)
                    & (group["close"] > group["open"])
                    & (group["close"] > group["close"].shift(1))
                    & (group["high"] > group["high"].shift(1))
                )
                short_followthrough = (
                    short_candidates.shift(1, fill_value=False)
                    & (group["close"] < group["open"])
                    & (group["close"] < group["close"].shift(1))
                    & (group["low"] < group["low"].shift(1))
                )
                long_signal_rows = group.loc[long_followthrough]
                short_signal_rows = group.loc[short_followthrough]
            else:
                long_signal_rows = group.loc[long_candidates]
                short_signal_rows = group.loc[short_candidates]
            if self.max_trades_per_side_per_day is not None:
                long_signal_rows = long_signal_rows.head(self.max_trades_per_side_per_day)
                short_signal_rows = short_signal_rows.head(self.max_trades_per_side_per_day)

            for row in long_signal_rows.itertuples():
                signal_rows.append(
                    {
                        "timestamp": row.timestamp,
                        "symbol": row.symbol,
                        "direction": "LONG",
                        "strength": float(abs(row.close - row.vwap) / max(row.atr, 1e-6)),
                        "reason": (
                            "reclaim_from_vwap_discount_followthrough"
                            if self.require_next_candle_followthrough
                            else "reclaim_from_vwap_discount"
                        ),
                        "stop_loss": float(row.close - (row.atr * self.stop_atr_multiple)),
                        "target": float(row.close + (row.atr * self.target_atr_multiple)),
                        "strategy_name": self.name,
                    }
                )
            for row in short_signal_rows.itertuples():
                signal_rows.append(
                    {
                        "timestamp": row.timestamp,
                        "symbol": row.symbol,
                        "direction": "SHORT",
                        "strength": float(abs(row.close - row.vwap) / max(row.atr, 1e-6)),
                        "reason": (
                            "fade_back_to_vwap_followthrough"
                            if self.require_next_candle_followthrough
                            else "fade_back_to_vwap"
                        ),
                        "stop_loss": float(row.close + (row.atr * self.stop_atr_multiple)),
                        "target": float(row.close - (row.atr * self.target_atr_multiple)),
                        "strategy_name": self.name,
                    }
                )
        return pd.DataFrame(signal_rows, columns=SIGNAL_COLUMNS)

    def _outside_midday_window(self, timestamps: pd.Series) -> pd.Series:
        if self.skip_midday_window_start is None or self.skip_midday_window_end is None:
            return pd.Series(True, index=timestamps.index)
        times = pd.to_datetime(timestamps).dt.time
        return ~times.between(self.skip_midday_window_start, self.skip_midday_window_end)
