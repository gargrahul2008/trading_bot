from __future__ import annotations

from dataclasses import dataclass
from datetime import time

import pandas as pd

from ..regime import build_trend_regime_filters
from ..strategy import SIGNAL_COLUMNS, Strategy


@dataclass
class OpeningRangeBreakoutStrategy(Strategy):
    name: str = "opening_range_breakout"
    opening_range_minutes: int = 15
    breakout_buffer_atr: float = 0.1
    stop_atr_multiple: float = 1.0
    target_atr_multiple: float = 2.0
    minimum_opening_range_width_atr: float = 0.0
    breakout_volume_ratio: float = 0.0
    max_breakouts_per_side_per_day: int | None = 1
    skip_midday_window_start: time | None = None
    skip_midday_window_end: time | None = None
    require_trend_alignment: bool = False
    minimum_trend_gap_atr: float = 0.0
    require_vwap_alignment: bool = False

    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        signal_rows: list[dict[str, object]] = []
        for _, group in frame.groupby(["symbol", "trade_date"], sort=False):
            opening_slice = group[group["session_minute"] < self.opening_range_minutes]
            opening_range_avg_volume = float(opening_slice["volume"].mean()) if not opening_slice.empty else 0.0
            prior_close = group["close"].shift(1)
            long_trigger = group["opening_range_high"] + (group["atr"] * self.breakout_buffer_atr)
            short_trigger = group["opening_range_low"] - (group["atr"] * self.breakout_buffer_atr)
            opening_range_width = group["opening_range_high"] - group["opening_range_low"]
            width_filter = opening_range_width >= (group["atr"] * self.minimum_opening_range_width_atr)
            volume_filter = (
                group["volume"] >= (opening_range_avg_volume * self.breakout_volume_ratio)
                if self.breakout_volume_ratio > 0 and opening_range_avg_volume > 0
                else pd.Series(True, index=group.index)
            )
            midday_filter = self._outside_midday_window(group["timestamp"])
            long_regime_filter = pd.Series(True, index=group.index)
            short_regime_filter = pd.Series(True, index=group.index)
            if self.require_trend_alignment:
                long_regime_filter, short_regime_filter = build_trend_regime_filters(
                    group,
                    minimum_trend_gap_atr=self.minimum_trend_gap_atr,
                    require_price_above_vwap_for_long=self.require_vwap_alignment,
                    require_price_below_vwap_for_short=self.require_vwap_alignment,
                )

            long_entries = (
                (group["session_minute"] >= self.opening_range_minutes)
                & (group["close"] > long_trigger)
                & (prior_close <= long_trigger)
                & (group["close"] > group["vwap"])
                & width_filter
                & volume_filter
                & midday_filter
                & long_regime_filter
            )
            short_entries = (
                (group["session_minute"] >= self.opening_range_minutes)
                & (group["close"] < short_trigger)
                & (prior_close >= short_trigger)
                & (group["close"] < group["vwap"])
                & width_filter
                & volume_filter
                & midday_filter
                & short_regime_filter
            )

            long_signal_rows = group.loc[long_entries]
            short_signal_rows = group.loc[short_entries]
            if self.max_breakouts_per_side_per_day is not None:
                long_signal_rows = long_signal_rows.head(self.max_breakouts_per_side_per_day)
                short_signal_rows = short_signal_rows.head(self.max_breakouts_per_side_per_day)

            for row in long_signal_rows.itertuples():
                signal_rows.append(
                    {
                        "timestamp": row.timestamp,
                        "symbol": row.symbol,
                        "direction": "LONG",
                        "strength": float((row.close - row.opening_range_high) / max(row.atr, 1e-6)),
                        "reason": "breakout_above_opening_range",
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
                        "strength": float((row.opening_range_low - row.close) / max(row.atr, 1e-6)),
                        "reason": "breakdown_below_opening_range",
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
