from __future__ import annotations

import numpy as np
import pandas as pd


class FeatureEngine:
    """Compute reusable intraday features from 1-minute candles."""

    def __init__(
        self,
        atr_window: int = 14,
        opening_range_minutes: int = 15,
        ema_fast_span: int = 20,
        ema_slow_span: int = 50,
    ) -> None:
        self.atr_window = atr_window
        self.opening_range_minutes = opening_range_minutes
        self.ema_fast_span = ema_fast_span
        self.ema_slow_span = ema_slow_span

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        featured = frame.copy()
        featured = featured.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        if "trade_date" not in featured.columns:
            featured["trade_date"] = pd.to_datetime(featured["timestamp"]).dt.date

        featured["session_minute"] = featured.groupby(["symbol", "trade_date"]).cumcount()
        featured["typical_price"] = (
            featured["high"] + featured["low"] + featured["close"]
        ) / 3.0

        grouped = featured.groupby(["symbol", "trade_date"], sort=False)
        featured["tpv"] = featured["typical_price"] * featured["volume"]
        cumulative_tpv = grouped["tpv"].cumsum()
        cumulative_volume = grouped["volume"].cumsum()
        featured["vwap"] = cumulative_tpv / cumulative_volume.replace(0, np.nan)

        featured["prev_close"] = featured.groupby("symbol")["close"].shift(1)
        reference_close = featured["prev_close"].fillna(featured["close"])
        featured["true_range"] = np.maximum.reduce(
            [
                featured["high"] - featured["low"],
                (featured["high"] - reference_close).abs(),
                (featured["low"] - reference_close).abs(),
            ]
        )
        featured["atr"] = (
            featured.groupby("symbol", sort=False)["true_range"]
            .transform(lambda series: series.rolling(self.atr_window, min_periods=1).mean())
        )
        featured["ema_fast"] = featured.groupby("symbol", sort=False)["close"].transform(
            lambda series: series.ewm(span=self.ema_fast_span, adjust=False, min_periods=1).mean()
        )
        featured["ema_slow"] = featured.groupby("symbol", sort=False)["close"].transform(
            lambda series: series.ewm(span=self.ema_slow_span, adjust=False, min_periods=1).mean()
        )
        featured["trend_gap_atr"] = (featured["ema_fast"] - featured["ema_slow"]) / featured["atr"].replace(0, np.nan)

        opening_mask = featured["session_minute"] < self.opening_range_minutes
        featured["opening_range_high"] = grouped["high"].transform(
            lambda series: series.where(opening_mask.loc[series.index]).cummax().ffill()
        )
        featured["opening_range_low"] = grouped["low"].transform(
            lambda series: series.where(opening_mask.loc[series.index]).cummin().ffill()
        )

        day_summary = (
            featured.groupby(["symbol", "trade_date"], sort=False)
            .agg(day_high=("high", "max"), day_low=("low", "min"), day_close=("close", "last"))
            .groupby(level=0, sort=False)
            .shift(1)
            .rename(
                columns={
                    "day_high": "prev_day_high",
                    "day_low": "prev_day_low",
                    "day_close": "prev_day_close",
                }
            )
            .reset_index()
        )
        featured = featured.merge(day_summary, on=["symbol", "trade_date"], how="left")
        return featured[
            [
                "timestamp",
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "session_minute",
                "vwap",
                "atr",
                "ema_fast",
                "ema_slow",
                "trend_gap_atr",
                "opening_range_high",
                "opening_range_low",
                "prev_day_high",
                "prev_day_low",
                "prev_day_close",
            ]
        ].copy()
