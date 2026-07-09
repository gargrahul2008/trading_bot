from __future__ import annotations

from dataclasses import dataclass
from datetime import time

import pandas as pd

from ..regime import build_trend_regime_filters
from ..strategy import SIGNAL_COLUMNS, Strategy


@dataclass
class _PullbackState:
    count: int = 0
    extreme_price: float | None = None
    volume_sum: float = 0.0
    structure_price: float | None = None

    def reset(self) -> None:
        self.count = 0
        self.extreme_price = None
        self.volume_sum = 0.0
        self.structure_price = None


@dataclass
class TrendPullbackStrategy(Strategy):
    name: str = "trend_pullback"
    opening_range_minutes: int = 15
    pullback_touch_buffer_atr: float = 0.1
    minimum_pullback_depth_atr: float = 0.1
    minimum_pullback_bars: int = 2
    stop_atr_multiple: float = 0.1
    target_atr_multiple: float = 1.8
    minimum_trend_gap_atr: float = 0.2
    minimum_reclaim_body_pct: float = 0.3
    minimum_close_location_pct: float = 0.5
    minimum_rejection_wick_to_body_ratio: float = 1.5
    minimum_signal_score: float = 0.0
    minimum_volume_expansion_ratio: float = 1.0
    volume_expansion_lookback: int = 5
    swing_lookback: int = 5
    structural_stop_buffer_atr: float | None = None
    maximum_consumed_target_pct: float = 0.40
    require_vwap_alignment: bool = True
    require_break_of_prior_candle: bool = False
    allow_intrabar_rejection_entry: bool = False
    skip_midday_window_start: time | None = None
    skip_midday_window_end: time | None = None
    max_trades_per_side_per_day: int | None = 1

    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        signal_rows: list[dict[str, object]] = []
        for _, group in frame.groupby(["symbol", "trade_date"], sort=False):
            signal_rows.extend(self._generate_group_signals(group.copy()))
        return pd.DataFrame(signal_rows, columns=SIGNAL_COLUMNS)

    def _generate_group_signals(self, group: pd.DataFrame) -> list[dict[str, object]]:
        group = group.reset_index(drop=True)
        signal_rows: list[dict[str, object]] = []

        prior_high = group["high"].shift(1)
        prior_low = group["low"].shift(1)
        candle_range = (group["high"] - group["low"]).replace(0, 1e-6)
        body = (group["close"] - group["open"]).abs()
        body_pct = body / candle_range
        close_location_pct = ((group["close"] - group["low"]) / candle_range).clip(lower=0.0, upper=1.0)
        lower_wick = (group[["open", "close"]].min(axis=1) - group["low"]).clip(lower=0.0)
        upper_wick = (group["high"] - group[["open", "close"]].max(axis=1)).clip(lower=0.0)

        volume_baseline = (
            group["volume"].shift(1).rolling(self.volume_expansion_lookback, min_periods=1).mean().fillna(group["volume"])
        )
        volume_ratio = (group["volume"] / volume_baseline.replace(0, pd.NA)).fillna(1.0)
        midday_filter = self._outside_midday_window(group["timestamp"])
        long_regime_filter, short_regime_filter = build_trend_regime_filters(
            group,
            minimum_trend_gap_atr=self.minimum_trend_gap_atr,
            require_price_above_vwap_for_long=self.require_vwap_alignment,
            require_price_below_vwap_for_short=self.require_vwap_alignment,
        )

        long_touch_level = group["ema_fast"] + (group["atr"] * self.pullback_touch_buffer_atr)
        short_touch_level = group["ema_fast"] - (group["atr"] * self.pullback_touch_buffer_atr)
        long_touch = group["low"] <= long_touch_level
        short_touch = group["high"] >= short_touch_level
        long_pullback_depth = (group["ema_fast"] - group["low"]) / group["atr"].replace(0, 1e-6)
        short_pullback_depth = (group["high"] - group["ema_fast"]) / group["atr"].replace(0, 1e-6)

        long_state = _PullbackState()
        short_state = _PullbackState()
        long_trades = 0
        short_trades = 0

        for idx, row in group.iterrows():
            row_atr = max(float(row["atr"]), 1e-6)
            row_baseline_volume = max(float(volume_baseline.iloc[idx]), 1e-6)
            row_body = float(body.iloc[idx])
            row_body_pct = float(body_pct.iloc[idx])
            row_close_quality = float(close_location_pct.iloc[idx])
            row_short_close_quality = float(1.0 - close_location_pct.iloc[idx])
            row_volume_ratio = float(volume_ratio.iloc[idx])
            row_long_depth = float(long_pullback_depth.iloc[idx])
            row_short_depth = float(short_pullback_depth.iloc[idx])

            long_temp_low = long_state.extreme_price
            if bool(long_touch.iloc[idx]):
                long_temp_low = float(row["low"]) if long_temp_low is None else min(long_temp_low, float(row["low"]))

            short_temp_high = short_state.extreme_price
            if bool(short_touch.iloc[idx]):
                short_temp_high = float(row["high"]) if short_temp_high is None else max(short_temp_high, float(row["high"]))

            if self._is_valid_long_candidate(
                idx=idx,
                row=row,
                group=group,
                state=long_state,
                trades_taken=long_trades,
                temp_pullback_low=long_temp_low,
                long_regime_filter=long_regime_filter,
                midday_filter=midday_filter,
                body_pct=row_body_pct,
                close_quality=row_close_quality,
                lower_wick=float(lower_wick.iloc[idx]),
                body_value=row_body,
                pullback_depth=row_long_depth,
                volume_baseline=row_baseline_volume,
                volume_ratio=row_volume_ratio,
                prior_high=prior_high,
            ):
                signal = self._build_long_signal(
                    row=row,
                    row_atr=row_atr,
                    row_body_pct=row_body_pct,
                    close_quality=row_close_quality,
                    baseline_volume=row_baseline_volume,
                    volume_ratio=row_volume_ratio,
                    pullback_depth=row_long_depth,
                    pullback_state=long_state,
                    pullback_low=long_temp_low,
                    lower_wick=float(lower_wick.iloc[idx]),
                    body_value=row_body,
                )
                if signal is not None:
                    signal_rows.append(signal)
                    long_trades += 1
                    long_state.reset()
            elif self._is_valid_short_candidate(
                idx=idx,
                row=row,
                group=group,
                state=short_state,
                trades_taken=short_trades,
                temp_pullback_high=short_temp_high,
                short_regime_filter=short_regime_filter,
                midday_filter=midday_filter,
                body_pct=row_body_pct,
                close_quality=row_short_close_quality,
                upper_wick=float(upper_wick.iloc[idx]),
                body_value=row_body,
                pullback_depth=row_short_depth,
                volume_baseline=row_baseline_volume,
                volume_ratio=row_volume_ratio,
                prior_low=prior_low,
            ):
                signal = self._build_short_signal(
                    row=row,
                    row_atr=row_atr,
                    row_body_pct=row_body_pct,
                    close_quality=row_short_close_quality,
                    baseline_volume=row_baseline_volume,
                    volume_ratio=row_volume_ratio,
                    pullback_depth=row_short_depth,
                    pullback_state=short_state,
                    pullback_high=short_temp_high,
                    upper_wick=float(upper_wick.iloc[idx]),
                    body_value=row_body,
                )
                if signal is not None:
                    signal_rows.append(signal)
                    short_trades += 1
                    short_state.reset()

            if self.max_trades_per_side_per_day is not None and long_trades >= self.max_trades_per_side_per_day:
                long_state.reset()
            if self.max_trades_per_side_per_day is not None and short_trades >= self.max_trades_per_side_per_day:
                short_state.reset()

            self._advance_long_state(idx, row, group, long_regime_filter, long_touch, long_state, long_trades)
            self._advance_short_state(idx, row, group, short_regime_filter, short_touch, short_state, short_trades)

        return signal_rows

    def _is_valid_long_candidate(
        self,
        *,
        idx: int,
        row: pd.Series,
        group: pd.DataFrame,
        state: _PullbackState,
        trades_taken: int,
        temp_pullback_low: float | None,
        long_regime_filter: pd.Series,
        midday_filter: pd.Series,
        body_pct: float,
        close_quality: float,
        lower_wick: float,
        body_value: float,
        pullback_depth: float,
        volume_baseline: float,
        volume_ratio: float,
        prior_high: pd.Series,
    ) -> bool:
        if state.count < self.minimum_pullback_bars or temp_pullback_low is None or state.structure_price is None:
            return False
        if self.max_trades_per_side_per_day is not None and trades_taken >= self.max_trades_per_side_per_day:
            return False
        if not bool(long_regime_filter.iloc[idx]) or not bool(midday_filter.iloc[idx]):
            return False
        if int(row["session_minute"]) < self.opening_range_minutes:
            return False
        if pullback_depth < self.minimum_pullback_depth_atr:
            return False
        if float(row["close"]) <= float(row["open"]):
            return False
        if body_pct < self.minimum_reclaim_body_pct:
            return False
        if close_quality < self.minimum_close_location_pct:
            return False
        if body_value <= 0 or lower_wick < (body_value * self.minimum_rejection_wick_to_body_ratio):
            return False
        if not self.allow_intrabar_rejection_entry and float(row["close"]) <= float(row["ema_fast"]):
            return False
        if self.allow_intrabar_rejection_entry:
            if float(row["close"]) <= float(row["ema_fast"]) and close_quality < self.minimum_close_location_pct:
                return False
        if self.require_break_of_prior_candle and not pd.isna(prior_high.iloc[idx]) and float(row["close"]) <= float(prior_high.iloc[idx]):
            return False
        if float(row["volume"]) < (volume_baseline * self.minimum_volume_expansion_ratio):
            return False
        if temp_pullback_low < state.structure_price:
            return False

        stop_price = state.structure_price - (float(row["atr"]) * self._structural_stop_buffer())
        if float(row["close"]) <= stop_price:
            return False
        move_already_run = float(row["close"]) - temp_pullback_low
        expected_move = float(row["atr"]) * self.target_atr_multiple
        if expected_move <= 0:
            return False
        if move_already_run / expected_move > self.maximum_consumed_target_pct:
            return False
        return True

    def _is_valid_short_candidate(
        self,
        *,
        idx: int,
        row: pd.Series,
        group: pd.DataFrame,
        state: _PullbackState,
        trades_taken: int,
        temp_pullback_high: float | None,
        short_regime_filter: pd.Series,
        midday_filter: pd.Series,
        body_pct: float,
        close_quality: float,
        upper_wick: float,
        body_value: float,
        pullback_depth: float,
        volume_baseline: float,
        volume_ratio: float,
        prior_low: pd.Series,
    ) -> bool:
        if state.count < self.minimum_pullback_bars or temp_pullback_high is None or state.structure_price is None:
            return False
        if self.max_trades_per_side_per_day is not None and trades_taken >= self.max_trades_per_side_per_day:
            return False
        if not bool(short_regime_filter.iloc[idx]) or not bool(midday_filter.iloc[idx]):
            return False
        if int(row["session_minute"]) < self.opening_range_minutes:
            return False
        if pullback_depth < self.minimum_pullback_depth_atr:
            return False
        if float(row["close"]) >= float(row["open"]):
            return False
        if body_pct < self.minimum_reclaim_body_pct:
            return False
        if close_quality < self.minimum_close_location_pct:
            return False
        if body_value <= 0 or upper_wick < (body_value * self.minimum_rejection_wick_to_body_ratio):
            return False
        if not self.allow_intrabar_rejection_entry and float(row["close"]) >= float(row["ema_fast"]):
            return False
        if self.allow_intrabar_rejection_entry:
            if float(row["close"]) >= float(row["ema_fast"]) and close_quality < self.minimum_close_location_pct:
                return False
        if self.require_break_of_prior_candle and not pd.isna(prior_low.iloc[idx]) and float(row["close"]) >= float(prior_low.iloc[idx]):
            return False
        if float(row["volume"]) < (volume_baseline * self.minimum_volume_expansion_ratio):
            return False
        if temp_pullback_high > state.structure_price:
            return False

        stop_price = state.structure_price + (float(row["atr"]) * self._structural_stop_buffer())
        if float(row["close"]) >= stop_price:
            return False
        move_already_run = temp_pullback_high - float(row["close"])
        expected_move = float(row["atr"]) * self.target_atr_multiple
        if expected_move <= 0:
            return False
        if move_already_run / expected_move > self.maximum_consumed_target_pct:
            return False
        return True

    def _advance_long_state(
        self,
        idx: int,
        row: pd.Series,
        group: pd.DataFrame,
        long_regime_filter: pd.Series,
        long_touch: pd.Series,
        state: _PullbackState,
        trades_taken: int,
    ) -> None:
        if self.max_trades_per_side_per_day is not None and trades_taken >= self.max_trades_per_side_per_day:
            state.reset()
            return
        if not bool(long_regime_filter.iloc[idx]):
            state.reset()
            return
        if bool(long_touch.iloc[idx]):
            if state.count == 0:
                state.structure_price = self._pre_pullback_swing_low(group, idx)
                state.extreme_price = float(row["low"])
                state.volume_sum = float(row["volume"])
                state.count = 1
            else:
                state.extreme_price = min(float(state.extreme_price), float(row["low"])) if state.extreme_price is not None else float(row["low"])
                state.volume_sum += float(row["volume"])
                state.count += 1
            return
        if float(row["close"]) > float(row["ema_fast"]):
            state.reset()

    def _advance_short_state(
        self,
        idx: int,
        row: pd.Series,
        group: pd.DataFrame,
        short_regime_filter: pd.Series,
        short_touch: pd.Series,
        state: _PullbackState,
        trades_taken: int,
    ) -> None:
        if self.max_trades_per_side_per_day is not None and trades_taken >= self.max_trades_per_side_per_day:
            state.reset()
            return
        if not bool(short_regime_filter.iloc[idx]):
            state.reset()
            return
        if bool(short_touch.iloc[idx]):
            if state.count == 0:
                state.structure_price = self._pre_pullback_swing_high(group, idx)
                state.extreme_price = float(row["high"])
                state.volume_sum = float(row["volume"])
                state.count = 1
            else:
                state.extreme_price = max(float(state.extreme_price), float(row["high"])) if state.extreme_price is not None else float(row["high"])
                state.volume_sum += float(row["volume"])
                state.count += 1
            return
        if float(row["close"]) < float(row["ema_fast"]):
            state.reset()

    def _build_long_signal(
        self,
        *,
        row: pd.Series,
        row_atr: float,
        row_body_pct: float,
        close_quality: float,
        baseline_volume: float,
        volume_ratio: float,
        pullback_depth: float,
        pullback_state: _PullbackState,
        pullback_low: float | None,
        lower_wick: float,
        body_value: float,
    ) -> dict[str, object] | None:
        stop_price = float(pullback_state.structure_price) - (row_atr * self._structural_stop_buffer())
        target_price = float(row["close"]) + (row_atr * self.target_atr_multiple)
        strength = self._score_signal(
            trend_gap_atr=abs(float(row["trend_gap_atr"])),
            pullback_depth=pullback_depth,
            body_pct=row_body_pct,
            close_quality=close_quality,
            pullback_volume_avg=(pullback_state.volume_sum / max(pullback_state.count, 1)),
            baseline_volume=max(float(baseline_volume), 1e-6),
            volume_ratio=volume_ratio,
            wick_to_body=(lower_wick / max(body_value, 1e-6)),
        )
        if strength < self.minimum_signal_score:
            return None
        return {
            "timestamp": row["timestamp"],
            "symbol": row["symbol"],
            "direction": "LONG",
            "strength": float(strength),
            "reason": (
                "trend_pullback_long_rejection"
                if self.allow_intrabar_rejection_entry and float(row["close"]) <= float(row["ema_fast"])
                else "trend_pullback_long_reclaim"
            ),
            "stop_loss": float(stop_price),
            "target": float(target_price),
            "strategy_name": self.name,
        }

    def _build_short_signal(
        self,
        *,
        row: pd.Series,
        row_atr: float,
        row_body_pct: float,
        close_quality: float,
        baseline_volume: float,
        volume_ratio: float,
        pullback_depth: float,
        pullback_state: _PullbackState,
        pullback_high: float | None,
        upper_wick: float,
        body_value: float,
    ) -> dict[str, object] | None:
        stop_price = float(pullback_state.structure_price) + (row_atr * self._structural_stop_buffer())
        target_price = float(row["close"]) - (row_atr * self.target_atr_multiple)
        strength = self._score_signal(
            trend_gap_atr=abs(float(row["trend_gap_atr"])),
            pullback_depth=pullback_depth,
            body_pct=row_body_pct,
            close_quality=close_quality,
            pullback_volume_avg=(pullback_state.volume_sum / max(pullback_state.count, 1)),
            baseline_volume=max(float(baseline_volume), 1e-6),
            volume_ratio=volume_ratio,
            wick_to_body=(upper_wick / max(body_value, 1e-6)),
        )
        if strength < self.minimum_signal_score:
            return None
        return {
            "timestamp": row["timestamp"],
            "symbol": row["symbol"],
            "direction": "SHORT",
            "strength": float(strength),
            "reason": (
                "trend_pullback_short_rejection"
                if self.allow_intrabar_rejection_entry and float(row["close"]) >= float(row["ema_fast"])
                else "trend_pullback_short_reclaim"
            ),
            "stop_loss": float(stop_price),
            "target": float(target_price),
            "strategy_name": self.name,
        }

    def _score_signal(
        self,
        *,
        trend_gap_atr: float,
        pullback_depth: float,
        body_pct: float,
        close_quality: float,
        pullback_volume_avg: float,
        baseline_volume: float,
        volume_ratio: float,
        wick_to_body: float,
    ) -> float:
        trend_score = self._scale_score(trend_gap_atr, max(self.minimum_trend_gap_atr * 2.0, 0.5), 20.0)
        depth_score = self._scale_score(pullback_depth, max(self.minimum_pullback_depth_atr * 2.0, 0.5), 20.0)
        body_score = self._scale_score(body_pct, 1.0, 15.0)
        close_score = self._scale_score(close_quality, 1.0, 15.0)
        dryup_ratio = 0.0 if baseline_volume <= 0 else max((baseline_volume - pullback_volume_avg) / baseline_volume, 0.0)
        dryup_score = self._scale_score(dryup_ratio, 0.5, 10.0)
        expansion_score = self._scale_score(
            max(volume_ratio - 1.0, 0.0),
            max(self.minimum_volume_expansion_ratio - 1.0, 0.25),
            10.0,
        )
        wick_score = self._scale_score(
            wick_to_body,
            max(self.minimum_rejection_wick_to_body_ratio * 1.5, 1.0),
            10.0,
        )
        return min(trend_score + depth_score + body_score + close_score + dryup_score + expansion_score + wick_score, 100.0)

    @staticmethod
    def _scale_score(value: float, cap: float, points: float) -> float:
        if cap <= 0:
            return points
        ratio = max(min(value / cap, 1.0), 0.0)
        return ratio * points

    def _structural_stop_buffer(self) -> float:
        if self.structural_stop_buffer_atr is not None:
            return float(self.structural_stop_buffer_atr)
        return float(self.stop_atr_multiple)

    def _pre_pullback_swing_low(self, group: pd.DataFrame, idx: int) -> float:
        start = max(0, idx - self.swing_lookback)
        history = group.iloc[start:idx]
        if history.empty:
            return float(group.iloc[idx]["low"])
        return float(history["low"].min())

    def _pre_pullback_swing_high(self, group: pd.DataFrame, idx: int) -> float:
        start = max(0, idx - self.swing_lookback)
        history = group.iloc[start:idx]
        if history.empty:
            return float(group.iloc[idx]["high"])
        return float(history["high"].max())

    def _outside_midday_window(self, timestamps: pd.Series) -> pd.Series:
        if self.skip_midday_window_start is None or self.skip_midday_window_end is None:
            return pd.Series(True, index=timestamps.index)
        times = pd.to_datetime(timestamps).dt.time
        return ~times.between(self.skip_midday_window_start, self.skip_midday_window_end)
