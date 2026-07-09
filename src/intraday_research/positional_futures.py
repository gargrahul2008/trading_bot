from __future__ import annotations

import math
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal

import pandas as pd


class FuturesSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True)
class FuturesInstrument:
    """Tradable continuous futures contract metadata."""

    symbol: str
    lot_size: int = 1
    underlying: str | None = None
    data_symbol: str | None = None


@dataclass(frozen=True)
class FuturesUniverse:
    """Universe definition for continuous futures positional research."""

    name: str
    instruments: tuple[FuturesInstrument, ...]
    benchmark_symbol: str | None = None

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(instrument.symbol for instrument in self.instruments)

    @property
    def data_symbols(self) -> tuple[str, ...]:
        return tuple(instrument.data_symbol or instrument.symbol for instrument in self.instruments)

    @property
    def instrument_map(self) -> dict[str, FuturesInstrument]:
        return {instrument.symbol: instrument for instrument in self.instruments}

    @property
    def data_symbol_map(self) -> dict[str, str]:
        return {instrument.data_symbol or instrument.symbol: instrument.symbol for instrument in self.instruments}


def load_futures_universe(path: str | Path) -> FuturesUniverse:
    """Load a futures universe JSON with symbols, lot sizes, and benchmark symbol."""

    payload = json.loads(Path(path).read_text())
    name = str(payload.get("name") or "").strip()
    benchmark_symbol = payload.get("benchmark_symbol")
    instruments: list[FuturesInstrument] = []
    for item in payload.get("symbols") or []:
        if isinstance(item, str):
            instruments.append(FuturesInstrument(symbol=item))
            continue
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        instruments.append(
            FuturesInstrument(
                symbol=symbol,
                lot_size=max(int(item.get("lot_size") or 1), 1),
                underlying=str(item.get("underlying") or "").strip() or None,
                data_symbol=str(item.get("data_symbol") or "").strip() or None,
            )
        )
    if not name:
        raise ValueError("Futures universe file must include a non-empty 'name'.")
    if not instruments:
        raise ValueError("Futures universe file must include at least one symbol.")
    return FuturesUniverse(name=name, instruments=tuple(instruments), benchmark_symbol=benchmark_symbol)


@dataclass(frozen=True)
class PositionalPullbackConfig:
    """Daily pullback strategy and execution controls for continuous futures."""

    ema_fast_span: int = 10
    ema_slow_span: int = 20
    atr_window: int = 14
    benchmark_symbol: str | None = None
    benchmark_trend_min_atr: float = 0.25
    benchmark_trend_max_atr: float = 1.25
    require_benchmark_alignment: bool = True
    allow_flat_benchmark: bool = False
    symbol_trend_min_atr: float = 0.35
    symbol_trend_max_atr: float | None = None
    pullback_touch_buffer_atr: float = 0.10
    pullback_min_depth_atr: float = 0.20
    pullback_max_depth_atr: float = 1.50
    minimum_pullback_bars: int = 2
    min_reclaim_body_pct: float = 0.35
    min_close_location_pct: float = 0.60
    require_rejection_wick: bool = False
    min_rejection_wick_to_body_ratio: float = 1.50
    require_volume_expansion: bool = False
    min_volume_ratio: float = 1.20
    volume_lookback: int = 20
    swing_lookback: int = 5
    structure_break_buffer_atr: float = 0.10
    target_atr_multiple: float = 2.0
    maximum_consumed_target_pct: float = 0.75
    structural_stop_buffer_atr: float = 0.0
    stop_mode: Literal["signal_candle", "structure"] = "signal_candle"
    allow_intrabar_rejection_entry: bool = False
    require_break_of_prior_candle: bool = False
    min_signal_score: float = 70.0
    entry_valid_days: int = 3
    reentry_days_after_stop: int = 3
    max_reentries: int = 1
    sizing_mode: Literal["fixed_lots", "capital", "risk"] = "risk"
    fixed_lots: int = 1
    initial_capital: float = 1_000_000.0
    capital_per_trade_pct: float = 0.20
    risk_per_trade_pct: float = 0.01
    max_capital_per_trade_pct: float = 0.25
    fee_bps_per_side: float = 1.0
    slippage_bps_per_side: float = 1.0
    max_holding_days: int | None = None
    trailing_stop_mode: Literal["off", "previous_bar"] = "previous_bar"
    trailing_activation_r: float = 1.0


@dataclass
class _FuturesPullbackState:
    count: int = 0
    extreme_price: float | None = None
    structure_price: float | None = None
    volume_sum: float = 0.0

    def reset(self) -> None:
        self.count = 0
        self.extreme_price = None
        self.structure_price = None
        self.volume_sum = 0.0

    @property
    def average_volume(self) -> float | None:
        if self.count <= 0:
            return None
        return self.volume_sum / self.count


@dataclass(frozen=True)
class PositionalSignal:
    symbol: str
    signal_time: pd.Timestamp
    side: FuturesSide
    score: float
    trigger_price: float
    stop_price: float
    valid_until: pd.Timestamp
    reason: str
    trend_gap_atr: float
    pullback_depth_atr: float
    benchmark_regime: str
    benchmark_trend_strength_atr: float
    structure_price: float | None = None
    consumed_target_pct: float | None = None


@dataclass(frozen=True)
class PositionalTrade:
    symbol: str
    side: FuturesSide
    lots: int
    lot_size: int
    effective_quantity: int
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    initial_stop_price: float
    final_stop_price: float
    gross_pnl: float
    costs: float
    net_pnl: float
    exit_reason: str
    reentry_number: int
    signal_score: float
    benchmark_regime: str


class DailyFuturesFeatureEngine:
    """Calculates daily trend, ATR, pullback, and benchmark-ready features."""

    def __init__(self, config: PositionalPullbackConfig | None = None) -> None:
        self.config = config or PositionalPullbackConfig()

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"Missing required daily OHLCV columns: {sorted(missing)}")

        data = frame.copy()
        data["timestamp"] = pd.to_datetime(data["timestamp"])
        if data["timestamp"].dt.tz is None:
            data["timestamp"] = data["timestamp"].dt.tz_localize("Asia/Kolkata")
        else:
            data["timestamp"] = data["timestamp"].dt.tz_convert("Asia/Kolkata")

        numeric_columns = ["open", "high", "low", "close", "volume"]
        data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors="raise")
        data = data.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        data["trade_date"] = data["timestamp"].dt.date

        pieces: list[pd.DataFrame] = []
        for _, group in data.groupby("symbol", sort=False):
            featured = group.copy().reset_index(drop=True)
            previous_close = featured["close"].shift(1)
            true_range = pd.concat(
                [
                    featured["high"] - featured["low"],
                    (featured["high"] - previous_close).abs(),
                    (featured["low"] - previous_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
            featured["atr"] = true_range.rolling(self.config.atr_window, min_periods=1).mean()
            featured["ema_fast"] = featured["close"].ewm(span=self.config.ema_fast_span, adjust=False).mean()
            featured["ema_slow"] = featured["close"].ewm(span=self.config.ema_slow_span, adjust=False).mean()
            featured["trend_gap_atr"] = (featured["ema_fast"] - featured["ema_slow"]) / featured["atr"].replace(0, pd.NA)
            featured["volume_sma"] = (
                featured["volume"].rolling(self.config.volume_lookback, min_periods=1).mean().shift(1)
            )
            featured["volume_ratio"] = featured["volume"] / featured["volume_sma"].replace(0, pd.NA)
            candle_range = (featured["high"] - featured["low"]).replace(0, pd.NA)
            featured["body_pct"] = (featured["close"] - featured["open"]).abs() / candle_range
            featured["close_location_pct"] = (featured["close"] - featured["low"]) / candle_range
            featured["close_location_short_pct"] = (featured["high"] - featured["close"]) / candle_range
            featured["pullback_depth_long_atr"] = (featured["ema_fast"] - featured["low"]) / featured["atr"].replace(0, pd.NA)
            featured["pullback_depth_short_atr"] = (featured["high"] - featured["ema_fast"]) / featured["atr"].replace(0, pd.NA)
            pieces.append(featured)

        return pd.concat(pieces, ignore_index=True) if pieces else data


class DailyPullbackFuturesStrategy:
    """
    Daily trend-pullback detector.

    The signal candle is not the trade. For longs, the signal day's high becomes
    the next-session breakout trigger and the signal day's low becomes the stop.
    Shorts are mirrored.
    """

    def __init__(self, config: PositionalPullbackConfig | None = None) -> None:
        self.config = config or PositionalPullbackConfig()

    def generate_signals(self, featured: pd.DataFrame) -> list[PositionalSignal]:
        benchmark = self._benchmark_by_date(featured)
        signals: list[PositionalSignal] = []
        symbols = [symbol for symbol in featured["symbol"].unique() if symbol != self.config.benchmark_symbol]
        for symbol in symbols:
            symbol_frame = featured.loc[featured["symbol"] == symbol].copy().reset_index(drop=True)
            signals.extend(self._generate_symbol_signals(symbol_frame, benchmark))
        return sorted(signals, key=lambda signal: (signal.signal_time, signal.symbol, signal.side.value))

    def diagnose_filters(self, featured: pd.DataFrame) -> pd.DataFrame:
        """Return a bar-by-bar filter audit for why daily pullback signals pass/fail."""

        benchmark = self._benchmark_by_date(featured)
        rows: list[dict[str, object]] = []
        symbols = [symbol for symbol in featured["symbol"].unique() if symbol != self.config.benchmark_symbol]
        for symbol in symbols:
            symbol_frame = featured.loc[featured["symbol"] == symbol].copy().reset_index(drop=True)
            long_state = _FuturesPullbackState()
            short_state = _FuturesPullbackState()
            for idx, row in symbol_frame.iterrows():
                bench = benchmark.get(row["trade_date"], {"regime": "FLAT", "trend_strength_atr": 0.0})
                rows.append(
                    self._diagnose_side(
                        row=row,
                        idx=idx,
                        side=FuturesSide.LONG,
                        benchmark=bench,
                        symbol_frame=symbol_frame,
                        state=long_state,
                        prior_candle_level=self._prior_high(symbol_frame, idx),
                    )
                )
                rows.append(
                    self._diagnose_side(
                        row=row,
                        idx=idx,
                        side=FuturesSide.SHORT,
                        benchmark=bench,
                        symbol_frame=symbol_frame,
                        state=short_state,
                        prior_candle_level=self._prior_low(symbol_frame, idx),
                    )
                )
                if self._has_required_values(row):
                    self._advance_state(symbol_frame, idx, row, FuturesSide.LONG, long_state)
                    self._advance_state(symbol_frame, idx, row, FuturesSide.SHORT, short_state)
                else:
                    long_state.reset()
                    short_state.reset()
        return pd.DataFrame(rows)

    def _generate_symbol_signals(
        self,
        symbol_frame: pd.DataFrame,
        benchmark: dict[object, dict[str, object]],
    ) -> list[PositionalSignal]:
        signals: list[PositionalSignal] = []
        long_state = _FuturesPullbackState()
        short_state = _FuturesPullbackState()

        for idx, row in symbol_frame.iterrows():
            if not self._has_required_values(row):
                long_state.reset()
                short_state.reset()
                continue

            bench = benchmark.get(row["trade_date"], {"regime": "FLAT", "trend_strength_atr": 0.0})
            prior_high = self._prior_high(symbol_frame, idx)
            prior_low = self._prior_low(symbol_frame, idx)

            long_signal = self._build_signal(
                row=row,
                idx=idx,
                side=FuturesSide.LONG,
                benchmark=bench,
                symbol_frame=symbol_frame,
                state=long_state,
                prior_candle_level=prior_high,
            )
            if long_signal is not None:
                signals.append(long_signal)
                long_state.reset()

            short_signal = self._build_signal(
                row=row,
                idx=idx,
                side=FuturesSide.SHORT,
                benchmark=bench,
                symbol_frame=symbol_frame,
                state=short_state,
                prior_candle_level=prior_low,
            )
            if short_signal is not None:
                signals.append(short_signal)
                short_state.reset()

            self._advance_state(symbol_frame, idx, row, FuturesSide.LONG, long_state)
            self._advance_state(symbol_frame, idx, row, FuturesSide.SHORT, short_state)

        return signals

    def _benchmark_by_date(self, featured: pd.DataFrame) -> dict[object, dict[str, object]]:
        if not self.config.benchmark_symbol:
            return {}
        benchmark = featured.loc[featured["symbol"] == self.config.benchmark_symbol].copy()
        regimes: dict[object, dict[str, object]] = {}
        for row in benchmark.itertuples():
            trend_gap = float(row.trend_gap_atr) if not pd.isna(row.trend_gap_atr) else 0.0
            abs_gap = abs(trend_gap)
            if abs_gap < self.config.benchmark_trend_min_atr:
                regime = "FLAT"
            elif self.config.benchmark_trend_max_atr is not None and abs_gap > self.config.benchmark_trend_max_atr:
                regime = "STRETCHED_LONG" if trend_gap > 0 else "STRETCHED_SHORT"
            elif trend_gap > 0:
                regime = "LONG"
            else:
                regime = "SHORT"
            regimes[row.trade_date] = {"regime": regime, "trend_strength_atr": abs_gap}
        return regimes

    def _build_signal(
        self,
        row: pd.Series,
        idx: int,
        side: FuturesSide,
        benchmark: dict[str, object],
        symbol_frame: pd.DataFrame,
        state: _FuturesPullbackState,
        prior_candle_level: float | None,
    ) -> PositionalSignal | None:
        if not self._benchmark_allows(side, str(benchmark.get("regime", "FLAT"))):
            return None
        if state.count < self.config.minimum_pullback_bars or state.extreme_price is None or state.structure_price is None:
            return None

        trend_gap = float(row["trend_gap_atr"])
        atr = float(row["atr"])
        if side is FuturesSide.LONG:
            if trend_gap < self.config.symbol_trend_min_atr:
                return None
            if self.config.symbol_trend_max_atr is not None and trend_gap > self.config.symbol_trend_max_atr:
                return None
            pullback_depth = self._long_pullback_depth(row)
            if not self._passes_common_pullback(row, pullback_depth, state=state, side=side, prior_candle_level=prior_candle_level):
                return None
            if not self._structure_holds(state=state, side=side, atr=atr):
                return None
            consumed_target_pct = (float(row["close"]) - state.extreme_price) / max(atr * self.config.target_atr_multiple, 1e-9)
            if consumed_target_pct > self.config.maximum_consumed_target_pct:
                return None
            trigger_price = float(row["high"])
            stop_price = self._stop_price(row, side=side, structure_price=state.structure_price, atr=atr)
        else:
            if trend_gap > -self.config.symbol_trend_min_atr:
                return None
            if self.config.symbol_trend_max_atr is not None and abs(trend_gap) > self.config.symbol_trend_max_atr:
                return None
            pullback_depth = self._short_pullback_depth(row)
            if not self._passes_common_pullback(row, pullback_depth, state=state, side=side, prior_candle_level=prior_candle_level):
                return None
            if not self._structure_holds(state=state, side=side, atr=atr):
                return None
            consumed_target_pct = (state.extreme_price - float(row["close"])) / max(atr * self.config.target_atr_multiple, 1e-9)
            if consumed_target_pct > self.config.maximum_consumed_target_pct:
                return None
            trigger_price = float(row["low"])
            stop_price = self._stop_price(row, side=side, structure_price=state.structure_price, atr=atr)

        risk = abs(trigger_price - stop_price)
        if risk <= 0 or atr <= 0:
            return None

        score = self._score(row=row, side=side, pullback_depth=pullback_depth, benchmark=benchmark, state=state)
        if score < self.config.min_signal_score:
            return None

        valid_index = min(idx + self.config.entry_valid_days, len(symbol_frame) - 1)
        valid_until = pd.Timestamp(symbol_frame.iloc[valid_index]["timestamp"])
        return PositionalSignal(
            symbol=str(row["symbol"]),
            signal_time=pd.Timestamp(row["timestamp"]),
            side=side,
            score=score,
            trigger_price=trigger_price,
            stop_price=stop_price,
            valid_until=valid_until,
            reason="daily_trend_pullback_reclaim",
            trend_gap_atr=trend_gap,
            pullback_depth_atr=pullback_depth,
            benchmark_regime=str(benchmark.get("regime", "FLAT")),
            benchmark_trend_strength_atr=float(benchmark.get("trend_strength_atr", 0.0)),
            structure_price=state.structure_price,
            consumed_target_pct=consumed_target_pct,
        )

    def _benchmark_allows(self, side: FuturesSide, regime: str) -> bool:
        if not self.config.benchmark_symbol:
            return True
        if not self.config.require_benchmark_alignment:
            return True
        if regime == "FLAT":
            return self.config.allow_flat_benchmark
        return (side is FuturesSide.LONG and regime == "LONG") or (side is FuturesSide.SHORT and regime == "SHORT")

    def _passes_common_pullback(
        self,
        row: pd.Series,
        pullback_depth: float,
        *,
        state: _FuturesPullbackState,
        side: FuturesSide,
        prior_candle_level: float | None,
    ) -> bool:
        if pd.isna(pullback_depth):
            return False
        if pullback_depth < self.config.pullback_min_depth_atr:
            return False
        if pullback_depth > self.config.pullback_max_depth_atr:
            return False
        if float(row["body_pct"]) < self.config.min_reclaim_body_pct:
            return False
        if self.config.require_volume_expansion and float(row["volume_ratio"]) < self.config.min_volume_ratio:
            return False
        body = abs(float(row["close"]) - float(row["open"]))
        if body <= 0:
            return False
        if side is FuturesSide.LONG:
            if float(row["close"]) <= float(row["open"]):
                return False
            lower_wick = max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0)
            if self.config.require_rejection_wick and lower_wick < body * self.config.min_rejection_wick_to_body_ratio:
                return False
            if not self.config.allow_intrabar_rejection_entry and float(row["close"]) <= float(row["ema_fast"]):
                return False
            if self.config.require_break_of_prior_candle and prior_candle_level is not None and float(row["close"]) <= prior_candle_level:
                return False
            return float(row["close_location_pct"]) >= self.config.min_close_location_pct
        if float(row["close"]) >= float(row["open"]):
            return False
        upper_wick = max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0)
        if self.config.require_rejection_wick and upper_wick < body * self.config.min_rejection_wick_to_body_ratio:
            return False
        if not self.config.allow_intrabar_rejection_entry and float(row["close"]) >= float(row["ema_fast"]):
            return False
        if self.config.require_break_of_prior_candle and prior_candle_level is not None and float(row["close"]) >= prior_candle_level:
            return False
        return float(row["close_location_short_pct"]) >= self.config.min_close_location_pct

    def _diagnose_side(
        self,
        *,
        row: pd.Series,
        idx: int,
        side: FuturesSide,
        benchmark: dict[str, object],
        symbol_frame: pd.DataFrame,
        state: _FuturesPullbackState,
        prior_candle_level: float | None,
    ) -> dict[str, object]:
        has_required = self._has_required_values(row)
        benchmark_allowed = self._benchmark_allows(side, str(benchmark.get("regime", "FLAT")))
        side_regime = has_required and self._side_regime(row, side)
        pullback_state_ready = state.count >= self.config.minimum_pullback_bars and state.extreme_price is not None and state.structure_price is not None
        pullback_depth = self._long_pullback_depth(row) if has_required and side is FuturesSide.LONG else self._short_pullback_depth(row) if has_required else float("nan")
        depth_ok = has_required and self.config.pullback_min_depth_atr <= pullback_depth <= self.config.pullback_max_depth_atr
        structure_ok = False
        consumed_target_pct = None
        if pullback_state_ready and has_required:
            if side is FuturesSide.LONG:
                structure_ok = self._structure_holds(state=state, side=side, atr=float(row["atr"]))
                consumed_target_pct = (float(row["close"]) - state.extreme_price) / max(float(row["atr"]) * self.config.target_atr_multiple, 1e-9)
            else:
                structure_ok = self._structure_holds(state=state, side=side, atr=float(row["atr"]))
                consumed_target_pct = (state.extreme_price - float(row["close"])) / max(float(row["atr"]) * self.config.target_atr_multiple, 1e-9)
        consumed_ok = consumed_target_pct is not None and consumed_target_pct <= self.config.maximum_consumed_target_pct
        body = abs(float(row["close"]) - float(row["open"])) if has_required else 0.0
        if side is FuturesSide.LONG:
            direction_ok = has_required and float(row["close"]) > float(row["open"])
            close_location = float(row["close_location_pct"]) if has_required else float("nan")
            wick = max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0) if has_required else 0.0
            ema_reclaim_ok = has_required and (self.config.allow_intrabar_rejection_entry or float(row["close"]) > float(row["ema_fast"]))
            prior_break_ok = (
                has_required
                and (
                    not self.config.require_break_of_prior_candle
                    or prior_candle_level is None
                    or float(row["close"]) > prior_candle_level
                )
            )
        else:
            direction_ok = has_required and float(row["close"]) < float(row["open"])
            close_location = float(row["close_location_short_pct"]) if has_required else float("nan")
            wick = max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0) if has_required else 0.0
            ema_reclaim_ok = has_required and (self.config.allow_intrabar_rejection_entry or float(row["close"]) < float(row["ema_fast"]))
            prior_break_ok = (
                has_required
                and (
                    not self.config.require_break_of_prior_candle
                    or prior_candle_level is None
                    or float(row["close"]) < prior_candle_level
                )
            )
        body_ok = has_required and float(row["body_pct"]) >= self.config.min_reclaim_body_pct
        close_location_ok = has_required and close_location >= self.config.min_close_location_pct
        wick_rejection_present = has_required and body > 0 and wick >= body * self.config.min_rejection_wick_to_body_ratio
        wick_ok = (not self.config.require_rejection_wick) or wick_rejection_present
        volume_expansion_present = has_required and float(row["volume_ratio"]) >= self.config.min_volume_ratio
        volume_ok = (not self.config.require_volume_expansion) or volume_expansion_present
        score = (
            self._score(row=row, side=side, pullback_depth=pullback_depth, benchmark=benchmark, state=state)
            if has_required and pullback_state_ready
            else 0.0
        )
        score_ok = score >= self.config.min_signal_score
        would_signal = all(
            [
                has_required,
                benchmark_allowed,
                side_regime,
                pullback_state_ready,
                depth_ok,
                structure_ok,
                direction_ok,
                body_ok,
                close_location_ok,
                wick_ok,
                ema_reclaim_ok,
                prior_break_ok,
                volume_ok,
                consumed_ok,
                score_ok,
            ]
        )
        return {
            "timestamp": row.get("timestamp"),
            "symbol": row.get("symbol"),
            "side": side.value,
            "benchmark_regime": benchmark.get("regime", "FLAT"),
            "benchmark_trend_strength_atr": benchmark.get("trend_strength_atr", 0.0),
            "pullback_state_count": state.count,
            "pullback_depth_atr": pullback_depth,
            "consumed_target_pct": consumed_target_pct,
            "signal_score": score,
            "has_required_values": has_required,
            "benchmark_allowed": benchmark_allowed,
            "side_regime": side_regime,
            "pullback_state_ready": pullback_state_ready,
            "depth_ok": depth_ok,
            "structure_ok": structure_ok,
            "direction_ok": direction_ok,
            "body_ok": body_ok,
            "close_location_ok": close_location_ok,
            "wick_ok": wick_ok,
            "wick_rejection_present": wick_rejection_present,
            "ema_reclaim_ok": ema_reclaim_ok,
            "prior_break_ok": prior_break_ok,
            "volume_ok": volume_ok,
            "volume_expansion_present": volume_expansion_present,
            "consumed_ok": consumed_ok,
            "score_ok": score_ok,
            "would_signal": would_signal,
        }

    def _score(
        self,
        *,
        row: pd.Series,
        side: FuturesSide,
        pullback_depth: float,
        benchmark: dict[str, object],
        state: _FuturesPullbackState,
    ) -> float:
        trend_score = min(abs(float(row["trend_gap_atr"])) / max(self.config.symbol_trend_min_atr, 0.01), 1.0) * 20.0
        pullback_score = min(pullback_depth / max(self.config.pullback_min_depth_atr, 0.01), 1.0) * 20.0
        body_score = min(float(row["body_pct"]) / max(self.config.min_reclaim_body_pct, 0.01), 1.0) * 15.0
        close_location = float(row["close_location_pct"] if side is FuturesSide.LONG else row["close_location_short_pct"])
        close_score = min(close_location / max(self.config.min_close_location_pct, 0.01), 1.0) * 15.0
        dryup_score = self._dryup_score(row, state)
        volume_score = min(float(row["volume_ratio"]) / max(self.config.min_volume_ratio, 0.01), 1.0) * 10.0
        body = abs(float(row["close"]) - float(row["open"]))
        wick = (
            max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0)
            if side is FuturesSide.LONG
            else max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0)
        )
        rejection_score = min(wick / max(body * self.config.min_rejection_wick_to_body_ratio, 1e-9), 1.0) * 10.0
        return trend_score + pullback_score + body_score + close_score + dryup_score + volume_score + rejection_score

    def _advance_state(
        self,
        symbol_frame: pd.DataFrame,
        idx: int,
        row: pd.Series,
        side: FuturesSide,
        state: _FuturesPullbackState,
    ) -> None:
        if not self._side_regime(row, side):
            state.reset()
            return
        touched = self._touches_pullback(row, side)
        reclaimed = (
            float(row["close"]) > float(row["ema_fast"])
            if side is FuturesSide.LONG
            else float(row["close"]) < float(row["ema_fast"])
        )
        if reclaimed and not touched:
            state.reset()
            return
        if not touched:
            return
        if state.count == 0:
            state.structure_price = self._structure_price(symbol_frame, idx, side)
            state.extreme_price = float(row["low"] if side is FuturesSide.LONG else row["high"])
            state.volume_sum = float(row["volume"])
            state.count = 1
            return
        state.count += 1
        state.volume_sum += float(row["volume"])
        if side is FuturesSide.LONG:
            state.extreme_price = min(float(state.extreme_price), float(row["low"]))
        else:
            state.extreme_price = max(float(state.extreme_price), float(row["high"]))

    def _touches_pullback(self, row: pd.Series, side: FuturesSide) -> bool:
        atr = float(row["atr"])
        if side is FuturesSide.LONG:
            touch_level = float(row["ema_fast"]) + atr * self.config.pullback_touch_buffer_atr
            return float(row["low"]) <= touch_level and self._long_pullback_depth(row) >= self.config.pullback_min_depth_atr
        touch_level = float(row["ema_fast"]) - atr * self.config.pullback_touch_buffer_atr
        return float(row["high"]) >= touch_level and self._short_pullback_depth(row) >= self.config.pullback_min_depth_atr

    def _side_regime(self, row: pd.Series, side: FuturesSide) -> bool:
        trend_gap = float(row["trend_gap_atr"])
        if side is FuturesSide.LONG:
            if trend_gap < self.config.symbol_trend_min_atr:
                return False
            return self.config.symbol_trend_max_atr is None or trend_gap <= self.config.symbol_trend_max_atr
        if trend_gap > -self.config.symbol_trend_min_atr:
            return False
        return self.config.symbol_trend_max_atr is None or abs(trend_gap) <= self.config.symbol_trend_max_atr

    def _structure_price(self, symbol_frame: pd.DataFrame, idx: int, side: FuturesSide) -> float | None:
        start = max(0, idx - self.config.swing_lookback)
        prior = symbol_frame.iloc[start:idx]
        if prior.empty:
            return None
        return float(prior["low"].min() if side is FuturesSide.LONG else prior["high"].max())

    def _structure_holds(self, *, state: _FuturesPullbackState, side: FuturesSide, atr: float) -> bool:
        if state.extreme_price is None or state.structure_price is None:
            return False
        buffer = max(float(atr), 0.0) * self.config.structure_break_buffer_atr
        if side is FuturesSide.LONG:
            return state.extreme_price >= state.structure_price - buffer
        return state.extreme_price <= state.structure_price + buffer

    def _stop_price(self, row: pd.Series, *, side: FuturesSide, structure_price: float, atr: float) -> float:
        if self.config.stop_mode == "structure":
            buffer = atr * self.config.structural_stop_buffer_atr
            return structure_price - buffer if side is FuturesSide.LONG else structure_price + buffer
        return float(row["low"] if side is FuturesSide.LONG else row["high"])

    def _dryup_score(self, row: pd.Series, state: _FuturesPullbackState) -> float:
        pullback_volume = state.average_volume
        baseline = row.get("volume_sma")
        if pullback_volume is None or pd.isna(baseline) or float(baseline) <= 0:
            return 0.0
        return 10.0 if pullback_volume < float(baseline) else 0.0

    @staticmethod
    def _long_pullback_depth(row: pd.Series) -> float:
        return float((float(row["ema_fast"]) - float(row["low"])) / max(float(row["atr"]), 1e-9))

    @staticmethod
    def _short_pullback_depth(row: pd.Series) -> float:
        return float((float(row["high"]) - float(row["ema_fast"])) / max(float(row["atr"]), 1e-9))

    @staticmethod
    def _prior_high(symbol_frame: pd.DataFrame, idx: int) -> float | None:
        if idx <= 0:
            return None
        return float(symbol_frame.iloc[idx - 1]["high"])

    @staticmethod
    def _prior_low(symbol_frame: pd.DataFrame, idx: int) -> float | None:
        if idx <= 0:
            return None
        return float(symbol_frame.iloc[idx - 1]["low"])

    @staticmethod
    def _has_required_values(row: pd.Series) -> bool:
        for field in ["atr", "ema_fast", "ema_slow", "trend_gap_atr", "volume_ratio", "body_pct"]:
            if pd.isna(row[field]):
                return False
        return True


class PositionalFuturesBacktester:
    """Trigger-based daily futures backtester for positional signals."""

    def __init__(
        self,
        instruments: dict[str, FuturesInstrument] | None = None,
        config: PositionalPullbackConfig | None = None,
    ) -> None:
        self.instruments = instruments or {}
        self.config = config or PositionalPullbackConfig()

    def run(self, featured: pd.DataFrame, signals: list[PositionalSignal]) -> pd.DataFrame:
        trades: list[PositionalTrade] = []
        by_symbol = {
            symbol: group.sort_values("timestamp").reset_index(drop=True)
            for symbol, group in featured.groupby("symbol", sort=False)
        }
        next_available_index: dict[str, int] = {}

        for signal in sorted(signals, key=lambda item: (item.signal_time, item.symbol)):
            symbol_frame = by_symbol.get(signal.symbol)
            if symbol_frame is None or symbol_frame.empty:
                continue
            signal_indices = symbol_frame.index[symbol_frame["timestamp"] == signal.signal_time].tolist()
            if not signal_indices:
                continue
            signal_index = int(signal_indices[0])
            if signal_index < next_available_index.get(signal.symbol, 0):
                continue
            trade, final_index = self._trade_signal(symbol_frame, signal, signal_index)
            if trade is None:
                continue
            trades.extend(trade)
            next_available_index[signal.symbol] = max(next_available_index.get(signal.symbol, 0), final_index + 1)

        return positional_trades_to_frame(trades)

    def _trade_signal(
        self,
        symbol_frame: pd.DataFrame,
        signal: PositionalSignal,
        signal_index: int,
    ) -> tuple[list[PositionalTrade] | None, int]:
        trades: list[PositionalTrade] = []
        attempts = 0
        scan_start = signal_index + 1
        final_index = signal_index
        valid_until = pd.Timestamp(signal.valid_until)

        while attempts <= self.config.max_reentries:
            entry_index = self._find_entry_index(symbol_frame, signal, scan_start, valid_until)
            if entry_index is None:
                break
            trade, exit_index = self._manage_position(symbol_frame, signal, entry_index, attempts)
            final_index = max(final_index, exit_index)
            trades.append(trade)
            if trade.exit_reason != "stop_loss":
                break
            holding_days = max((trade.exit_time.date() - trade.entry_time.date()).days, 0)
            if holding_days > self.config.reentry_days_after_stop:
                break
            attempts += 1
            scan_start = exit_index + 1

        return (trades if trades else None), final_index

    def _find_entry_index(
        self,
        symbol_frame: pd.DataFrame,
        signal: PositionalSignal,
        start_index: int,
        valid_until: pd.Timestamp,
    ) -> int | None:
        for index in range(start_index, len(symbol_frame)):
            row = symbol_frame.iloc[index]
            if pd.Timestamp(row["timestamp"]) > valid_until:
                return None
            if signal.side is FuturesSide.LONG and float(row["high"]) >= signal.trigger_price:
                return index
            if signal.side is FuturesSide.SHORT and float(row["low"]) <= signal.trigger_price:
                return index
        return None

    def _manage_position(
        self,
        symbol_frame: pd.DataFrame,
        signal: PositionalSignal,
        entry_index: int,
        reentry_number: int,
    ) -> tuple[PositionalTrade, int]:
        entry_row = symbol_frame.iloc[entry_index]
        entry_price = signal.trigger_price
        stop_price = signal.stop_price
        current_stop = stop_price
        lots = self._determine_lots(signal.symbol, entry_price, stop_price)
        lot_size = self._instrument(signal.symbol).lot_size
        effective_quantity = lots * lot_size
        max_exit_index = len(symbol_frame) - 1
        if self.config.max_holding_days is not None:
            max_exit_index = min(max_exit_index, entry_index + self.config.max_holding_days)

        exit_index = max_exit_index
        exit_price = float(symbol_frame.iloc[max_exit_index]["close"])
        exit_reason = "time_exit" if self.config.max_holding_days is not None else "end_of_data"
        best_price = entry_price
        initial_risk = abs(entry_price - stop_price)

        for index in range(entry_index, max_exit_index + 1):
            row = symbol_frame.iloc[index]
            if signal.side is FuturesSide.LONG:
                if index > entry_index and self._trailing_is_active(signal.side, entry_price, best_price, initial_risk):
                    previous_low = float(symbol_frame.iloc[index - 1]["low"])
                    current_stop = max(current_stop, previous_low)
                if float(row["low"]) <= current_stop:
                    exit_index = index
                    exit_price = current_stop
                    exit_reason = "stop_loss"
                    break
                best_price = max(best_price, float(row["high"]))
            else:
                if index > entry_index and self._trailing_is_active(signal.side, entry_price, best_price, initial_risk):
                    previous_high = float(symbol_frame.iloc[index - 1]["high"])
                    current_stop = min(current_stop, previous_high)
                if float(row["high"]) >= current_stop:
                    exit_index = index
                    exit_price = current_stop
                    exit_reason = "stop_loss"
                    break
                best_price = min(best_price, float(row["low"]))

        entry_time = pd.Timestamp(entry_row["timestamp"])
        exit_time = pd.Timestamp(symbol_frame.iloc[exit_index]["timestamp"])
        gross_pnl = self._gross_pnl(signal.side, entry_price, exit_price, effective_quantity)
        costs = self._round_trip_cost(entry_price, exit_price, effective_quantity)
        trade = PositionalTrade(
            symbol=signal.symbol,
            side=signal.side,
            lots=lots,
            lot_size=lot_size,
            effective_quantity=effective_quantity,
            signal_time=signal.signal_time,
            entry_time=entry_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            initial_stop_price=stop_price,
            final_stop_price=current_stop,
            gross_pnl=gross_pnl,
            costs=costs,
            net_pnl=gross_pnl - costs,
            exit_reason=exit_reason,
            reentry_number=reentry_number,
            signal_score=signal.score,
            benchmark_regime=signal.benchmark_regime,
        )
        return trade, exit_index

    def _determine_lots(self, symbol: str, entry_price: float, stop_price: float) -> int:
        lot_size = self._instrument(symbol).lot_size
        if self.config.sizing_mode == "fixed_lots":
            return max(int(self.config.fixed_lots), 1)

        max_capital = self.config.initial_capital * self.config.max_capital_per_trade_pct
        if self.config.sizing_mode == "capital":
            capital = self.config.initial_capital * self.config.capital_per_trade_pct
            return max(int(min(capital, max_capital) // (entry_price * lot_size)), 1)

        risk_amount = self.config.initial_capital * self.config.risk_per_trade_pct
        risk_per_lot = abs(entry_price - stop_price) * lot_size
        if risk_per_lot <= 0:
            return 0
        risk_lots = int(risk_amount // risk_per_lot)
        capital_lots = int(max_capital // (entry_price * lot_size))
        return max(min(risk_lots, capital_lots), 1)

    def _round_trip_cost(self, entry_price: float, exit_price: float, effective_quantity: int) -> float:
        turnover = (abs(entry_price) + abs(exit_price)) * effective_quantity
        return turnover * ((self.config.fee_bps_per_side + self.config.slippage_bps_per_side) * 2.0 / 10_000.0)

    def _trailing_is_active(
        self,
        side: FuturesSide,
        entry_price: float,
        best_price: float,
        initial_risk: float,
    ) -> bool:
        if self.config.trailing_stop_mode == "off":
            return False
        if initial_risk <= 0:
            return False
        favorable_move = best_price - entry_price if side is FuturesSide.LONG else entry_price - best_price
        return favorable_move >= initial_risk * self.config.trailing_activation_r

    def _instrument(self, symbol: str) -> FuturesInstrument:
        return self.instruments.get(symbol, FuturesInstrument(symbol=symbol, lot_size=1))

    @staticmethod
    def _gross_pnl(side: FuturesSide, entry_price: float, exit_price: float, effective_quantity: int) -> float:
        direction = 1.0 if side is FuturesSide.LONG else -1.0
        return (exit_price - entry_price) * direction * effective_quantity


def positional_trades_to_frame(trades: list[PositionalTrade]) -> pd.DataFrame:
    rows = []
    for trade in trades:
        rows.append(
            {
                "symbol": trade.symbol,
                "side": trade.side.value,
                "lots": trade.lots,
                "lot_size": trade.lot_size,
                "effective_quantity": trade.effective_quantity,
                "signal_time": trade.signal_time,
                "entry_time": trade.entry_time,
                "exit_time": trade.exit_time,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "initial_stop_price": trade.initial_stop_price,
                "final_stop_price": trade.final_stop_price,
                "gross_pnl": trade.gross_pnl,
                "costs": trade.costs,
                "net_pnl": trade.net_pnl,
                "exit_reason": trade.exit_reason,
                "reentry_number": trade.reentry_number,
                "signal_score": trade.signal_score,
                "benchmark_regime": trade.benchmark_regime,
            }
        )
    return pd.DataFrame(rows)


def positional_summary(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "win_rate": 0.0,
            "avg_net_per_trade": 0.0,
            "profit_factor": 0.0,
        }
    wins = trades.loc[trades["net_pnl"] > 0, "net_pnl"]
    losses = trades.loc[trades["net_pnl"] < 0, "net_pnl"]
    gross_profit = float(wins.sum())
    gross_loss = abs(float(losses.sum()))
    return {
        "trades": int(len(trades)),
        "gross_pnl": float(trades["gross_pnl"].sum()),
        "net_pnl": float(trades["net_pnl"].sum()),
        "win_rate": float((trades["net_pnl"] > 0).mean() * 100.0),
        "avg_net_per_trade": float(trades["net_pnl"].mean()),
        "profit_factor": math.inf if gross_loss == 0 and gross_profit > 0 else gross_profit / gross_loss if gross_loss else 0.0,
    }
