from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time, timedelta
from pathlib import Path
from typing import Callable, Iterable, Literal

import pandas as pd

from .analytics import write_backtest_outputs
from .combiner import SignalCombiner
from .costs import TransactionCostModel
from .features import FeatureEngine
from .reporting import daily_pnl_report, trades_to_frame
from .risk import RiskManager
from .selector import SignalSelector
from .session import SESSION_END, SESSION_START, SESSION_TIMEZONE
from .strategy import Strategy
from .types import CombinedSignal, InstrumentSpec, Position, SignalSide, Trade


SignalFilter = Callable[[list[CombinedSignal], pd.DataFrame], tuple[list[CombinedSignal], list[dict[str, object]]]]


@dataclass
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    max_positions: int = 1
    max_daily_loss: float = 5_000.0
    quantity_per_trade: int = 1
    sizing_mode: Literal["fixed", "capital_based", "risk_based"] = "fixed"
    capital_per_trade_pct: float = 0.10
    capital_per_trade_amount: float | None = None
    risk_per_trade_pct: float = 0.005
    max_capital_per_trade_pct: float = 0.10
    output_dir: str = "artifacts/research"
    time_exit_minutes: int | None = 30
    force_square_off_minutes_before_close: int = 1
    first_trade_time: time = SESSION_START
    last_entry_time: time = time(hour=15, minute=0)
    fresh_entry_cutoff_time: time | None = None
    max_trades_per_symbol_per_day: int | None = None
    cooldown_minutes_after_exit: int = 0
    no_reentry_same_side_until_new_signal: bool = False
    minimum_signal_score: float = 0.0
    minimum_reward_to_risk: float = 0.0
    minimum_signal_score_by_strategy: dict[str, float] = field(default_factory=dict)
    minimum_reward_to_risk_by_strategy: dict[str, float] = field(default_factory=dict)
    instrument_specs: dict[str, InstrumentSpec] = field(default_factory=dict)


class IntradayBacktester:
    """Simple intraday backtester with central signal combination and global risk."""

    def __init__(
        self,
        strategies: Iterable[Strategy],
        feature_engine: FeatureEngine | None = None,
        combiner: SignalCombiner | None = None,
        signal_selector: SignalSelector | None = None,
        risk_manager: RiskManager | None = None,
        cost_model: TransactionCostModel | None = None,
        signal_filter: SignalFilter | None = None,
    ) -> None:
        self.strategies = list(strategies)
        self.feature_engine = feature_engine or FeatureEngine()
        self.combiner = combiner or SignalCombiner()
        self.signal_selector = signal_selector or SignalSelector()
        self.risk_manager = risk_manager or RiskManager()
        self.cost_model = cost_model or TransactionCostModel()
        self.signal_filter = signal_filter

    def run(
        self,
        market_data: pd.DataFrame,
        config: BacktestConfig,
        *,
        featured: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        self.risk_manager.max_positions = config.max_positions
        self.risk_manager.max_daily_loss = config.max_daily_loss
        self.risk_manager.quantity_per_trade = config.quantity_per_trade

        featured = featured.copy() if featured is not None else self.feature_engine.transform(market_data)
        featured["is_session_last_bar"] = featured.groupby(["symbol", "trade_date"])["timestamp"].transform(
            "max"
        ).eq(featured["timestamp"])
        price_lookup = featured.set_index(["timestamp", "symbol"])["close"]
        strategy_frames: list[pd.DataFrame] = []
        for strategy in self.strategies:
            strategy_signals = strategy.generate_signals(featured)
            if not strategy_signals.empty:
                strategy_frames.append(strategy_signals.copy())
        strategy_daily_trade_limits = {
            strategy.name: int(limit)
            for strategy in self.strategies
            if (limit := getattr(strategy, "max_trades_per_day", None)) is not None
        }
        strategy_daily_consecutive_loss_limits = {
            strategy.name: int(limit)
            for strategy in self.strategies
            if (limit := getattr(strategy, "max_consecutive_losses_per_day", None)) is not None
        }

        all_signals = (
            pd.concat(strategy_frames, ignore_index=True)
            if strategy_frames
            else pd.DataFrame(columns=["timestamp", "symbol", "direction", "strength", "reason", "stop_loss", "target", "strategy_name"])
        )
        combined_signals = self.combiner.combine(all_signals)
        raw_executable_signals = self._build_executable_signals(all_signals, price_lookup)
        executable_signals, rejected_signal_rows = self._filter_signals(raw_executable_signals, config)
        if self.signal_filter is not None:
            executable_signals, external_rejections = self.signal_filter(executable_signals, featured)
            for rejection in external_rejections:
                if "stage" not in rejection:
                    rejection["stage"] = "signal_filter"
            rejected_signal_rows.extend(external_rejections)
        signal_lookup: dict[tuple[pd.Timestamp, str], list[CombinedSignal]] = {}
        for signal in executable_signals:
            signal_lookup.setdefault((signal.timestamp, signal.symbol), []).append(signal)

        trades: list[Trade] = []
        rejected_trade_rows: list[dict[str, object]] = []
        rejected_signal_frame = pd.DataFrame(rejected_signal_rows)
        open_positions: dict[tuple[str, str], Position] = {}
        realized_pnl_by_day: dict[object, float] = {}
        realized_total_pnl = 0.0
        symbol_trade_counts_by_day: dict[tuple[object, str], int] = {}
        strategy_trade_counts_by_day: dict[tuple[object, str, str], int] = {}
        consecutive_losses_by_day_book: dict[tuple[object, str, str], int] = {}
        last_exit_by_book: dict[tuple[str, str], pd.Timestamp] = {}
        blocked_reentry_side_by_book: dict[tuple[str, str], SignalSide] = {}
        square_off_cutoff = timedelta(minutes=config.force_square_off_minutes_before_close)

        for row in featured.sort_values(["timestamp", "symbol"]).itertuples():
            trade_date = row.trade_date
            realized_today = realized_pnl_by_day.get(trade_date, 0.0)
            exited_books_this_bar: set[tuple[str, str]] = set()
            open_position_items = [
                (position_key, position)
                for position_key, position in open_positions.items()
                if position.symbol == row.symbol
            ]
            for position_key, position in open_position_items:
                exit_decision = self._resolve_exit(position, row, config.time_exit_minutes, square_off_cutoff)
                if exit_decision is None:
                    continue
                exit_price, exit_reason = exit_decision
                trade = self._close_position(position, row.timestamp, exit_price, exit_reason)
                trades.append(trade)
                realized_today = realized_pnl_by_day.get(trade_date, 0.0) + trade.pnl
                realized_pnl_by_day[trade_date] = realized_today
                realized_total_pnl += trade.pnl
                open_positions.pop(position_key, None)
                last_exit_by_book[position_key] = row.timestamp
                loss_key = (trade_date, position.symbol, position.strategy_name)
                if trade.pnl < 0:
                    consecutive_losses_by_day_book[loss_key] = (
                        consecutive_losses_by_day_book.get(loss_key, 0) + 1
                    )
                else:
                    consecutive_losses_by_day_book[loss_key] = 0
                if config.no_reentry_same_side_until_new_signal:
                    blocked_reentry_side_by_book[position_key] = trade.side
                exited_books_this_bar.add(position_key)

            bar_signals = signal_lookup.get((row.timestamp, row.symbol), [])
            for signal in bar_signals:
                position_key = (signal.symbol, signal.strategy_name)
                if config.no_reentry_same_side_until_new_signal:
                    blocked_side = blocked_reentry_side_by_book.get(position_key)
                    if blocked_side is not None and signal.side is not blocked_side:
                        blocked_reentry_side_by_book.pop(position_key, None)

                if position_key in exited_books_this_bar:
                    continue

                allow_entry, rejection_reason = self._can_enter_new_position(
                    timestamp=row.timestamp,
                    config=config,
                    symbol=row.symbol,
                    strategy_name=signal.strategy_name,
                    signal_side=signal.side,
                    trade_date=trade_date,
                    symbol_trade_counts_by_day=symbol_trade_counts_by_day,
                    strategy_trade_counts_by_day=strategy_trade_counts_by_day,
                    consecutive_losses_by_day_book=consecutive_losses_by_day_book,
                    strategy_daily_trade_limits=strategy_daily_trade_limits,
                    strategy_daily_consecutive_loss_limits=strategy_daily_consecutive_loss_limits,
                    last_exit_by_book=last_exit_by_book,
                    blocked_reentry_side_by_book=blocked_reentry_side_by_book,
                )
                if not allow_entry:
                    rejected_trade_rows.append(
                        self._build_rejected_trade_log(row.timestamp, row.symbol, signal, rejection_reason)
                    )
                    continue
                decision = self.risk_manager.evaluate_entry(
                    signal=signal,
                    open_positions=open_positions,
                    realized_daily_pnl=realized_pnl_by_day.get(trade_date, 0.0),
                )
                if decision.approved:
                    quantity, sizing_rejection_reason = self._determine_entry_quantity(
                        signal=signal,
                        config=config,
                        current_equity=config.initial_capital + realized_total_pnl,
                        instrument_spec=self._get_instrument_spec(row.symbol, config),
                    )
                    if quantity <= 0:
                        rejected_trade_rows.append(
                            self._build_rejected_trade_log(
                                row.timestamp,
                                row.symbol,
                                signal,
                                sizing_rejection_reason,
                            )
                        )
                        continue
                    instrument_spec = self._get_instrument_spec(row.symbol, config)
                    open_positions[position_key] = Position(
                        symbol=row.symbol,
                        side=signal.side,
                        quantity=quantity,
                        effective_quantity=self._effective_quantity(quantity, instrument_spec),
                        lot_size=instrument_spec.lot_size,
                        entry_time=row.timestamp,
                        entry_price=float(signal.price),
                        stop_price=signal.stop_price,
                        target_price=signal.target_price,
                        entry_reason=signal.reason,
                        strategy_name=signal.strategy_name,
                    )
                    symbol_trade_counts_by_day[(trade_date, row.symbol)] = (
                        symbol_trade_counts_by_day.get((trade_date, row.symbol), 0) + 1
                    )
                    strategy_trade_counts_by_day[(trade_date, row.symbol, signal.strategy_name)] = (
                        strategy_trade_counts_by_day.get((trade_date, row.symbol, signal.strategy_name), 0) + 1
                    )
                else:
                    rejected_trade_rows.append(
                        self._build_rejected_trade_log(row.timestamp, row.symbol, signal, decision.reason)
                    )

        trade_frame = trades_to_frame(trades)
        rejected_trade_frame = pd.DataFrame(rejected_trade_rows)
        self._validate_trade_journal(trade_frame, config)
        daily_report = daily_pnl_report(trades)
        output_paths = write_backtest_outputs(
            trade_frame,
            daily_report,
            Path(config.output_dir),
            rejected_trades=rejected_trade_frame,
        )
        total_pnl = float(trade_frame["net_pnl"].sum()) if not trade_frame.empty else 0.0

        return {
            "trades": trade_frame,
            "rejected_trades": rejected_trade_frame,
            "daily_pnl": daily_report,
            "total_pnl": total_pnl,
            "ending_capital": config.initial_capital + total_pnl,
            "report_paths": output_paths,
            "combined_signals": combined_signals,
            "strategy_signals": raw_executable_signals,
            "selected_strategy_signals": executable_signals,
            "rejected_signals": rejected_signal_frame,
        }

    def _filter_signals(
        self,
        signals: list[CombinedSignal],
        config: BacktestConfig,
    ) -> tuple[list[CombinedSignal], list[dict[str, object]]]:
        selected: list[CombinedSignal] = []
        rejected_rows: list[dict[str, object]] = []
        for signal in signals:
            decision = self.signal_selector.select_signal(
                signal,
                minimum_signal_score=config.minimum_signal_score,
                minimum_reward_to_risk=config.minimum_reward_to_risk,
                minimum_signal_score_by_strategy=config.minimum_signal_score_by_strategy,
                minimum_reward_to_risk_by_strategy=config.minimum_reward_to_risk_by_strategy,
            )
            if decision.approved:
                selected.append(signal)
                continue
            rejected_rows.append(
                {
                    "timestamp": signal.timestamp,
                    "symbol": signal.symbol,
                    "strategy_name": signal.strategy_name,
                    "side": signal.side.value,
                    "signal_score": float(signal.final_score),
                    "reward_to_risk": decision.reward_to_risk,
                    "reason": decision.reason,
                    "stage": "signal_selector",
                }
            )
        return selected, rejected_rows

    @staticmethod
    def _build_executable_signals(
        signals: pd.DataFrame,
        price_lookup: pd.Series,
    ) -> list[CombinedSignal]:
        if signals.empty:
            return []

        executable_signals: list[CombinedSignal] = []
        has_entry_price = "entry_price" in signals.columns
        for signal in signals.itertuples():
            close_price = price_lookup.get((signal.timestamp, signal.symbol))
            if close_price is None:
                continue
            # Use the signal's own entry_price when provided (e.g. limit orders at a
            # specific level such as fib_618). Fall back to the bar's close price.
            if has_entry_price and pd.notna(getattr(signal, "entry_price", None)):
                signal_price = float(signal.entry_price)
            else:
                signal_price = float(close_price)
            side = SignalSide(signal.direction)
            score = abs(float(signal.strength))
            executable_signals.append(
                CombinedSignal(
                    timestamp=pd.Timestamp(signal.timestamp),
                    symbol=str(signal.symbol),
                    final_decision=side,
                    final_score=score,
                    price=float(signal_price),
                    stop_price=float(signal.stop_loss) if pd.notna(signal.stop_loss) else None,
                    target_price=float(signal.target) if pd.notna(signal.target) else None,
                    explanation=f"{signal.strategy_name} {signal.direction} score={score:.3f} reason={signal.reason}",
                    strategy_name=str(signal.strategy_name),
                )
            )
        return executable_signals

    @staticmethod
    def _determine_entry_quantity(
        signal,
        config: BacktestConfig,
        current_equity: float,
        instrument_spec: InstrumentSpec,
    ) -> tuple[int, str]:
        if config.sizing_mode == "fixed":
            quantity = int(config.quantity_per_trade)
            return (quantity, "approved") if quantity > 0 else (0, "invalid_fixed_quantity")

        if config.sizing_mode == "capital_based":
            capital_amount = (
                float(config.capital_per_trade_amount)
                if config.capital_per_trade_amount is not None
                else max(float(current_equity), 0.0) * float(config.capital_per_trade_pct)
            )
            quantity = IntradayBacktester._quantity_from_base_units(
                int(capital_amount // abs(float(signal.price))) if abs(float(signal.price)) > 0 else 0,
                instrument_spec,
            )
            if quantity <= 0:
                return 0, "quantity_below_one_after_capital_sizing"
            return quantity, "approved"

        if signal.stop_price is None:
            return 0, "missing_stop_loss_for_risk_sizing"

        stop_distance = abs(float(signal.price) - float(signal.stop_price))
        if stop_distance <= 0:
            return 0, "invalid_stop_distance"

        risk_amount = max(float(current_equity), 0.0) * float(config.risk_per_trade_pct)
        capital_limit = max(float(current_equity), 0.0) * float(config.max_capital_per_trade_pct)
        base_units_by_risk = int(risk_amount // stop_distance)
        base_units_by_capital = int(capital_limit // abs(float(signal.price))) if abs(float(signal.price)) > 0 else 0
        quantity_by_risk = IntradayBacktester._quantity_from_base_units(base_units_by_risk, instrument_spec)
        quantity_by_capital = IntradayBacktester._quantity_from_base_units(base_units_by_capital, instrument_spec)
        quantity = min(quantity_by_risk, quantity_by_capital)
        if quantity <= 0:
            return 0, "quantity_below_one_after_risk_sizing"
        return quantity, "approved"

    @staticmethod
    def _quantity_from_base_units(base_units: int, instrument_spec: InstrumentSpec) -> int:
        if instrument_spec.quantity_mode == "lots":
            return int(base_units) // max(int(instrument_spec.lot_size), 1)
        return int(base_units)

    @staticmethod
    def _get_instrument_spec(symbol: str, config: BacktestConfig) -> InstrumentSpec:
        return config.instrument_specs.get(symbol, InstrumentSpec())

    @staticmethod
    def _effective_quantity(quantity: int, instrument_spec: InstrumentSpec) -> int:
        if instrument_spec.quantity_mode == "lots":
            return int(quantity) * max(int(instrument_spec.lot_size), 1)
        return int(quantity)

    @staticmethod
    def _can_enter_new_position(
        timestamp: pd.Timestamp,
        config: BacktestConfig,
        symbol: str,
        strategy_name: str,
        signal_side: SignalSide,
        trade_date: object,
        symbol_trade_counts_by_day: dict[tuple[object, str], int],
        strategy_trade_counts_by_day: dict[tuple[object, str, str], int],
        consecutive_losses_by_day_book: dict[tuple[object, str, str], int],
        strategy_daily_trade_limits: dict[str, int],
        strategy_daily_consecutive_loss_limits: dict[str, int],
        last_exit_by_book: dict[tuple[str, str], pd.Timestamp],
        blocked_reentry_side_by_book: dict[tuple[str, str], SignalSide],
    ) -> tuple[bool, str]:
        timestamp = IntradayBacktester._ensure_kolkata_timestamp(timestamp)
        first_trade_time = config.first_trade_time
        last_entry_time = config.fresh_entry_cutoff_time or config.last_entry_time
        position_key = (symbol, strategy_name)

        if timestamp.time() < first_trade_time:
            return False, "before_first_trade_time"
        if timestamp.time() > last_entry_time:
            return False, "after_last_entry_time"
        if config.max_trades_per_symbol_per_day is not None:
            current_trades = symbol_trade_counts_by_day.get((trade_date, symbol), 0)
            if current_trades >= config.max_trades_per_symbol_per_day:
                return False, "max_trades_per_symbol_per_day_reached"
        strategy_trade_limit = strategy_daily_trade_limits.get(strategy_name)
        if strategy_trade_limit is not None:
            current_strategy_trades = strategy_trade_counts_by_day.get((trade_date, symbol, strategy_name), 0)
            if current_strategy_trades >= strategy_trade_limit:
                return False, "max_trades_per_day_reached"
        strategy_loss_limit = strategy_daily_consecutive_loss_limits.get(strategy_name)
        if strategy_loss_limit is not None:
            current_consecutive_losses = consecutive_losses_by_day_book.get((trade_date, symbol, strategy_name), 0)
            if current_consecutive_losses >= strategy_loss_limit:
                return False, "max_consecutive_losses_per_day_reached"
        if config.cooldown_minutes_after_exit > 0:
            last_exit = last_exit_by_book.get(position_key)
            if last_exit is not None:
                cooldown_deadline = last_exit + pd.Timedelta(minutes=config.cooldown_minutes_after_exit)
                if timestamp < cooldown_deadline:
                    return False, "cooldown_minutes_after_exit"
        if config.no_reentry_same_side_until_new_signal:
            blocked_side = blocked_reentry_side_by_book.get(position_key)
            if blocked_side is signal_side:
                return False, "no_reentry_same_side_until_new_signal"
        return True, "approved"

    @staticmethod
    def _ensure_kolkata_timestamp(timestamp: pd.Timestamp) -> pd.Timestamp:
        if timestamp.tzinfo is None:
            raise ValueError("Trade timestamps must be timezone-aware Asia/Kolkata values.")
        converted = timestamp.tz_convert(SESSION_TIMEZONE)
        if str(converted.tzinfo) != SESSION_TIMEZONE:
            raise ValueError("Trade timestamps must use Asia/Kolkata timezone.")
        return converted

    @staticmethod
    def _resolve_exit(
        position: Position,
        row: pd.Series,
        time_exit_minutes: int | None,
        square_off_cutoff: timedelta,
    ) -> tuple[float, str] | None:
        if position.side is SignalSide.LONG and position.stop_price is not None:
            if float(row.low) <= position.stop_price:
                return float(position.stop_price), "stop_loss"
        if position.side is SignalSide.SHORT and position.stop_price is not None:
            if float(row.high) >= position.stop_price:
                return float(position.stop_price), "stop_loss"

        if position.side is SignalSide.LONG and position.target_price is not None:
            if float(row.high) >= position.target_price:
                return float(position.target_price), "target"
        if position.side is SignalSide.SHORT and position.target_price is not None:
            if float(row.low) <= position.target_price:
                return float(position.target_price), "target"

        if time_exit_minutes is not None:
            holding_minutes = int((row.timestamp - position.entry_time).total_seconds() // 60)
            if holding_minutes >= int(time_exit_minutes):
                return float(row.close), "time_exit"

        session_close = row.timestamp.normalize() + pd.Timedelta(
            hours=SESSION_END.hour,
            minutes=SESSION_END.minute,
        )
        if row.timestamp >= session_close - square_off_cutoff:
            return float(row.close), "force_square_off"
        return None

    def _close_position(
        self,
        position: Position,
        exit_time: pd.Timestamp,
        exit_price: float,
        exit_reason: str,
    ) -> Trade:
        exit_time = self._ensure_kolkata_timestamp(exit_time)
        direction = 1.0 if position.side is SignalSide.LONG else -1.0
        gross_pnl = (exit_price - position.entry_price) * position.quantity * direction
        gross_pnl = (exit_price - position.entry_price) * position.effective_quantity * direction
        cost_breakdown = self.cost_model.estimate_round_trip_costs(
            entry_price=position.entry_price,
            exit_price=exit_price,
            quantity=position.effective_quantity,
        )
        net_pnl = gross_pnl - cost_breakdown["total_cost"]
        return Trade(
            symbol=position.symbol,
            side=position.side,
            quantity=position.quantity,
            effective_quantity=position.effective_quantity,
            lot_size=position.lot_size,
            entry_time=position.entry_time,
            exit_time=exit_time,
            entry_price=position.entry_price,
            exit_price=exit_price,
            gross_pnl=gross_pnl,
            fees=cost_breakdown["total_cost"] - cost_breakdown["slippage"],
            slippage=cost_breakdown["slippage"],
            net_pnl=net_pnl,
            brokerage=cost_breakdown["brokerage"],
            stt=cost_breakdown["stt"],
            exchange_charges=cost_breakdown["exchange_charges"],
            gst=cost_breakdown["gst"],
            sebi_charges=cost_breakdown["sebi_charges"],
            stamp_duty=cost_breakdown["stamp_duty"],
            entry_slippage=cost_breakdown["entry_slippage"],
            exit_slippage=cost_breakdown["exit_slippage"],
            strategy_name=position.strategy_name,
            strategy_reason=position.entry_reason,
            exit_reason=exit_reason,
        )

    @staticmethod
    def _validate_trade_journal(trades: pd.DataFrame, config: BacktestConfig) -> None:
        if trades.empty:
            return

        session_close_cutoff = pd.Timedelta(
            hours=SESSION_END.hour,
            minutes=SESSION_END.minute,
        ) - pd.Timedelta(minutes=config.force_square_off_minutes_before_close)

        for trade in trades.itertuples():
            entry_time = IntradayBacktester._ensure_kolkata_timestamp(pd.Timestamp(trade.entry_time))
            exit_time = IntradayBacktester._ensure_kolkata_timestamp(pd.Timestamp(trade.exit_time))
            if entry_time.time() < SESSION_START:
                raise ValueError(f"Trade before session start detected: {trade.symbol} {entry_time.isoformat()}")
            last_entry_time = config.fresh_entry_cutoff_time or config.last_entry_time
            if entry_time.time() < config.first_trade_time:
                raise ValueError(f"Trade before configured first_trade_time detected: {trade.symbol} {entry_time.isoformat()}")
            if entry_time.time() > last_entry_time:
                raise ValueError(f"Fresh entry after cutoff detected: {trade.symbol} {entry_time.isoformat()}")
            close_deadline = entry_time.normalize() + session_close_cutoff
            if exit_time > close_deadline:
                raise ValueError(f"Trade not squared off before close cutoff: {trade.symbol} {exit_time.isoformat()}")
            if exit_time < entry_time:
                raise ValueError(f"Trade exit before entry detected: {trade.symbol}")

        if config.max_trades_per_symbol_per_day is not None:
            counts = (
                trades.assign(trade_date=lambda df: pd.to_datetime(df["entry_time"]).dt.date)
                .groupby(["symbol", "trade_date"])
                .size()
            )
            offenders = counts[counts > config.max_trades_per_symbol_per_day]
            if not offenders.empty:
                raise ValueError(f"Per-symbol daily trade limit exceeded: {offenders.to_dict()}")

        sorted_trades = trades.sort_values(["symbol", "strategy_name", "entry_time", "exit_time"]).reset_index(drop=True)
        for _, group in sorted_trades.groupby(["symbol", "strategy_name"], sort=False):
            previous_exit = None
            for trade in group.itertuples():
                if previous_exit is not None and pd.Timestamp(trade.entry_time) <= previous_exit:
                    raise ValueError(
                        f"Overlapping trades detected for symbol {trade.symbol} strategy {trade.strategy_name}"
                    )
                previous_exit = pd.Timestamp(trade.exit_time)

    @staticmethod
    def _build_rejected_trade_log(
        timestamp: pd.Timestamp,
        symbol: str,
        signal,
        rejection_reason: str,
    ) -> dict[str, object]:
        return {
            "timestamp": timestamp,
            "symbol": symbol,
            "side": signal.side.value,
            "strategy_name": signal.strategy_name,
            "final_score": signal.final_score,
            "reason": rejection_reason,
            "explanation": signal.explanation,
            "price": signal.price,
            "stop_price": signal.stop_price,
            "target_price": signal.target_price,
        }
