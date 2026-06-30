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
    quantity_per_trade: float = 1.0
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
    session_end: time = SESSION_END
    fresh_entry_cutoff_time: time | None = None
    max_trades_per_symbol_per_day: int | None = None
    cooldown_minutes_after_exit: int = 0
    no_reentry_same_side_until_new_signal: bool = False
    use_trailing_stop: bool = False
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
                exit_decision = self._resolve_exit(position, row, config.time_exit_minutes, square_off_cutoff, config.session_end, config.use_trailing_stop)
                if exit_decision is None:
                    continue
                exit_price, exit_reason = exit_decision

                if exit_reason == "partial_target":
                    # Book 50% at the target and keep the remaining 50% open.
                    # Fall back to a full exit if position is too small to split.
                    if position.effective_quantity <= 1e-8:
                        exit_reason = "target"
                        trade = self._close_position(position, row.timestamp, exit_price, exit_reason)
                        trades.append(trade)
                        realized_today = realized_pnl_by_day.get(trade_date, 0.0) + trade.pnl
                        realized_pnl_by_day[trade_date] = realized_today
                        realized_total_pnl += trade.pnl
                        open_positions.pop(position_key, None)
                        last_exit_by_book[position_key] = row.timestamp
                        loss_key = (trade_date, position.symbol, position.strategy_name)
                        consecutive_losses_by_day_book[loss_key] = 0 if trade.pnl >= 0 else (
                            consecutive_losses_by_day_book.get(loss_key, 0) + 1
                        )
                        if config.no_reentry_same_side_until_new_signal:
                            blocked_reentry_side_by_book[position_key] = trade.side
                        exited_books_this_bar.add(position_key)
                    else:
                        partial_qty = position.quantity / 2
                        partial_eff_qty = position.effective_quantity / 2
                        partial_trade = self._close_position(
                            position, row.timestamp, exit_price, exit_reason,
                            quantity_override=partial_qty,
                            effective_quantity_override=partial_eff_qty,
                        )
                        trades.append(partial_trade)
                        realized_today = realized_pnl_by_day.get(trade_date, 0.0) + partial_trade.pnl
                        realized_pnl_by_day[trade_date] = realized_today
                        realized_total_pnl += partial_trade.pnl
                        position.quantity -= partial_qty
                        position.effective_quantity -= partial_eff_qty
                        position.partial_exit_done = True
                    continue

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
                        trail_milestones=list(signal.trail_milestones),
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
        has_trail_milestones = "trail_milestones" in signals.columns
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
            trail_ms_raw = getattr(signal, "trail_milestones", None) if has_trail_milestones else None
            if trail_ms_raw is not None and hasattr(trail_ms_raw, "__iter__") and not isinstance(trail_ms_raw, (float, int)):
                trail_ms: tuple[float, ...] = tuple(float(x) for x in trail_ms_raw)
            else:
                trail_ms = ()
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
                    trail_milestones=trail_ms,
                )
            )
        return executable_signals

    @staticmethod
    def _determine_entry_quantity(
        signal,
        config: BacktestConfig,
        current_equity: float,
        instrument_spec: InstrumentSpec,
    ) -> tuple[float, str]:
        if config.sizing_mode == "fixed":
            quantity = float(config.quantity_per_trade)
            return (quantity, "approved") if quantity > 1e-8 else (0.0, "invalid_fixed_quantity")

        if config.sizing_mode == "capital_based":
            capital_amount = (
                float(config.capital_per_trade_amount)
                if config.capital_per_trade_amount is not None
                else max(float(current_equity), 0.0) * float(config.capital_per_trade_pct)
            )
            base_units = capital_amount / abs(float(signal.price)) if abs(float(signal.price)) > 0 else 0.0
            quantity = IntradayBacktester._quantity_from_base_units(base_units, instrument_spec)
            if quantity <= 1e-8:
                return 0.0, "quantity_below_min_after_capital_sizing"
            return quantity, "approved"

        if signal.stop_price is None:
            return 0.0, "missing_stop_loss_for_risk_sizing"

        stop_distance = abs(float(signal.price) - float(signal.stop_price))
        if stop_distance <= 0:
            return 0.0, "invalid_stop_distance"

        risk_amount = max(float(current_equity), 0.0) * float(config.risk_per_trade_pct)
        capital_limit = max(float(current_equity), 0.0) * float(config.max_capital_per_trade_pct)
        base_units_by_risk = risk_amount / stop_distance
        base_units_by_capital = capital_limit / abs(float(signal.price)) if abs(float(signal.price)) > 0 else 0.0
        quantity_by_risk = IntradayBacktester._quantity_from_base_units(base_units_by_risk, instrument_spec)
        quantity_by_capital = IntradayBacktester._quantity_from_base_units(base_units_by_capital, instrument_spec)
        quantity = min(quantity_by_risk, quantity_by_capital)
        if quantity <= 1e-8:
            return 0.0, "quantity_below_min_after_risk_sizing"
        return quantity, "approved"

    @staticmethod
    def _quantity_from_base_units(base_units: float, instrument_spec: InstrumentSpec) -> float:
        if instrument_spec.quantity_mode == "lots":
            # Lots must be whole numbers (e.g. NIFTY futures lot = 75)
            return float(int(base_units) // max(int(instrument_spec.lot_size), 1))
        # Units mode: return float as-is (supports fractional crypto quantities)
        return float(base_units)

    @staticmethod
    def _get_instrument_spec(symbol: str, config: BacktestConfig) -> InstrumentSpec:
        return config.instrument_specs.get(symbol, InstrumentSpec())

    @staticmethod
    def _effective_quantity(quantity: float, instrument_spec: InstrumentSpec) -> float:
        if instrument_spec.quantity_mode == "lots":
            return float(int(quantity)) * max(int(instrument_spec.lot_size), 1)
        return float(quantity)

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
        session_end: time,
        use_trailing_stop: bool = False,
    ) -> tuple[float, str] | None:
        # Carry-over guard: if the position's entry date != the current bar's trade_date, the
        # position was not squared off before the previous session ended (e.g. data gap or the
        # partial exit fired on the very last bar).  Force-close immediately on the first bar
        # of the new session without applying any pending stop logic.
        if hasattr(row, "trade_date"):
            row_date = pd.Timestamp(row.trade_date).date()
            pos_date = pd.Timestamp(position.entry_time).tz_convert("Asia/Kolkata").date()
            if row_date != pos_date:
                return float(row.close), "force_square_off"

        # Step 1: Apply pending stop from the previous bar (one-bar delay avoids same-bar exits).
        if position.stop_pending is not None:
            position.stop_price = position.stop_pending
            position.stop_pending = None

        # Step 2: Check stop.
        if position.side is SignalSide.LONG and position.stop_price is not None:
            if float(row.low) <= position.stop_price:
                reason = "trail_stop" if position.trail_milestone_idx > 0 else "stop_loss"
                return float(position.stop_price), reason
        if position.side is SignalSide.SHORT and position.stop_price is not None:
            if float(row.high) >= position.stop_price:
                reason = "trail_stop" if position.trail_milestone_idx > 0 else "stop_loss"
                return float(position.stop_price), reason

        # Step 3: Step-up milestone trail — advance the stop to each confirmed Fib level.
        # The last milestone is the configured target: reaching it triggers the 50% partial exit.
        # Loop so a strong-move bar that clears multiple milestones in one candle still reaches
        # the partial-exit check (e.g. ext=1.272 bar hits both BOS and target simultaneously).
        if use_trailing_stop and position.trail_milestones:
            while position.trail_milestone_idx < len(position.trail_milestones):
                m_idx = position.trail_milestone_idx
                next_m = position.trail_milestones[m_idx]
                milestone_hit = (
                    (position.side is SignalSide.LONG and float(row.high) >= next_m) or
                    (position.side is SignalSide.SHORT and float(row.low) <= next_m)
                )
                if not milestone_hit:
                    break
                position.stop_pending = next_m
                position.trail_milestone_idx += 1
                if m_idx == len(position.trail_milestones) - 1 and not position.partial_exit_done:
                    return float(next_m), "partial_target"

        # Step 4: Full target exit (trailing disabled, or partial already done).
        if position.side is SignalSide.LONG and position.target_price is not None:
            if float(row.high) >= position.target_price:
                return float(position.target_price), "target"
        if position.side is SignalSide.SHORT and position.target_price is not None:
            if float(row.low) <= position.target_price:
                return float(position.target_price), "target"

        # Step 5: Time exit.
        if time_exit_minutes is not None:
            holding_minutes = int((row.timestamp - position.entry_time).total_seconds() // 60)
            if holding_minutes >= int(time_exit_minutes):
                return float(row.close), "time_exit"

        # Step 6: Force square off.
        session_close = row.timestamp.normalize() + pd.Timedelta(
            hours=session_end.hour,
            minutes=session_end.minute,
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
        quantity_override: int | None = None,
        effective_quantity_override: int | None = None,
    ) -> Trade:
        exit_time = self._ensure_kolkata_timestamp(exit_time)
        direction = 1.0 if position.side is SignalSide.LONG else -1.0
        qty = quantity_override if quantity_override is not None else position.quantity
        eff_qty = effective_quantity_override if effective_quantity_override is not None else position.effective_quantity
        gross_pnl = (exit_price - position.entry_price) * eff_qty * direction
        cost_breakdown = self.cost_model.estimate_round_trip_costs(
            entry_price=position.entry_price,
            exit_price=exit_price,
            quantity=eff_qty,
            exit_reason=exit_reason,
        )
        net_pnl = gross_pnl - cost_breakdown["total_cost"]
        return Trade(
            symbol=position.symbol,
            side=position.side,
            quantity=qty,
            effective_quantity=eff_qty,
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
            is_partial=(exit_reason == "partial_target"),
        )

    @staticmethod
    def _validate_trade_journal(trades: pd.DataFrame, config: BacktestConfig) -> None:
        if trades.empty:
            return

        # Hard deadline = session_end itself.  The force_square_off cutoff is only
        # the trigger threshold; actual exits (trail_stop, target) can legitimately
        # fire on the very last bar of the session (session_end), e.g. when a partial
        # exit sets stop_pending on bar N and the remaining fires trail_stop on bar N+1.
        session_close_cutoff = pd.Timedelta(
            hours=config.session_end.hour,
            minutes=config.session_end.minute,
        )

        for trade in trades.itertuples():
            entry_time = IntradayBacktester._ensure_kolkata_timestamp(pd.Timestamp(trade.entry_time))
            exit_time = IntradayBacktester._ensure_kolkata_timestamp(pd.Timestamp(trade.exit_time))
            if entry_time.time() < config.first_trade_time:
                raise ValueError(f"Trade before session start detected: {trade.symbol} {entry_time.isoformat()}")
            last_entry_time = config.fresh_entry_cutoff_time or config.last_entry_time
            if entry_time.time() < config.first_trade_time:
                raise ValueError(f"Trade before configured first_trade_time detected: {trade.symbol} {entry_time.isoformat()}")
            if entry_time.time() > last_entry_time:
                raise ValueError(f"Fresh entry after cutoff detected: {trade.symbol} {entry_time.isoformat()}")
            close_deadline = entry_time.normalize() + session_close_cutoff
            # force_square_off exits are exempt: the carry-over guard may fire them at the
            # first bar of the following session when the prior session's data ended early.
            if exit_time > close_deadline and getattr(trade, "exit_reason", "") != "force_square_off":
                raise ValueError(f"Trade not squared off before close cutoff: {trade.symbol} {exit_time.isoformat()}")
            if exit_time < entry_time:
                raise ValueError(f"Trade exit before entry detected: {trade.symbol}")

        if config.max_trades_per_symbol_per_day is not None:
            # Partial exits (is_partial=True) share the same entry as their main trade; exclude them.
            non_partial = trades[~trades["is_partial"]] if "is_partial" in trades.columns else trades
            counts = (
                non_partial.assign(trade_date=lambda df: pd.to_datetime(df["entry_time"]).dt.date)
                .groupby(["symbol", "trade_date"])
                .size()
            )
            offenders = counts[counts > config.max_trades_per_symbol_per_day]
            if not offenders.empty:
                raise ValueError(f"Per-symbol daily trade limit exceeded: {offenders.to_dict()}")

        sorted_trades = trades.sort_values(["symbol", "strategy_name", "entry_time", "exit_time"]).reset_index(drop=True)
        for _, group in sorted_trades.groupby(["symbol", "strategy_name"], sort=False):
            previous_exit: pd.Timestamp | None = None
            previous_is_partial = False
            for trade in group.itertuples():
                # A partial trade and the remaining-half trade share the same entry_time.
                # Allow the overlap: the remaining-half entry <= partial exit is expected.
                if (previous_exit is not None
                        and pd.Timestamp(trade.entry_time) <= previous_exit
                        and not previous_is_partial):
                    raise ValueError(
                        f"Overlapping trades detected for symbol {trade.symbol} strategy {trade.strategy_name}"
                    )
                previous_exit = pd.Timestamp(trade.exit_time)
                previous_is_partial = bool(getattr(trade, "is_partial", False))

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
