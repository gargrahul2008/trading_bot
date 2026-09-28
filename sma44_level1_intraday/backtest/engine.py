"""Execution engine for the 44-SMA uptrend-pullback scanner strategy.

Consumes:
  - `entry_bars`: OHLC bars (one configured timeframe) for one or more symbols
    (timestamp, symbol, trade_date, open, high, low, close).
  - `candidate_signals`: pure pattern-match rows from signals.py (one row per
    qualifying touch event the scanner found, with trigger_price/stop_loss/
    target already computed).

Owns all runtime state: pending (not-yet-triggered) setups, open positions,
trade limits, position sizing, the daily-loss circuit breaker, time-of-day
rules, and the same-candle SL/target ambiguity rule. Produces the trade
journal (see TRADE_JOURNAL_COLUMNS) — this is the only module that knows how
a signal becomes a trade.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ..config.schema import StrategyConfig
from ..exits import ChandelierState, ChandelierTrailingStop, resolve_exit_mode
from .costs import compute_trade_costs

TRADE_JOURNAL_COLUMNS = [
    "symbol", "trade_date", "direction", "timeframe",
    "sma_44", "ma_length", "ma_alignment",
    "confluence_gap_skipped", "confluence_gap_ma", "confluence_gap_atr", "close_distance_pct", "low_distance_pct", "high_distance_pct", "ma_slope_pct", "trend_class",
    "min5_trend_class", "min15_trend_class", "min30_trend_class", "hourly_trend_class",
    "daily_trend_class", "weekly_trend_class", "monthly_trend_class",
    "touch_count", "touch_number",
    "first_interaction_time", "previous_touch_time", "current_touch_time",
    "bars_since_previous_touch", "max_distance_reached_pct", "wicked_through_sma", "is_new_event",
    "signal_candle_time", "signal_open", "signal_high", "signal_low", "signal_close",
    "daily_volume",
    "trigger_price", "entry_time", "entry_price", "initial_stop", "stop_loss", "target",
    "exit_time", "exit_price", "exit_reason", "qty", "gross_pnl", "costs", "net_pnl",
    "r_multiple", "holding_minutes",
]

REJECTED_COLUMNS = ["timestamp", "symbol", "direction", "stage", "reason"]


@dataclass
class _PendingSetup:
    signal: dict
    bars_waited: int = 0


@dataclass
class _OpenPosition:
    symbol: str
    direction: str
    stop_loss: float
    target: float
    qty: float
    entry_time: pd.Timestamp
    entry_price: float
    signal: dict
    trail: Optional[ChandelierState] = None  # only for exit_mode="atr_chandelier"


class ScannerBacktester:
    def __init__(self, config: StrategyConfig):
        self.config = config
        self._exit_mode = resolve_exit_mode(config.execution)
        self._trailer = ChandelierTrailingStop(config.execution)
        self._trail_log: list[dict] = []

    # ── Public API ─────────────────────────────────────────────────────────

    def run(self, entry_bars: pd.DataFrame, candidate_signals: pd.DataFrame) -> dict[str, pd.DataFrame]:
        cfg = self.config
        market = cfg.market
        continuous = market.continuous_session
        self._trail_log = []  # per-bar trailing-stop path (for plotting/debug)

        candidates_lookup: dict[tuple, dict] = {}
        if not candidate_signals.empty:
            for rec in candidate_signals.to_dict("records"):
                candidates_lookup[(rec["timestamp"], rec["symbol"])] = rec

        limits = cfg.trade_limits
        pending: dict[str, list[_PendingSetup]] = {}
        open_positions: dict[str, list[_OpenPosition]] = {}
        trades: list[dict] = []
        rejected: list[dict] = []

        realized_pnl_by_day: dict[object, float] = {}
        equity_at_day_start: dict[object, float] = {}
        daily_loss_hit: set = set()
        symbol_trade_count_by_day: dict[tuple, int] = {}
        total_trade_count_by_day: dict[object, int] = {}
        last_close_by_symbol: dict[str, float] = {}
        realized_total_pnl = 0.0
        seen_days: set = set()

        if entry_bars.empty:
            return {
                "trades": pd.DataFrame(columns=TRADE_JOURNAL_COLUMNS),
                "rejected": pd.DataFrame(columns=REJECTED_COLUMNS),
                "trail_log": pd.DataFrame(columns=["symbol", "timestamp", "entry_time", "direction", "stop", "activated"]),
            }

        for rec in entry_bars.sort_values(["timestamp", "symbol"]).to_dict("records"):
            symbol = rec["symbol"]
            ts = rec["timestamp"]
            trade_date = rec["trade_date"]
            bar_time = ts.time()
            last_close_by_symbol[symbol] = float(rec["close"])

            if trade_date not in seen_days:
                seen_days.add(trade_date)
                equity_at_day_start[trade_date] = cfg.risk.initial_capital + realized_total_pnl
                realized_pnl_by_day.setdefault(trade_date, 0.0)

            # ── 1. Resolve exits for existing open positions of this symbol ──
            remaining: list[_OpenPosition] = []
            for pos in open_positions.get(symbol, []):
                exit_info = self._resolve_exit(pos, rec, bar_time, continuous)
                if exit_info is None:
                    remaining.append(pos)
                    continue
                exit_price, exit_reason = exit_info
                trade = self._close_position(pos, exit_price, exit_reason, ts)
                trades.append(trade)
                realized_total_pnl += trade["net_pnl"]
                realized_pnl_by_day[trade_date] = realized_pnl_by_day.get(trade_date, 0.0) + trade["net_pnl"]
            if remaining:
                open_positions[symbol] = remaining
            else:
                open_positions.pop(symbol, None)

            # ── Daily-loss circuit breaker ────────────────────────────────
            threshold = (cfg.risk.max_daily_loss_pct / 100.0) * equity_at_day_start[trade_date]
            # A day that STARTS with zero/negative notional equity (cumulative
            # losses already exceeded initial_capital) has a zero/negative
            # threshold, which "realized <= -threshold" would read as an
            # instant daily-loss hit on EVERY such day, forever, silently
            # rejecting every later setup (see the 2026-01-27 case in the
            # notebook backtest). Sizing here doesn't depend on equity, so
            # there's nothing meaningful to protect -- skip the breaker then.
            if (threshold > 0 and trade_date not in daily_loss_hit
                    and realized_pnl_by_day[trade_date] <= -threshold):
                daily_loss_hit.add(trade_date)
                if cfg.risk.close_positions_on_daily_loss_hit:
                    for sym2, poslist in list(open_positions.items()):
                        for pos2 in poslist:
                            px = last_close_by_symbol.get(sym2, pos2.entry_price)
                            trade2 = self._close_position(pos2, px, "daily_loss_exit", ts)
                            trades.append(trade2)
                            realized_total_pnl += trade2["net_pnl"]
                            realized_pnl_by_day[trade_date] = realized_pnl_by_day.get(trade_date, 0.0) + trade2["net_pnl"]
                        open_positions.pop(sym2, None)

            # ── 2. Advance / trigger every pending setup for this symbol ─────
            # Several setups may be armed at once (see max_pending_setups_per_symbol);
            # each is advanced independently and any that trigger open a position.
            newly_opened_list: list[_OpenPosition] = []
            still_pending: list[_PendingSetup] = []
            for ps in pending.get(symbol, []):
                ps.bars_waited += 1
                sig = ps.signal
                past_cutoff = (not continuous) and bar_time > market.no_new_entry_after
                triggered = False
                invalidated = False
                if not past_cutoff:
                    triggered, invalidated = self._resolve_pending_trigger(sig, rec)
                if triggered:
                    allow, reason = self._new_trade_allowed(
                        symbol, trade_date, open_positions, daily_loss_hit,
                        symbol_trade_count_by_day, total_trade_count_by_day,
                    )
                    if allow:
                        current_equity = cfg.risk.initial_capital + realized_total_pnl
                        qty = self._compute_qty(sig, current_equity)
                        if qty > 0:
                            entry_price = float(sig["trigger_price"])
                            initial_stop = float(sig["stop_loss"])
                            opened = _OpenPosition(
                                symbol=symbol, direction=sig["direction"],
                                stop_loss=initial_stop, target=float(sig["target"]),
                                qty=qty, entry_time=ts, entry_price=entry_price,
                                signal=sig,
                            )
                            if self._exit_mode == "atr_chandelier":
                                # The signal's stop becomes the initial stop and defines 1R.
                                # The fixed 2R target is ignored unless use_fixed_take_profit.
                                opened.trail = self._trailer.open(
                                    sig["direction"], entry_price, initial_stop)
                                ex = cfg.execution
                                opened.target = (
                                    entry_price + opened.trail.r_value * ex.fixed_rr_target
                                    if ex.use_fixed_take_profit and sig["direction"] == "LONG"
                                    else entry_price - opened.trail.r_value * ex.fixed_rr_target
                                    if ex.use_fixed_take_profit
                                    else float("nan")
                                )
                            open_positions.setdefault(symbol, []).append(opened)
                            newly_opened_list.append(opened)
                            symbol_trade_count_by_day[(trade_date, symbol)] = (
                                symbol_trade_count_by_day.get((trade_date, symbol), 0) + 1)
                            total_trade_count_by_day[trade_date] = total_trade_count_by_day.get(trade_date, 0) + 1
                        else:
                            rejected.append(self._rejection(ts, symbol, sig["direction"], "trigger", "quantity_below_min"))
                    else:
                        rejected.append(self._rejection(ts, symbol, sig["direction"], "trigger", reason))
                elif invalidated:
                    rejected.append(self._rejection(ts, symbol, sig["direction"], "trigger", "stop_breached_before_entry"))
                elif past_cutoff:
                    rejected.append(self._rejection(ts, symbol, sig["direction"], "trigger", "triggered_after_no_new_entry_cutoff"))
                elif ps.bars_waited >= cfg.scanner.setup_expiry_bars:
                    rejected.append(self._rejection(ts, symbol, sig["direction"], "setup", "setup_expired"))
                else:
                    still_pending.append(ps)  # neither triggered nor expired — keep waiting
            if still_pending:
                pending[symbol] = still_pending
            else:
                pending.pop(symbol, None)

            # ── 3. Same-candle entry+exit (a violent bar can both fill and
            #      stop/target out a position it just opened) ─────────────
            if cfg.execution.allow_entry_and_exit_same_candle:
                for opened in newly_opened_list:
                    exit_info = self._resolve_exit(opened, rec, bar_time, continuous)
                    if exit_info is not None:
                        exit_price, exit_reason = exit_info
                        trade = self._close_position(opened, exit_price, exit_reason, ts)
                        trades.append(trade)
                        realized_total_pnl += trade["net_pnl"]
                        realized_pnl_by_day[trade_date] = realized_pnl_by_day.get(trade_date, 0.0) + trade["net_pnl"]
                        open_positions[symbol].remove(opened)
                        if not open_positions[symbol]:
                            open_positions.pop(symbol, None)

            # ── 4. New candidate signal → new pending setup ──────────────
            cand = candidates_lookup.get((ts, symbol))
            if cand is not None:
                max_pending = limits.max_pending_setups_per_symbol
                room = max_pending is None or len(pending.get(symbol, [])) < max_pending
                after_start = continuous or bar_time >= market.market_start_time
                before_cutoff = continuous or bar_time <= market.no_new_entry_after
                if not room:
                    # Previously this candidate vanished silently; now it's logged.
                    rejected.append(self._rejection(ts, symbol, cand["direction"], "setup",
                                                    "max_pending_setups_per_symbol_reached"))
                elif after_start and before_cutoff:
                    allow, reason = self._new_trade_allowed(
                        symbol, trade_date, open_positions, daily_loss_hit,
                        symbol_trade_count_by_day, total_trade_count_by_day,
                    )
                    if allow:
                        pending.setdefault(symbol, []).append(_PendingSetup(signal=cand))
                    else:
                        rejected.append(self._rejection(ts, symbol, cand["direction"], "setup", reason))

        # ── Finalize: close any positions still open at the end of data ──
        for symbol, poslist in open_positions.items():
            for pos in poslist:
                px = last_close_by_symbol.get(symbol, pos.entry_price)
                trade = self._close_position(pos, px, "time_exit", pos.entry_time)
                trades.append(trade)

        trades_df = pd.DataFrame(trades, columns=TRADE_JOURNAL_COLUMNS) if trades else pd.DataFrame(columns=TRADE_JOURNAL_COLUMNS)
        rejected_df = pd.DataFrame(rejected, columns=REJECTED_COLUMNS) if rejected else pd.DataFrame(columns=REJECTED_COLUMNS)
        trail_df = pd.DataFrame(self._trail_log) if self._trail_log else pd.DataFrame(
            columns=["symbol", "timestamp", "entry_time", "direction", "stop", "activated"])
        return {"trades": trades_df, "rejected": rejected_df, "trail_log": trail_df}

    # ── Pending-setup trigger / pre-entry invalidation ────────────────────

    @staticmethod
    def _resolve_pending_trigger(sig: dict, rec: dict) -> tuple[bool, bool]:
        """Does this bar TRIGGER the pending setup's entry, INVALIDATE it, neither,
        or (if a single bar's range spans both levels) which happened first?

        INVALIDATE checks against the signal candle's own OPPOSITE extreme —
        sig['signal_low'] for a LONG setup (the support it pulled back to),
        sig['signal_high'] for a SHORT setup (the resistance) — NOT
        sig['stop_loss']. Those are deliberately different levels:
        sig['stop_loss'] is the rule-2-widened stop (pulled out to the
        stop_lookback_bars window and the 44-SMA itself, since the stop must
        never sit inside the support/resistance line) — the right level once
        a position is actually OPEN, but too generous for deciding whether a
        still-PENDING setup remains valid: a setup whose own signal candle
        got broken the wrong way before ever triggering isn't a "pullback
        that's still developing" anymore, regardless of where the eventual
        stop would sit once filled.

        Same-bar ambiguity is resolved with the identical OHLC-shape
        assumption the engine already uses for same-candle SL/target
        ambiguity on OPEN positions (see _resolve_priority's use_ohlc_path):
        a bullish bar (close >= open) is assumed to have dipped to its low
        before rallying to its high; a bearish bar the reverse. For a LONG
        setup the trigger is the bar's HIGH and invalidation is its LOW, so a
        bullish bar (low first) invalidates before it could ever trigger,
        while a bearish bar (high first) triggers, with invalidation then only
        possibly relevant on a LATER bar (once open, the position's own stop
        — sig['stop_loss'] — takes over, including via the existing
        same-candle-exit path). SHORT is the exact mirror (trigger = LOW,
        invalidation = HIGH).

        Returns (triggered, invalidated) — at most one True.
        """
        long = sig["direction"] == "LONG"
        trigger_hit = (rec["high"] >= sig["trigger_price"]) if long else (rec["low"] <= sig["trigger_price"])
        invalidation_level = sig["signal_low"] if long else sig["signal_high"]
        stop_hit = (rec["low"] <= invalidation_level) if long else (rec["high"] >= invalidation_level)

        if trigger_hit and stop_hit:
            low_visited_first = rec["close"] >= rec["open"]  # bullish bar assumption
            # LONG: trigger=high (visited 2nd if low first), stop=low (visited 1st if low first).
            # SHORT: trigger=low (visited 1st if low first), stop=high (visited 2nd if low first).
            trigger_first = (not low_visited_first) if long else low_visited_first
            return (True, False) if trigger_first else (False, True)
        if trigger_hit:
            return True, False
        if stop_hit:
            return False, True
        return False, False

    # ── Trade-limit gating (shared by setup-creation and trigger-fill) ────

    def _new_trade_allowed(
        self,
        symbol: str,
        trade_date: object,
        open_positions: dict[str, list[_OpenPosition]],
        daily_loss_hit: set,
        symbol_trade_count_by_day: dict[tuple, int],
        total_trade_count_by_day: dict[object, int],
    ) -> tuple[bool, str]:
        limits = self.config.trade_limits
        if trade_date in daily_loss_hit:
            return False, "max_daily_loss_hit"
        if limits.one_active_position_per_symbol and open_positions.get(symbol):
            return False, "one_active_position_per_symbol"
        if limits.max_open_positions is not None:
            total_open = sum(len(v) for v in open_positions.values())
            if total_open >= limits.max_open_positions:
                return False, "max_open_positions_reached"
        if limits.max_trades_per_symbol_per_day is not None:
            if symbol_trade_count_by_day.get((trade_date, symbol), 0) >= limits.max_trades_per_symbol_per_day:
                return False, "max_trades_per_symbol_per_day_reached"
        if limits.max_total_trades_per_day is not None:
            if total_trade_count_by_day.get(trade_date, 0) >= limits.max_total_trades_per_day:
                return False, "max_total_trades_per_day_reached"
        return True, "approved"

    # ── Sizing ─────────────────────────────────────────────────────────────

    def _compute_qty(self, sig: dict, current_equity: float) -> float:
        risk_cfg = self.config.risk
        if risk_cfg.position_sizing_mode == "fixed_qty":
            return float(risk_cfg.fixed_qty)

        if risk_cfg.position_sizing_mode == "capital_based":
            capital_amount = (
                float(risk_cfg.capital_per_trade_amount)
                if risk_cfg.capital_per_trade_amount is not None
                else current_equity * (risk_cfg.capital_per_trade_pct / 100.0)
            )
            trigger_price = abs(float(sig["trigger_price"]))
            if trigger_price <= 0:
                return 0.0
            # Floor to whole units — safe default for equities; crypto configs that
            # want fractional sizing should use position_sizing_mode="fixed_qty".
            return float(int(capital_amount / trigger_price))

        stop_distance = abs(float(sig["trigger_price"]) - float(sig["stop_loss"]))
        if stop_distance <= 0:
            return 0.0
        risk_amount = (
            float(risk_cfg.risk_per_trade_amount)
            if risk_cfg.risk_per_trade_amount is not None
            else current_equity * (risk_cfg.risk_per_trade_pct / 100.0)
        )
        # Floor to whole units — safe default for equities; crypto configs that
        # want fractional sizing should use position_sizing_mode="fixed_qty".
        return float(int(risk_amount / stop_distance))

    # ── Exit resolution ────────────────────────────────────────────────────

    def _resolve_exit(
        self,
        pos: _OpenPosition,
        rec: dict,
        bar_time,
        continuous: bool,
    ) -> Optional[tuple[float, str]]:
        long = pos.direction == "LONG"
        mode = self._exit_mode

        if mode == "atr_chandelier":
            return self._resolve_exit_chandelier(pos, rec, bar_time, continuous)

        if mode == "candle_trail":
            # Trailing-stop-only mode: no fixed target. pos.stop_loss is the LIVE
            # trailing stop (the previous completed candle's extreme). If this bar
            # breaks it, exit; otherwise ratchet the stop to this bar's extreme
            # for the next bar (one-bar delay = no lookahead).
            hit_sl = (rec["low"] <= pos.stop_loss) if long else (rec["high"] >= pos.stop_loss)
            if hit_sl:
                trailed = pos.stop_loss != float(pos.signal["stop_loss"])
                return pos.stop_loss, ("trail_stop" if trailed else "sl")
            if not continuous and bar_time >= self.config.market.force_exit_time:
                return float(rec["close"]), "force_exit"
            if long:
                pos.stop_loss = max(pos.stop_loss, float(rec["low"]))
            else:
                pos.stop_loss = min(pos.stop_loss, float(rec["high"]))
            return None

        hit_sl = (rec["low"] <= pos.stop_loss) if long else (rec["high"] >= pos.stop_loss)
        hit_tgt = (rec["high"] >= pos.target) if long else (rec["low"] <= pos.target)
        if hit_sl or hit_tgt:
            return self._resolve_priority(pos, rec, hit_sl, hit_tgt)
        if not continuous and bar_time >= self.config.market.force_exit_time:
            return float(rec["close"]), "force_exit"
        return None

    def _resolve_exit_chandelier(
        self,
        pos: _OpenPosition,
        rec: dict,
        bar_time,
        continuous: bool,
    ) -> Optional[tuple[float, str]]:
        """ATR Chandelier trade management. Strict two-phase order per bar:
        judge the bar against the stop set by EARLIER bars, then fold this bar in
        and recompute the stop for the NEXT bar (no lookahead / no repaint)."""
        state = pos.trail
        if "atr" not in rec:
            raise ValueError(
                "exit_mode='atr_chandelier' needs an 'atr' column on the entry bars "
                "(pipeline.prepare_symbol_entry_bars adds it)."
            )

        # ── Phase 1: exits, evaluated against the pre-existing stop ──────────
        stop_exit = self._trailer.check_exit(state, rec)
        tp_exit = self._trailer.check_take_profit(state, rec)
        if stop_exit and tp_exit:
            # Both breached inside one bar — fall back to the configured rule.
            priority = self.config.execution.same_candle_priority
            if priority == "target_first":
                return tp_exit
            if priority == "sl_first":
                return stop_exit
            bullish_bar = rec["close"] >= rec["open"]
            long = pos.direction == "LONG"
            if long:
                return stop_exit if bullish_bar else tp_exit
            return tp_exit if bullish_bar else stop_exit
        if stop_exit:
            return stop_exit
        if tp_exit:
            return tp_exit

        if not continuous and bar_time >= self.config.market.force_exit_time:
            return float(rec["close"]), "force_exit"

        # ── Phase 2: advance the run-up extreme / arm / ratchet the stop ─────
        self._trailer.update(state, rec)
        pos.stop_loss = state.stop  # keep the journal's stop in sync with the trail
        self._trail_log.append({
            "symbol": pos.symbol, "timestamp": rec["timestamp"], "entry_time": pos.entry_time,
            "direction": pos.direction, "stop": state.stop, "activated": state.activated,
        })
        return None

    def _resolve_priority(self, pos: _OpenPosition, rec: dict, hit_sl: bool, hit_tgt: bool) -> tuple[float, str]:
        if hit_sl and not hit_tgt:
            return pos.stop_loss, "sl"
        if hit_tgt and not hit_sl:
            return pos.target, "target"

        priority = self.config.execution.same_candle_priority
        if priority == "sl_first":
            return pos.stop_loss, "sl"
        if priority == "target_first":
            return pos.target, "target"

        # use_ohlc_path: approximate the intrabar path from the bar's own shape
        # (no tick data available). A bullish bar (close >= open) is assumed to
        # have dipped to its low before rallying to its high; a bearish bar the
        # reverse. Whichever level (SL or target) sits on the side visited first
        # is deemed hit first.
        bullish_bar = rec["close"] >= rec["open"]
        long = pos.direction == "LONG"
        if long:
            return (pos.stop_loss, "sl") if bullish_bar else (pos.target, "target")
        return (pos.target, "target") if bullish_bar else (pos.stop_loss, "sl")

    # ── Trade construction ─────────────────────────────────────────────────

    def _close_position(self, pos: _OpenPosition, exit_price: float, exit_reason: str, exit_time: pd.Timestamp) -> dict:
        direction_sign = 1.0 if pos.direction == "LONG" else -1.0
        gross_pnl = (exit_price - pos.entry_price) * pos.qty * direction_sign
        costs = compute_trade_costs(
            entry_price=pos.entry_price, exit_price=exit_price,
            qty=pos.qty, costs_cfg=self.config.costs,
        )
        net_pnl = gross_pnl - costs
        risk_per_unit = abs(float(pos.signal["trigger_price"]) - float(pos.signal["stop_loss"]))
        r_multiple = net_pnl / (risk_per_unit * pos.qty) if risk_per_unit > 0 and pos.qty > 0 else 0.0
        holding_minutes = (exit_time - pos.entry_time).total_seconds() / 60.0
        sig = pos.signal
        return {
            "symbol": pos.symbol,
            "trade_date": pos.entry_time.date(),
            "direction": pos.direction,
            "timeframe": sig["timeframe"],
            "sma_44": sig["sma_44"],
            "ma_length": sig.get("ma_length"),
            "ma_alignment": sig.get("ma_alignment"),
            "confluence_gap_skipped": sig.get("confluence_gap_skipped"),
            "confluence_gap_ma": sig.get("confluence_gap_ma"),
            "confluence_gap_atr": sig.get("confluence_gap_atr"),
            "close_distance_pct": sig["close_distance_pct"],
            "low_distance_pct": sig["low_distance_pct"],
            "high_distance_pct": sig["high_distance_pct"],
            "ma_slope_pct": sig["ma_slope_pct"],
            "trend_class": sig["trend_class"],
            "min5_trend_class": sig["min5_trend_class"],
            "min15_trend_class": sig["min15_trend_class"],
            "min30_trend_class": sig["min30_trend_class"],
            "hourly_trend_class": sig["hourly_trend_class"],
            "daily_trend_class": sig["daily_trend_class"],
            "weekly_trend_class": sig["weekly_trend_class"],
            "monthly_trend_class": sig["monthly_trend_class"],
            "touch_count": sig["touch_count"],
            "touch_number": sig["touch_number"],
            "first_interaction_time": sig["first_interaction_time"],
            "previous_touch_time": sig["previous_touch_time"],
            "current_touch_time": sig["current_touch_time"],
            "bars_since_previous_touch": sig["bars_since_previous_touch"],
            "max_distance_reached_pct": sig["max_distance_reached_pct"],
            "wicked_through_sma": sig["wicked_through_sma"],
            "is_new_event": sig["is_new_event"],
            "signal_candle_time": sig["timestamp"],
            "signal_open": sig["signal_open"],
            "signal_high": sig["signal_high"],
            "signal_low": sig["signal_low"],
            "signal_close": sig["signal_close"],
            "daily_volume": sig["daily_volume"],
            "trigger_price": sig["trigger_price"],
            "entry_time": pos.entry_time,
            "entry_price": pos.entry_price,
            # The signal's original stop (defines 1R). `stop_loss` is the stop as
            # it stood at exit — for trailing modes that's the final trailed level.
            "initial_stop": float(sig["stop_loss"]),
            "stop_loss": pos.stop_loss,
            "target": pos.target,
            "exit_time": exit_time,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "qty": pos.qty,
            "gross_pnl": gross_pnl,
            "costs": costs,
            "net_pnl": net_pnl,
            "r_multiple": r_multiple,
            "holding_minutes": holding_minutes,
        }

    @staticmethod
    def _rejection(ts, symbol: str, direction: str, stage: str, reason: str) -> dict:
        return {"timestamp": ts, "symbol": symbol, "direction": direction, "stage": stage, "reason": reason}
