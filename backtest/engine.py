"""
Backtest engine for pct_ladder strategy.
Reuses LadderPctStrategy.on_prices() and GlobalState/SymbolState directly —
same logic as live trading, no mocking.
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple

from common.engine.state import GlobalState, SymbolState
from common.engine.strategy_base import OrderIntent
from strategies.pct_ladder.strategy import LadderPctStrategy

D0 = Decimal("0")


def _dec(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if x is None:
        return D0
    try:
        return Decimal(str(x))
    except Exception:
        return D0


class BacktestEngine:
    def __init__(
        self,
        strategy: LadderPctStrategy,
        symbol: str,
        initial_cash: Decimal,
        initial_eth: Decimal,
        initial_eth_cost: Decimal,         # avg cost basis for initial holding; 0 = unknown
        quote_reserve: Decimal = D0,
        use_inventory_buffer: bool = True,
        compound_interval: str = None,     # None | 'daily' | 'weekly' | 'monthly'
    ):
        self.strategy = strategy
        self.symbol = symbol
        self.quote_reserve = quote_reserve

        # Build state
        self.state = GlobalState()
        self.state.cash = _dec(initial_cash)
        self.state.ensure_symbols([symbol])

        ss = self.state.symbol_states[symbol]
        ss.traded_qty = _dec(initial_eth)
        if initial_eth > D0:
            ss.lots = [{"qty": _dec(initial_eth), "price": _dec(initial_eth_cost)}]
        ss.reference_price = None  # will be set on first candle open

        self.state.extras["use_inventory_buffer"] = use_inventory_buffer

        # Compounding
        self.compound_interval = compound_interval
        initial_equity = float(initial_cash) + float(initial_eth) * float(initial_eth_cost)
        if initial_equity > 0 and compound_interval:
            # buy_quote as fraction of equity — maintained over time
            self._compound_ratio = float(strategy.cfg.buy_quote) / initial_equity
            # ETH value as fraction of equity — maintained over time (keeps both sides balanced)
            self._eth_ratio = (float(initial_eth) * float(initial_eth_cost)) / initial_equity
        else:
            self._compound_ratio = None
            self._eth_ratio = None
        self.compound_log: List[dict] = []  # track step size changes

        # Output
        self.trades: List[dict] = []
        self.equity_curve: List[Tuple[str, float]] = []

    # ------------------------------------------------------------------
    # Fill simulation
    # ------------------------------------------------------------------

    def _apply_fill(
        self,
        side: str,
        qty: Decimal,
        price: Decimal,
        reason: str,
        ts: str,
    ) -> None:
        ss = self.state.symbol_states[self.symbol]

        if side == "BUY":
            cost = qty * price
            self.state.cash -= cost
            ss.lots.append({"qty": qty, "price": price})
            ss.traded_qty += qty

        else:  # SELL
            proceeds = qty * price
            self.state.cash += proceeds

            to_sell = qty
            realized = D0
            # LIFO cost basis (same as live runner)
            while to_sell > D0 and ss.lots:
                lot = ss.lots[-1]
                lot_qty = _dec(lot["qty"])
                lot_px = _dec(lot["price"])
                take = min(to_sell, lot_qty)
                realized += take * (price - lot_px)
                ss.traded_qty -= take
                to_sell -= take
                if take >= lot_qty:
                    ss.lots.pop()
                else:
                    lot["qty"] = lot_qty - take

            ss.realized_pnl += realized

        if not reason.startswith("rebalance_"):
            ss.reference_price = price

        self.trades.append({
            "ts": ts,
            "side": side,
            "qty": float(qty),
            "price": float(price),
            "reason": reason,
            "cash_after": float(self.state.cash),
            "eth_after": float(ss.traded_qty),
            "realized_pnl_cumulative": float(ss.realized_pnl),
            "buy_quote": float(self.strategy.cfg.buy_quote),
            "upper_pct": float(self.strategy.cfg.upper_pct),
        })

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self, candles) -> dict:
        """
        candles: iterable of dicts / namedtuples with: ts, open, high, low, close
        Returns summary dict.
        """
        import pandas as pd

        ss = self.state.symbol_states[self.symbol]
        _last_compound_period = None

        for candle in candles:
            ts = str(candle["ts"])
            o = _dec(candle["open"])
            h = _dec(candle["high"])
            l = _dec(candle["low"])
            c = _dec(candle["close"])

            # ── Compounding: resize step at interval boundaries ────────────────
            if self._compound_ratio and self.compound_interval:
                ts_pd = pd.Timestamp(ts)
                if self.compound_interval == 'daily':
                    period = ts_pd.date()
                elif self.compound_interval == 'weekly':
                    period = ts_pd.isocalendar()[:2]   # (year, week)
                elif self.compound_interval == 'biweekly':
                    # 2-week periods: week_of_year // 2
                    iso = ts_pd.isocalendar()
                    period = (iso[0], iso[1] // 2)
                else:  # monthly
                    period = (ts_pd.year, ts_pd.month)

                if period != _last_compound_period and _last_compound_period is not None:
                    current_equity = float(self.state.cash) + float(ss.traded_qty) * float(c)

                    # 1. Resize step size
                    new_quote = Decimal(str(round(current_equity * self._compound_ratio, 2)))
                    if new_quote > D0:
                        old_quote = self.strategy.cfg.buy_quote
                        self.strategy.cfg.buy_quote  = new_quote
                        self.strategy.cfg.sell_quote = new_quote

                        # 2. Rebalance ETH to restore initial ratio (keeps both sides symmetric)
                        target_eth_value = current_equity * self._eth_ratio
                        target_eth_qty   = _dec(target_eth_value) / c if c > D0 else D0
                        target_eth_qty   = (target_eth_qty / self.strategy.cfg.qty_step).to_integral_value(
                            rounding=ROUND_DOWN) * self.strategy.cfg.qty_step
                        delta = target_eth_qty - ss.traded_qty

                        if delta > D0:
                            # Need more ETH — buy
                            cost = delta * c
                            available = max(self.state.cash - self.quote_reserve, D0)
                            delta = min(delta, available / c if c > D0 else D0)
                            delta = (delta / self.strategy.cfg.qty_step).to_integral_value(
                                rounding=ROUND_DOWN) * self.strategy.cfg.qty_step
                            if delta > D0:
                                self._apply_fill("BUY", delta, c, "rebalance_compound", ts)
                        elif delta < D0:
                            # Too much ETH — sell
                            sell_qty = min(-delta, ss.traded_qty)
                            sell_qty = (sell_qty / self.strategy.cfg.qty_step).to_integral_value(
                                rounding=ROUND_DOWN) * self.strategy.cfg.qty_step
                            if sell_qty > D0:
                                self._apply_fill("SELL", sell_qty, c, "rebalance_compound", ts)

                        self.compound_log.append({
                            'ts': ts, 'period': str(period),
                            'equity': round(current_equity, 2),
                            'old_quote': float(old_quote),
                            'new_quote': float(new_quote),
                            'eth_before': float(ss.traded_qty - (delta if delta > D0 else D0)),
                            'eth_after':  float(ss.traded_qty),
                            'eth_target': float(target_eth_qty),
                        })
                _last_compound_period = period

            # Initialise reference price on very first candle
            if ss.reference_price is None:
                ss.reference_price = o
                ss.initialized = True

            # Simulate intra-candle price path.
            # Bearish candle: high likely came first, then low.
            # Bullish candle: low likely came first, then high.
            # This is the standard conservative assumption used in backtesting.
            if c >= o:
                price_seq = [o, l, h, c]
            else:
                price_seq = [o, h, l, c]

            for price in price_seq:
                # Expose actual simulated ETH balance to strategy
                # (mirrors runner's broker_base_qty_{sym} set in _update_extras_crypto)
                self.state.extras[f"broker_base_qty_{self.symbol}"] = str(ss.traded_qty)
                self.state.last_prices[self.symbol] = price

                intents = self.strategy.on_prices(
                    {self.symbol: price}, self.state, ts
                )

                for intent in intents:
                    qty = _dec(intent.qty)
                    if qty <= D0:
                        continue

                    if intent.side == "BUY":
                        cost = qty * price
                        available = max(self.state.cash - self.quote_reserve, D0)
                        if available <= D0:
                            continue
                        if cost > available:
                            # Partial fill: buy as much as cash allows
                            qty = (available / price).quantize(
                                self.strategy.cfg.qty_step or Decimal("0.000001"),
                                rounding=ROUND_DOWN,
                            )
                            if qty <= D0:
                                continue

                    else:  # SELL
                        # Cap to what we actually hold
                        qty = min(qty, ss.traded_qty)
                        if qty <= D0:
                            continue

                    self._apply_fill(intent.side, qty, price, intent.reason, ts)

            # Record equity snapshot at candle close
            equity = float(self.state.cash) + float(ss.traded_qty) * float(c)
            self.equity_curve.append((ts, equity))

        final_price = _dec(candles[-1]["close"]) if len(candles) else D0
        return self._summary(final_price)

    # ------------------------------------------------------------------
    # Summary stats
    # ------------------------------------------------------------------

    def _summary(self, final_price: Decimal) -> dict:
        ss = self.state.symbol_states[self.symbol]

        ladder_buys  = [t for t in self.trades if t["side"] == "BUY"  and not t["reason"].startswith("rebalance_")]
        ladder_sells = [t for t in self.trades if t["side"] == "SELL" and not t["reason"].startswith("rebalance_")]
        rebalances   = [t for t in self.trades if t["reason"].startswith("rebalance_")]

        cycles = min(len(ladder_buys), len(ladder_sells))

        # Equity curve stats
        equities = [e for _, e in self.equity_curve]
        initial_equity = equities[0] if equities else 0.0
        final_equity   = equities[-1] if equities else 0.0

        max_dd = 0.0
        if equities:
            peak = equities[0]
            for e in equities:
                if e > peak:
                    peak = e
                if peak > 0:
                    dd = (peak - e) / peak
                    if dd > max_dd:
                        max_dd = dd

        return {
            "initial_equity":    initial_equity,
            "final_equity":      final_equity,
            "final_cash":        float(self.state.cash),
            "final_eth_qty":     float(ss.traded_qty),
            "final_eth_value":   float(ss.traded_qty * final_price),
            "realized_pnl":      float(ss.realized_pnl),
            "total_trades":      len(self.trades),
            "ladder_buys":       len(ladder_buys),
            "ladder_sells":      len(ladder_sells),
            "cycles":            cycles,
            "rebalance_trades":  len(rebalances),
            "max_drawdown_pct":  round(max_dd * 100, 2),
        }
