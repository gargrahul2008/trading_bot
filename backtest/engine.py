"""
Backtest engine for pct_ladder strategy.
Reuses LadderPctStrategy.on_prices() and GlobalState/SymbolState directly —
same logic as live trading, no mocking.
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import random as _random
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
        compound_rebalance_eth: bool = True,  # False = resize step only, let bot handle ETH naturally
        compound_basis: str = 'equity',       # 'equity' = total portfolio | 'cycle_pnl' = only cycle profits
        max_buy_quote: Decimal = D0,          # 0 = unlimited; e.g. Decimal('10000') caps step at $10K
        price_path_mode: str = 'random',      # 'optimistic' | 'pessimistic' | 'random'
        seed: int = 42,                       # random seed for 'random' mode (reproducible)
    ):
        self.strategy = strategy
        self.symbol = symbol
        self.quote_reserve = quote_reserve
        self.compound_rebalance_eth = compound_rebalance_eth
        self.compound_basis = compound_basis
        self.max_buy_quote = max_buy_quote
        self.price_path_mode = price_path_mode
        self._rng = _random.Random(seed)

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

        # Track initial lot cost so we can separate trading PnL from initial holding gains
        self._initial_eth = _dec(initial_eth)
        self._initial_eth_cost = _dec(initial_eth_cost)

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
        self._initial_equity = initial_equity
        self._initial_buy_quote  = float(strategy.cfg.buy_quote)
        self._cum_cycle_pnl      = 0.0   # LIFO realized PnL on ladder sells
        self._usdc_ladder_sells  = 0.0   # USDC received from all ladder sells
        self._usdc_ladder_buys   = 0.0   # USDC spent on all ladder buys
        self._eth_ladder_buys    = 0.0   # ETH bought via ladder
        self._eth_ladder_sells   = 0.0   # ETH sold via ladder
        self._ladder_buy_count   = 0
        self._ladder_sell_count  = 0
        self.compound_log: List[dict] = []  # track step size changes

        # Output
        self.trades: List[dict] = []
        self.equity_curve: List[Tuple[str, float]] = []

    # ------------------------------------------------------------------
    # Price path selection
    # ------------------------------------------------------------------

    def _price_seq(self, o: Decimal, h: Decimal, l: Decimal, c: Decimal) -> List[Decimal]:
        """
        Return the 4-price intra-candle sequence used for signal simulation.

        Modes:
          optimistic  — bullish candle: low first then high (best case for grid bot)
          pessimistic — bullish candle: high first then low (worst case for grid bot)
          random      — randomly pick one of the two orderings per candle (unbiased, seeded)

        Note: the actual intra-candle path is unknowable from OHLC data.
        'random' gives an unbiased estimate; compare optimistic vs pessimistic to
        bracket the realistic range.
        """
        if self.price_path_mode == 'optimistic':
            low_first = (c >= o)
        elif self.price_path_mode == 'pessimistic':
            low_first = (c < o)
        else:  # 'random'
            low_first = self._rng.random() < 0.5

        if low_first:
            return [o, l, h, c]
        else:
            return [o, h, l, c]

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
        ref_before = ss.reference_price   # capture before update (unused currently, kept for debugging)

        if side == "BUY":
            cost = qty * price
            self.state.cash -= cost
            ss.lots.append({"qty": qty, "price": price})
            ss.traded_qty += qty
            if not reason.startswith("rebalance_"):
                self._usdc_ladder_buys += float(cost)
                self._eth_ladder_buys  += float(qty)
                self._ladder_buy_count += 1

        else:  # SELL
            proceeds = qty * price
            self.state.cash += proceeds

            to_sell = qty
            realized = D0
            cycle_realized = D0
            # LIFO cost basis (same as live runner)
            while to_sell > D0 and ss.lots:
                lot = ss.lots[-1]
                lot_qty = _dec(lot["qty"])
                lot_px = _dec(lot["price"])
                take = min(to_sell, lot_qty)
                pnl = take * (price - lot_px)
                realized += pnl
                cycle_realized += pnl
                ss.traded_qty -= take
                to_sell -= take
                if take >= lot_qty:
                    ss.lots.pop()
                else:
                    lot["qty"] = lot_qty - take

            ss.realized_pnl += realized

            if not reason.startswith("rebalance_"):
                self._cum_cycle_pnl     += float(cycle_realized)
                self._usdc_ladder_sells += float(proceeds)
                self._eth_ladder_sells  += float(qty)
                self._ladder_sell_count += 1

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
                    if self.compound_basis == 'cycle_pnl':
                        # Grow step only from cumulative cycle profits
                        compound_equity = self._initial_equity + self._cum_cycle_pnl
                    else:
                        # Grow step from total portfolio value
                        compound_equity = current_equity
                    new_quote = Decimal(str(round(compound_equity * self._compound_ratio, 2)))
                    if self.max_buy_quote > D0:
                        new_quote = min(new_quote, self.max_buy_quote)
                    if new_quote > D0:
                        old_quote = self.strategy.cfg.buy_quote
                        self.strategy.cfg.buy_quote  = new_quote
                        self.strategy.cfg.sell_quote = new_quote

                        delta = D0
                        target_eth_qty = ss.traded_qty  # default: no change

                        # 2. Optionally rebalance ETH to restore initial ratio
                        if self.compound_rebalance_eth:
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
                            'compound_equity': round(compound_equity, 2),
                            'cum_cycle_pnl': round(self._cum_cycle_pnl, 2),
                            'old_quote': float(old_quote),
                            'new_quote': float(new_quote),
                            'eth_before': float(ss.traded_qty + (-delta if delta < D0 else D0)),
                            'eth_after':  float(ss.traded_qty),
                            'eth_target': float(target_eth_qty),
                        })
                _last_compound_period = period

            # Initialise reference price on very first candle
            if ss.reference_price is None:
                ss.reference_price = o
                ss.initialized = True

            # Simulate intra-candle price path.
            price_seq = self._price_seq(o, h, l, c)

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

        # Trading PnL = only ladder cycle profits (LIFO realized, excludes initial ETH cost basis gains)
        # = _cum_cycle_pnl (exact LIFO match, non-rebalance sells only)
        # Note: distorted when rebalance_sells reorder lots (prefer usdc_cycle_net / eth_cycle_net)
        trading_pnl = self._cum_cycle_pnl

        # Portfolio PnL = full mark-to-market change (includes ETH price drift on initial holding)
        portfolio_pnl = final_equity - initial_equity

        # --- Actual cycle economics (mode-aware) ---
        # Attribute USDC/ETH flows only to matched cycles (min of buys/sells).
        # fixed_quote:  buy_usdc ≈ sell_usdc → usdc_cycle_net ≈ $0, profit stored as ETH
        # banded_qty:   buy_qty  ≈ sell_qty  → eth_cycle_net  ≈ 0,  profit stored as USDC
        n_b = self._ladder_buy_count
        n_s = self._ladder_sell_count
        n   = min(n_b, n_s)  # matched cycles only

        adj_buy_usdc = self._usdc_ladder_buys * (n / n_b) if n_b > 0 else 0.0
        adj_buy_eth  = self._eth_ladder_buys  * (n / n_b) if n_b > 0 else 0.0
        adj_sell_usdc = self._usdc_ladder_sells * (n / n_s) if n_s > 0 else 0.0
        adj_sell_eth  = self._eth_ladder_sells  * (n / n_s) if n_s > 0 else 0.0

        usdc_cycle_net = adj_sell_usdc - adj_buy_usdc   # +ve = USDC profit stored (banded_qty)
        eth_cycle_net  = adj_buy_eth   - adj_sell_eth   # +ve = ETH profit stored  (fixed_quote)

        usdc_per_cycle = usdc_cycle_net / n if n > 0 else 0.0
        eth_per_cycle  = eth_cycle_net  / n if n > 0 else 0.0

        return {
            "initial_equity":    initial_equity,
            "final_equity":      final_equity,
            "final_cash":        float(self.state.cash),
            "final_eth_qty":     float(ss.traded_qty),
            "final_eth_value":   float(ss.traded_qty * final_price),
            "realized_pnl":      float(ss.realized_pnl),   # LIFO all sells (incl rebalance & initial ETH)
            "trading_pnl":       trading_pnl,              # LIFO ladder sells only (distorted by rebalances)
            "usdc_cycle_net":    round(usdc_cycle_net, 4), # actual USDC harvested over matched cycles
            "eth_cycle_net":     round(eth_cycle_net, 6),  # actual ETH harvested over matched cycles
            "usdc_per_cycle":    round(usdc_per_cycle, 4), # avg USDC per matched cycle
            "eth_per_cycle":     round(eth_per_cycle, 6),  # avg ETH per matched cycle
            "portfolio_pnl":     portfolio_pnl,            # total mark-to-market change
            "total_trades":      len(self.trades),
            "ladder_buys":       len(ladder_buys),
            "ladder_sells":      len(ladder_sells),
            "cycles":            cycles,
            "rebalance_trades":  len(rebalances),
            "max_drawdown_pct":  round(max_dd * 100, 2),
            "price_path_mode":   self.price_path_mode,
            # raw flow data for diagnostics
            "usdc_ladder_buys":  round(self._usdc_ladder_buys, 2),
            "usdc_ladder_sells": round(self._usdc_ladder_sells, 2),
            "eth_ladder_buys":   round(self._eth_ladder_buys, 6),
            "eth_ladder_sells":  round(self._eth_ladder_sells, 6),
        }
