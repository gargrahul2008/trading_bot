"""
ProactiveBacktestEngine — replicates live proactive runner behaviour.

Key differences from the reactive BacktestEngine:
  1. Pre-places N limit orders per side at ref ± k*pct% (k=1..N)
  2. Fills happen at the EXACT limit price when candle low/high crosses it
     (not at the OHLC sequence price as in the reactive engine)
  3. On any fill → cancel all remaining levels → re-center ref → place fresh N+N
  4. Multiple fills within one candle are handled by sequential re-centering
  5. Uses banded_qty sizing (same formula as live bot)

PnL metrics produced match the telegram report exactly:
  - LIFO cycle PnL  (same LIFO stack as compute_metrics in mexc_telegram_report.py)
  - Avg-cost PnL    (running avg cost basis, same formula)
  - Portfolio value = cash + eth_qty * price
  - Cycles = number of LIFO-matched buy→sell pairs
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple, Optional, Dict, Any

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


def _band_mid(price: Decimal, band_width: Decimal) -> Decimal:
    """Same formula as live bot banded_qty mode."""
    return (price // band_width) * band_width + band_width / Decimal("2")


def _round_qty(qty: Decimal, qty_step: Decimal, min_qty: Decimal) -> Decimal:
    if qty_step <= D0:
        return qty
    q = (qty / qty_step).to_integral_value(rounding=ROUND_DOWN) * qty_step
    return q if q >= min_qty else D0


class ProactiveBacktestEngine:
    """
    Simulates the proactive runner with N pre-placed limit orders per side.

    Parameters
    ----------
    symbol          : trading pair, e.g. 'ETHUSDC'
    initial_cash    : starting USDC balance
    initial_eth     : starting ETH balance (already held)
    initial_eth_cost: avg cost basis of the initial ETH (for LIFO seeding)
    lower_pct       : buy grid spacing in % (e.g. 0.2)
    upper_pct       : sell grid spacing in % (e.g. 0.2)
    buy_quote       : USDC per buy order
    sell_quote      : USDC per sell order
    band_width      : banded_qty band width in $ (e.g. 100)
    qty_step        : minimum qty increment (e.g. 0.000001)
    min_qty         : minimum order qty
    pro_levels      : number of limit orders per side (N)
    quote_reserve   : USDC kept aside (not used for orders)
    rebalance_threshold_steps : if >0, rebalance when cash/inventory runs out
    compound_interval : None | 'daily' | 'weekly' | 'monthly'
    compound_initial_equity : starting equity for compound ratio
    price_path_mode : 'random' | 'optimistic' | 'pessimistic' | 'realistic'
                      realistic = close-direction + max 1 fill per candle (best live match)
    seed            : random seed for reproducibility
    """

    def __init__(
        self,
        symbol: str = "ETHUSDC",
        initial_cash: float = 50000.0,
        initial_eth: float = 0.0,
        initial_eth_cost: float = 0.0,
        lower_pct: float = 0.2,
        upper_pct: float = 0.2,
        buy_quote: float = 2512.0,
        sell_quote: float = 2512.0,
        band_width: float = 100.0,
        qty_step: float = 0.000001,
        min_qty: float = 0.000001,
        pro_levels: int = 2,
        quote_reserve: float = 500.0,
        rebalance_threshold_steps: int = 1,
        compound_interval: Optional[str] = None,   # None | 'daily' | 'weekly' | 'monthly'
        compound_initial_equity: Optional[float] = None,
        price_path_mode: str = "random",
        seed: int = 42,
    ):
        import random
        self._rng = random.Random(seed)

        self.symbol = symbol
        self.lower_pct = _dec(lower_pct)
        self.upper_pct = _dec(upper_pct)
        self.buy_quote = _dec(buy_quote)
        self.sell_quote = _dec(sell_quote)
        self.band_width = _dec(band_width)
        self.qty_step = _dec(qty_step)
        self.min_qty = _dec(min_qty)
        self.pro_levels = int(pro_levels)
        self.quote_reserve = _dec(quote_reserve)
        self.rebalance_threshold_steps = int(rebalance_threshold_steps)
        self.compound_interval = compound_interval
        self.price_path_mode = price_path_mode

        # State
        self.cash = _dec(initial_cash)
        self.eth_qty = _dec(initial_eth)
        self.reference_price: Optional[Decimal] = None

        # LIFO lot stack for cost basis tracking (same as live bot)
        # Each entry: [qty, price]
        self.lots: List[List[Decimal]] = []
        if initial_eth > 0 and initial_eth_cost > 0:
            self.lots.append([_dec(initial_eth), _dec(initial_eth_cost)])

        # Compounding
        initial_equity = initial_cash + initial_eth * initial_eth_cost
        if compound_interval and initial_equity > 0:
            basis = float(compound_initial_equity or initial_equity)
            self._compound_ratio = float(buy_quote) / basis
            self._compound_initial_equity = _dec(compound_initial_equity or initial_equity)
        else:
            self._compound_ratio = None
            self._compound_initial_equity = _dec(initial_equity)
        self._last_compound_period = None

        # PnL tracking (mirrors telegram report metrics)
        self._lifo_stack: List[List] = []   # [qty, price] — separate LIFO for cycle PnL calc
        if initial_eth > 0 and initial_eth_cost > 0:
            self._lifo_stack.append([float(initial_eth), float(initial_eth_cost)])

        self._avg_cost_qty: float = initial_eth
        self._avg_cost_cost: float = initial_eth * initial_eth_cost

        self.all_time_cycles: int = 0
        self.all_time_lifo_pnl: float = 0.0
        self.all_time_avg_cost_pnl: float = 0.0    # grid sells vs avg cost

        # Rebalance tracking
        self.rebal_count_buy: int = 0
        self.rebal_count_sell: int = 0
        self.rebal_usdc_spent: float = 0.0          # USDC used to buy ETH via rebalance
        self.rebal_usdc_received: float = 0.0       # USDC received from selling ETH via rebalance
        self.rebal_eth_bought: float = 0.0          # ETH accumulated via rebalance buys
        self.rebal_eth_sold: float = 0.0            # ETH released via rebalance sells
        self.rebal_avg_cost_pnl: float = 0.0        # avg-cost PnL on rebalance sells
        self._avg_cost_log: List[Dict] = []         # (ts, avg_cost, eth_qty) snapshots

        # Output
        self.trades: List[Dict[str, Any]] = []
        self.equity_curve: List[Tuple[Any, float]] = []
        self.cash_curve: List[Tuple[Any, float]] = []
        self.compound_log: List[Dict] = []

    # ------------------------------------------------------------------
    # Qty calculation (banded_qty)
    # ------------------------------------------------------------------

    def _buy_qty(self, level_price: Decimal) -> Decimal:
        bm = _band_mid(level_price, self.band_width)
        return _round_qty(self.buy_quote / bm, self.qty_step, self.min_qty)

    def _sell_qty(self, level_price: Decimal) -> Decimal:
        bm = _band_mid(level_price, self.band_width)
        return _round_qty(self.sell_quote / bm, self.qty_step, self.min_qty)

    # ------------------------------------------------------------------
    # Level calculation
    # ------------------------------------------------------------------

    def _buy_levels(self, ref: Decimal) -> List[Tuple[int, Decimal, Decimal]]:
        """Returns [(k, price, qty)] for k=1..N."""
        levels = []
        for k in range(1, self.pro_levels + 1):
            price = ref * (Decimal("1") - k * self.lower_pct / Decimal("100"))
            qty = self._buy_qty(price)
            if qty > D0:
                levels.append((k, price, qty))
        return levels

    def _sell_levels(self, ref: Decimal) -> List[Tuple[int, Decimal, Decimal]]:
        """Returns [(k, price, qty)] for k=1..N."""
        levels = []
        for k in range(1, self.pro_levels + 1):
            price = ref * (Decimal("1") + k * self.upper_pct / Decimal("100"))
            qty = self._sell_qty(price)
            if qty > D0:
                levels.append((k, price, qty))
        return levels

    # ------------------------------------------------------------------
    # Fill application
    # ------------------------------------------------------------------

    def _apply_fill(self, side: str, qty: Decimal, price: Decimal, reason: str, ts: str) -> None:
        is_rebal = reason.startswith("rebalance")

        if side == "BUY":
            cost = qty * price
            self.cash -= cost
            self.eth_qty += qty
            self.lots.append([qty, price])

            # LIFO stack + avg cost
            self._lifo_stack.append([float(qty), float(price)])
            notional = float(qty * price)
            self._avg_cost_qty += float(qty)
            self._avg_cost_cost += notional

            if is_rebal:
                self.rebal_count_buy += 1
                self.rebal_usdc_spent += float(cost)
                self.rebal_eth_bought += float(qty)

        else:  # SELL
            proceeds = qty * price
            self.cash += proceeds
            self.eth_qty -= qty

            # LIFO cost basis (live bot uses same)
            to_sell = qty
            while to_sell > D0 and self.lots:
                lot_qty, lot_px = _dec(self.lots[-1][0]), _dec(self.lots[-1][1])
                take = min(to_sell, lot_qty)
                self.lots[-1][0] = lot_qty - take
                if self.lots[-1][0] <= D0:
                    self.lots.pop()
                to_sell -= take

            # LIFO cycle PnL (same LIFO stack as telegram script)
            if not is_rebal:
                sell_qty = float(qty)
                sell_price = float(price)
                i = len(self._lifo_stack) - 1
                while i >= 0:
                    entry = self._lifo_stack[i]
                    if sell_price > entry[1]:
                        take = min(sell_qty, entry[0])
                        cycle_pnl = take * (sell_price - entry[1])
                        self.all_time_lifo_pnl += cycle_pnl
                        self.all_time_cycles += 1
                        entry[0] -= take
                        if entry[0] <= 0:
                            self._lifo_stack.pop(i)
                        break
                    i -= 1

            # Avg-cost PnL (grid sells and rebalance sells tracked separately)
            if self._avg_cost_qty > 0:
                avg_cost = self._avg_cost_cost / self._avg_cost_qty
                avg_pnl = float(qty) * (float(price) - avg_cost)
                sell_qty_f = min(float(qty), self._avg_cost_qty)
                self._avg_cost_cost -= sell_qty_f * avg_cost
                self._avg_cost_qty -= sell_qty_f
                if is_rebal:
                    self.rebal_avg_cost_pnl += avg_pnl
                else:
                    self.all_time_avg_cost_pnl += avg_pnl

            if is_rebal:
                self.rebal_count_sell += 1
                self.rebal_usdc_received += float(proceeds)
                self.rebal_eth_sold += float(qty)

        # Snapshot avg cost after every fill (for avg cost curve)
        avg_cost_now = (self._avg_cost_cost / self._avg_cost_qty) if self._avg_cost_qty > 0 else 0.0
        self._avg_cost_log.append({
            "ts": ts,
            "avg_cost": avg_cost_now,
            "eth_qty": float(self.eth_qty),
            "side": side,
            "is_rebal": is_rebal,
            "price": float(price),
        })

        # Update reference price on grid fill
        if not is_rebal:
            self.reference_price = price

        self.trades.append({
            "ts": ts,
            "side": side,
            "qty": float(qty),
            "price": float(price),
            "reason": reason,
            "cash_after": float(self.cash),
            "eth_after": float(self.eth_qty),
            "buy_quote": float(self.buy_quote),
        })

    # ------------------------------------------------------------------
    # Rebalance (mirrors live _rebalance_sync)
    # ------------------------------------------------------------------

    def _rebalance_buy(self, ref: Decimal, price: Decimal, ts: str) -> None:
        """Sell ETH to raise cash for buy orders."""
        total_needed = sum(qty * lvl for _, lvl, qty in self._buy_levels(ref))
        available = max(self.cash - self.quote_reserve, D0)
        if total_needed <= available:
            return
        need = total_needed - available
        sell_qty = _round_qty(need / price, self.qty_step, self.min_qty)
        sell_qty = min(sell_qty, self.eth_qty)
        if sell_qty > D0:
            self._apply_fill("SELL", sell_qty, price, "rebalance_sell_for_buy", ts)

    def _rebalance_sell(self, ref: Decimal, price: Decimal, ts: str) -> None:
        """Buy ETH to cover sell orders."""
        total_needed = sum(qty for _, _, qty in self._sell_levels(ref))
        if self.eth_qty >= total_needed:
            return
        need = total_needed - self.eth_qty
        buy_qty = _round_qty(need, self.qty_step, self.min_qty)
        cost = buy_qty * price
        available = max(self.cash - self.quote_reserve, D0)
        if cost > available:
            buy_qty = _round_qty(available / price, self.qty_step, self.min_qty)
        if buy_qty > D0:
            self._apply_fill("BUY", buy_qty, price, "rebalance_buy_for_sell", ts)

    # ------------------------------------------------------------------
    # Intra-candle price path (same logic as reactive engine)
    # ------------------------------------------------------------------

    def _price_path(self, o: Decimal, h: Decimal, l: Decimal, c: Decimal) -> str:
        """Returns 'low_first' or 'high_first'."""
        if self.price_path_mode in ("optimistic", "realistic"):
            return "low_first" if c >= o else "high_first"
        elif self.price_path_mode == "pessimistic":
            return "low_first" if c < o else "high_first"
        else:
            return "low_first" if self._rng.random() < 0.5 else "high_first"

    # ------------------------------------------------------------------
    # Compounding
    # ------------------------------------------------------------------

    def _maybe_compound(self, ts_pd, price: Decimal, ts: str) -> None:
        if not self._compound_ratio or not self.compound_interval:
            return

        if self.compound_interval == "daily":
            period = ts_pd.date()
        elif self.compound_interval == "weekly":
            iso = ts_pd.isocalendar()
            period = (iso[0], iso[1])
        elif self.compound_interval == "monthly":
            period = (ts_pd.year, ts_pd.month)
        else:
            return

        if period == self._last_compound_period or self._last_compound_period is None:
            self._last_compound_period = period
            return

        self._last_compound_period = period
        current_equity = float(self.cash) + float(self.eth_qty) * float(price)
        new_quote = _dec(round(current_equity * self._compound_ratio, 2))
        if new_quote > D0:
            old_quote = self.buy_quote
            self.buy_quote = new_quote
            self.sell_quote = new_quote
            self.compound_log.append({
                "ts": ts,
                "equity": round(current_equity, 2),
                "old_quote": float(old_quote),
                "new_quote": float(new_quote),
            })

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self, candles) -> dict:
        """
        candles: DataFrame or list of dicts with columns: ts, open, high, low, close
        Returns summary dict with all metrics.
        """
        import pandas as pd

        candle_list = candles.to_dict("records") if hasattr(candles, "to_dict") else list(candles)

        for candle in candle_list:
            ts = str(candle["ts"])
            o = _dec(candle["open"])
            h = _dec(candle["high"])
            l = _dec(candle["low"])
            c = _dec(candle["close"])

            # Compounding at period boundary
            try:
                ts_pd = pd.Timestamp(ts)
                self._maybe_compound(ts_pd, c, ts)
            except Exception:
                pass

            # Initialise reference on first candle
            if self.reference_price is None:
                self.reference_price = o
                if self.rebalance_threshold_steps > 0:
                    self._rebalance_buy(o, o, ts)
                    self._rebalance_sell(o, o, ts)

            # Simulate intra-candle fills (can re-center multiple times per candle)
            path = self._price_path(o, h, l, c)
            self._simulate_candle_fills(l, h, path, ts)

            # Equity snapshot at close
            equity = float(self.cash) + float(self.eth_qty) * float(c)
            self.equity_curve.append((candle["ts"], equity))
            self.cash_curve.append((candle["ts"], float(self.cash)))

        final_price = _dec(candle_list[-1]["close"]) if candle_list else D0
        return self._summary(final_price)

    def _simulate_candle_fills(
        self, low: Decimal, high: Decimal, path: str, ts: str
    ) -> None:
        """
        Check all N buy and sell levels against candle low/high.
        Fill at exact limit price (not at low/high).
        On any fill → re-center → repeat (handles multiple fills per candle).
        Max iterations = 2*N to prevent infinite loops on flat candles.

        'realistic' mode: max 1 fill per candle, simulating the dead window
        (cancel + re-order) that the live bot incurs after each fill.
        Direction is close-based (bullish candle → try high/sell first).
        """
        max_iters = 1 if self.price_path_mode == "realistic" else self.pro_levels * 4
        for _ in range(max_iters):
            ref = self.reference_price
            if ref is None:
                break

            buy_lvls = self._buy_levels(ref)
            sell_lvls = self._sell_levels(ref)

            filled = False

            if path == "low_first":
                # Check buys first (low side), then sells
                filled = self._try_fill_buys(buy_lvls, low, high, ts)
                if not filled:
                    filled = self._try_fill_sells(sell_lvls, low, high, ts)
            else:
                # Check sells first (high side), then buys
                filled = self._try_fill_sells(sell_lvls, low, high, ts)
                if not filled:
                    filled = self._try_fill_buys(buy_lvls, low, high, ts)

            if not filled:
                break   # no more fills this candle

    def _try_fill_buys(
        self,
        buy_lvls: List[Tuple[int, Decimal, Decimal]],
        low: Decimal, high: Decimal, ts: str,
    ) -> bool:
        """Try to fill the deepest hit buy level. Returns True if filled."""
        ref = self.reference_price

        # Find deepest (largest k) buy level that was hit
        hit = None
        for k, lvl_price, qty in reversed(buy_lvls):
            if low <= lvl_price:
                hit = (k, lvl_price, qty)
                break   # take deepest hit only (most conservative)

        if hit is None:
            return False

        k, lvl_price, qty = hit

        # Check cash availability
        cost = qty * lvl_price
        available = max(self.cash - self.quote_reserve, D0)
        if cost > available:
            if self.rebalance_threshold_steps > 0:
                self._rebalance_buy(ref, lvl_price, ts)
                available = max(self.cash - self.quote_reserve, D0)
            if cost > available:
                # Partial fill with available cash
                qty = _round_qty(available / lvl_price, self.qty_step, self.min_qty)
                if qty <= D0:
                    return False

        self._apply_fill("BUY", qty, lvl_price,
                         f"pro_buy_L{k}|ref-{k}x{float(self.lower_pct)}%", ts)
        return True

    def _try_fill_sells(
        self,
        sell_lvls: List[Tuple[int, Decimal, Decimal]],
        low: Decimal, high: Decimal, ts: str,
    ) -> bool:
        """Try to fill the highest hit sell level. Returns True if filled."""
        # Find highest (largest k) sell level that was hit
        hit = None
        for k, lvl_price, qty in reversed(sell_lvls):
            if high >= lvl_price:
                hit = (k, lvl_price, qty)
                break

        if hit is None:
            return False

        k, lvl_price, qty = hit

        # Check inventory
        if qty > self.eth_qty:
            if self.rebalance_threshold_steps > 0:
                ref = self.reference_price
                self._rebalance_sell(ref, lvl_price, ts)
            qty = min(qty, self.eth_qty)
            qty = _round_qty(qty, self.qty_step, self.min_qty)
            if qty <= D0:
                return False

        self._apply_fill("SELL", qty, lvl_price,
                         f"pro_sell_L{k}|ref+{k}x{float(self.upper_pct)}%", ts)
        return True

    # ------------------------------------------------------------------
    # Summary (mirrors telegram report metrics)
    # ------------------------------------------------------------------

    def _summary(self, final_price: Decimal) -> dict:
        ladder_buys  = [t for t in self.trades if t["side"] == "BUY"  and not t["reason"].startswith("rebalance")]
        ladder_sells = [t for t in self.trades if t["side"] == "SELL" and not t["reason"].startswith("rebalance")]
        rebalances   = [t for t in self.trades if t["reason"].startswith("rebalance")]

        equities = [e for _, e in self.equity_curve]
        initial_equity = equities[0] if equities else float(self.cash)
        final_equity   = equities[-1] if equities else float(self.cash) + float(self.eth_qty * final_price)

        # Max drawdown
        max_dd = 0.0
        peak = initial_equity
        for e in equities:
            if e > peak:
                peak = e
            if peak > 0:
                dd = (peak - e) / peak
                if dd > max_dd:
                    max_dd = dd

        # Open inventory unrealized PnL
        open_eth = float(self.eth_qty)
        open_avg_cost = 0.0
        if self._lifo_stack:
            total_q = sum(e[0] for e in self._lifo_stack)
            total_c = sum(e[0] * e[1] for e in self._lifo_stack)
            open_avg_cost = total_c / total_q if total_q > 0 else 0.0
        unrealized_pnl = open_eth * (float(final_price) - open_avg_cost) if open_avg_cost > 0 else 0.0

        rebal_net_usdc = self.rebal_usdc_received - self.rebal_usdc_spent
        rebal_net_eth  = self.rebal_eth_bought - self.rebal_eth_sold

        return {
            # Portfolio
            "initial_equity":        round(initial_equity, 2),
            "final_equity":          round(final_equity, 2),
            "portfolio_pnl":         round(final_equity - initial_equity, 2),
            "portfolio_pnl_pct":     round((final_equity - initial_equity) / initial_equity * 100, 3) if initial_equity > 0 else 0.0,
            "final_cash":            round(float(self.cash), 2),
            "final_eth_qty":         round(float(self.eth_qty), 6),
            "final_eth_value":       round(float(self.eth_qty * final_price), 2),
            "final_price":           float(final_price),
            # Grid PnL
            "lifo_cycle_pnl":        round(self.all_time_lifo_pnl, 2),
            "avg_cost_pnl":          round(self.all_time_avg_cost_pnl, 2),
            "open_avg_cost":         round(open_avg_cost, 4),
            "unrealized_pnl":        round(unrealized_pnl, 2),
            "net_pnl":               round(self.all_time_lifo_pnl + unrealized_pnl, 2),
            # Rebalance PnL
            "rebal_count_buy":       self.rebal_count_buy,
            "rebal_count_sell":      self.rebal_count_sell,
            "rebal_usdc_spent":      round(self.rebal_usdc_spent, 2),
            "rebal_usdc_received":   round(self.rebal_usdc_received, 2),
            "rebal_net_usdc":        round(rebal_net_usdc, 2),
            "rebal_eth_bought":      round(self.rebal_eth_bought, 6),
            "rebal_eth_sold":        round(self.rebal_eth_sold, 6),
            "rebal_net_eth":         round(rebal_net_eth, 6),
            "rebal_avg_cost_pnl":    round(self.rebal_avg_cost_pnl, 2),
            "total_pnl_incl_rebal":  round(self.all_time_avg_cost_pnl + self.rebal_avg_cost_pnl, 2),
            # Trades
            "total_trades":          len(self.trades),
            "ladder_buys":           len(ladder_buys),
            "ladder_sells":          len(ladder_sells),
            "cycles":                self.all_time_cycles,
            "rebalance_trades":      len(rebalances),
            # Risk
            "max_drawdown_pct":      round(max_dd * 100, 2),
            "price_path_mode":       self.price_path_mode,
        }
