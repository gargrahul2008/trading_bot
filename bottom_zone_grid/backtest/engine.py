"""Bottom-zone grid backtest engine.

Walks through intraday candles and simulates grid buy/sell fills with
full cost accounting, LIFO inventory, and capital tracking.

Inventory entries are 4-tuples: (buy_price, qty, timestamp, grid_level)
  - buy_price: actual price paid (used for cost basis / PnL)
  - grid_level: the ladder level this position is assigned to (determines sell target)
For normal buys these are the same; for initial deploy, buy_price = CMP
while grid_level spans the ladder.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from bottom_zone_grid.scanner.grid_calculator import (
    build_grid_ladder,
    calculate_grid_gap_from_bar_range,
    calculate_round_trip_cost,
    round_to_tick,
    NSE_TICK_SIZE,
)

LOG = logging.getLogger("bzg.backtest")


@dataclass
class Trade:
    timestamp: str
    side: str
    price: float
    qty: int
    grid_level: float
    cost: float
    pnl: float = 0.0


@dataclass
class Cycle:
    buy_trade: Trade
    sell_trade: Trade
    gross_pnl: float
    cost: float
    net_pnl: float


@dataclass
class BacktestResult:
    symbol: str
    range_low: float
    range_high: float
    grid_gap: float
    total_levels: int
    order_qty: int

    avg_bar_range_15m: float
    avg_bar_range_5m: Optional[float]

    start_date: str
    end_date: str
    total_candles: int
    trading_days: int

    total_cycles: int = 0
    winning_cycles: int = 0
    losing_cycles: int = 0

    gross_pnl: float = 0.0
    total_cost: float = 0.0
    net_pnl: float = 0.0

    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    peak_equity: float = 0.0

    max_inventory_qty: int = 0
    max_inventory_value: float = 0.0
    avg_inventory_value: float = 0.0

    capital_used: float = 0.0
    capital_per_slot: float = 0.0
    return_on_capital_pct: float = 0.0
    annualized_return_pct: float = 0.0

    avg_cycle_pnl: float = 0.0
    avg_cycle_duration_candles: float = 0.0
    cycles_per_day: float = 0.0

    total_buys: int = 0
    total_sells: int = 0

    trades: List[Trade] = field(default_factory=list)
    cycles: List[Cycle] = field(default_factory=list)
    equity_curve: List[dict] = field(default_factory=list)


def _half_cost(price: float, qty: int, config: Dict, side: str) -> float:
    """Cost for one leg (buy or sell) of a round trip."""
    costs = config.get("costs", {})
    value = price * qty

    brokerage = costs.get("brokerage_per_order", 20)
    if side == "BUY":
        stt = value * costs.get("stt_buy_pct", 0.1) / 100
        stamp = value * costs.get("stamp_duty_buy_pct", 0.015) / 100
    else:
        stt = value * costs.get("stt_sell_pct", 0.025) / 100
        stamp = 0

    exchange_txn = value * costs.get("exchange_txn_pct", 0.00345) / 100
    gst_pct = costs.get("gst_pct", 18.0) / 100
    gst = (brokerage + exchange_txn) * gst_pct
    sebi = value / 10_000_000 * costs.get("sebi_per_crore", 10)
    slippage = value * costs.get("slippage_pct", 0.05) / 100

    return brokerage + stt + stamp + exchange_txn + gst + sebi + slippage


class GridBacktester:
    def __init__(
        self,
        range_low: float,
        range_high: float,
        grid_gap: float,
        order_qty: int,
        capital_per_slot: float,
        config: Dict,
    ):
        self.range_low = range_low
        self.range_high = range_high
        self.grid_gap = grid_gap
        self.order_qty = order_qty
        self.capital_per_slot = capital_per_slot
        self.config = config

        self.ladder = build_grid_ladder(range_low, range_high, grid_gap)
        self.total_levels = len(self.ladder)

        gcfg = config.get("grid", {})
        self.advance_buy_orders = gcfg.get("advance_buy_orders", 2)

        # Inventory: (buy_price, qty, buy_timestamp, grid_level)
        self.inventory: List[Tuple[float, int, str, float]] = []
        self.cash = capital_per_slot
        self.realized_pnl = 0.0
        self.gross_pnl = 0.0
        self.total_cost = 0.0
        self.peak_equity = capital_per_slot
        self.max_drawdown = 0.0

        self.trades: List[Trade] = []
        self.cycles: List[Cycle] = []
        self.equity_curve: List[dict] = []

        self.total_buys = 0
        self.total_sells = 0
        self.max_inventory_qty = 0
        self.max_inventory_value = 0.0
        self.inventory_value_sum = 0.0
        self.inventory_value_samples = 0

        # Track filled grid levels to prevent duplicate buys
        self._filled_buy_levels: set = set()
        self._last_close: Optional[float] = None

    def deploy_initial_inventory(self, entry_price: float, timestamp: str):
        """Deploy capital across grid levels at entry.

        All shares are bought at entry_price (the actual CMP). Each position
        is assigned a grid_level that determines its sell target.
        Levels below entry_price are left empty as cash reserve for dips.
        """
        for lvl in sorted(self.ladder):
            if lvl >= entry_price - self.grid_gap * 0.01:
                self._try_buy(entry_price, timestamp, grid_level=lvl)
        self._last_close = entry_price

    def _inventory_qty(self) -> int:
        return sum(q for _, q, _, _ in self.inventory)

    def _inventory_value(self, mark_price: float) -> float:
        return self._inventory_qty() * mark_price

    def _inventory_cost_basis(self) -> float:
        return sum(bp * q for bp, q, _, _ in self.inventory)

    def _equity(self, mark_price: float) -> float:
        return self.cash + self._inventory_value(mark_price)

    def _try_buy(self, price: float, timestamp: str,
                 grid_level: Optional[float] = None) -> Optional[Trade]:
        gl = grid_level if grid_level is not None else price
        if gl in self._filled_buy_levels:
            return None

        buy_value = price * self.order_qty
        buy_cost = _half_cost(price, self.order_qty, self.config, "BUY")
        total_needed = buy_value + buy_cost

        if self.cash < total_needed:
            return None

        self.cash -= total_needed
        self.inventory.append((price, self.order_qty, timestamp, gl))
        self._filled_buy_levels.add(gl)
        self.total_buys += 1
        self.total_cost += buy_cost

        trade = Trade(
            timestamp=timestamp, side="BUY", price=price,
            qty=self.order_qty, grid_level=gl, cost=buy_cost,
        )
        self.trades.append(trade)
        return trade

    def _try_sell(self, level: float, timestamp: str, fill_price: Optional[float] = None) -> Optional[Trade]:
        if not self.inventory:
            return None

        match_idx = None
        for i in range(len(self.inventory) - 1, -1, -1):
            _, qty, _, gl = self.inventory[i]
            target_sell = round_to_tick(gl + self.grid_gap)
            if abs(target_sell - level) < NSE_TICK_SIZE * 0.5:
                match_idx = i
                break

        if match_idx is None:
            return None

        price = fill_price if fill_price is not None else level
        buy_price, qty, buy_ts, gl = self.inventory.pop(match_idx)
        self._filled_buy_levels.discard(gl)

        sell_value = price * qty
        sell_cost = _half_cost(price, qty, self.config, "SELL")

        gross = (price - buy_price) * qty
        buy_cost = _half_cost(buy_price, qty, self.config, "BUY")
        net = gross - buy_cost - sell_cost

        self.cash += sell_value - sell_cost
        self.realized_pnl += net
        self.gross_pnl += gross
        self.total_cost += sell_cost
        self.total_sells += 1

        trade = Trade(
            timestamp=timestamp, side="SELL", price=price,
            qty=qty, grid_level=gl, cost=sell_cost, pnl=net,
        )
        self.trades.append(trade)

        buy_trade = Trade(
            timestamp=buy_ts, side="BUY", price=buy_price,
            qty=qty, grid_level=gl, cost=buy_cost,
        )
        self.cycles.append(Cycle(
            buy_trade=buy_trade, sell_trade=trade,
            gross_pnl=round(gross, 2), cost=round(buy_cost + sell_cost, 2),
            net_pnl=round(net, 2),
        ))

        if hasattr(self, '_candle_sell_levels'):
            self._candle_sell_levels.add(level)

        return trade

    def process_candle(self, row: dict):
        """Process one intraday candle. Simulate fills if price crosses grid levels."""
        ts = str(row.get("datetime", row.get("timestamp", "")))
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]

        self._candle_sell_levels = set()

        bullish = c >= o

        if bullish:
            self._process_buys(o, l, h, ts)
            self._process_sells(o, l, h, ts)
        else:
            self._process_sells(o, l, h, ts)
            self._process_buys(o, l, h, ts)

        # Track inventory stats
        inv_qty = self._inventory_qty()
        inv_val = self._inventory_value(c)
        if inv_qty > self.max_inventory_qty:
            self.max_inventory_qty = inv_qty
        if inv_val > self.max_inventory_value:
            self.max_inventory_value = inv_val
        self.inventory_value_sum += inv_val
        self.inventory_value_samples += 1

        # Equity and drawdown
        eq = self._equity(c)
        if eq > self.peak_equity:
            self.peak_equity = eq
        dd = self.peak_equity - eq
        if dd > self.max_drawdown:
            self.max_drawdown = dd

        self.equity_curve.append({
            "timestamp": ts,
            "equity": round(eq, 2),
            "cash": round(self.cash, 2),
            "inventory_qty": inv_qty,
            "inventory_value": round(inv_val, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(inv_val - self._inventory_cost_basis(), 2),
        })

        self._last_close = c

    def force_exit(self, price: float, timestamp: str) -> float:
        """Sell all inventory at given price. Returns total net PnL from forced sells."""
        total_net = 0.0
        for buy_price, qty, buy_ts, gl in list(self.inventory):
            sell_cost = _half_cost(price, qty, self.config, "SELL")
            buy_cost = _half_cost(buy_price, qty, self.config, "BUY")
            gross = (price - buy_price) * qty
            net = gross - buy_cost - sell_cost

            self.cash += price * qty - sell_cost
            self.realized_pnl += net
            self.gross_pnl += gross
            self.total_cost += sell_cost
            self.total_sells += 1
            total_net += net

            trade = Trade(
                timestamp=timestamp, side="SELL", price=price,
                qty=qty, grid_level=gl, cost=sell_cost, pnl=net,
            )
            self.trades.append(trade)

            buy_trade = Trade(
                timestamp=buy_ts, side="BUY", price=buy_price,
                qty=qty, grid_level=gl, cost=buy_cost,
            )
            self.cycles.append(Cycle(
                buy_trade=buy_trade, sell_trade=trade,
                gross_pnl=round(gross, 2), cost=round(buy_cost + sell_cost, 2),
                net_pnl=round(net, 2),
            ))
        self.inventory = []
        self._filled_buy_levels.clear()
        return round(total_net, 2)

    def _get_active_buy_levels(self, ref_price: float) -> List[float]:
        """Next N unfilled grid levels below ref_price (advance_buy_orders)."""
        below = [
            lvl for lvl in self.ladder
            if lvl < ref_price - self.grid_gap * 0.01
            and lvl not in self._filled_buy_levels
        ]
        below.sort(reverse=True)
        return below[:self.advance_buy_orders]

    def _get_active_sell_levels(self) -> List[float]:
        """Sell targets for current inventory (grid_level + gap)."""
        targets = []
        for _, qty, _, gl in self.inventory:
            target = round_to_tick(gl + self.grid_gap)
            targets.append(target)
        targets.sort()
        return targets

    def _process_buys(self, candle_open: float, low: float, high: float, ts: str):
        ref = self._last_close if self._last_close else high
        active_buys = self._get_active_buy_levels(ref)
        sold_this_candle = getattr(self, '_candle_sell_levels', set())
        for lvl in active_buys:
            if lvl in sold_this_candle:
                continue
            if lvl >= low:
                fill = candle_open if candle_open < lvl else lvl
                self._try_buy(fill, ts, grid_level=lvl)

    def _process_sells(self, candle_open: float, low: float, high: float, ts: str):
        active_sells = self._get_active_sell_levels()
        for lvl in active_sells:
            if lvl <= high:
                fill = candle_open if candle_open > lvl else lvl
                self._try_sell(lvl, ts, fill_price=fill)

    def run(self, candles: list) -> BacktestResult:
        """Run backtest on a list of candle dicts."""
        if not candles:
            return BacktestResult(
                symbol="", range_low=self.range_low, range_high=self.range_high,
                grid_gap=self.grid_gap, total_levels=self.total_levels,
                order_qty=self.order_qty, avg_bar_range_15m=0, avg_bar_range_5m=None,
                start_date="", end_date="", total_candles=0, trading_days=0,
                capital_per_slot=self.capital_per_slot,
            )

        for candle in candles:
            self.process_candle(candle)

        total_candles = len(candles)
        dates = set()
        for c in candles:
            dt_str = str(c.get("datetime", c.get("timestamp", "")))
            if len(dt_str) >= 10:
                dates.add(dt_str[:10])
        trading_days = max(len(dates), 1)

        total_cycles = len(self.cycles)
        winning = sum(1 for cy in self.cycles if cy.net_pnl > 0)
        losing = total_cycles - winning

        avg_inv_val = (self.inventory_value_sum / self.inventory_value_samples
                       if self.inventory_value_samples > 0 else 0)

        avg_cycle_pnl = self.realized_pnl / total_cycles if total_cycles > 0 else 0
        cycles_per_day = total_cycles / trading_days if trading_days > 0 else 0

        dd_pct = (self.max_drawdown / self.capital_per_slot * 100) if self.capital_per_slot > 0 else 0
        roc = (self.realized_pnl / self.capital_per_slot * 100) if self.capital_per_slot > 0 else 0
        ann_return = roc * (252 / trading_days) if trading_days > 0 else 0

        return BacktestResult(
            symbol="",
            range_low=self.range_low,
            range_high=self.range_high,
            grid_gap=self.grid_gap,
            total_levels=self.total_levels,
            order_qty=self.order_qty,
            avg_bar_range_15m=0,
            avg_bar_range_5m=None,
            start_date=str(candles[0].get("datetime", ""))[:10],
            end_date=str(candles[-1].get("datetime", ""))[:10],
            total_candles=total_candles,
            trading_days=trading_days,
            total_cycles=total_cycles,
            winning_cycles=winning,
            losing_cycles=losing,
            gross_pnl=round(self.gross_pnl, 2),
            total_cost=round(self.total_cost, 2),
            net_pnl=round(self.realized_pnl, 2),
            max_drawdown=round(self.max_drawdown, 2),
            max_drawdown_pct=round(dd_pct, 2),
            peak_equity=round(self.peak_equity, 2),
            max_inventory_qty=self.max_inventory_qty,
            max_inventory_value=round(self.max_inventory_value, 2),
            avg_inventory_value=round(avg_inv_val, 2),
            capital_used=self.capital_per_slot,
            capital_per_slot=self.capital_per_slot,
            return_on_capital_pct=round(roc, 4),
            annualized_return_pct=round(ann_return, 2),
            avg_cycle_pnl=round(avg_cycle_pnl, 2),
            cycles_per_day=round(cycles_per_day, 2),
            total_buys=self.total_buys,
            total_sells=self.total_sells,
            trades=self.trades,
            cycles=self.cycles,
            equity_curve=self.equity_curve,
        )
