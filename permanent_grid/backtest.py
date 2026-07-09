#!/usr/bin/env python3
"""
Permanent Grid Strategy Backtest Engine
Implements all rules from RULEBOOK.md v1.0

Usage:
    python permanent_grid/backtest.py [--capital 10000000] [--grid-pct 0.01] [--runway 0.25]
"""

import sys, os, pickle, math, argparse
sys.path.insert(0, "/root/trading_bot")

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from permanent_grid.support import find_swing_lows, cluster_supports, find_nearest_support


# ── Enums ──

class MarketState(Enum):
    NORMAL = "NORMAL"
    CAUTION = "CAUTION"
    STRESS = "STRESS"
    CRASH_DEPLOY = "CRASH_DEPLOY"
    RECOVERY = "RECOVERY"

class StockState(Enum):
    WAITING = "WAITING"
    ACTIVE = "ACTIVE"
    UPSIDE_OUT = "UPSIDE_OUT"
    HOLD = "HOLD"
    BLOCKED = "BLOCKED"
    SELL_ONLY = "SELL_ONLY"


# ── Data classes ──

@dataclass
class Fill:
    date: str
    symbol: str
    side: str       # BUY / SELL / STOP_LOSS
    price: float
    shares: float
    pnl: float = 0  # realized pnl on this fill (sells only)

@dataclass
class StockGrid:
    symbol: str
    sector: str
    state: StockState = StockState.WAITING
    capital: float = 0.0

    # Grid params (set on entry)
    entry_price: float = 0.0
    entry_date: str = ""
    grid_step: float = 0.0       # absolute price step
    shares_per_level: float = 0.0
    max_levels: int = 25

    # Position tracking — simple level-based model
    # ref_price moves with each fill: down on buy, up on sell
    ref_price: float = 0.0
    inventory: int = 0            # number of lots held (0 to max_levels+1)
    inventory_cost: float = 0.0   # total cost basis

    # PnL
    realized_pnl: float = 0.0
    total_buys: int = 0
    total_sells: int = 0
    grid_cycles: int = 0         # complete round trips

    # State tracking
    blocked_until_idx: int = -1
    stop_count: int = 0
    upside_out_date: str = ""

    fills: List[Fill] = field(default_factory=list)

    @property
    def avg_cost(self) -> float:
        if self.inventory <= 0:
            return 0
        return self.inventory_cost / (self.inventory * self.shares_per_level)

    @property
    def next_buy_price(self) -> float:
        return self.ref_price * (1 - self.grid_step / self.ref_price) if self.ref_price > 0 else 0

    @property
    def next_sell_price(self) -> float:
        return self.ref_price * (1 + self.grid_step / self.ref_price) if self.ref_price > 0 else 0


# ── Market State Tracker (Rule 4) ──

class MarketStateTracker:
    def __init__(self, nifty_df: pd.DataFrame):
        self.nifty = nifty_df.sort_values("date").reset_index(drop=True)
        self.state = MarketState.NORMAL
        self._high_52w = 0.0
        self._current = 0.0

    def update(self, date) -> MarketState:
        mask = self.nifty["date"] <= date
        if mask.sum() == 0:
            return self.state

        subset = self.nifty[mask]
        self._current = float(subset.iloc[-1]["close"])

        lookback = min(252, len(subset))
        self._high_52w = float(subset.iloc[-lookback:]["high"].max())

        dd_pct = (self._high_52w - self._current) / self._high_52w * 100

        if dd_pct >= 25:
            new_state = MarketState.CRASH_DEPLOY
        elif dd_pct >= 15:
            new_state = MarketState.STRESS
        elif dd_pct >= 8:
            new_state = MarketState.CAUTION
        elif self.state in (MarketState.CRASH_DEPLOY, MarketState.STRESS,
                            MarketState.RECOVERY):
            new_state = MarketState.NORMAL if dd_pct <= 10 else MarketState.RECOVERY
        else:
            new_state = MarketState.NORMAL

        if new_state != self.state:
            self.state = new_state
        return self.state


# ── Main Backtest Engine ──

class PermanentGridBacktest:

    def __init__(self, basket: List[dict], daily_data: Dict[str, pd.DataFrame],
                 nifty_df: pd.DataFrame, total_capital: float = 10_000_000,
                 grid_pct: float = 0.01, runway_pct: float = 0.25,
                 initial_levels: int = None, sim_start: str = None, sim_end: str = None,
                 universe: List[dict] = None, universe_data: Dict[str, pd.DataFrame] = None,
                 dynamic_replacement: bool = True,
                 entry_mode: str = "basket",
                 max_entries_per_week: int = 3):
        """
        entry_mode:
          "basket"   — pre-assign 10 basket stocks as WAITING, enter when _can_enter passes (original)
          "universe" — start empty, scan all ~50 universe stocks each day, pick best candidates
                       gated by market state + rate limiter

        max_entries_per_week: rate limiter for universe mode (ignored in basket mode)
        """

        self.grid_pct = grid_pct
        self.runway_pct = runway_pct
        self.num_levels = int(runway_pct / grid_pct)  # 25
        self.fixed_initial_levels = initial_levels  # None = use support-based pro-rata
        self.total_capital = total_capital
        self.grid_capital = total_capital * 0.80
        self.crash_reserve = total_capital * 0.20
        self.max_grids = len(basket)
        self.capital_per_stock = self.grid_capital / self.max_grids
        self.dynamic_replacement = dynamic_replacement and universe_data is not None
        self.entry_mode = entry_mode
        self.max_entries_per_week = max_entries_per_week
        self._entries_this_week = 0
        self._current_week = None

        # Prepare daily data indexed by symbol name
        self.daily = {}
        self.sym_to_fsym = {}
        for item in basket:
            sym = item["symbol"]
            fsym = item["fyers_symbol"]
            self.sym_to_fsym[sym] = fsym
            if fsym in daily_data:
                df = daily_data[fsym].sort_values("date").reset_index(drop=True)
                self.daily[sym] = df

        # Universe data for dynamic replacement (all ~50 Nifty50 stocks)
        self.universe = universe or []
        self.universe_daily = {}
        self.universe_info = {}
        if universe_data:
            for item in (universe or []):
                sym = item["symbol"]
                fsym = item["fyers_symbol"]
                self.universe_info[sym] = {"fyers_symbol": fsym, "sector": item["sector"]}
                if fsym in universe_data:
                    df = universe_data[fsym].sort_values("date").reset_index(drop=True)
                    self.universe_daily[sym] = df
                    if sym not in self.daily:
                        self.daily[sym] = df

        # Determine sim range
        all_dates = set()
        for df in self.daily.values():
            all_dates.update(df["date"].tolist())
        all_dates = sorted(all_dates)

        if sim_start:
            ss = pd.Timestamp(sim_start)
            all_dates = [d for d in all_dates if d >= ss]
        if sim_end:
            se = pd.Timestamp(sim_end)
            all_dates = [d for d in all_dates if d <= se]
        self.trading_dates = all_dates

        # Build grids for initial basket (basket mode) or start empty (universe mode)
        self.grids: Dict[str, StockGrid] = {}
        if self.entry_mode == "basket":
            for item in basket:
                sym = item["symbol"]
                self.grids[sym] = StockGrid(
                    symbol=sym, sector=item["sector"],
                    capital=self.capital_per_stock,
                )

        self.market = MarketStateTracker(nifty_df)
        self.daily_snapshots = []
        self.events = []
        self.replacements = 0
        self.retired_grids: List[StockGrid] = []

    # ── Indicator helpers ──

    def _dma(self, sym: str, date, period: int) -> Optional[float]:
        df = self.daily.get(sym)
        if df is None:
            return None
        subset = df[df["date"] <= date]
        if len(subset) < period:
            return None
        return float(subset.iloc[-period:]["close"].mean())

    def _range_52w(self, sym: str, date) -> Tuple[float, float]:
        df = self.daily.get(sym)
        if df is None:
            return (0, 0)
        subset = df[df["date"] <= date]
        n = min(252, len(subset))
        recent = subset.iloc[-n:]
        return (float(recent["low"].min()), float(recent["high"].max()))

    # ── Support detection ──

    def _find_support(self, sym: str, date) -> Optional[float]:
        """Find nearest strong support below current price using historical data up to `date`."""
        df = self.daily.get(sym)
        if df is None:
            return None
        subset = df[df["date"] <= date].copy()
        if len(subset) < 60:
            return None

        swing_lows = find_swing_lows(subset, window=10, min_bounce_pct=3.0)
        zones = cluster_supports(swing_lows, cluster_pct=3.0)
        close = float(subset.iloc[-1]["close"])
        nearest = find_nearest_support(zones, close, max_distance_pct=25.0)
        return nearest.price if nearest else None

    def _compute_initial_levels(self, entry_price: float, support_price: float) -> int:
        """Pro-rata: levels above = inventory to seed, levels below = runway."""
        if entry_price <= support_price:
            return self.num_levels  # at or below support, fully loaded
        pct_above = (entry_price / support_price - 1)
        levels_below = min(int(pct_above / self.grid_pct), self.num_levels - 1)
        levels_above = self.num_levels - levels_below
        return max(levels_above, 1)

    # ── Rule 1: Entry check ──

    def _can_enter(self, sym: str, close: float, date) -> bool:
        dma200 = self._dma(sym, date, 200)
        if dma200 is None:
            dma50 = self._dma(sym, date, 50)
            if dma50 is None:
                return False
            if abs(close / dma50 - 1) > 0.15:
                return False
        else:
            if abs(close / dma200 - 1) > 0.10:
                return False

        low_52w, high_52w = self._range_52w(sym, date)
        if high_52w > 0 and (high_52w - close) / high_52w < 0.05:
            return False
        if low_52w > 0 and (close / low_52w - 1) < 0.15:
            return False

        # Must have identifiable support within 25%
        support = self._find_support(sym, date)
        if support is None:
            return False

        return True

    # ── Rule 2 re-entry check (UPSIDE_OUT → wait for pullback) ──

    def _can_reanchor(self, grid: StockGrid, close: float, date) -> bool:
        # Option A: within 5% of 50-DMA (mean reversion)
        dma50 = self._dma(grid.symbol, date, 50)
        if dma50 and abs(close / dma50 - 1) <= 0.05:
            return True
        # Option B: price pulled back ≥3% from the ref (which was pushed up by sells)
        if grid.ref_price > 0 and close <= grid.ref_price * 0.97:
            return True
        return False

    # ── Dynamic stock replacement ──

    def _score_for_grid(self, sym: str, date) -> float:
        """Score how suitable a stock is for grid trading at this date."""
        df = self.daily.get(sym)
        if df is None:
            return -999

        subset = df[df["date"] <= date]
        if len(subset) < 200:
            return -999

        close = float(subset.iloc[-1]["close"])

        # Must pass entry conditions
        if not self._can_enter(sym, close, date):
            return -999

        # Trailing 200-day volatility (annualized) — want HIGH
        rets = subset["close"].pct_change().dropna().tail(200)
        vol = float(rets.std() * np.sqrt(252))

        # Support quality — must have support
        support = self._find_support(sym, date)
        if support is None:
            return -999

        # Sector diversity bonus: penalize if same sector as active grids
        active_sectors = set()
        for s, g in self.grids.items():
            if g.state in (StockState.ACTIVE, StockState.SELL_ONLY):
                active_sectors.add(g.sector)
        info = self.universe_info.get(sym, {})
        sector_penalty = 0.5 if info.get("sector") in active_sectors else 1.0

        return vol * sector_penalty

    def _find_replacement(self, date, exiting_sym: str) -> Optional[str]:
        """Find the best universe stock to replace an exiting grid."""
        if not self.dynamic_replacement:
            return None

        active_syms = {s for s, g in self.grids.items()
                       if g.state in (StockState.ACTIVE, StockState.SELL_ONLY,
                                      StockState.WAITING)}

        best_sym = None
        best_score = -999
        # Also score the exiting stock itself (it may still be the best choice)
        candidates = set(self.universe_daily.keys()) | {exiting_sym}

        for sym in candidates:
            if sym in active_syms:
                continue
            score = self._score_for_grid(sym, date)
            if score > best_score:
                best_score = score
                best_sym = sym

        return best_sym

    # ── Grid setup ──

    def _enter_grid(self, grid: StockGrid, price: float, date_str: str,
                    date=None):
        grid.entry_price = price
        grid.entry_date = date_str
        grid.grid_step = price * self.grid_pct
        grid.shares_per_level = grid.capital / (price * self.num_levels)
        grid.ref_price = price
        grid.state = StockState.ACTIVE

        # Pro-rata seed based on support distance
        if self.fixed_initial_levels is not None:
            seed = self.fixed_initial_levels
            support_px = 0
        else:
            support_px = self._find_support(grid.symbol, date or pd.Timestamp(date_str))
            if support_px is None:
                support_px = price * (1 - self.runway_pct)
            seed = self._compute_initial_levels(price, support_px)

        seed = min(seed, self.num_levels)
        grid.inventory = seed
        grid.inventory_cost = seed * grid.shares_per_level * price
        grid.total_buys += seed

        levels_below = self.num_levels - seed
        fill = Fill(date=date_str, symbol=grid.symbol, side="BUY",
                    price=price, shares=seed * grid.shares_per_level)
        grid.fills.append(fill)
        self.events.append(f"{date_str} ENTER {grid.symbol} @ {price:.2f} "
                           f"(support={support_px:.0f}, {seed}↑/{levels_below}↓, "
                           f"{grid.shares_per_level:.1f} sh/lvl)")

    # ── Core: process one day of grid fills ──

    def _process_day(self, grid: StockGrid, row: pd.Series, date_str: str,
                     date_idx: int, mkt: MarketState):
        if grid.state not in (StockState.ACTIVE, StockState.SELL_ONLY):
            return

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        step = grid.grid_step

        # Rule 3: hard stop at -35% (only if market not in STRESS/CRASH)
        stop_price = grid.entry_price * 0.65
        if mkt not in (MarketState.STRESS, MarketState.CRASH_DEPLOY):
            if l <= stop_price and grid.inventory > 0:
                exit_px = stop_price
                exit_value = grid.inventory * grid.shares_per_level * exit_px
                pnl = exit_value - grid.inventory_cost
                grid.realized_pnl += pnl

                fill = Fill(date=date_str, symbol=grid.symbol, side="STOP_LOSS",
                            price=exit_px, shares=grid.inventory * grid.shares_per_level,
                            pnl=pnl)
                grid.fills.append(fill)

                self.events.append(f"{date_str} STOP_LOSS {grid.symbol} @ {exit_px:.2f} "
                                   f"inv={grid.inventory} lots, pnl={pnl:,.0f}")
                grid.inventory = 0
                grid.inventory_cost = 0
                grid.state = StockState.BLOCKED
                grid.stop_count += 1
                grid.blocked_until_idx = date_idx + 60
                return

        # Determine intraday path for fill ordering
        # Bullish (close >= open): assume open → low → high → close
        # Bearish (close < open):  assume open → high → low → close
        if c >= o:
            phases = [("DOWN", l), ("UP", h)]
        else:
            phases = [("UP", h), ("DOWN", l)]

        sell_only = (grid.state == StockState.SELL_ONLY or
                     mkt == MarketState.STRESS)

        for direction, extreme in phases:
            if direction == "DOWN" and not sell_only:
                # Process buys: ref goes down
                while grid.inventory < self.num_levels + 1:
                    buy_px = grid.ref_price - step
                    if buy_px <= 0:
                        break
                    if extreme <= buy_px:
                        grid.inventory += 1
                        grid.inventory_cost += grid.shares_per_level * buy_px
                        grid.ref_price = buy_px
                        grid.total_buys += 1
                        fill = Fill(date=date_str, symbol=grid.symbol, side="BUY",
                                    price=buy_px, shares=grid.shares_per_level)
                        grid.fills.append(fill)
                    else:
                        break

            elif direction == "UP":
                # Process sells: ref goes up
                while grid.inventory > 0:
                    sell_px = grid.ref_price + step
                    if extreme >= sell_px:
                        sell_value = grid.shares_per_level * sell_px
                        cost_per_lot = grid.inventory_cost / grid.inventory if grid.inventory > 0 else 0
                        pnl = sell_value - cost_per_lot
                        grid.realized_pnl += pnl
                        grid.inventory_cost -= cost_per_lot
                        grid.inventory -= 1
                        grid.ref_price = sell_px
                        grid.total_sells += 1
                        grid.grid_cycles += 1
                        fill = Fill(date=date_str, symbol=grid.symbol, side="SELL",
                                    price=sell_px, shares=grid.shares_per_level, pnl=pnl)
                        grid.fills.append(fill)
                    else:
                        break

        # Check UPSIDE_OUT: all seeded inventory sold (ref walked up enough)
        # Triggers when inventory = 0 — stock has absorbed all initial + bought lots via sells
        if grid.inventory <= 0:
            grid.state = StockState.UPSIDE_OUT
            grid.upside_out_date = date_str
            grid.inventory_cost = 0
            self.events.append(f"{date_str} UPSIDE_OUT {grid.symbol} "
                               f"ref={grid.ref_price:.2f} (entry={grid.entry_price:.2f}), "
                               f"realized={grid.realized_pnl:,.0f}")

        # Note: when inventory == num_levels + 1, the buy loop naturally stops.
        # Grid stays ACTIVE so sells keep processing on rallies.

    # ── Re-entry logic for non-ACTIVE states ──

    def _check_reentry(self, grid: StockGrid, close: float, date_str: str,
                       date_idx: int, mkt: MarketState):
        # New entries / re-anchors require NORMAL market
        if mkt not in (MarketState.NORMAL,):
            return

        if grid.state == StockState.BLOCKED:
            if date_idx < grid.blocked_until_idx:
                return
            ts = pd.Timestamp(date_str)
            self._try_replacement_or_reenter(grid, close, date_str, ts, "post-block")

        elif grid.state == StockState.UPSIDE_OUT:
            ts = pd.Timestamp(date_str)
            if self._can_reanchor(grid, close, ts):
                self._try_replacement_or_reenter(grid, close, date_str, ts, "re-anchor")

    def _try_replacement_or_reenter(self, grid: StockGrid, close: float,
                                     date_str: str, ts, reason: str):
        """Try dynamic replacement; fall back to re-entering same stock."""
        replacement = self._find_replacement(ts, grid.symbol)

        if replacement and replacement != grid.symbol:
            # Replace with a different stock
            rep_df = self.daily.get(replacement)
            if rep_df is not None:
                rep_day = rep_df[rep_df["date"] == ts]
                if not rep_day.empty:
                    rep_close = float(rep_day.iloc[0]["close"])
                    old_sym = grid.symbol
                    info = self.universe_info.get(replacement, {})
                    sector = info.get("sector", "Unknown")

                    new_grid = StockGrid(
                        symbol=replacement, sector=sector,
                        capital=self.capital_per_stock,
                    )
                    self._enter_grid(new_grid, rep_close, date_str, date=ts)
                    self.retired_grids.append(grid)
                    # If replacement stock already has a grid (e.g. BLOCKED), retire it
                    if replacement in self.grids:
                        self.retired_grids.append(self.grids[replacement])
                    self.grids[replacement] = new_grid
                    del self.grids[old_sym]
                    self.replacements += 1
                    self.events.append(
                        f"{date_str} REPLACE {old_sym} → {replacement} @ {rep_close:.2f} "
                        f"({reason}, sector={sector})")
                    return

        # No replacement found or same stock is best — re-enter same stock
        if self._can_enter(grid.symbol, close, ts):
            prev_pnl = grid.realized_pnl
            prev_cycles = grid.grid_cycles
            prev_stops = grid.stop_count
            self._enter_grid(grid, close, date_str, date=ts)
            grid.realized_pnl = prev_pnl
            grid.grid_cycles = prev_cycles
            grid.stop_count = prev_stops
            label = "RE-ANCHOR" if reason == "re-anchor" else "RE-ENTER"
            self.events.append(f"{date_str} {label} {grid.symbol} @ {close:.2f}")

    # ── Universe mode: scan all stocks for open slots ──

    def _scan_universe_for_entries(self, date, date_str: str, mkt: MarketState):
        """Scan all ~50 universe stocks and fill open grid slots with the best candidates."""
        if mkt != MarketState.NORMAL:
            return

        # Rate limiter: reset counter on new ISO week
        week = pd.Timestamp(date).isocalendar()[:2]
        if week != self._current_week:
            self._current_week = week
            self._entries_this_week = 0

        if self._entries_this_week >= self.max_entries_per_week:
            return

        # How many open slots?
        active_count = sum(1 for g in self.grids.values()
                           if g.state in (StockState.ACTIVE, StockState.SELL_ONLY,
                                          StockState.WAITING))
        open_slots = self.max_grids - active_count
        if open_slots <= 0:
            return

        entries_remaining = self.max_entries_per_week - self._entries_this_week
        slots_to_fill = min(open_slots, entries_remaining)

        # Score all universe stocks not currently in a grid
        active_syms = set(self.grids.keys())
        scored = []
        for sym in self.universe_daily:
            if sym in active_syms:
                continue
            score = self._score_for_grid(sym, date)
            if score > 0:
                scored.append((sym, score))

        scored.sort(key=lambda x: x[1], reverse=True)

        for sym, score in scored[:slots_to_fill]:
            df = self.daily.get(sym)
            if df is None:
                continue
            day = df[df["date"] == date]
            if day.empty:
                continue
            close = float(day.iloc[0]["close"])

            info = self.universe_info.get(sym, {})
            sector = info.get("sector", "Unknown")
            new_grid = StockGrid(
                symbol=sym, sector=sector,
                capital=self.capital_per_stock,
            )
            self._enter_grid(new_grid, close, date_str, date=date)
            self.grids[sym] = new_grid
            self._entries_this_week += 1
            self.events.append(
                f"{date_str} UNIVERSE_PICK {sym} @ {close:.2f} "
                f"(score={score:.3f}, sector={sector}, "
                f"slot {len(self.grids)}/{self.max_grids})")

    # ── Sell-only mode for STRESS ──

    def _apply_market_state(self, mkt: MarketState):
        for grid in self.grids.values():
            if grid.state == StockState.ACTIVE and mkt == MarketState.STRESS:
                grid.state = StockState.SELL_ONLY
            elif grid.state == StockState.SELL_ONLY and mkt == MarketState.NORMAL:
                grid.state = StockState.ACTIVE

    # ── Run ──

    def run(self):
        dates = self.trading_dates
        if not dates:
            print("No trading dates!")
            return self

        print(f"Sim: {dates[0]} → {dates[-1]} ({len(dates)} trading days)")
        print(f"Capital: ₹{self.total_capital:,.0f}  Grid: ₹{self.grid_capital:,.0f}  "
              f"Reserve: ₹{self.crash_reserve:,.0f}")
        print(f"Per stock: ₹{self.capital_per_stock:,.0f}  "
              f"Grid: {self.grid_pct*100}% step, {self.runway_pct*100}% runway, "
              f"{self.num_levels} levels")
        print(f"Entry mode: {self.entry_mode}" +
              (f" (max {self.max_entries_per_week}/week)" if self.entry_mode == "universe" else ""))
        if self.entry_mode == "basket":
            print(f"Stocks: {', '.join(self.grids.keys())}")
        else:
            print(f"Starting empty — will scan {len(self.universe_daily)} universe stocks")
        print()

        prev_mkt = MarketState.NORMAL
        for i, date in enumerate(dates):
            date_str = str(date).split(" ")[0] if not isinstance(date, str) else date
            mkt = self.market.update(date)

            if mkt != prev_mkt:
                self.events.append(f"{date_str} MARKET → {mkt.value} "
                                   f"(Nifty={self.market._current:.0f}, "
                                   f"52wH={self.market._high_52w:.0f})")
                self._apply_market_state(mkt)
                prev_mkt = mkt

            for sym, grid in list(self.grids.items()):
                # Skip if this grid was replaced earlier in this day's loop
                if sym not in self.grids or self.grids[sym] is not grid:
                    continue
                df = self.daily.get(sym)
                if df is None:
                    continue
                day = df[df["date"] == date]
                if day.empty:
                    continue
                row = day.iloc[0]
                close = float(row["close"])

                if grid.state == StockState.WAITING:
                    if mkt == MarketState.NORMAL:
                        if self._can_enter(sym, close, date):
                            self._enter_grid(grid, close, date_str, date=date)

                elif grid.state in (StockState.ACTIVE, StockState.SELL_ONLY):
                    self._process_day(grid, row, date_str, i, mkt)

                elif grid.state in (StockState.UPSIDE_OUT, StockState.BLOCKED):
                    if self.entry_mode == "universe":
                        # In universe mode: retire exited grids, free the slot
                        if grid.state == StockState.BLOCKED and i < grid.blocked_until_idx:
                            pass  # still in cooldown
                        else:
                            self.retired_grids.append(grid)
                            del self.grids[sym]
                    else:
                        self._check_reentry(grid, close, date_str, i, mkt)

            # Universe mode: scan for new entries to fill open slots
            if self.entry_mode == "universe":
                self._scan_universe_for_entries(date, date_str, mkt)

            # Daily snapshot (active + retired grids)
            total_realized = 0
            total_unrealized = 0
            for g in list(self.grids.values()) + self.retired_grids:
                total_realized += g.realized_pnl
                if g.inventory > 0:
                    df = self.daily.get(g.symbol)
                    if df is not None:
                        day = df[df["date"] == date]
                        if not day.empty:
                            cpx = float(day.iloc[0]["close"])
                            total_unrealized += (g.inventory * g.shares_per_level * cpx
                                                 - g.inventory_cost)

            self.daily_snapshots.append({
                "date": date,
                "market_state": mkt.value,
                "realized": total_realized,
                "unrealized": total_unrealized,
                "total_pnl": total_realized + total_unrealized,
                "active": sum(1 for g in self.grids.values()
                              if g.state in (StockState.ACTIVE, StockState.SELL_ONLY)),
            })

        self._print_results()
        return self

    # ── Results ──

    def _print_results(self):
        if not self.daily_snapshots:
            return

        final = self.daily_snapshots[-1]
        print("=" * 80)
        print("PERMANENT GRID BACKTEST RESULTS")
        print("=" * 80)

        print(f"\nCapital:     ₹{self.total_capital:,.0f}")
        print(f"Realized:    ₹{final['realized']:,.0f}")
        print(f"Unrealized:  ₹{final['unrealized']:,.0f}")
        print(f"Total PnL:   ₹{final['total_pnl']:,.0f}")
        print(f"Return:      {final['total_pnl']/self.total_capital*100:.2f}%")

        # Drawdown
        pnls = [s["total_pnl"] for s in self.daily_snapshots]
        running_max = []
        mx = float("-inf")
        for p in pnls:
            mx = max(mx, p)
            running_max.append(mx)
        drawdowns = [p - rm for p, rm in zip(pnls, running_max)]
        max_dd = min(drawdowns)
        max_dd_idx = drawdowns.index(max_dd)
        print(f"\nMax Drawdown: ₹{max_dd:,.0f} "
              f"({max_dd/self.total_capital*100:.2f}%) "
              f"on {self.daily_snapshots[max_dd_idx]['date']}")

        peak_pnl = max(pnls)
        peak_idx = pnls.index(peak_pnl)
        print(f"Peak PnL:    ₹{peak_pnl:,.0f} on {self.daily_snapshots[peak_idx]['date']}")

        # Per-stock table (active + retired)
        all_grids_list = list(self.grids.values()) + self.retired_grids
        print(f"\n{'Stock':12s} {'State':11s} {'Entry Px':>9s} {'Fills':>6s} "
              f"{'Cycles':>7s} {'Realized':>12s} {'Unrealized':>12s} {'Total':>12s}")
        print("-" * 90)

        for g in sorted(all_grids_list, key=lambda x: x.symbol):
            n_fills = len(g.fills)
            unr = 0
            if g.inventory > 0:
                df = self.daily.get(g.symbol)
                if df is not None and not df.empty:
                    last_px = float(df.iloc[-1]["close"])
                    unr = g.inventory * g.shares_per_level * last_px - g.inventory_cost

            total = g.realized_pnl + unr
            epx = f"{g.entry_price:.0f}" if g.entry_price > 0 else "-"
            state_label = g.state.value
            if g in self.retired_grids:
                state_label = "REPLACED"

            print(f"{g.symbol:12s} {state_label:11s} {epx:>9s} {n_fills:>6d} "
                  f"{g.grid_cycles:>7d} {g.realized_pnl:>12,.0f} {unr:>12,.0f} "
                  f"{total:>12,.0f}")

        # Aggregate fills
        all_fills = []
        for g in all_grids_list:
            all_fills.extend(g.fills)
        buys = sum(1 for f in all_fills if f.side == "BUY")
        sells = sum(1 for f in all_fills if f.side == "SELL")
        stops = sum(1 for f in all_fills if f.side == "STOP_LOSS")
        cycles = sum(g.grid_cycles for g in all_grids_list)
        print(f"\nTotal fills: {len(all_fills)} ({buys} buys, {sells} sells, {stops} stops)")
        print(f"Grid cycles: {cycles}")
        unique_syms = {g.symbol for g in all_grids_list}
        if self.replacements > 0:
            print(f"Stock replacements: {self.replacements} ({len(unique_syms)} unique stocks used)")
        elif self.entry_mode == "universe":
            print(f"Universe picks: {len(all_grids_list)} grids across {len(unique_syms)} unique stocks")
        if cycles > 0:
            total_cycle_pnl = sum(f.pnl for f in all_fills if f.side == "SELL")
            print(f"Avg PnL/cycle: ₹{total_cycle_pnl/cycles:,.0f}")

        # Market state days
        from collections import Counter
        states = Counter(s["market_state"] for s in self.daily_snapshots)
        print(f"\nMarket days: {dict(states)}")

        # Events
        print(f"\n{'='*80}")
        print(f"KEY EVENTS ({len(self.events)})")
        print(f"{'='*80}")
        for e in self.events:
            print(f"  {e}")

    def get_equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(self.daily_snapshots)


# ── CLI ──

def main():
    parser = argparse.ArgumentParser(description="Permanent Grid Backtest")
    parser.add_argument("--capital", type=float, default=10_000_000,
                        help="Total capital in Rs (default: 1Cr)")
    parser.add_argument("--grid-pct", type=float, default=0.01,
                        help="Grid step %% (default: 0.01 = 1%%)")
    parser.add_argument("--runway", type=float, default=0.25,
                        help="Downside runway %% (default: 0.25 = 25%%)")
    parser.add_argument("--initial-levels", type=int, default=None,
                        help="Fixed initial lots (default: None = support-based pro-rata)")
    parser.add_argument("--data", type=str,
                        default="/root/trading_bot/permanent_grid/data/basket_3y.pkl",
                        help="Path to data pickle")
    parser.add_argument("--start", type=str, default=None, help="Sim start YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None, help="Sim end YYYY-MM-DD")
    parser.add_argument("--entry-mode", type=str, default="basket",
                        choices=["basket", "universe"],
                        help="Entry mode: basket (pre-assign 10) or universe (scan all, staggered)")
    parser.add_argument("--max-entries-per-week", type=int, default=3,
                        help="Max new entries per week in universe mode (default: 3)")
    args = parser.parse_args()

    print(f"Loading data from {args.data}...")
    with open(args.data, "rb") as f:
        data = pickle.load(f)

    basket = data["basket"]
    daily_data = data["daily_data"]
    nifty_df = data["nifty_data"]
    universe = data.get("universe")
    universe_data = data.get("universe_data")

    sim_start = args.start or data.get("start_date")
    sim_end = args.end

    sel_period = data.get("selection_period", "unknown")
    has_universe = universe_data is not None
    print(f"Selection period: {sel_period}")
    print(f"Basket: {len(basket)} stocks")
    print(f"Universe: {len(universe_data) if has_universe else 'N/A'} stocks (dynamic replacement {'ON' if has_universe else 'OFF'})")
    print(f"Nifty data: {len(nifty_df)} days")
    print(f"Sim range: {sim_start or 'auto'} → {sim_end or 'auto'}")
    for item in basket:
        fsym = item["fyers_symbol"]
        if fsym in daily_data:
            df = daily_data[fsym]
            print(f"  {item['symbol']:12s} {len(df)} days  "
                  f"{df['date'].min()} → {df['date'].max()}")
    print()

    bt = PermanentGridBacktest(
        basket=basket, daily_data=daily_data, nifty_df=nifty_df,
        total_capital=args.capital, grid_pct=args.grid_pct,
        runway_pct=args.runway, initial_levels=args.initial_levels,
        sim_start=sim_start, sim_end=sim_end,
        universe=universe, universe_data=universe_data,
        entry_mode=args.entry_mode,
        max_entries_per_week=args.max_entries_per_week,
    )
    bt.run()

    # Save equity curve
    eq = bt.get_equity_curve()
    eq_path = "/root/trading_bot/permanent_grid/data/equity_curve.csv"
    eq.to_csv(eq_path, index=False)
    print(f"\nEquity curve saved to {eq_path}")


if __name__ == "__main__":
    main()
