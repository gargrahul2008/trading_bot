#!/usr/bin/env python3
"""
Permanent Grid Strategy — Realistic 5-Minute Backtest Engine

Processes each 5-minute candle sequentially, no assumed intraday path.
Includes all real transaction costs for India equity delivery (CNC):
  - Brokerage: Rs 20/order flat
  - STT: 0.1% on buy + 0.1% on sell (delivery)
  - Exchange transaction charges: 0.00297% (NSE)
  - SEBI charges: Rs 10/crore (0.0001%)
  - Stamp duty: 0.015% on buy side
  - GST: 18% on (brokerage + exchange + SEBI)

Usage:
    env/bin/python permanent_grid/backtest_5min.py [--grid-pct 0.02] [--runway 0.25]
    env/bin/python permanent_grid/backtest_5min.py --master --start 2024-01-01 --end 2026-05-31
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
from permanent_grid.universe import (
    MASTER_STOCKS, INDEX_CHANGES, NIFTY_SYMBOL,
    get_nifty50_at_date, get_index_changes_in_range,
)


def auto_select_basket(daily_data: dict, universe_syms: set, sim_start: str,
                       n_stocks: int = 10) -> list:
    """Score and select a diversified basket from universe at sim_start.
    Uses pre-start daily data for volatility + correlation scoring."""

    start_ts = pd.Timestamp(sim_start)
    candidates = {}
    for sym in sorted(universe_syms):
        df = daily_data.get(sym)
        if df is None:
            continue
        pre = df[df["date"] < start_ts]
        if len(pre) < 200:
            continue
        rets = pre["close"].pct_change().dropna().tail(200).reset_index(drop=True)
        candidates[sym] = rets

    if len(candidates) < n_stocks:
        print(f"  WARNING: only {len(candidates)} candidates with enough pre-start data")

    ret_df = pd.DataFrame(candidates)
    if ret_df.empty:
        return []

    corr = ret_df.corr(min_periods=100)
    avg_corr = corr.mean()
    vol = ret_df.std() * np.sqrt(252)
    vol = vol.dropna()
    avg_corr = avg_corr.reindex(vol.index)

    vol_norm = (vol - vol.min()) / max(vol.max() - vol.min(), 1e-9)
    corr_norm = (avg_corr - avg_corr.min()) / max(avg_corr.max() - avg_corr.min(), 1e-9)
    score = (vol_norm * 0.6 - corr_norm * 0.4).sort_values(ascending=False)

    basket = []
    basket_sectors = set()

    for sym in score.index:
        if len(basket) >= n_stocks:
            break
        info = MASTER_STOCKS.get(sym, {})
        sector = info.get("sector", "Unknown")
        if sector in basket_sectors:
            continue
        too_corr = any(corr.loc[sym, existing] > 0.50 for existing in basket)
        if too_corr:
            continue
        basket.append(sym)
        basket_sectors.add(sector)

    selected = []
    for sym in basket:
        info = MASTER_STOCKS.get(sym, {})
        selected.append({
            "symbol": sym,
            "fyers_symbol": info.get("fyers", f"NSE:{sym}-EQ"),
            "sector": info.get("sector", "Unknown"),
        })
    return selected


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


# ── Transaction cost model — India equity delivery (CNC) ──

@dataclass
class CostModel:
    brokerage_per_order: float = 20.0
    stt_pct: float = 0.001           # 0.1% both sides for delivery
    exchange_pct: float = 0.0000297  # NSE transaction charge
    sebi_pct: float = 0.000001       # Rs 10 per crore
    stamp_duty_pct: float = 0.00015  # 0.015% on buy side only
    gst_pct: float = 0.18           # 18% on (brokerage + exchange + SEBI)
    slippage_pct: float = 0.0005    # 0.05% slippage per fill

    def buy_cost(self, value: float) -> float:
        brok = self.brokerage_per_order
        stt = value * self.stt_pct
        exch = value * self.exchange_pct
        sebi = value * self.sebi_pct
        stamp = value * self.stamp_duty_pct
        gst = (brok + exch + sebi) * self.gst_pct
        slippage = value * self.slippage_pct
        return brok + stt + exch + sebi + stamp + gst + slippage

    def sell_cost(self, value: float) -> float:
        brok = self.brokerage_per_order
        stt = value * self.stt_pct
        exch = value * self.exchange_pct
        sebi = value * self.sebi_pct
        gst = (brok + exch + sebi) * self.gst_pct
        slippage = value * self.slippage_pct
        return brok + stt + exch + sebi + gst + slippage

    def round_trip_cost(self, value: float) -> float:
        return self.buy_cost(value) + self.sell_cost(value)

    def round_trip_pct(self, value: float) -> float:
        return self.round_trip_cost(value) / value * 100


# ── Data classes ──

@dataclass
class Fill:
    datetime: str       # full datetime string
    date: str           # date only
    symbol: str
    side: str           # BUY / SELL / STOP_LOSS
    price: float
    shares: float
    gross_pnl: float = 0   # gross PnL (sells only, before costs)
    cost: float = 0         # transaction cost for this fill
    net_pnl: float = 0      # net PnL after costs

@dataclass
class StockGrid:
    symbol: str
    sector: str
    state: StockState = StockState.WAITING
    capital: float = 0.0

    entry_price: float = 0.0
    entry_date: str = ""
    grid_step: float = 0.0
    shares_per_level: float = 0.0
    max_levels: int = 25

    ref_price: float = 0.0
    inventory: int = 0
    inventory_cost: float = 0.0

    realized_gross: float = 0.0
    realized_costs: float = 0.0
    realized_net: float = 0.0
    total_buys: int = 0
    total_sells: int = 0
    grid_cycles: int = 0

    blocked_until: str = ""
    stop_count: int = 0
    upside_out_date: str = ""
    range_high: float = 0.0          # top of the runway band (entry*(1+up))
    range_low: float = 0.0           # bottom of the runway band (entry*(1-down))
    reentries: int = 0
    empty_active: bool = False       # reactivated seedless, running with 0 inv until first buy
    interest_paid: float = 0.0       # cumulative MTF financing cost on borrowed funds

    fills: List[Fill] = field(default_factory=list)

    @property
    def avg_cost(self) -> float:
        if self.inventory <= 0:
            return 0
        return self.inventory_cost / (self.inventory * self.shares_per_level)

    @property
    def next_buy_price(self) -> float:
        return self.ref_price - self.grid_step if self.ref_price > 0 else 0

    @property
    def next_sell_price(self) -> float:
        return self.ref_price + self.grid_step if self.ref_price > 0 else 0


# ── Market State Tracker ──

class MarketStateTracker:
    def __init__(self, nifty_5min: pd.DataFrame):
        self.nifty = nifty_5min.sort_values("datetime").reset_index(drop=True)
        self._build_daily_close()
        self.state = MarketState.NORMAL
        self._current = 0.0
        self._high_52w = 0.0

    def _build_daily_close(self):
        df = self.nifty.copy()
        df["date_only"] = df["datetime"].dt.date
        self._daily = df.groupby("date_only").agg(
            close=("close", "last"),
            high=("high", "max"),
        ).reset_index()
        self._daily = self._daily.sort_values("date_only").reset_index(drop=True)

    def update(self, date) -> MarketState:
        if isinstance(date, str):
            date = pd.Timestamp(date).date()
        elif hasattr(date, 'date'):
            date = date.date()

        subset = self._daily[self._daily["date_only"] <= date]
        if len(subset) == 0:
            return self.state

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


# ── Main 5-Min Backtest Engine ──

class PermanentGridBacktest5Min:

    def __init__(self, basket: List[dict], daily_data: Dict[str, pd.DataFrame],
                 data_5min: Dict[str, pd.DataFrame], nifty_5min: pd.DataFrame,
                 total_capital: float = 10_000_000,
                 grid_pct: float = 0.02, runway_pct: float = 0.25,
                 initial_levels: int = None,
                 entry_filter: bool = True,
                 up_runway_pct: float = None, down_runway_pct: float = None,
                 capital_map: dict = None, auto_reenter: bool = False,
                 leverage: float = 1.0, mtf_interest_annual_pct: float = 0.0,
                 sim_start: str = None, sim_end: str = None,
                 universe: List[dict] = None,
                 universe_data: Dict[str, pd.DataFrame] = None,
                 universe_5min: Dict[str, pd.DataFrame] = None,
                 dynamic_replacement: bool = True,
                 entry_mode: str = "basket",
                 max_entries_per_week: int = 3,
                 cost_model: CostModel = None,
                 index_changes: list = None,
                 master_daily: dict = None,
                 master_5min: dict = None):

        self.grid_pct = grid_pct
        self.entry_filter = entry_filter
        # Runway: symmetric (runway_pct) OR asymmetric (up + down).
        # Asymmetric: seed inventory to cover `up_runway_pct` upside immediately,
        # reserve cash to buy `down_runway_pct` of downside. total = up + down.
        if up_runway_pct is not None and down_runway_pct is not None:
            self.asymmetric = True
            self.up_runway_pct = up_runway_pct
            self.down_runway_pct = down_runway_pct
            self.runway_pct = up_runway_pct + down_runway_pct
            self.levels_up = max(1, int(round(up_runway_pct / grid_pct)))
            self.levels_down = max(0, int(round(down_runway_pct / grid_pct)))
            self.num_levels = self.levels_up + self.levels_down
        else:
            self.asymmetric = False
            self.up_runway_pct = runway_pct
            self.down_runway_pct = 0.0
            self.runway_pct = runway_pct
            self.num_levels = int(runway_pct / grid_pct)
            self.levels_up = self.num_levels
            self.levels_down = 0
        self.fixed_initial_levels = initial_levels
        self.auto_reenter = auto_reenter
        # Leverage / MTF: positions sized at capital*leverage; broker funds (1-1/lev)
        # of each position, charged daily at mtf_interest_annual_pct%/yr.
        self.leverage = max(1.0, float(leverage))
        self.mtf_interest_annual_pct = float(mtf_interest_annual_pct)
        self.mtf_daily_rate = self.mtf_interest_annual_pct / 100.0 / 365.0
        # Per-ticker capital (keyed by symbol NAME) overrides the equal split.
        self.capital_map = capital_map or {}
        self.total_capital = total_capital
        self.max_grids = len(basket)
        if self.capital_map:
            self.grid_capital = sum(self.capital_map.values())
            self.crash_reserve = 0.0
            self.capital_per_stock = self.grid_capital / self.max_grids  # fallback for unlisted
        else:
            self.grid_capital = total_capital * 0.80
            self.crash_reserve = total_capital * 0.20
            self.capital_per_stock = self.grid_capital / self.max_grids
        self.dynamic_replacement = dynamic_replacement and universe_data is not None
        self.entry_mode = entry_mode
        self.max_entries_per_week = max_entries_per_week
        self._entries_this_week = 0
        self._current_week = None
        self.cost_model = cost_model or CostModel()

        # Daily data for support detection and DMA calculations
        self.daily = {}
        self.sym_to_fsym = {}
        for item in basket:
            sym = item["symbol"]
            fsym = item["fyers_symbol"]
            self.sym_to_fsym[sym] = fsym
            if fsym in daily_data:
                self.daily[sym] = daily_data[fsym].sort_values("date").reset_index(drop=True)

        # 5-minute data keyed by symbol name
        self.candles = {}
        for item in basket:
            sym = item["symbol"]
            fsym = item["fyers_symbol"]
            if fsym in data_5min:
                self.candles[sym] = data_5min[fsym].sort_values("datetime").reset_index(drop=True)

        # Universe data
        self.universe = universe or []
        self.universe_daily = {}
        self.universe_info = {}
        if universe_data:
            for item in (universe or []):
                sym = item["symbol"]
                fsym = item["fyers_symbol"]
                self.universe_info[sym] = {"fyers_symbol": fsym, "sector": item["sector"]}
                if fsym in universe_data:
                    self.universe_daily[sym] = universe_data[fsym].sort_values("date").reset_index(drop=True)
                    if sym not in self.daily:
                        self.daily[sym] = self.universe_daily[sym]
                if universe_5min and fsym in universe_5min:
                    if sym not in self.candles:
                        self.candles[sym] = universe_5min[fsym].sort_values("datetime").reset_index(drop=True)

        # Build trading date list from 5-min data
        all_dates = set()
        for df in self.candles.values():
            all_dates.update(df["date"].dt.date.tolist())
        all_dates = sorted(all_dates)

        if sim_start:
            ss = pd.Timestamp(sim_start).date()
            all_dates = [d for d in all_dates if d >= ss]
        if sim_end:
            se = pd.Timestamp(sim_end).date()
            all_dates = [d for d in all_dates if d <= se]
        self.trading_dates = all_dates

        # Build per-symbol per-date candle index for fast lookup
        self._candle_idx = {}
        for sym, df in self.candles.items():
            idx = df.groupby(df["date"].dt.date).apply(
                lambda g: g.index.tolist(), include_groups=False).to_dict()
            self._candle_idx[sym] = idx

        # Build grids
        self.grids: Dict[str, StockGrid] = {}
        if self.entry_mode == "basket":
            for item in basket:
                sym = item["symbol"]
                self.grids[sym] = StockGrid(
                    symbol=sym, sector=item["sector"],
                    capital=self.capital_map.get(sym, self.capital_per_stock),
                )

        self.market = MarketStateTracker(nifty_5min)
        self.daily_snapshots = []
        self.events = []
        self.replacements = 0
        self.retired_grids: List[StockGrid] = []

        # Dynamic universe tracking (active when from_master is used)
        self._master_daily = master_daily or {}
        self._master_5min = master_5min or {}
        self._current_universe = set()
        self._pending_changes = []

        if index_changes:
            start_date = str(self.trading_dates[0]) if self.trading_dates else sim_start
            if start_date:
                self._current_universe = get_nifty50_at_date(str(start_date))
            for date_str, added, removed in index_changes:
                change_date = pd.Timestamp(date_str).date()
                if self.trading_dates and change_date > self.trading_dates[0]:
                    if sim_end:
                        end_check = pd.Timestamp(sim_end).date()
                        if change_date > end_check:
                            continue
                    self._pending_changes.append((change_date, added, removed))
            self._pending_changes.sort()

    # ── Class method: create from master data ──

    @classmethod
    def from_master(cls, master_pkl: str, sim_start: str, sim_end: str,
                    total_capital: float = 10_000_000,
                    grid_pct: float = 0.02, runway_pct: float = 0.25,
                    initial_levels: int = None,
                    entry_mode: str = "basket",
                    max_entries_per_week: int = 3,
                    cost_model: CostModel = None,
                    n_basket: int = 10):
        """Create backtest from master_data.pkl with auto basket selection."""

        with open(master_pkl, "rb") as f:
            master = pickle.load(f)

        all_daily = master["daily"]       # keyed by symbol name
        all_5min = master["min5"]         # keyed by symbol name
        nifty_5min = master["nifty_5min"]

        # Universe at sim start
        universe_syms = get_nifty50_at_date(sim_start)
        print(f"Universe at {sim_start}: {len(universe_syms)} stocks")

        # Auto-select basket
        basket = auto_select_basket(all_daily, universe_syms, sim_start, n_stocks=n_basket)
        print(f"Auto-selected basket: {[b['symbol'] for b in basket]}")

        # Build universe list (all stocks that could ever appear in the sim range)
        all_period_syms = set(universe_syms)
        changes = get_index_changes_in_range(sim_start, sim_end)
        for _, added, removed in changes:
            all_period_syms.update(added)
            all_period_syms.update(removed)

        universe = []
        for sym in sorted(all_period_syms):
            info = MASTER_STOCKS.get(sym, {})
            if info:
                universe.append({
                    "symbol": sym,
                    "fyers_symbol": info.get("fyers", f"NSE:{sym}-EQ"),
                    "sector": info.get("sector", "Unknown"),
                })

        # Build data dicts keyed by fyers symbol (constructor expects this)
        daily_data = {}
        data_5min_dict = {}
        universe_data = {}
        universe_5min = {}

        for sym in all_period_syms:
            info = MASTER_STOCKS.get(sym, {})
            fsym = info.get("fyers", f"NSE:{sym}-EQ")
            if sym in all_daily:
                daily_data[fsym] = all_daily[sym]
                universe_data[fsym] = all_daily[sym]
            if sym in all_5min:
                data_5min_dict[fsym] = all_5min[sym]
                universe_5min[fsym] = all_5min[sym]

        return cls(
            basket=basket, daily_data=daily_data,
            data_5min=data_5min_dict, nifty_5min=nifty_5min,
            total_capital=total_capital, grid_pct=grid_pct,
            runway_pct=runway_pct, initial_levels=initial_levels,
            sim_start=sim_start, sim_end=sim_end,
            universe=universe, universe_data=universe_data,
            universe_5min=universe_5min,
            dynamic_replacement=True,
            entry_mode=entry_mode,
            max_entries_per_week=max_entries_per_week,
            cost_model=cost_model,
            index_changes=INDEX_CHANGES,
            master_daily=all_daily,
            master_5min=all_5min,
        )

    # ── Index change handling ──

    def _apply_index_change(self, date, date_str, added_syms, removed_syms):
        """Update universe when Nifty50 composition changes mid-simulation."""
        for sym in removed_syms:
            self._current_universe.discard(sym)
            if sym in self.grids and self.grids[sym].state == StockState.ACTIVE:
                self.events.append(
                    f"{date_str} INDEX_REMOVED {sym} — will replace on next exit")

        for sym in added_syms:
            self._current_universe.add(sym)
            info = MASTER_STOCKS.get(sym, {})
            fsym = info.get("fyers", f"NSE:{sym}-EQ")
            sector = info.get("sector", "Unknown")
            if sym not in self.universe_info:
                self.universe_info[sym] = {"fyers_symbol": fsym, "sector": sector}
            if sym in self._master_daily and sym not in self.daily:
                self.daily[sym] = self._master_daily[sym].sort_values("date").reset_index(drop=True)
                self.universe_daily[sym] = self.daily[sym]
            if sym in self._master_5min and sym not in self.candles:
                df = self._master_5min[sym].sort_values("datetime").reset_index(drop=True)
                self.candles[sym] = df
                self._candle_idx[sym] = df.groupby(df["date"].dt.date).apply(
                    lambda g: g.index.tolist(), include_groups=False).to_dict()

        self.events.append(
            f"{date_str} INDEX_REBALANCE: +{','.join(added_syms)} "
            f"-{','.join(removed_syms)} "
            f"(universe now {len(self._current_universe)} stocks)")

    # ── Indicator helpers (use daily data) ──

    def _dma(self, sym: str, date, period: int) -> Optional[float]:
        df = self.daily.get(sym)
        if df is None:
            return None
        subset = df[df["date"] <= pd.Timestamp(date)]
        if len(subset) < period:
            return None
        return float(subset.iloc[-period:]["close"].mean())

    def _range_52w(self, sym: str, date) -> Tuple[float, float]:
        df = self.daily.get(sym)
        if df is None:
            return (0, 0)
        subset = df[df["date"] <= pd.Timestamp(date)]
        n = min(252, len(subset))
        recent = subset.iloc[-n:]
        return (float(recent["low"].min()), float(recent["high"].max()))

    def _find_support(self, sym: str, date) -> Optional[float]:
        df = self.daily.get(sym)
        if df is None:
            return None
        subset = df[df["date"] <= pd.Timestamp(date)].copy()
        if len(subset) < 60:
            return None
        swing_lows = find_swing_lows(subset, window=10, min_bounce_pct=3.0)
        zones = cluster_supports(swing_lows, cluster_pct=3.0)
        close = float(subset.iloc[-1]["close"])
        nearest = find_nearest_support(zones, close, max_distance_pct=25.0)
        return nearest.price if nearest else None

    def _compute_initial_levels(self, entry_price: float, support_price: float) -> int:
        if entry_price <= support_price:
            return self.num_levels
        pct_above = (entry_price / support_price - 1)
        levels_below = min(int(pct_above / self.grid_pct), self.num_levels - 1)
        levels_above = self.num_levels - levels_below
        return max(levels_above, 1)

    # ── Entry checks ──

    def _can_enter(self, sym: str, close: float, date) -> bool:
        if self._current_universe and sym not in self._current_universe:
            return False
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

        support = self._find_support(sym, date)
        if support is None:
            return False
        return True

    def _can_reanchor(self, grid: StockGrid, close: float, date) -> bool:
        dma50 = self._dma(grid.symbol, date, 50)
        if dma50 and abs(close / dma50 - 1) <= 0.05:
            return True
        if grid.ref_price > 0 and close <= grid.ref_price * 0.97:
            return True
        return False

    # ── Dynamic replacement scoring ──

    def _score_for_grid(self, sym: str, date) -> float:
        df = self.daily.get(sym)
        if df is None:
            return -999
        subset = df[df["date"] <= pd.Timestamp(date)]
        if len(subset) < 200:
            return -999
        close = float(subset.iloc[-1]["close"])
        if not self._can_enter(sym, close, date):
            return -999

        rets = subset["close"].pct_change().dropna().tail(200)
        vol = float(rets.std() * np.sqrt(252))

        support = self._find_support(sym, date)
        if support is None:
            return -999

        active_sectors = set()
        for s, g in self.grids.items():
            if g.state in (StockState.ACTIVE, StockState.SELL_ONLY):
                active_sectors.add(g.sector)
        info = self.universe_info.get(sym, {})
        sector_penalty = 0.5 if info.get("sector") in active_sectors else 1.0

        return vol * sector_penalty

    def _find_replacement(self, date, exiting_sym: str) -> Optional[str]:
        if not self.dynamic_replacement:
            return None
        active_syms = {s for s, g in self.grids.items()
                       if g.state in (StockState.ACTIVE, StockState.SELL_ONLY,
                                      StockState.WAITING)}
        best_sym, best_score = None, -999
        candidates = set(self.universe_daily.keys()) | {exiting_sym}
        for sym in candidates:
            if sym in active_syms:
                continue
            score = self._score_for_grid(sym, date)
            if score > best_score:
                best_score = score
                best_sym = sym
        return best_sym

    # ── Grid entry ──

    def _enter_grid(self, grid: StockGrid, price: float, dt_str: str, date_str: str):
        grid.entry_price = price
        grid.entry_date = date_str
        grid.grid_step = price * self.grid_pct
        grid.shares_per_level = grid.capital * self.leverage / (price * self.num_levels)
        grid.ref_price = price
        grid.state = StockState.ACTIVE
        # Runway band — used by auto_reenter to detect "back in range".
        down = self.down_runway_pct if self.asymmetric else self.runway_pct
        grid.range_high = price * (1 + self.up_runway_pct)
        grid.range_low = price * (1 - down)

        if self.asymmetric:
            # Deploy up-runway into stock now; reserve down-runway as cash.
            seed = self.levels_up
            support_px = price * (1 - self.down_runway_pct)   # for the log line only
        elif self.fixed_initial_levels is not None:
            seed = self.fixed_initial_levels
            support_px = 0
        else:
            support_px = self._find_support(grid.symbol, pd.Timestamp(date_str))
            if support_px is None:
                support_px = price * (1 - self.runway_pct)
            seed = self._compute_initial_levels(price, support_px)

        seed = min(seed, self.num_levels)
        trade_value = seed * grid.shares_per_level * price
        entry_cost = self.cost_model.buy_cost(trade_value)

        grid.inventory = seed
        grid.inventory_cost = trade_value + entry_cost
        grid.realized_costs += entry_cost
        grid.total_buys += seed

        levels_below = self.num_levels - seed
        fill = Fill(datetime=dt_str, date=date_str, symbol=grid.symbol, side="BUY",
                    price=price, shares=seed * grid.shares_per_level,
                    cost=entry_cost)
        grid.fills.append(fill)
        self.events.append(f"{date_str} ENTER {grid.symbol} @ {price:.2f} "
                           f"(support={support_px:.0f}, {seed}↑/{levels_below}↓, "
                           f"{grid.shares_per_level:.1f} sh/lvl, cost=₹{entry_cost:.0f})")

    # ── Core: process one 5-min candle ──

    def _process_candle(self, grid: StockGrid, candle_row, dt_str: str,
                        date_str: str, mkt: MarketState):
        if grid.state not in (StockState.ACTIVE, StockState.SELL_ONLY):
            return

        h = float(candle_row["high"])
        l = float(candle_row["low"])
        step = grid.grid_step

        # Stop-loss check
        stop_price = grid.entry_price * 0.65
        if mkt not in (MarketState.STRESS, MarketState.CRASH_DEPLOY):
            if l <= stop_price and grid.inventory > 0:
                exit_px = stop_price
                exit_value = grid.inventory * grid.shares_per_level * exit_px
                sell_cost = self.cost_model.sell_cost(exit_value)
                gross_pnl = exit_value - grid.inventory_cost
                net_pnl = gross_pnl - sell_cost

                grid.realized_gross += gross_pnl
                grid.realized_costs += sell_cost
                grid.realized_net += net_pnl

                fill = Fill(datetime=dt_str, date=date_str, symbol=grid.symbol,
                            side="STOP_LOSS", price=exit_px,
                            shares=grid.inventory * grid.shares_per_level,
                            gross_pnl=gross_pnl, cost=sell_cost, net_pnl=net_pnl)
                grid.fills.append(fill)

                self.events.append(f"{date_str} STOP_LOSS {grid.symbol} @ {exit_px:.2f} "
                                   f"inv={grid.inventory}, gross={gross_pnl:,.0f}, "
                                   f"cost={sell_cost:,.0f}, net={net_pnl:,.0f}")
                grid.inventory = 0
                grid.inventory_cost = 0
                grid.state = StockState.BLOCKED
                grid.stop_count += 1
                grid.blocked_until = str(pd.Timestamp(date_str) + pd.Timedelta(days=60))[:10]
                return

        sell_only = (grid.state == StockState.SELL_ONLY or
                     mkt == MarketState.STRESS)

        # Buy check: low crossed below next buy level
        if not sell_only and grid.inventory < self.num_levels + 1:
            buy_px = grid.ref_price - step
            if buy_px > 0 and l <= buy_px:
                # Fill at the grid level price (limit order would be placed there)
                fill_px = buy_px
                trade_val = grid.shares_per_level * fill_px
                buy_cost = self.cost_model.buy_cost(trade_val)

                grid.inventory += 1
                grid.inventory_cost += trade_val + buy_cost
                grid.ref_price = fill_px
                grid.total_buys += 1
                grid.realized_costs += buy_cost
                grid.empty_active = False   # first buy after seedless reactivation

                fill = Fill(datetime=dt_str, date=date_str, symbol=grid.symbol,
                            side="BUY", price=fill_px,
                            shares=grid.shares_per_level, cost=buy_cost)
                grid.fills.append(fill)

        # Sell check: high crossed above next sell level
        if grid.inventory > 0:
            sell_px = grid.ref_price + step
            if h >= sell_px:
                fill_px = sell_px
                trade_val = grid.shares_per_level * fill_px
                sell_cost = self.cost_model.sell_cost(trade_val)

                cost_per_lot = grid.inventory_cost / grid.inventory if grid.inventory > 0 else 0
                gross_pnl = trade_val - cost_per_lot
                net_pnl = gross_pnl - sell_cost

                grid.realized_gross += gross_pnl
                grid.realized_costs += sell_cost
                grid.realized_net += net_pnl
                grid.inventory_cost -= cost_per_lot
                grid.inventory -= 1
                grid.ref_price = fill_px
                grid.total_sells += 1
                grid.grid_cycles += 1

                fill = Fill(datetime=dt_str, date=date_str, symbol=grid.symbol,
                            side="SELL", price=fill_px,
                            shares=grid.shares_per_level,
                            gross_pnl=gross_pnl, cost=sell_cost, net_pnl=net_pnl)
                grid.fills.append(fill)

        # UPSIDE_OUT check — skip while a seedless-reactivated grid waits for its first buy
        if grid.inventory <= 0 and grid.state == StockState.ACTIVE and not grid.empty_active:
            grid.state = StockState.UPSIDE_OUT
            grid.upside_out_date = date_str
            grid.inventory_cost = 0
            self.events.append(f"{date_str} UPSIDE_OUT {grid.symbol} "
                               f"ref={grid.ref_price:.2f} (entry={grid.entry_price:.2f}), "
                               f"gross={grid.realized_gross:,.0f}, "
                               f"costs={grid.realized_costs:,.0f}, "
                               f"net={grid.realized_net:,.0f}")

    # ── Re-entry / replacement logic ──

    def _auto_reenter(self, grid: StockGrid, close: float, date_str: str):
        """Seedless reactivation: when an UPSIDE_OUT / BLOCKED grid's price is back
        inside its runway band, just resume the grid as a normal grid — NO seed buy,
        NO new capital. Inventory stays 0 and rebuilds organically from buys on dips.
        Re-anchors ref at the current price; keeps original lot size & step."""
        if not (grid.range_low <= close <= grid.range_high):
            return
        grid.state = StockState.ACTIVE
        grid.ref_price = close                 # re-anchor so it doesn't cascade-buy
        grid.empty_active = True               # don't re-trigger UPSIDE_OUT before first buy
        grid.blocked_until = ""                # clear the stop-loss block
        # recompute the band around the new anchor for the next out/breakdown
        down = self.down_runway_pct if self.asymmetric else self.runway_pct
        grid.range_high = close * (1 + self.up_runway_pct)
        grid.range_low = close * (1 - down)
        grid.reentries += 1
        self.events.append(f"{date_str} REACTIVATE {grid.symbol} @ {close:.2f} "
                           f"(#{grid.reentries}, seedless, inv=0)")

    def _check_reentry(self, grid: StockGrid, close: float, date_str: str, mkt: MarketState):
        if mkt != MarketState.NORMAL:
            return

        if grid.state == StockState.BLOCKED:
            if date_str < grid.blocked_until:
                return
            ts = pd.Timestamp(date_str)
            self._try_replacement_or_reenter(grid, close, date_str, ts, "post-block")

        elif grid.state == StockState.UPSIDE_OUT:
            ts = pd.Timestamp(date_str)
            if self._can_reanchor(grid, close, ts):
                self._try_replacement_or_reenter(grid, close, date_str, ts, "re-anchor")

    def _try_replacement_or_reenter(self, grid, close, date_str, ts, reason):
        replacement = self._find_replacement(ts, grid.symbol)

        if replacement and replacement != grid.symbol:
            # Get the replacement stock's close from daily data
            rep_df = self.daily.get(replacement)
            if rep_df is not None:
                rep_day = rep_df[rep_df["date"] <= ts]
                if not rep_day.empty:
                    rep_close = float(rep_day.iloc[-1]["close"])
                    old_sym = grid.symbol
                    info = self.universe_info.get(replacement, {})
                    sector = info.get("sector", "Unknown")

                    dt_str = date_str + " 09:15:00"
                    new_grid = StockGrid(
                        symbol=replacement, sector=sector,
                        capital=self.capital_per_stock,
                    )
                    self._enter_grid(new_grid, rep_close, dt_str, date_str)
                    self.retired_grids.append(grid)
                    if replacement in self.grids:
                        self.retired_grids.append(self.grids[replacement])
                    self.grids[replacement] = new_grid
                    del self.grids[old_sym]
                    self.replacements += 1
                    self.events.append(
                        f"{date_str} REPLACE {old_sym} → {replacement} @ {rep_close:.2f} "
                        f"({reason}, sector={sector})")
                    return

        if self._can_enter(grid.symbol, close, ts):
            prev_gross = grid.realized_gross
            prev_costs = grid.realized_costs
            prev_net = grid.realized_net
            prev_cycles = grid.grid_cycles
            prev_stops = grid.stop_count
            dt_str = date_str + " 09:15:00"
            self._enter_grid(grid, close, dt_str, date_str)
            grid.realized_gross = prev_gross
            grid.realized_costs = prev_costs
            grid.realized_net = prev_net
            grid.grid_cycles = prev_cycles
            grid.stop_count = prev_stops
            label = "RE-ANCHOR" if reason == "re-anchor" else "RE-ENTER"
            self.events.append(f"{date_str} {label} {grid.symbol} @ {close:.2f}")

    # ── Universe scanning ──

    def _scan_universe_for_entries(self, date_str: str, mkt: MarketState):
        if mkt != MarketState.NORMAL:
            return

        week = pd.Timestamp(date_str).isocalendar()[:2]
        if week != self._current_week:
            self._current_week = week
            self._entries_this_week = 0

        if self._entries_this_week >= self.max_entries_per_week:
            return

        active_count = sum(1 for g in self.grids.values()
                           if g.state in (StockState.ACTIVE, StockState.SELL_ONLY,
                                          StockState.WAITING))
        open_slots = self.max_grids - active_count
        if open_slots <= 0:
            return

        entries_remaining = self.max_entries_per_week - self._entries_this_week
        slots_to_fill = min(open_slots, entries_remaining)

        active_syms = set(self.grids.keys())
        scored = []
        for sym in self.universe_daily:
            if sym in active_syms:
                continue
            score = self._score_for_grid(sym, date_str)
            if score > 0:
                scored.append((sym, score))

        scored.sort(key=lambda x: x[1], reverse=True)

        for sym, score in scored[:slots_to_fill]:
            df = self.daily.get(sym)
            if df is None:
                continue
            day = df[df["date"] <= pd.Timestamp(date_str)]
            if day.empty:
                continue
            close = float(day.iloc[-1]["close"])

            info = self.universe_info.get(sym, {})
            sector = info.get("sector", "Unknown")
            new_grid = StockGrid(symbol=sym, sector=sector, capital=self.capital_per_stock)
            dt_str = date_str + " 09:15:00"
            self._enter_grid(new_grid, close, dt_str, date_str)
            self.grids[sym] = new_grid
            self._entries_this_week += 1
            self.events.append(
                f"{date_str} UNIVERSE_PICK {sym} @ {close:.2f} "
                f"(score={score:.3f}, sector={sector}, "
                f"slot {len(self.grids)}/{self.max_grids})")

    # ── Market state effects ──

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
        if self.asymmetric:
            runway_desc = (f"{self.up_runway_pct*100:g}% up ({self.levels_up} lvl) / "
                           f"{self.down_runway_pct*100:g}% down ({self.levels_down} lvl)")
        else:
            runway_desc = f"{self.runway_pct*100:g}% runway"
        lev_desc = (f" | leverage={self.leverage:g}X @ {self.mtf_interest_annual_pct:g}%/yr"
                    if self.leverage > 1 else " | leverage=1X (cash)")
        print(f"Per stock: ₹{self.capital_per_stock:,.0f}  "
              f"Grid: {self.grid_pct*100:g}% step, {runway_desc}, "
              f"{self.num_levels} levels | entry_filter={'ON' if self.entry_filter else 'OFF'}"
              f"{lev_desc}")
        print(f"Entry mode: {self.entry_mode}"
              + (f" (max {self.max_entries_per_week}/week)" if self.entry_mode == "universe" else ""))
        print(f"Cost model: brokerage=₹{self.cost_model.brokerage_per_order}/order, "
              f"STT={self.cost_model.stt_pct*100}%, "
              f"slippage={self.cost_model.slippage_pct*100}%")
        if self.entry_mode == "basket":
            print(f"Stocks: {', '.join(self.grids.keys())}")
        else:
            print(f"Starting empty — scanning {len(self.universe_daily)} universe stocks")
        print()

        prev_mkt = MarketState.NORMAL
        total_candles = 0

        for di, date in enumerate(dates):
            date_str = str(date)
            mkt = self.market.update(date)

            # Check for index rebalancing
            while self._pending_changes and date >= self._pending_changes[0][0]:
                change_date, added, removed = self._pending_changes.pop(0)
                self._apply_index_change(date, date_str, added, removed)

            if mkt != prev_mkt:
                self.events.append(f"{date_str} MARKET → {mkt.value} "
                                   f"(Nifty={self.market._current:.0f}, "
                                   f"52wH={self.market._high_52w:.0f})")
                self._apply_market_state(mkt)
                prev_mkt = mkt

            # Process each 5-min candle for active grids
            for sym, grid in list(self.grids.items()):
                if sym not in self.grids or self.grids[sym] is not grid:
                    continue

                candle_indices = self._candle_idx.get(sym, {}).get(date, [])
                if not candle_indices:
                    continue

                df = self.candles[sym]

                if grid.state == StockState.WAITING:
                    # entry_filter OFF → enter immediately on first available candle,
                    # bypassing market-regime + _can_enter (DMA/52w/support) gates.
                    if not self.entry_filter:
                        first_candle = df.iloc[candle_indices[0]]
                        open_px = float(first_candle["open"])
                        dt_str = str(first_candle["datetime"])
                        self._enter_grid(grid, open_px, dt_str, date_str)
                    elif mkt == MarketState.NORMAL:
                        last_close = float(df.iloc[candle_indices[-1]]["close"])
                        if self._can_enter(sym, last_close, date):
                            first_candle = df.iloc[candle_indices[0]]
                            open_px = float(first_candle["open"])
                            dt_str = str(first_candle["datetime"])
                            self._enter_grid(grid, open_px, dt_str, date_str)

                elif grid.state in (StockState.ACTIVE, StockState.SELL_ONLY):
                    for ci in candle_indices:
                        if grid.state not in (StockState.ACTIVE, StockState.SELL_ONLY):
                            break
                        candle = df.iloc[ci]
                        dt_str = str(candle["datetime"])
                        self._process_candle(grid, candle, dt_str, date_str, mkt)
                        total_candles += 1

                elif grid.state in (StockState.UPSIDE_OUT, StockState.BLOCKED):
                    last_close = float(df.iloc[candle_indices[-1]]["close"])
                    if self.auto_reenter:
                        # auto re-seed when price is back in the runway band (no filters)
                        self._auto_reenter(grid, last_close, date_str)
                    elif self.entry_mode == "universe":
                        if grid.state == StockState.BLOCKED and date_str < grid.blocked_until:
                            pass
                        else:
                            self.retired_grids.append(grid)
                            del self.grids[sym]
                    else:
                        self._check_reentry(grid, last_close, date_str, mkt)

            # Universe mode: scan for new entries at end of day
            if self.entry_mode == "universe":
                self._scan_universe_for_entries(date_str, mkt)

            # MTF financing: charge daily interest on borrowed funds (broker-funded
            # fraction = inventory_cost*(1-1/leverage)) for the calendar days elapsed.
            if self.leverage > 1.0 and self.mtf_daily_rate > 0:
                days_elapsed = (date - dates[di - 1]).days if di > 0 else 1
                if days_elapsed > 0:
                    funded_frac = 1.0 - 1.0 / self.leverage
                    for g in list(self.grids.values()) + self.retired_grids:
                        if g.inventory_cost > 0:
                            borrowed = g.inventory_cost * funded_frac
                            interest = borrowed * self.mtf_daily_rate * days_elapsed
                            g.interest_paid += interest
                            g.realized_costs += interest   # keeps net = gross - costs
                            g.realized_net -= interest

            # Daily snapshot
            total_realized_gross = 0
            total_realized_costs = 0
            total_realized_net = 0
            total_unrealized = 0
            for g in list(self.grids.values()) + self.retired_grids:
                total_realized_gross += g.realized_gross
                total_realized_costs += g.realized_costs
                total_realized_net += g.realized_net
                if g.inventory > 0:
                    sym_candles = self._candle_idx.get(g.symbol, {}).get(date, [])
                    if sym_candles:
                        last_px = float(self.candles[g.symbol].iloc[sym_candles[-1]]["close"])
                        unr = g.inventory * g.shares_per_level * last_px - g.inventory_cost
                        total_unrealized += unr

            self.daily_snapshots.append({
                "date": date,
                "market_state": mkt.value,
                "realized_gross": total_realized_gross,
                "realized_costs": total_realized_costs,
                "realized_net": total_realized_net,
                "unrealized": total_unrealized,
                "total_pnl_gross": total_realized_gross + total_unrealized,
                "total_pnl_net": total_realized_net + total_unrealized,
                "active": sum(1 for g in self.grids.values()
                              if g.state in (StockState.ACTIVE, StockState.SELL_ONLY)),
            })

            if (di + 1) % 100 == 0:
                snap = self.daily_snapshots[-1]
                print(f"  Day {di+1}/{len(dates)}: {date_str}  "
                      f"gross=₹{snap['total_pnl_gross']:,.0f}  "
                      f"costs=₹{total_realized_costs:,.0f}  "
                      f"net=₹{snap['total_pnl_net']:,.0f}  "
                      f"active={snap['active']}")

        print(f"\nProcessed {total_candles:,} candles across {len(dates)} days")
        self._print_results()
        return self

    # ── Results ──

    def _print_results(self):
        if not self.daily_snapshots:
            return

        final = self.daily_snapshots[-1]
        print("=" * 90)
        print("5-MINUTE BACKTEST RESULTS (with real transaction costs)")
        print("=" * 90)

        print(f"\nCapital:         ₹{self.total_capital:,.0f}")
        print(f"Realized Gross:  ₹{final['realized_gross']:,.0f}")
        print(f"Total Costs:     ₹{final['realized_costs']:,.0f}")
        print(f"Realized Net:    ₹{final['realized_net']:,.0f}")
        print(f"Unrealized:      ₹{final['unrealized']:,.0f}")
        print(f"Total PnL Gross: ₹{final['total_pnl_gross']:,.0f} "
              f"({final['total_pnl_gross']/self.total_capital*100:.2f}%)")
        print(f"Total PnL Net:   ₹{final['total_pnl_net']:,.0f} "
              f"({final['total_pnl_net']/self.total_capital*100:.2f}%)")

        # Drawdown (on net PnL)
        pnls = [s["total_pnl_net"] for s in self.daily_snapshots]
        running_max = []
        mx = float("-inf")
        for p in pnls:
            mx = max(mx, p)
            running_max.append(mx)
        drawdowns = [p - rm for p, rm in zip(pnls, running_max)]
        max_dd = min(drawdowns)
        max_dd_idx = drawdowns.index(max_dd)
        print(f"\nMax Drawdown:    ₹{max_dd:,.0f} "
              f"({max_dd/self.total_capital*100:.2f}%) "
              f"on {self.daily_snapshots[max_dd_idx]['date']}")

        peak_pnl = max(pnls)
        peak_idx = pnls.index(peak_pnl)
        print(f"Peak Net PnL:    ₹{peak_pnl:,.0f} on {self.daily_snapshots[peak_idx]['date']}")

        # Per-stock table
        all_grids = list(self.grids.values()) + self.retired_grids
        print(f"\n{'Stock':12s} {'State':11s} {'Entry':>9s} {'Fills':>6s} "
              f"{'Cycles':>7s} {'Gross':>12s} {'Costs':>10s} {'Net':>12s} {'Unreal':>10s}")
        print("-" * 100)

        for g in sorted(all_grids, key=lambda x: x.realized_net, reverse=True):
            n_fills = len(g.fills)
            unr = 0
            if g.inventory > 0 and self.daily_snapshots:
                last_date = self.daily_snapshots[-1]["date"]
                sym_candles = self._candle_idx.get(g.symbol, {}).get(last_date, [])
                if sym_candles:
                    last_px = float(self.candles[g.symbol].iloc[sym_candles[-1]]["close"])
                    unr = g.inventory * g.shares_per_level * last_px - g.inventory_cost

            state_label = g.state.value
            if g in self.retired_grids:
                state_label = "REPLACED"
            epx = f"{g.entry_price:.0f}" if g.entry_price > 0 else "-"

            print(f"{g.symbol:12s} {state_label:11s} {epx:>9s} {n_fills:>6d} "
                  f"{g.grid_cycles:>7d} {g.realized_gross:>12,.0f} "
                  f"{g.realized_costs:>10,.0f} {g.realized_net:>12,.0f} {unr:>10,.0f}")

        # Aggregate
        all_fills = []
        for g in all_grids:
            all_fills.extend(g.fills)
        buys = sum(1 for f in all_fills if f.side == "BUY")
        sells = sum(1 for f in all_fills if f.side == "SELL")
        stops = sum(1 for f in all_fills if f.side == "STOP_LOSS")
        cycles = sum(g.grid_cycles for g in all_grids)
        total_costs = sum(g.realized_costs for g in all_grids)

        print(f"\nTotal fills: {len(all_fills)} ({buys} buys, {sells} sells, {stops} stops)")
        print(f"Grid cycles: {cycles}")
        print(f"Total transaction costs: ₹{total_costs:,.0f}")
        if cycles > 0:
            avg_gross = sum(f.gross_pnl for f in all_fills if f.side == "SELL") / cycles
            avg_cost = total_costs / (buys + sells + stops) if (buys + sells + stops) > 0 else 0
            print(f"Avg gross PnL/cycle: ₹{avg_gross:,.0f}")
            print(f"Avg cost/fill: ₹{avg_cost:,.0f}")

        unique_syms = {g.symbol for g in all_grids}
        if self.replacements > 0:
            print(f"Stock replacements: {self.replacements} ({len(unique_syms)} unique stocks)")
        elif self.entry_mode == "universe":
            print(f"Universe picks: {len(all_grids)} grids across {len(unique_syms)} unique stocks")

        from collections import Counter
        states = Counter(s["market_state"] for s in self.daily_snapshots)
        print(f"\nMarket days: {dict(states)}")

        print(f"\n{'='*90}")
        print(f"KEY EVENTS ({len(self.events)})")
        print(f"{'='*90}")
        for e in self.events:
            print(f"  {e}")

    def get_equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(self.daily_snapshots)


# ── CLI ──

def main():
    parser = argparse.ArgumentParser(description="5-Min Permanent Grid Backtest")
    parser.add_argument("--capital", type=float, default=10_000_000)
    parser.add_argument("--grid-pct", type=float, default=0.02)
    parser.add_argument("--runway", type=float, default=0.25)
    parser.add_argument("--initial-levels", type=int, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--entry-mode", type=str, default="basket",
                        choices=["basket", "universe"])
    parser.add_argument("--max-entries-per-week", type=int, default=3)
    parser.add_argument("--no-costs", action="store_true", help="Disable transaction costs")
    parser.add_argument("--no-slippage", action="store_true", help="Disable slippage")
    parser.add_argument("--master", action="store_true",
                        help="Use master_data.pkl with auto basket selection + dynamic universe")
    args = parser.parse_args()

    cost_model = CostModel()
    if args.no_costs:
        cost_model = CostModel(brokerage_per_order=0, stt_pct=0, exchange_pct=0,
                               sebi_pct=0, stamp_duty_pct=0, gst_pct=0,
                               slippage_pct=0 if args.no_slippage else 0.0005)
    elif args.no_slippage:
        cost_model.slippage_pct = 0

    if args.master:
        # ── New mode: master data with auto basket selection ──
        master_pkl = "/root/trading_bot/permanent_grid/data/master_data.pkl"
        if not os.path.exists(master_pkl):
            print(f"ERROR: {master_pkl} not found. Run fetch_master_data.py first.")
            return
        if not args.start or not args.end:
            print("ERROR: --start and --end are required with --master mode")
            return

        print(f"Loading master data from {master_pkl}...")
        bt = PermanentGridBacktest5Min.from_master(
            master_pkl=master_pkl,
            sim_start=args.start, sim_end=args.end,
            total_capital=args.capital, grid_pct=args.grid_pct,
            runway_pct=args.runway, initial_levels=args.initial_levels,
            entry_mode=args.entry_mode,
            max_entries_per_week=args.max_entries_per_week,
            cost_model=cost_model,
        )
    else:
        # ── Legacy mode: separate basket + 5min pickle files ──
        basket_pkl = "/root/trading_bot/permanent_grid/data/basket_3y.pkl"
        min5_pkl = "/root/trading_bot/permanent_grid/data/universe_5min.pkl"

        print(f"Loading daily data from {basket_pkl}...")
        with open(basket_pkl, "rb") as f:
            basket_data = pickle.load(f)

        print(f"Loading 5-min data from {min5_pkl}...")
        with open(min5_pkl, "rb") as f:
            min5_data = pickle.load(f)

        basket = basket_data["basket"]
        daily_data = basket_data["daily_data"]
        universe = basket_data.get("universe")
        universe_data = basket_data.get("universe_data")

        data_5min = min5_data["data_5min"]
        nifty_5min = data_5min.get("NIFTY50")
        if nifty_5min is None:
            print("ERROR: No NIFTY50 5-min data")
            return

        sim_start = args.start or basket_data.get("start_date")

        bt = PermanentGridBacktest5Min(
            basket=basket, daily_data=daily_data,
            data_5min=data_5min, nifty_5min=nifty_5min,
            total_capital=args.capital, grid_pct=args.grid_pct,
            runway_pct=args.runway, initial_levels=args.initial_levels,
            sim_start=sim_start, sim_end=args.end,
            universe=universe, universe_data=universe_data,
            universe_5min=data_5min,
            entry_mode=args.entry_mode,
            max_entries_per_week=args.max_entries_per_week,
            cost_model=cost_model,
        )

    bt.run()

    eq = bt.get_equity_curve()
    eq_path = "/root/trading_bot/permanent_grid/data/equity_curve_5min.csv"
    eq.to_csv(eq_path, index=False)
    print(f"\nEquity curve saved to {eq_path}")


if __name__ == "__main__":
    main()
