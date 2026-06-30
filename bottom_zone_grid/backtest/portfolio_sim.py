"""Rolling portfolio simulation for bottom-zone grid strategy.

Simulates the full lifecycle: universe refresh → scan → entry → grid trade → exit → rotate.
No look-ahead bias: all computations use only data available up to the current sim date.
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from bottom_zone_grid.scanner.range_calculator import validate_range
from bottom_zone_grid.scanner.grid_calculator import (
    calculate_grid_gap_from_bar_range,
    adjust_gap_for_range_coverage,
    derive_qty_for_full_range,
    build_grid_ladder,
)
from bottom_zone_grid.scanner.daily_scanner import (
    evaluate_stock,
    is_breakdown_confirmed,
)
from bottom_zone_grid.backtest.engine import GridBacktester

LOG = logging.getLogger("bzg.portsim")


@dataclass
class CampaignRecord:
    symbol: str
    entry_date: str
    exit_date: Optional[str] = None
    exit_reason: Optional[str] = None
    entry_price: float = 0
    exit_price: float = 0
    range_low: float = 0
    range_high: float = 0
    grid_gap: float = 0
    avg_bar_range: float = 0
    order_qty: int = 0
    capital_allocated: float = 0
    grid_cycles: int = 0
    exit_cycles: int = 0
    gross_pnl: float = 0
    total_cost: float = 0
    net_pnl: float = 0
    exit_pnl: float = 0
    max_drawdown: float = 0
    days_active: int = 0
    # Entry metadata for analysis
    dma50_value: float = 0
    dma200_value: float = 0
    entry_vs_dma50_pct: float = 0
    entry_vs_range_high_pct: float = 0
    entry_vs_range_low_pct: float = 0
    range_width_pct: float = 0
    entry_score: float = 0
    watchlist_days: int = 0
    entry_volume_ratio: float = 0
    range_position_pct: float = 0


@dataclass
class PortfolioResult:
    start_date: str
    end_date: str
    trading_days: int
    total_capital: float
    capital_per_slot: float
    max_slots: int

    campaigns: List[CampaignRecord] = field(default_factory=list)
    daily_equity: List[dict] = field(default_factory=list)

    total_campaigns: int = 0
    total_cycles: int = 0
    gross_pnl: float = 0
    total_cost: float = 0
    net_pnl: float = 0
    return_pct: float = 0
    annualized_return_pct: float = 0
    max_drawdown: float = 0
    max_drawdown_pct: float = 0
    avg_slots_used: float = 0
    avg_cycles_per_day: float = 0
    regime_blocked_days: int = 0
    stock_filtered_count: int = 0
    watchlist_added: int = 0
    watchlist_recovered: int = 0
    watchlist_expired: int = 0
    watchlist_breakdown: int = 0


class Campaign:
    """Manages one stock's grid trading lifecycle."""

    def __init__(
        self,
        symbol: str,
        fyers_symbol: str,
        range_low: float,
        range_high: float,
        grid_gap: float,
        avg_bar_range: float,
        order_qty: int,
        capital: float,
        config: dict,
        entry_date: date,
    ):
        self.symbol = symbol
        self.fyers_symbol = fyers_symbol
        self.range_low = range_low
        self.range_high = range_high
        self.grid_gap = grid_gap
        self.avg_bar_range = avg_bar_range
        self.order_qty = order_qty
        self.capital = capital
        self.config = config
        self.entry_date = entry_date
        self.entry_price: float = 0
        self.exit_price: float = 0
        self.exit_date: Optional[date] = None
        self.exit_reason: Optional[str] = None

        self.engine = GridBacktester(
            range_low=range_low,
            range_high=range_high,
            grid_gap=grid_gap,
            order_qty=order_qty,
            capital_per_slot=capital,
            config=config,
        )

        self.had_inventory = False
        self.last_trade_date = entry_date
        self.cycles_before_exit = 0
        # Entry metadata
        self.dma50_value: float = 0
        self.dma200_value: float = 0
        self.entry_score: float = 0
        self.watchlist_days: int = 0
        self.entry_volume_ratio: float = 0

    def process_day_candles(self, candles: list, current_date: date):
        prev_trades = len(self.engine.trades)
        ecfg = self.config.get("exit", {})
        check_sold_out = ecfg.get("exit_when_inventory_sold_out", True)

        for candle in candles:
            self.engine.process_candle(candle)

            if self.engine._inventory_qty() > 0:
                self.had_inventory = True
            elif check_sold_out and self.had_inventory and len(self.engine.cycles) > 0:
                break

        if len(self.engine.trades) > prev_trades:
            self.last_trade_date = current_date

    def check_exit(self, daily_close: float, current_date: date, daily_df: pd.DataFrame = None) -> Optional[str]:
        ecfg = self.config.get("exit", {})

        # Inventory sold out (profitable exit — all grid sells completed)
        if (ecfg.get("exit_when_inventory_sold_out", True)
                and self.had_inventory
                and self.engine._inventory_qty() == 0
                and len(self.engine.cycles) > 0):
            return "INVENTORY_SOLD_OUT"

        # Entry-based breakdown: X% below entry_price for N days
        bd_entry_pct = ecfg.get("breakdown_from_entry_pct", 0)
        if bd_entry_pct > 0 and self.entry_price > 0 and daily_df is not None and not daily_df.empty:
            bd_confirm = ecfg.get("breakdown_from_entry_days", 1)
            bd_level = self.entry_price * (1 - bd_entry_pct / 100)
            recent = daily_df["close"].tail(bd_confirm).values
            if len(recent) >= bd_confirm and all(c < bd_level for c in recent):
                return "ENTRY_BREAKDOWN"

        # Max campaign drawdown (0 = disabled)
        max_dd_pct = ecfg.get("max_campaign_drawdown_pct", 8.0)
        if max_dd_pct > 0:
            equity = self.engine._equity(daily_close)
            dd_pct = (self.capital - equity) / self.capital * 100
            if dd_pct > max_dd_pct:
                return f"MAX_DRAWDOWN ({dd_pct:.1f}%)"

        # No trade timeout
        timeout_days = ecfg.get("no_trade_timeout_days", 10)
        days_since_trade = (current_date - self.last_trade_date).days
        if days_since_trade > timeout_days and self.had_inventory:
            return f"NO_TRADE_TIMEOUT ({days_since_trade}d)"

        return None

    def force_exit(self, price: float, current_date: date, reason: str):
        self.cycles_before_exit = len(self.engine.cycles)
        ts = str(current_date)
        self.engine.force_exit(price, ts)
        self.exit_date = current_date
        self.exit_reason = reason

    def get_equity(self, price: float) -> float:
        return self.engine._equity(price)

    def to_record(self) -> CampaignRecord:
        total_cycles = len(self.engine.cycles)
        exit_cycles = total_cycles - self.cycles_before_exit

        # PnL from grid cycles only (not force-exit)
        grid_pnl = sum(c.net_pnl for c in self.engine.cycles[:self.cycles_before_exit])
        exit_pnl = sum(c.net_pnl for c in self.engine.cycles[self.cycles_before_exit:])

        days = 0
        if self.exit_date and isinstance(self.entry_date, date):
            days = (self.exit_date - self.entry_date).days

        ep = self.entry_price if self.entry_price > 0 else 1
        rw = self.range_high - self.range_low if self.range_high > self.range_low else 1
        range_position = (ep - self.range_low) / rw * 100

        return CampaignRecord(
            symbol=self.symbol,
            entry_date=str(self.entry_date),
            exit_date=str(self.exit_date) if self.exit_date else None,
            exit_reason=self.exit_reason,
            entry_price=round(self.entry_price, 2),
            exit_price=round(self.exit_price, 2),
            range_low=self.range_low,
            range_high=self.range_high,
            grid_gap=self.grid_gap,
            avg_bar_range=round(self.avg_bar_range, 2),
            order_qty=self.order_qty,
            capital_allocated=self.capital,
            grid_cycles=self.cycles_before_exit,
            exit_cycles=exit_cycles,
            gross_pnl=round(self.engine.gross_pnl, 2),
            total_cost=round(self.engine.total_cost, 2),
            net_pnl=round(self.engine.realized_pnl, 2),
            exit_pnl=round(exit_pnl, 2),
            max_drawdown=round(self.engine.max_drawdown, 2),
            days_active=days,
            dma50_value=round(self.dma50_value, 2),
            dma200_value=round(self.dma200_value, 2),
            entry_vs_dma50_pct=round((ep / self.dma50_value - 1) * 100, 2) if self.dma50_value > 0 else 0,
            entry_vs_range_high_pct=round((self.range_high - ep) / ep * 100, 2),
            entry_vs_range_low_pct=round((ep - self.range_low) / ep * 100, 2),
            range_width_pct=round(rw / self.range_low * 100, 2) if self.range_low > 0 else 0,
            entry_score=round(self.entry_score, 3),
            watchlist_days=self.watchlist_days,
            entry_volume_ratio=round(self.entry_volume_ratio, 2),
            range_position_pct=round(range_position, 1),
        )


class PortfolioSimulator:
    def __init__(
        self,
        config: dict,
        universe: List[dict],
        daily_data: Dict[str, pd.DataFrame],
        intraday_data: Dict[str, pd.DataFrame],
        sim_start: Optional[date] = None,
        sim_end: Optional[date] = None,
        unlimited_mode: bool = False,
    ):
        self.config = config
        self.universe = universe
        self.daily_data = daily_data
        self.intraday_data = intraday_data
        self.sim_start = sim_start
        self.sim_end = sim_end
        self.unlimited_mode = unlimited_mode

        pcfg = config.get("portfolio", {})
        if unlimited_mode:
            self.max_slots = 9999
            self.capital_per_slot = pcfg.get("capital_per_slot", 1000000)
            self.total_capital = self.capital_per_slot * self.max_slots
        else:
            self.max_slots = pcfg.get("base_active_stocks", 5)
            self.capital_per_slot = pcfg.get("capital_per_slot", 1000000)
            self.total_capital = config.get("total_strategy_capital", 5000000)
        self.refresh_days = config.get("universe", {}).get("review_frequency_days", 15)

        brcfg = config.get("bar_range", {})
        self.primary_tf = brcfg.get("primary_timeframe", "15")
        self.gap_multiplier = brcfg.get("gap_multiplier", 1.5)
        self.bar_range_lookback = brcfg.get("intraday_lookback_days", 10)

        gcfg = config.get("grid", {})
        self.reserve_buffer_pct = gcfg.get("reserve_charges_buffer_pct", 0.10)
        self.min_order_value = gcfg.get("min_order_value", 100000)
        self.exit_buffer_pct = gcfg.get("exit_buffer_pct", 0)

        self.regime_cfg = config.get("regime_filter", {})
        self.stock_filter_cfg = config.get("stock_filter", {})

        cfilt = self.stock_filter_cfg.get("candidate_filter", {})
        self.cand_max_score = cfilt.get("max_score", 1.0)
        self.cand_min_wl_days = cfilt.get("min_watchlist_days", 0)
        self.cand_max_wl_days = cfilt.get("max_watchlist_days", 999)
        self.cand_min_range_pos = cfilt.get("min_range_position_pct", 0)
        self.cand_min_dma50_pct = cfilt.get("min_entry_vs_dma50_pct", 0)

        self.active: Dict[str, Campaign] = {}
        self.completed: List[Campaign] = []
        self.cooldowns: Dict[str, date] = {}
        self.available_capital = self.total_capital
        self._last_scan_results: List[dict] = []
        self._last_scan_date: Optional[date] = None
        self._needs_rescan = True
        self._regime_blocked_days = 0
        self._stock_filtered_count = 0

        wl_cfg = self.stock_filter_cfg.get("watchlist", {})
        self._watchlist_enabled = (
            self.stock_filter_cfg.get("enabled", False)
            and wl_cfg.get("enabled", False)
        )
        self._watchlist_max_age = wl_cfg.get("max_age_days", 30)
        self._watchlist_remove_breakdown = wl_cfg.get("remove_on_breakdown", False)
        self._watchlist_scan_days = wl_cfg.get("scan_frequency_days", 5)
        self._watchlist: Dict[str, dict] = {}
        self._wl_added = 0
        self._wl_recovered = 0
        self._wl_expired = 0
        self._wl_breakdown = 0

    def _get_trading_dates(self) -> List[date]:
        all_dates = set()
        for df in self.intraday_data.values():
            if not df.empty and "datetime" in df.columns:
                for d in df["datetime"].dt.date.unique():
                    all_dates.add(d)
        dates = sorted(all_dates)
        if self.sim_start:
            dates = [d for d in dates if d >= self.sim_start]
        if self.sim_end:
            dates = [d for d in dates if d <= self.sim_end]
        return dates

    def _get_daily_up_to(self, symbol: str, as_of: date) -> pd.DataFrame:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return pd.DataFrame()
        return df[df["date"] <= as_of].copy()

    def _get_day_candles(self, symbol: str, day: date) -> list:
        df = self.intraday_data.get(symbol)
        if df is None or df.empty:
            return []
        day_df = df[df["datetime"].dt.date == day]
        return day_df.to_dict("records")

    def _compute_bar_range_on_date(self, symbol: str, as_of: date) -> Optional[float]:
        df = self.intraday_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["datetime"].dt.date <= as_of]
        if up_to.empty:
            return None
        dates = sorted(up_to["datetime"].dt.date.unique())
        if len(dates) < 3:
            return None
        cutoff = dates[-self.bar_range_lookback] if len(dates) >= self.bar_range_lookback else dates[0]
        window = up_to[up_to["datetime"].dt.date >= cutoff]
        valid = window[window["bar_range"] > 0]["bar_range"]
        if len(valid) < 10:
            return None
        return float(valid.mean())

    def _get_daily_close(self, symbol: str, as_of: date) -> Optional[float]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["date"] <= as_of]
        if up_to.empty:
            return None
        return float(up_to["close"].iloc[-1])

    def _get_daily_close_dated(self, symbol: str, as_of: date) -> Tuple[Optional[float], Optional[date]]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None, None
        up_to = df[df["date"] <= as_of]
        if up_to.empty:
            return None, None
        return float(up_to["close"].iloc[-1]), up_to["date"].iloc[-1]

    def _get_daily_high(self, symbol: str, as_of: date) -> Optional[float]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        row = df[df["date"] == as_of]
        if row.empty:
            return None
        return float(row["high"].iloc[-1])

    def _compute_sma(self, symbol: str, as_of: date, period: int) -> Optional[float]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["date"] <= as_of]
        if len(up_to) < period:
            return None
        return float(up_to["close"].iloc[-period:].mean())

    def _compute_rsi(self, symbol: str, as_of: date, period: int = 14) -> Optional[float]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["date"] <= as_of]
        if len(up_to) < period + 1:
            return None
        closes = up_to["close"].iloc[-(period + 1):]
        delta = closes.diff().dropna()
        gain = delta.where(delta > 0, 0.0).mean()
        loss = (-delta.where(delta < 0, 0.0)).mean()
        if loss == 0:
            return 100.0
        rs = gain / loss
        return float(100 - (100 / (1 + rs)))

    def _compute_breadth(self, as_of: date, ma_days: int = 50) -> Optional[float]:
        total = 0
        above = 0
        for sym_info in self.universe:
            fsym = sym_info["fyers_symbol"]
            sma = self._compute_sma(fsym, as_of, ma_days)
            close = self._get_daily_close(fsym, as_of)
            if sma is not None and close is not None:
                total += 1
                if close > sma:
                    above += 1
        if total == 0:
            return None
        return float(above / total * 100)

    def _check_regime(self, as_of: date) -> Tuple[bool, str]:
        rcfg = self.regime_cfg
        if not rcfg.get("enabled", False):
            return True, ""

        filters = rcfg.get("filters", {})
        mode = rcfg.get("mode", "all")
        nifty_sym = rcfg.get("nifty_symbol", "NSE:NIFTY50-INDEX")
        vix_sym = rcfg.get("vix_symbol", "NSE:INDIAVIX-INDEX")
        nifty_close = self._get_daily_close(nifty_sym, as_of)

        results = []

        f = filters.get("nifty_above_200dma", {})
        if f.get("enabled"):
            sma = self._compute_sma(nifty_sym, as_of, 200)
            if sma and nifty_close:
                passed = nifty_close > sma
                results.append(("200DMA", passed, f"Nifty {nifty_close:.0f} {'>' if passed else '<'} 200DMA {sma:.0f}"))
            else:
                results.append(("200DMA", False, "insufficient data"))

        f = filters.get("nifty_above_50dma", {})
        if f.get("enabled"):
            sma = self._compute_sma(nifty_sym, as_of, 50)
            if sma and nifty_close:
                passed = nifty_close > sma
                results.append(("50DMA", passed, f"Nifty {nifty_close:.0f} {'>' if passed else '<'} 50DMA {sma:.0f}"))
            else:
                results.append(("50DMA", False, "insufficient data"))

        f = filters.get("nifty_20_50_crossover", {})
        if f.get("enabled"):
            sma20 = self._compute_sma(nifty_sym, as_of, 20)
            sma50 = self._compute_sma(nifty_sym, as_of, 50)
            if sma20 and sma50:
                passed = sma20 > sma50
                results.append(("20/50X", passed, f"20DMA {sma20:.0f} {'>' if passed else '<'} 50DMA {sma50:.0f}"))
            else:
                results.append(("20/50X", False, "insufficient data"))

        f = filters.get("nifty_rsi_above", {})
        if f.get("enabled"):
            threshold = f.get("threshold", 40)
            rsi = self._compute_rsi(nifty_sym, as_of)
            if rsi is not None:
                passed = rsi > threshold
                results.append(("RSI", passed, f"RSI {rsi:.1f} {'>' if passed else '<'} {threshold}"))
            else:
                results.append(("RSI", False, "insufficient data"))

        f = filters.get("vix_below", {})
        if f.get("enabled"):
            threshold = f.get("threshold", 20)
            vix_close = self._get_daily_close(vix_sym, as_of)
            if vix_close is not None:
                passed = vix_close < threshold
                results.append(("VIX", passed, f"VIX {vix_close:.1f} {'<' if passed else '>'} {threshold}"))
            else:
                results.append(("VIX", False, "insufficient data"))

        f = filters.get("breadth_above", {})
        if f.get("enabled"):
            threshold = f.get("threshold", 40)
            ma_days = f.get("breadth_ma_days", 50)
            breadth = self._compute_breadth(as_of, ma_days)
            if breadth is not None:
                passed = breadth > threshold
                results.append(("BREADTH", passed, f"Breadth {breadth:.0f}% {'>' if passed else '<'} {threshold}%"))
            else:
                results.append(("BREADTH", False, "insufficient data"))

        if not results:
            return True, ""

        if mode == "all":
            ok = all(r[1] for r in results)
        else:
            ok = any(r[1] for r in results)

        detail = " | ".join(f"{name}:{'OK' if p else 'FAIL'}" for name, p, _ in results)
        if not ok:
            reason = " | ".join(msg for _, p, msg in results if not p)
            LOG.info("Regime BLOCKED on %s [%s]: %s", as_of, mode, reason)
        return ok, detail

    def _check_stock_strength(self, symbol: str, as_of: date) -> Tuple[bool, str]:
        scfg = self.stock_filter_cfg
        if not scfg.get("enabled", False):
            return True, ""

        filters = scfg.get("filters", {})
        mode = scfg.get("mode", "all")
        nifty_sym = self.regime_cfg.get("nifty_symbol", "NSE:NIFTY50-INDEX")
        results = []

        f = filters.get("above_200dma", {})
        if f.get("enabled"):
            sma = self._compute_sma(symbol, as_of, 200)
            close = self._get_daily_close(symbol, as_of)
            if sma and close:
                passed = close > sma
                results.append(("200DMA", passed, f"{close:.2f} {'>' if passed else '<'} 200DMA {sma:.2f}"))
            else:
                results.append(("200DMA", False, "insufficient data"))

        f = filters.get("above_50dma", {})
        if f.get("enabled"):
            sma = self._compute_sma(symbol, as_of, 50)
            close = self._get_daily_close(symbol, as_of)
            if sma and close:
                passed = close > sma
                results.append(("50DMA", passed, f"{close:.2f} {'>' if passed else '<'} 50DMA {sma:.2f}"))
            else:
                results.append(("50DMA", False, "insufficient data"))

        f = filters.get("rsi_above", {})
        if f.get("enabled"):
            threshold = f.get("threshold", 30)
            rsi = self._compute_rsi(symbol, as_of)
            if rsi is not None:
                passed = rsi > threshold
                results.append(("RSI", passed, f"RSI {rsi:.1f} {'>' if passed else '<'} {threshold}"))
            else:
                results.append(("RSI", False, "insufficient data"))

        f = filters.get("relative_strength_vs_nifty", {})
        if f.get("enabled"):
            lookback = f.get("lookback_days", 20)
            threshold = f.get("threshold", -5)
            stock_ret = self._compute_return_pct(symbol, as_of, lookback)
            nifty_ret = self._compute_return_pct(nifty_sym, as_of, lookback)
            if stock_ret is not None and nifty_ret is not None:
                rs = stock_ret - nifty_ret
                passed = rs > threshold
                results.append(("RS", passed, f"RS {rs:+.1f}% (stock {stock_ret:+.1f}% vs Nifty {nifty_ret:+.1f}%) {'>' if passed else '<'} {threshold}%"))
            else:
                results.append(("RS", False, "insufficient data"))

        f = filters.get("volume_above_avg", {})
        if f.get("enabled"):
            multiplier = f.get("multiplier", 0.8)
            lookback = f.get("lookback_days", 20)
            vol_ok = self._check_volume_above_avg(symbol, as_of, lookback, multiplier)
            if vol_ok is not None:
                results.append(("VOL", vol_ok, f"recent vol {'>' if vol_ok else '<'} {multiplier}x avg"))
            else:
                results.append(("VOL", False, "insufficient data"))

        if not results:
            return True, ""

        if mode == "all":
            ok = all(r[1] for r in results)
        else:
            ok = any(r[1] for r in results)

        detail = " | ".join(f"{name}:{'OK' if p else 'FAIL'}" for name, p, _ in results)
        if not ok:
            reason = " | ".join(msg for _, p, msg in results if not p)
            LOG.info("Stock filter REJECTED %s on %s [%s]: %s", symbol, as_of, mode, reason)
        return ok, detail

    def _compute_return_pct(self, symbol: str, as_of: date, days: int) -> Optional[float]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["date"] <= as_of]
        if len(up_to) < days + 1:
            return None
        close_now = float(up_to["close"].iloc[-1])
        close_past = float(up_to["close"].iloc[-(days + 1)])
        if close_past == 0:
            return None
        return (close_now - close_past) / close_past * 100

    def _check_volume_above_avg(self, symbol: str, as_of: date, lookback: int, multiplier: float) -> Optional[bool]:
        df = self.daily_data.get(symbol)
        if df is None or df.empty:
            return None
        up_to = df[df["date"] <= as_of]
        if len(up_to) < lookback + 5:
            return None
        recent_vol = float(up_to["volume"].iloc[-5:].mean())
        avg_vol = float(up_to["volume"].iloc[-(lookback + 5):-5].mean())
        if avg_vol == 0:
            return None
        return recent_vol > avg_vol * multiplier

    def _update_watchlist(self, scan_results: List[dict], as_of: date):
        active_syms = set(self.active.keys())
        cooldown_syms = {s for s, cd in self.cooldowns.items() if cd > as_of}

        for row in scan_results:
            if row["final_status"] != "TRADE_READY":
                continue
            fsym = row["fyers_symbol"]
            if fsym in active_syms or fsym in cooldown_syms:
                continue
            if fsym not in self._watchlist:
                self._watchlist[fsym] = {
                    "fyers_symbol": fsym,
                    "symbol": row.get("symbol", row.get("nse_symbol", "")),
                    "added_date": as_of,
                    "range_low": row["range_low"],
                    "range_high": row["range_high"],
                }
                self._wl_added += 1
                LOG.info("Watchlist ADD %s on %s (range %.2f-%.2f)",
                         fsym, as_of, row["range_low"], row["range_high"])

    def _get_watchlist_candidates(self, as_of: date) -> List[dict]:
        """Check watchlist stocks: passed filter + price back in original range."""
        active_syms = set(self.active.keys())
        cooldown_syms = {s for s, cd in self.cooldowns.items() if cd > as_of}
        candidates = []

        for fsym, wl_entry in list(self._watchlist.items()):
            if fsym in active_syms or fsym in cooldown_syms:
                continue

            close, close_date = self._get_daily_close_dated(fsym, as_of)
            if close is None or close_date is None:
                continue
            if (as_of - close_date).days > 3:
                continue

            range_low = wl_entry["range_low"]
            range_high = wl_entry["range_high"]
            range_width = range_high - range_low
            if range_width <= 0:
                continue

            # Close and day's high must both be within range
            daily_high = self._get_daily_high(fsym, as_of)
            if close < range_low or close > range_high:
                continue
            if daily_high is not None and daily_high > range_high:
                continue

            # Must pass stock filter (e.g. above 50DMA)
            stock_ok, detail = self._check_stock_strength(fsym, as_of)
            if not stock_ok:
                continue

            if not wl_entry.get("_recovered"):
                wl_entry["_recovered"] = True
                self._wl_recovered += 1
                LOG.info("Watchlist RECOVERED %s on %s (%d days, price %.2f in range %.2f-%.2f): %s",
                         fsym, as_of,
                         (as_of - wl_entry["added_date"]).days,
                         close, range_low, range_high, detail)

            range_position = (close - range_low) / range_width
            candidate_score = max(0, 1.0 - range_position)
            wl_age = (as_of - wl_entry["added_date"]).days
            range_pos_pct = range_position * 100

            if candidate_score > self.cand_max_score:
                continue
            if wl_age < self.cand_min_wl_days or wl_age > self.cand_max_wl_days:
                continue
            if range_pos_pct < self.cand_min_range_pos:
                continue
            if self.cand_min_dma50_pct > 0:
                daily_df = self._get_daily_up_to(fsym, as_of)
                if not daily_df.empty and len(daily_df) >= 50:
                    dma50 = float(daily_df["close"].iloc[-50:].mean())
                    if dma50 > 0 and ((close / dma50 - 1) * 100) < self.cand_min_dma50_pct:
                        continue

            row = {
                "fyers_symbol": fsym,
                "symbol": wl_entry.get("symbol", ""),
                "range_low": range_low,
                "range_high": range_high,
                "cmp": close,
                "_watchlist_age": wl_age,
                "candidate_score": candidate_score,
                "final_status": "TRADE_READY",
            }
            candidates.append(row)

        candidates.sort(key=lambda r: -(r.get("candidate_score") or 0))
        return candidates

    def _scan_on_date(self, as_of: date) -> List[dict]:
        rows = []
        for sym_info in self.universe:
            fsym = sym_info["fyers_symbol"]
            daily_df = self._get_daily_up_to(fsym, as_of)
            if daily_df.empty or len(daily_df) < 30:
                continue

            cmp = float(daily_df["close"].iloc[-1])
            avg_br = self._compute_bar_range_on_date(fsym, as_of)
            bar_range_data = {self.primary_tf: avg_br}

            row = evaluate_stock(sym_info, daily_df, cmp, self.config, bar_range_data)
            rows.append(row)

        rows.sort(key=lambda r: (
            0 if r["final_status"] == "TRADE_READY" else 1,
            -(r.get("candidate_score") or 0),
        ))

        trade_ready = sum(1 for r in rows if r["final_status"] == "TRADE_READY")
        LOG.info("Scan on %s: %d analyzed, %d TRADE_READY", as_of, len(rows), trade_ready)

        self._last_scan_results = rows
        self._last_scan_date = as_of
        self._needs_rescan = False
        return rows

    def _start_campaign(self, row: dict, as_of: date) -> bool:
        fsym = row["fyers_symbol"]
        symbol = row["symbol"]

        avg_br = self._compute_bar_range_on_date(fsym, as_of)
        if not avg_br or avg_br <= 0:
            LOG.debug("Skip %s: no bar range data", fsym)
            return False

        bar_range_gap = calculate_grid_gap_from_bar_range(avg_br, self.gap_multiplier)
        if bar_range_gap <= 0:
            LOG.debug("Skip %s: bar_range_gap <= 0", fsym)
            return False

        range_low = row["range_low"]
        range_high = row["range_high"]
        if self.exit_buffer_pct > 0:
            range_high = range_high * (1 - self.exit_buffer_pct / 100)
        cmp = row["cmp"]

        day_candles = self._get_day_candles(fsym, as_of)
        if day_candles:
            day_low = min(c["low"] for c in day_candles)
            entry_price = cmp if day_low <= cmp else day_candles[0]["open"]
        else:
            entry_price = cmp

        grid_gap = adjust_gap_for_range_coverage(
            bar_range_gap, range_low, range_high,
            self.capital_per_slot, self.min_order_value,
        )

        ladder = build_grid_ladder(range_low, range_high, grid_gap)
        if len(ladder) <= 2:
            order_qty = int(self.capital_per_slot / (entry_price * 1.005))
        else:
            order_qty = derive_qty_for_full_range(ladder, self.capital_per_slot, self.reserve_buffer_pct)
        if order_qty <= 0:
            LOG.debug("Skip %s: order_qty <= 0", fsym)
            return False

        campaign = Campaign(
            symbol=symbol, fyers_symbol=fsym,
            range_low=range_low, range_high=range_high,
            grid_gap=grid_gap, avg_bar_range=avg_br,
            order_qty=order_qty,
            capital=self.capital_per_slot, config=self.config,
            entry_date=as_of,
        )

        campaign.entry_price = entry_price

        # Compute entry metadata
        daily_df = self._get_daily_up_to(fsym, as_of)
        if not daily_df.empty and len(daily_df) >= 50:
            campaign.dma50_value = float(daily_df["close"].iloc[-50:].mean())
        if not daily_df.empty and len(daily_df) >= 200:
            campaign.dma200_value = float(daily_df["close"].iloc[-200:].mean())
        if not daily_df.empty and len(daily_df) >= 25:
            recent_vol = float(daily_df["volume"].iloc[-5:].mean())
            avg_vol = float(daily_df["volume"].iloc[-25:-5].mean())
            campaign.entry_volume_ratio = recent_vol / avg_vol if avg_vol > 0 else 0
        campaign.entry_score = row.get("candidate_score", 0)
        campaign.watchlist_days = row.get("_watchlist_age", 0)

        if len(ladder) <= 2:
            campaign.engine._try_buy(entry_price, str(as_of), grid_level=ladder[0])
            campaign.engine._last_close = entry_price
        else:
            campaign.engine.deploy_initial_inventory(entry_price, str(as_of))
        initial_qty = campaign.engine._inventory_qty()
        if initial_qty > 0:
            campaign.had_inventory = True

        if day_candles:
            campaign.process_day_candles(day_candles, as_of)

        self.active[fsym] = campaign
        self.available_capital -= self.capital_per_slot

        LOG.info("[%s] ENTER on %s: entry=%.2f range=%.2f-%.2f gap=%.2f qty=%d init_pos=%d capital=₹%.0f",
                 symbol, as_of, entry_price, range_low, range_high, grid_gap, order_qty,
                 initial_qty, self.capital_per_slot)
        return True

    def _exit_campaign(self, fsym: str, price: float, current_date: date, reason: str):
        campaign = self.active.pop(fsym)
        if reason == "INVENTORY_SOLD_OUT" and campaign.engine.trades:
            sell_trades = [t for t in campaign.engine.trades if t.side == "SELL"]
            campaign.exit_price = sell_trades[-1].price if sell_trades else price
        else:
            campaign.exit_price = price
        campaign.force_exit(price, current_date, reason)
        self.completed.append(campaign)
        self.available_capital += campaign.engine.cash

        ecfg = self.config.get("exit", {})
        if reason == "INVENTORY_SOLD_OUT":
            cooldown = ecfg.get("profit_exit_cooldown_days", 1)
        elif "BREAKDOWN" in reason:
            cooldown = ecfg.get("breakdown_cooldown_days", 20)
        else:
            cooldown = 5
        self.cooldowns[fsym] = current_date + timedelta(days=cooldown)

        grid_pnl = sum(c.net_pnl for c in campaign.engine.cycles[:campaign.cycles_before_exit])
        exit_pnl = campaign.engine.realized_pnl - grid_pnl

        LOG.info("[%s] EXIT on %s: %s | grid_cycles=%d grid_pnl=₹%.0f exit_pnl=₹%.0f total=₹%.0f",
                 campaign.symbol, current_date, reason,
                 campaign.cycles_before_exit, grid_pnl, exit_pnl, campaign.engine.realized_pnl)

        # Mark that we need a rescan to fill this slot
        self._needs_rescan = True

    def run(self) -> PortfolioResult:
        trading_dates = self._get_trading_dates()
        if len(trading_dates) < 15:
            raise ValueError(f"Not enough trading dates: {len(trading_dates)}")

        warmup = self.bar_range_lookback
        sim_dates = trading_dates[warmup:]
        LOG.info("Simulation: %d trading days (%s → %s), warmup=%d days",
                 len(sim_dates), sim_dates[0], sim_dates[-1], warmup)

        peak_equity = self.total_capital
        max_drawdown = 0.0
        slots_used_sum = 0
        total_day_cycles = 0
        daily_equity_list = []

        for day_idx, today in enumerate(sim_dates):
            # 1. Scheduled scans: full refresh every N days + watchlist scan at its own cadence
            if day_idx % self.refresh_days == 0:
                LOG.info("Day %d (%s): Scheduled universe refresh", day_idx + 1, today)
                self._scan_on_date(today)
                if self._watchlist_enabled:
                    self._update_watchlist(self._last_scan_results, today)
            elif self._watchlist_enabled and day_idx % self._watchlist_scan_days == 0:
                self._scan_on_date(today)
                self._update_watchlist(self._last_scan_results, today)

            # 2. Process active campaigns
            prev_total_cycles = sum(len(c.engine.cycles) for c in self.active.values())
            for fsym, campaign in list(self.active.items()):
                candles = self._get_day_candles(fsym, today)
                if candles:
                    campaign.process_day_candles(candles, today)

            new_cycles = sum(len(c.engine.cycles) for c in self.active.values()) - prev_total_cycles
            total_day_cycles += new_cycles

            # 3. Check exits
            for fsym, campaign in list(self.active.items()):
                close = self._get_daily_close(fsym, today)
                if close is None:
                    continue

                daily_df = self._get_daily_up_to(fsym, today)

                # Range-low breakdown (original)
                if not daily_df.empty and is_breakdown_confirmed(daily_df, campaign.range_low, self.config):
                    self._exit_campaign(fsym, close, today, "BREAKDOWN")
                    continue

                exit_reason = campaign.check_exit(close, today, daily_df=daily_df)
                if exit_reason:
                    self._exit_campaign(fsym, close, today, exit_reason)

            # 4. Fill empty slots
            empty_slots = self.max_slots - len(self.active)
            regime_ok, regime_detail = self._check_regime(today)
            if not regime_ok:
                self._regime_blocked_days += 1

            if empty_slots > 0 and self.available_capital >= self.capital_per_slot and regime_ok:
                self._scan_on_date(today)

                if self._watchlist_enabled:
                    self._update_watchlist(self._last_scan_results, today)
                    candidates = self._get_watchlist_candidates(today)

                    started = 0
                    for row in candidates:
                        if started >= empty_slots or self.available_capital < self.capital_per_slot:
                            break
                        if self._start_campaign(row, today):
                            fsym = row["fyers_symbol"]
                            if fsym in self._watchlist:
                                del self._watchlist[fsym]
                            started += 1
                else:
                    active_syms = set(self.active.keys())
                    cooldown_syms = {s for s, cd in self.cooldowns.items() if cd > today}

                    candidates = [
                        r for r in self._last_scan_results
                        if r["final_status"] == "TRADE_READY"
                        and r["fyers_symbol"] not in active_syms
                        and r["fyers_symbol"] not in cooldown_syms
                        and self.available_capital >= self.capital_per_slot
                    ]

                    started = 0
                    for row in candidates:
                        if started >= empty_slots:
                            break
                        stock_ok, stock_detail = self._check_stock_strength(row["fyers_symbol"], today)
                        if not stock_ok:
                            self._stock_filtered_count += 1
                            continue
                        if self._start_campaign(row, today):
                            started += 1

            # 4b. Watchlist cleanup: age-based expiry only (no breakdown removal)
            if self._watchlist_enabled and self._watchlist:
                expired = []
                for fsym, entry in self._watchlist.items():
                    age = (today - entry["added_date"]).days
                    if age > self._watchlist_max_age:
                        expired.append(fsym)
                for fsym in expired:
                    if fsym in self._watchlist:
                        self._wl_expired += 1
                        del self._watchlist[fsym]

            # 5. Daily portfolio stats
            portfolio_equity = self.available_capital
            for fsym, campaign in self.active.items():
                close = self._get_daily_close(fsym, today)
                if close:
                    portfolio_equity += campaign.get_equity(close)

            if portfolio_equity > peak_equity:
                peak_equity = portfolio_equity
            dd = peak_equity - portfolio_equity
            if dd > max_drawdown:
                max_drawdown = dd

            slots_used_sum += len(self.active)

            daily_equity_list.append({
                "date": str(today),
                "equity": round(portfolio_equity, 2),
                "active_slots": len(self.active),
                "available_capital": round(self.available_capital, 2),
                "day_cycles": new_cycles,
                "regime_ok": regime_ok,
                "active_symbols": ",".join(c.symbol for c in self.active.values()),
            })

        # Force exit remaining campaigns at last close
        last_date = sim_dates[-1]
        for fsym, campaign in list(self.active.items()):
            close = self._get_daily_close(fsym, last_date)
            if close:
                self._exit_campaign(fsym, close, last_date, "SIM_END")

        # Build result
        all_campaigns = self.completed
        total_cycles = sum(c.cycles_before_exit for c in all_campaigns)
        gross_pnl = sum(c.engine.gross_pnl for c in all_campaigns)
        total_cost = sum(c.engine.total_cost for c in all_campaigns)
        net_pnl = sum(c.engine.realized_pnl for c in all_campaigns)
        roc = (net_pnl / self.total_capital * 100) if self.total_capital > 0 else 0
        ann = roc * (252 / len(sim_dates)) if sim_dates else 0
        dd_pct = (max_drawdown / self.total_capital * 100) if self.total_capital > 0 else 0

        if self._watchlist_enabled:
            LOG.info("Watchlist funnel: added=%d recovered=%d expired=%d breakdown=%d remaining=%d",
                     self._wl_added, self._wl_recovered, self._wl_expired,
                     self._wl_breakdown, len(self._watchlist))

        return PortfolioResult(
            start_date=str(sim_dates[0]),
            end_date=str(sim_dates[-1]),
            trading_days=len(sim_dates),
            total_capital=self.total_capital,
            capital_per_slot=self.capital_per_slot,
            max_slots=self.max_slots,
            campaigns=[c.to_record() for c in all_campaigns],
            daily_equity=daily_equity_list,
            total_campaigns=len(all_campaigns),
            total_cycles=total_cycles,
            gross_pnl=round(gross_pnl, 2),
            total_cost=round(total_cost, 2),
            net_pnl=round(net_pnl, 2),
            return_pct=round(roc, 4),
            annualized_return_pct=round(ann, 2),
            max_drawdown=round(max_drawdown, 2),
            max_drawdown_pct=round(dd_pct, 2),
            avg_slots_used=round(slots_used_sum / len(sim_dates), 1) if sim_dates else 0,
            avg_cycles_per_day=round(total_day_cycles / len(sim_dates), 1) if sim_dates else 0,
            regime_blocked_days=self._regime_blocked_days,
            stock_filtered_count=self._stock_filtered_count,
            watchlist_added=self._wl_added,
            watchlist_recovered=self._wl_recovered,
            watchlist_expired=self._wl_expired,
            watchlist_breakdown=self._wl_breakdown,
        )
