#!/usr/bin/env python3
"""
BOS+Fib scalping paper/live bot for MEXC spot.

Paper mode (default): mirrors the backtester's bar-close logic exactly.
  - Entry fill is simulated when bar low ≤ entry limit (LONG) or bar high ≥ entry limit (SHORT)
  - Stop/target checked at each bar close against OHLC — same as the backtester
  - Trail stop milestones updated at bar close
  - Time exit at entry_bar + time_exit_bars

Live mode (--live): places real orders via MexcSpotClient.
  - Entry: GTC limit order at fib 61.8%
  - Stop: marketable limit exit when bar closes below stop (or intrabar poll)
  - Target: GTC limit order placed immediately at entry fill

Usage:
    python scripts/live_fib_bot.py --config configs/fib_live.json
    python scripts/live_fib_bot.py --config configs/fib_live.json --live
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests as _requests

# ── repo on path ──────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from intraday_research.features import FeatureEngine
from intraday_research.strategies import NiftyBOSFibScalpStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("fib_bot")

# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    symbol: str
    side: str                   # LONG | SHORT
    qty: float
    entry_price: float
    stop_price: float
    target_price: float
    entry_time_utc: str         # ISO-8601 UTC
    entry_bar_ts: str           # bar timestamp that generated the signal
    bars_held: int = 0
    trail_milestones: List[float] = field(default_factory=list)
    trail_milestone_idx: int = 0
    stop_pending: Optional[float] = None  # trail stop to apply next bar
    partial_done: bool = False
    entry_binance: Optional[float] = None  # dual-feed: signal-feed level at entry (for basis)
    # live-mode order ids
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "OpenPosition":
        d = dict(d)
        d.setdefault("bars_held", 0)
        d.setdefault("stop_pending", None)
        d.setdefault("partial_done", False)
        d.setdefault("entry_binance", None)
        d.setdefault("entry_order_id", None)
        d.setdefault("exit_order_id", None)
        return cls(**d)


@dataclass
class PendingEntry:
    symbol: str
    side: str
    qty: float
    entry_limit: float          # limit price to fill at
    stop_price: float
    target_price: float
    signal_bar_ts: str          # bar that generated the signal
    signal_time_utc: str
    trail_milestones: List[float] = field(default_factory=list)
    bars_waiting: int = 0
    max_bars_wait: int = 3      # cancel if not filled within N bars
    order_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "PendingEntry":
        d = dict(d)
        d.setdefault("bars_waiting", 0)
        d.setdefault("max_bars_wait", 3)
        d.setdefault("order_id", None)
        return cls(**d)


# ─────────────────────────────────────────────────────────────────────────────
# MEXC klines fetcher
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_bars_mexc(symbol: str, n: int = 350) -> pd.DataFrame:
    """Fetch last N *closed* 1-min bars from MEXC public klines."""
    url = "https://api.mexc.com/api/v3/klines"
    r = _requests.get(url, params={"symbol": symbol, "interval": "1m", "limit": n + 1}, timeout=15)
    r.raise_for_status()
    raw = r.json()
    rows = raw[:-1]  # last bar is still open — drop it

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": [pd.Timestamp(int(b[0]), unit="ms", tz="UTC") for b in rows],
        "symbol":    f"MEXC:{symbol}",
        "open":      [float(b[1]) for b in rows],
        "high":      [float(b[2]) for b in rows],
        "low":       [float(b[3]) for b in rows],
        "close":     [float(b[4]) for b in rows],
        "volume":    [float(b[5]) for b in rows],
    })
    df["trade_date"] = df["timestamp"].dt.date.astype(str)
    return df.reset_index(drop=True)


def _fetch_ltp_mexc(symbol: str) -> float:
    url = "https://api.mexc.com/api/v3/ticker/price"
    r = _requests.get(url, params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    return float(r.json()["price"])


def _fetch_bars_binance(symbol: str, n: int = 350) -> pd.DataFrame:
    """Fetch last N *closed* 1-min bars from Binance public klines (clean/deep feed)."""
    url = "https://api.binance.com/api/v3/klines"
    r = _requests.get(url, params={"symbol": symbol, "interval": "1m", "limit": n + 1}, timeout=15)
    r.raise_for_status()
    rows = r.json()[:-1]  # last bar still open — drop it
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp(int(b[0]), unit="ms", tz="UTC") for b in rows],
        "symbol":    f"BINANCE:{symbol}",
        "open":      [float(b[1]) for b in rows],
        "high":      [float(b[2]) for b in rows],
        "low":       [float(b[3]) for b in rows],
        "close":     [float(b[4]) for b in rows],
        "volume":    [float(b[5]) for b in rows],
    })
    df["trade_date"] = df["timestamp"].dt.date.astype(str)
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# Signal extraction
# ─────────────────────────────────────────────────────────────────────────────

def _make_strategy(cfg: dict, med_price: float) -> NiftyBOSFibScalpStrategy:
    min_imp = round(med_price * cfg["min_impulse_pct"] / 100, 4)
    # stop_buffer_floor: minimum cushion beyond the impulse extreme. The old fixed 0.05
    # dominates for low-priced coins (SOL ~$73 → 0.009% = 0.0066 « 0.05), inflating risk
    # and halving RR. Configurable; default 0.05 for backward-compat.
    _floor = float(cfg.get("stop_buffer_floor", 0.05))
    stop_buf = round(max(med_price * cfg.get("stop_buffer_pct", 0.009) / 100, _floor), 4)
    variant = cfg.get("variant", "EMA").upper()
    use_vwap = "VWAP" in variant
    use_ema  = "EMA"  in variant
    return NiftyBOSFibScalpStrategy(
        name=f"BOS_FIB_{variant}",
        pivot_lookback=2, min_swing_points=2,
        fib_zone_low=0.50, fib_zone_high=0.618,
        stop_buffer_points=stop_buf, min_impulse_points=min_imp,
        max_hold_bars=50, max_trades_per_day=500,
        max_consecutive_losses_per_day=200,
        entry_mode="limit_618", min_confirmation_rr=0.0,
        use_vwap_filter=use_vwap, use_ema_filter=use_ema,
        target_extension_ratio=float(cfg.get("target_ext", 1.618)),
        same_bar_entry_needs_open_near_extreme=bool(cfg.get("same_bar_entry_needs_open_near_extreme", False)),
        same_bar_open_pos_threshold=float(cfg.get("same_bar_open_pos_threshold", 0.5)),
    )


_FE = FeatureEngine()


def get_signal_for_last_bar(df: pd.DataFrame, strat_cfg: dict,
                            ref_median: Optional[float] = None) -> Optional[dict]:
    """
    Run strategy on `df` and return the signal row for the LAST closed bar,
    or None if no signal.

    `ref_median` scales min_impulse/stop thresholds. When None (legacy path),
    the trailing-window median is used. Live + the live-faithful backtester pass
    the daily-frozen lookback median so both compute identical thresholds.
    """
    if df.empty or len(df) < 20:
        return None

    med = float(ref_median) if ref_median is not None else float(df["close"].median())
    strategy = _make_strategy(strat_cfg, med)

    featured = _FE.transform(df)
    signals_df = strategy.generate_signals(featured)

    if signals_df.empty:
        return None

    last_ts = df["timestamp"].iloc[-1]
    row = signals_df[signals_df["timestamp"] == last_ts]
    if row.empty:
        return None

    r = row.iloc[-1]  # take last if multiple (shouldn't happen)
    return {
        "timestamp":       str(r["timestamp"]),
        "direction":       str(r["direction"]),
        "entry_price":     float(r["entry_price"]),
        "stop_loss":       float(r["stop_loss"]),
        "target":          float(r["target"]),
        "trail_milestones": list(r["trail_milestones"]) if r["trail_milestones"] else [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Position exit resolver  (mirrors backtester._resolve_exit)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_exit_on_bar(
    pos: OpenPosition,
    row: pd.Series,
    time_exit_bars: int,
    use_trail: bool,
) -> Optional[Tuple[float, str]]:
    """
    Check whether the position exits on this bar.
    Returns (exit_price, reason) or None.
    Mirrors backtester logic: stop > trail > partial_target > target > time_exit.
    """
    is_long  = pos.side == "LONG"
    is_short = pos.side == "SHORT"
    hi, lo   = float(row["high"]), float(row["low"])

    # ── 1. Apply pending trail stop from previous bar ─────────────────────
    if pos.stop_pending is not None:
        pos.stop_price = pos.stop_pending
        pos.stop_pending = None

    # ── 2. Stop loss ──────────────────────────────────────────────────────
    stop_hit = (is_long and lo <= pos.stop_price) or (is_short and hi >= pos.stop_price)
    if stop_hit:
        exit_price = pos.stop_price
        reason = "trail_stop" if use_trail and pos.trail_milestone_idx > 0 else "stop_loss"
        return exit_price, reason

    # ── 3. Trail milestones (while-loop — catches multiple in one bar) ────
    if use_trail and pos.trail_milestones:
        while pos.trail_milestone_idx < len(pos.trail_milestones):
            m_idx    = pos.trail_milestone_idx
            next_m   = pos.trail_milestones[m_idx]
            m_hit    = (is_long and hi >= next_m) or (is_short and lo <= next_m)
            if not m_hit:
                break
            pos.stop_pending = next_m
            pos.trail_milestone_idx += 1
            is_last = (m_idx == len(pos.trail_milestones) - 1)
            if is_last and not pos.partial_done:
                pos.partial_done = True
                return float(next_m), "partial_target"

    # ── 4. Full target ────────────────────────────────────────────────────
    target_hit = (is_long and hi >= pos.target_price) or (is_short and lo <= pos.target_price)
    if target_hit:
        return pos.target_price, "target"

    # ── 5. Time exit ──────────────────────────────────────────────────────
    if pos.bars_held >= time_exit_bars:
        close_price = float(row["close"])
        return close_price, "time_exit"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Bot class
# ─────────────────────────────────────────────────────────────────────────────

class FibLiveBot:
    def __init__(self, cfg: dict, paper: bool = True, broker=None):
        self.cfg       = cfg
        self.paper     = paper
        self.broker    = broker
        self.symbols: List[str] = cfg["symbols"]
        self.strat_cfg: dict    = cfg["strategy"]
        self.trade_value_usd: float = float(cfg.get("trade_value_usd", 5000))
        self.use_trail: bool    = bool(self.strat_cfg.get("use_trailing_stop", True))
        self.time_exit_bars: int = 90  # 90 min for ext > 1.0
        self.max_bars_wait: int = int(cfg.get("max_bars_wait", 3))  # limit-entry expiry
        # When True: fill at bar N+1 open (matches ENTRY_NEXT_BAR_OPEN=True backtest).
        # When False: wait for price to touch the fib-618 limit within max_bars_wait bars.
        self.entry_next_bar_open: bool = bool(cfg.get("entry_next_bar_open", False))

        # ── Dual-feed: signals on one exchange, fills captured from another ────
        # Generate signals + exit triggers on `signals` feed (Binance — clean/deep),
        # but capture the ACTUAL marketable fill price from `fills` feed (MEXC — where
        # the order executes). Every fill (entry, stop, target, trail, time) is treated
        # as marketable: Binance detects the trigger, MEXC gives the fill price. Measures
        # the real basis/slippage. Default off = single feed (unchanged behaviour).
        _df_cfg = cfg.get("dual_feed", {}) or {}
        self.dual_feed: bool = bool(_df_cfg.get("enabled", False))
        self.signal_feed: str = str(_df_cfg.get("signals", "mexc")).lower()
        self.fill_feed: str = str(_df_cfg.get("fills", "mexc")).lower()
        if self.dual_feed:
            LOG.info("Dual-feed ON: signals=%s  fills=%s", self.signal_feed, self.fill_feed)

        # ── Threshold/qty scaling basis ──────────────────────────────────────
        # "window"  : legacy — median of the trailing fetch window (re-rolls each bar)
        # "lookback": median over trailing `lookback_days`, FROZEN per UTC day.
        #             Lookahead-free and identical in live + backtest.
        self.scaling_cfg: dict = cfg.get("scaling", {}) or {}
        self.scaling_mode: str = self.scaling_cfg.get("mode", "window")
        self.lookback_days: int = int(self.scaling_cfg.get("lookback_days", 7))

        out = Path(cfg.get("output_dir", "artifacts/fib_live"))
        out.mkdir(parents=True, exist_ok=True)
        self.state_path  = out / "bot_state.json"
        self.trades_path = out / "paper_trades.jsonl"
        self.log_path    = out / "bot.log"

        # Add file handler
        fh = logging.FileHandler(self.log_path)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOG.addHandler(fh)

        # ── Telegram alerts (entry/exit only) — purely informational ──────────
        self.tg_token: Optional[str] = None
        self.tg_chat_id: Optional[str] = None
        tg_cfg = cfg.get("telegram", {})
        if tg_cfg.get("enabled"):
            try:
                sec_file = tg_cfg["secrets_file"]
                sec_path = Path(sec_file)
                if not sec_path.is_absolute():
                    sec_path = _ROOT / sec_path
                with open(sec_path) as f:
                    self.tg_token = json.load(f)["bot_token"]
                self.tg_chat_id = str(tg_cfg["chat_id"])  # single id — alerts go only here
                LOG.info("Telegram alerts enabled → chat_id %s", self.tg_chat_id)
            except Exception as e:
                LOG.warning("Telegram disabled (config load failed: %s)", e)
                self.tg_token = self.tg_chat_id = None

        # Per-symbol state
        self.positions: Dict[str, Optional[OpenPosition]] = {s: None for s in self.symbols}
        self.pending:   Dict[str, Optional[PendingEntry]]  = {s: None for s in self.symbols}

        # Paper P&L tracker (initialised before load so _load_state can restore it)
        self.paper_pnl: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.paper_trades: Dict[str, List[dict]] = {s: [] for s in self.symbols}

        # Daily-frozen lookback reference median per symbol (scaling_mode=lookback)
        self.ref_median: Dict[str, Optional[float]] = {s: None for s in self.symbols}
        self.ref_median_day: Dict[str, Optional[str]] = {s: None for s in self.symbols}

        self._load_state()

    # ── state persistence ─────────────────────────────────────────────────────

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            with open(self.state_path) as f:
                st = json.load(f)
            saved_pnl = st.get("paper_pnl", {})
            saved_med = st.get("ref_median", {})
            saved_med_day = st.get("ref_median_day", {})
            for sym in self.symbols:
                p = st.get("positions", {}).get(sym)
                pe = st.get("pending", {}).get(sym)
                self.positions[sym] = OpenPosition.from_dict(p) if p else None
                self.pending[sym]   = PendingEntry.from_dict(pe) if pe else None
                self.paper_pnl[sym] = float(saved_pnl.get(sym, 0.0))
                self.ref_median[sym] = saved_med.get(sym)
                self.ref_median_day[sym] = saved_med_day.get(sym)
            LOG.info("State loaded from %s (cum PnL: %s)",
                     self.state_path,
                     {s: round(v, 2) for s, v in self.paper_pnl.items()})
        except Exception as e:
            LOG.warning("State load failed (%s) — starting fresh", e)

    def _save_state(self) -> None:
        st = {
            "positions": {s: (p.to_dict() if p else None) for s, p in self.positions.items()},
            "pending":   {s: (pe.to_dict() if pe else None) for s, pe in self.pending.items()},
            "paper_pnl": {s: round(v, 6) for s, v in self.paper_pnl.items()},
            "ref_median": {s: v for s, v in self.ref_median.items()},
            "ref_median_day": {s: v for s, v in self.ref_median_day.items()},
            "saved_at":  datetime.now(timezone.utc).isoformat(),
        }
        with open(self.state_path, "w") as f:
            json.dump(st, f, indent=2, default=str)

    def _log_trade(self, sym: str, rec: dict) -> None:
        with open(self.trades_path, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    def _send_telegram(self, text: str) -> None:
        """Best-effort send to the single configured chat. Never raises."""
        if not (self.tg_token and self.tg_chat_id):
            return
        try:
            _requests.post(
                f"https://api.telegram.org/bot{self.tg_token}/sendMessage",
                data={"chat_id": self.tg_chat_id, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=10,
            )
        except Exception as e:
            LOG.warning("Telegram send failed: %s", e)

    # ── reference median (daily-frozen lookback) ─────────────────────────────

    @staticmethod
    def _fetch_klines_range(symbol: str, start_ms: int, end_ms: int) -> list:
        """Fetch [start_ms, end_ms) 1-min klines from MEXC in <=500-bar chunks."""
        rows, cur = [], start_ms
        for _ in range(400):  # safety cap
            if cur >= end_ms:
                break
            r = _requests.get("https://api.mexc.com/api/v3/klines", params={
                "symbol": symbol, "interval": "1m",
                "startTime": cur, "endTime": end_ms, "limit": 500}, timeout=20)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            nxt = int(batch[-1][0]) + 60_000
            if nxt <= cur:
                break
            cur = nxt
            time.sleep(0.15)
        return rows

    @staticmethod
    def _fetch_binance_klines_range(symbol: str, start_ms: int, end_ms: int) -> list:
        """Fetch [start_ms, end_ms) 1-min klines from Binance in <=1000-bar chunks."""
        rows, cur = [], start_ms
        for _ in range(400):
            if cur >= end_ms:
                break
            r = _requests.get("https://api.binance.com/api/v3/klines", params={
                "symbol": symbol, "interval": "1m",
                "startTime": cur, "endTime": end_ms, "limit": 1000}, timeout=20)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            nxt = int(batch[-1][0]) + 60_000
            if nxt <= cur:
                break
            cur = nxt
            time.sleep(0.1)
        return rows

    def _signal_klines_range(self, symbol: str, start_ms: int, end_ms: int) -> list:
        """Range fetch from the SIGNAL feed (Binance in dual-feed, else MEXC)."""
        if self.dual_feed and self.signal_feed == "binance":
            return self._fetch_binance_klines_range(symbol, start_ms, end_ms)
        return self._fetch_klines_range(symbol, start_ms, end_ms)

    def _fetch_signal_bars(self, symbol: str, n: int = 350) -> pd.DataFrame:
        """Fetch the trailing signal-feed bars (Binance in dual-feed, else MEXC)."""
        if self.dual_feed and self.signal_feed == "binance":
            return _fetch_bars_binance(symbol, n)
        return _fetch_bars_mexc(symbol, n)

    def _mexc_marketable_price(self, symbol: str, is_buy: bool) -> Optional[float]:
        """MEXC marketable fill price: ask for a buy, bid for a sell — the price you
        actually cross. Returns None on failure (caller falls back to the signal level)."""
        try:
            r = _requests.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                              params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            d = r.json()
            return float(d["askPrice"]) if is_buy else float(d["bidPrice"])
        except Exception as e:
            LOG.warning("[%s] MEXC bookTicker fetch failed: %s", symbol, e)
            return None

    def _capture_fill(self, symbol: str, signal_price: float, is_buy: bool):
        """Return (fill_price, signal_level). In dual-feed the fill is the MEXC marketable
        price and signal_level is the signal-feed price (for basis). Else identity."""
        if not self.dual_feed:
            return signal_price, None
        mx = self._mexc_marketable_price(symbol, is_buy)
        return (mx if mx is not None else signal_price), signal_price

    def _compute_ref_median(self, symbol: str, day_start: pd.Timestamp) -> Optional[float]:
        """Median close over [day_start - lookback_days, day_start) — lookahead-free.
        Uses the SIGNAL feed so scaling is consistent with signal generation."""
        end_ms = int(day_start.timestamp() * 1000)
        start_ms = end_ms - self.lookback_days * 86_400_000
        rows = self._signal_klines_range(symbol, start_ms, end_ms)
        if not rows:
            return None
        return float(pd.Series([float(b[4]) for b in rows]).median())

    def _ensure_ref_median(self, sym: str, now_utc: pd.Timestamp) -> None:
        """Freeze the reference median for the current UTC day (recompute on rollover)."""
        if self.scaling_mode != "lookback":
            return
        day = str(now_utc.date())
        if self.ref_median_day.get(sym) == day and self.ref_median.get(sym) is not None:
            return
        med = self._compute_ref_median(sym, now_utc.normalize())
        if med is None:
            LOG.warning("[%s] ref-median fetch failed — keeping previous (%s)",
                        sym, self.ref_median.get(sym))
            return
        self.ref_median[sym] = med
        self.ref_median_day[sym] = day
        min_imp = round(med * self.strat_cfg["min_impulse_pct"] / 100, 4)
        LOG.info("[%s] ref median (%dd lookback, frozen %s UTC) = %.4f  → min_impulse=%.4f",
                 sym, self.lookback_days, day, med, min_imp)

    # ── quantity resolver ─────────────────────────────────────────────────────

    def _resolve_qty(self, df: pd.DataFrame, ref_median: Optional[float] = None) -> float:
        med = float(ref_median) if ref_median is not None else float(df["close"].median())
        return round(self.trade_value_usd / med, 6)

    # ── paper simulation helpers ──────────────────────────────────────────────

    def _paper_open(self, sym: str, pe: PendingEntry, fill_price: float, ts: str,
                    binance_level: Optional[float] = None) -> None:
        pos = OpenPosition(
            symbol=sym,
            side=pe.side,
            qty=pe.qty,
            entry_price=fill_price,
            stop_price=pe.stop_price,
            target_price=pe.target_price,
            entry_time_utc=ts,
            entry_bar_ts=pe.signal_bar_ts,
            trail_milestones=list(pe.trail_milestones),
            entry_binance=binance_level,
        )
        self.positions[sym] = pos
        self.pending[sym]   = None
        tag = "PAPER" if self.paper else "LIVE"
        LOG.info(
            "[%s OPEN] %s %s qty=%.6g @ %.4f  stop=%.4f  target=%.4f  milestones=%s",
            tag, sym, pe.side, pe.qty, fill_price, pe.stop_price, pe.target_price,
            [f"{m:.4f}" for m in pe.trail_milestones],
        )
        basis_line = ""
        if binance_level is not None:
            basis_line = f"\nsignal {binance_level:.4f} → MEXC fill {fill_price:.4f}  (basis {fill_price-binance_level:+.4f})"
        emoji = "🟢" if pe.side == "LONG" else "🔻"
        self._send_telegram(
            f"{emoji} <b>ENTRY</b> [{tag}]  <b>{sym[0]} {pe.side}</b>\n"
            f"qty: {pe.qty:.6g}  @ {fill_price:.4f}\n"
            f"stop: {pe.stop_price:.4f}   target: {pe.target_price:.4f}\n"
            f"notional: ${fill_price * pe.qty:,.0f}{basis_line}"
        )

    def _paper_close(self, sym: str, pos: OpenPosition, exit_price: float,
                     reason: str, is_partial: bool, ts: str,
                     binance_level: Optional[float] = None) -> None:
        is_long = pos.side == "LONG"
        qty = pos.qty / 2 if is_partial else pos.qty
        gross = (exit_price - pos.entry_price) * qty * (1 if is_long else -1)
        self.paper_pnl[sym] = self.paper_pnl.get(sym, 0.0) + gross

        pct_return = round(
            (exit_price / pos.entry_price - 1) * 100 * (1 if is_long else -1), 4
        ) if pos.entry_price else 0.0
        rec = {
            "ts": ts, "symbol": sym, "side": pos.side,
            "qty": qty, "entry": pos.entry_price, "exit": exit_price,
            "reason": reason, "gross_pnl": round(gross, 4),
            "cumulative_pnl": round(self.paper_pnl[sym], 4),
            "is_partial": is_partial,
            # ── context (logging only — for understanding each trade) ──
            "entry_time": pos.entry_time_utc,
            "entry_bar": pos.entry_bar_ts,
            "bars_held": pos.bars_held,
            "stop": pos.stop_price,
            "target": pos.target_price,
            "pct_return": pct_return,
            "notional_usd": round(pos.entry_price * qty, 2),
        }
        if self.dual_feed:
            # signal-feed levels vs the actual MEXC fills → the real basis/slippage
            rec["entry_binance"] = pos.entry_binance
            rec["exit_binance"] = binance_level
            rec["entry_basis"] = (round(pos.entry_price - pos.entry_binance, 4)
                                  if pos.entry_binance is not None else None)
            rec["exit_basis"] = (round(exit_price - binance_level, 4)
                                 if binance_level is not None else None)
        self.paper_trades[sym].append(rec)
        self._log_trade(sym, rec)

        tag = "PAPER" if self.paper else "LIVE"
        LOG.info(
            "[%s %s] %s %s qty=%.6g  entry=%.4f → exit=%.4f  gross=%+.4f  "
            "cum=%+.4f  reason=%s",
            tag, "PARTIAL" if is_partial else "CLOSE",
            sym, pos.side, qty, pos.entry_price, exit_price, gross,
            self.paper_pnl[sym], reason,
        )
        total_pnl = sum(self.paper_pnl.values())
        emoji = "✅" if gross >= 0 else "❌"
        kind = "PARTIAL EXIT" if is_partial else "EXIT"
        self._send_telegram(
            f"{emoji} <b>{kind}</b> [{tag}]  <b>{sym[0]} {pos.side}</b>  ({reason})\n"
            f"entry {pos.entry_price:.4f} → exit {exit_price:.4f}\n"
            f"held: {pos.bars_held} bars\n"
            f"trade PnL: <b>${gross:+,.2f}</b> ({pct_return:+.2f}%)\n"
            f"{sym[0]} cum: ${self.paper_pnl[sym]:+,.2f}\n"
            f"total PnL: <b>${total_pnl:+,.2f}</b>"
        )

        if not is_partial:
            self.positions[sym] = None
        else:
            # halve the remaining qty
            pos.qty /= 2

    # ── per-bar processing ────────────────────────────────────────────────────

    def on_bar(self, sym: str, df: pd.DataFrame) -> None:
        """Called once per bar close for each symbol."""
        if df.empty:
            return
        last_row = df.iloc[-1]
        # Time is derived from the bar (not wall-clock) so behaviour is identical
        # in live and in the backtest replay.
        bar_ts = pd.Timestamp(last_row["timestamp"])
        ts = str(bar_ts.isoformat())
        self._ensure_ref_median(sym, bar_ts)
        ref_med = self.ref_median.get(sym)

        # ── A. Manage open position ───────────────────────────────────────
        pos = self.positions[sym]
        if pos is not None:
            pos.bars_held += 1
            result = resolve_exit_on_bar(pos, last_row, self.time_exit_bars, self.use_trail)
            if result is not None:
                exit_price, reason = result
                is_partial = (reason == "partial_target")
                # exiting a SHORT = buy back (ask); exiting a LONG = sell (bid)
                fill, blevel = self._capture_fill(sym, exit_price, is_buy=(pos.side == "SHORT"))
                self._paper_close(sym, pos, fill, reason, is_partial, ts, binance_level=blevel)
            return  # don't look for new entries while in position

        # ── B. Check pending entry ────────────────────────────────────────
        pe = self.pending[sym]
        if pe is not None:
            pe.bars_waiting += 1
            hi, lo = float(last_row["high"]), float(last_row["low"])
            if self.entry_next_bar_open:
                # Fill at this bar's open (first bar after signal) — matches
                # ENTRY_NEXT_BAR_OPEN=True backtest behavior.
                if pe.bars_waiting == 1:
                    fill, blevel = self._capture_fill(sym, float(last_row["open"]),
                                                      is_buy=(pe.side == "LONG"))
                    self._paper_open(sym, pe, fill, ts, binance_level=blevel)
                else:
                    # Should not happen (max_bars_wait=1 implied), but guard anyway
                    self.pending[sym] = None
            else:
                filled = (
                    (pe.side == "LONG"  and lo  <= pe.entry_limit) or
                    (pe.side == "SHORT" and hi >= pe.entry_limit)
                )
                if filled:
                    fill, blevel = self._capture_fill(sym, pe.entry_limit,
                                                      is_buy=(pe.side == "LONG"))
                    self._paper_open(sym, pe, fill, ts, binance_level=blevel)
                elif pe.bars_waiting >= pe.max_bars_wait:
                    LOG.info("[PAPER CANCEL] %s %s entry order expired (%d bars, not filled)",
                             sym, pe.side, pe.bars_waiting)
                    self.pending[sym] = None
            return  # either just filled/cancelled, or still waiting

        # ── C. Look for new signal (no position, no pending) ─────────────
        if self.strat_cfg.get("target_ext", 1.618) <= 1.0:
            t_exit = 60
        else:
            t_exit = 90
        self.time_exit_bars = t_exit

        signal = get_signal_for_last_bar(df, self.strat_cfg, ref_median=ref_med)
        if signal is None:
            return

        qty = self._resolve_qty(df, ref_med)
        side = "LONG" if signal["direction"] == "LONG" else "SHORT"

        pe = PendingEntry(
            symbol=sym,
            side=side,
            qty=qty,
            entry_limit=signal["entry_price"],
            stop_price=signal["stop_loss"],
            target_price=signal["target"],
            signal_bar_ts=signal["timestamp"],
            signal_time_utc=ts,
            trail_milestones=signal["trail_milestones"],
            max_bars_wait=self.max_bars_wait,
        )
        self.pending[sym] = pe
        LOG.info(
            "[PAPER SIGNAL] %s %s @ limit=%.4f  stop=%.4f  target=%.4f  "
            "milestones=%s  qty=%.6g",
            sym, side, pe.entry_limit, pe.stop_price, pe.target_price,
            [f"{m:.4f}" for m in pe.trail_milestones], qty,
        )

    # ── main loop ─────────────────────────────────────────────────────────────

    def _sleep_until_bar_close(self) -> None:
        """Sleep until 2 seconds after the next 1-min bar close."""
        now = datetime.now(timezone.utc)
        secs_past = now.second + now.microsecond / 1e6
        wait = (60 - secs_past + 2) % 60
        if wait < 2:
            wait += 60
        LOG.info("Waiting %.0fs until next bar close…", wait)
        time.sleep(wait)

    def _print_status(self) -> None:
        lines = []
        for sym in self.symbols:
            pos = self.positions[sym]
            pe  = self.pending[sym]
            cum = self.paper_pnl.get(sym, 0.0)
            if pos:
                lines.append(
                    f"  {sym}: IN {pos.side} qty={pos.qty:.4g} "
                    f"entry={pos.entry_price:.4f} stop={pos.stop_price:.4f} "
                    f"target={pos.target_price:.4f}  cumPnL={cum:+.2f}"
                )
            elif pe:
                lines.append(
                    f"  {sym}: WAITING {pe.side} limit={pe.entry_limit:.4f} "
                    f"({pe.bars_waiting}/{pe.max_bars_wait} bars)  cumPnL={cum:+.2f}"
                )
            else:
                lines.append(f"  {sym}: FLAT  cumPnL={cum:+.2f}")
        LOG.info("─── Status ───\n%s", "\n".join(lines))

    def run(self) -> None:
        LOG.info("=" * 60)
        LOG.info("FibLiveBot starting  paper=%s  symbols=%s", self.paper, self.symbols)
        LOG.info("Strategy: %s  imp=%.2f%%  ext=%.3f  trail=%s",
                 self.strat_cfg.get("variant", "EMA"),
                 self.strat_cfg.get("min_impulse_pct", 0.25),
                 self.strat_cfg.get("target_ext", 1.618),
                 self.use_trail)
        LOG.info("=" * 60)

        try:
            while True:
                self._sleep_until_bar_close()
                bar_time = datetime.now(timezone.utc).strftime("%H:%M UTC")
                LOG.info("── Bar close %s ──", bar_time)

                for sym in self.symbols:
                    try:
                        df = self._fetch_signal_bars(sym, 350)
                        self.on_bar(sym, df)
                    except Exception as e:
                        LOG.error("[%s] Bar processing error: %s", sym, e)

                self._print_status()
                self._save_state()

        except KeyboardInterrupt:
            LOG.info("Stopped by user.")
            self._save_state()
            self._final_summary()

    def _final_summary(self) -> None:
        LOG.info("─── Final paper P&L ───")
        total = 0.0
        for sym in self.symbols:
            cum = self.paper_pnl.get(sym, 0.0)
            n   = len(self.paper_trades.get(sym, []))
            LOG.info("  %s: %d trades  cumPnL=$%+.2f", sym, n, cum)
            total += cum
        LOG.info("  TOTAL: $%+.2f", total)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="BOS+Fib live/paper bot for MEXC")
    ap.add_argument("--config", required=True, help="Path to fib_live.json config")
    ap.add_argument("--live", action="store_true", help="Live mode (default: paper)")
    args = ap.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    paper = not args.live
    broker = None

    if not paper:
        # import here to keep paper-only runs lightweight
        from common.broker.mexc_spot_client import MexcSpotClient
        sec_path = Path(args.config).parent / cfg["mexc"]["secrets_file"]
        with open(sec_path) as f:
            sec = json.load(f)
        broker = MexcSpotClient(
            api_key=sec["api_key"],
            api_secret=sec["api_secret"],
        )
        LOG.info("Live mode: MEXC broker connected")
    else:
        LOG.info("Paper mode: no real orders will be placed")

    bot = FibLiveBot(cfg, paper=paper, broker=broker)
    bot.run()


if __name__ == "__main__":
    main()
