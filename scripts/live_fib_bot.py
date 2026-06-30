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


# ─────────────────────────────────────────────────────────────────────────────
# Signal extraction
# ─────────────────────────────────────────────────────────────────────────────

def _make_strategy(cfg: dict, med_price: float) -> NiftyBOSFibScalpStrategy:
    min_imp = round(med_price * cfg["min_impulse_pct"] / 100, 4)
    stop_buf = round(max(med_price * cfg.get("stop_buffer_pct", 0.009) / 100, 0.05), 4)
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
    )


_FE = FeatureEngine()


def get_signal_for_last_bar(df: pd.DataFrame, strat_cfg: dict) -> Optional[dict]:
    """
    Run strategy on `df` and return the signal row for the LAST closed bar,
    or None if no signal.
    """
    if df.empty or len(df) < 20:
        return None

    med = float(df["close"].median())
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

        out = Path(cfg.get("output_dir", "artifacts/fib_live"))
        out.mkdir(parents=True, exist_ok=True)
        self.state_path  = out / "bot_state.json"
        self.trades_path = out / "paper_trades.jsonl"
        self.log_path    = out / "bot.log"

        # Add file handler
        fh = logging.FileHandler(self.log_path)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOG.addHandler(fh)

        # Per-symbol state
        self.positions: Dict[str, Optional[OpenPosition]] = {s: None for s in self.symbols}
        self.pending:   Dict[str, Optional[PendingEntry]]  = {s: None for s in self.symbols}
        self._load_state()

        # Paper P&L tracker
        self.paper_pnl: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.paper_trades: Dict[str, List[dict]] = {s: [] for s in self.symbols}

    # ── state persistence ─────────────────────────────────────────────────────

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            with open(self.state_path) as f:
                st = json.load(f)
            for sym in self.symbols:
                p = st.get("positions", {}).get(sym)
                pe = st.get("pending", {}).get(sym)
                self.positions[sym] = OpenPosition.from_dict(p) if p else None
                self.pending[sym]   = PendingEntry.from_dict(pe) if pe else None
            LOG.info("State loaded from %s", self.state_path)
        except Exception as e:
            LOG.warning("State load failed (%s) — starting fresh", e)

    def _save_state(self) -> None:
        st = {
            "positions": {s: (p.to_dict() if p else None) for s, p in self.positions.items()},
            "pending":   {s: (pe.to_dict() if pe else None) for s, pe in self.pending.items()},
            "saved_at":  datetime.now(timezone.utc).isoformat(),
        }
        with open(self.state_path, "w") as f:
            json.dump(st, f, indent=2, default=str)

    def _log_trade(self, sym: str, rec: dict) -> None:
        with open(self.trades_path, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    # ── quantity resolver ─────────────────────────────────────────────────────

    def _resolve_qty(self, df: pd.DataFrame) -> float:
        med = float(df["close"].median())
        return round(self.trade_value_usd / med, 6)

    # ── paper simulation helpers ──────────────────────────────────────────────

    def _paper_open(self, sym: str, pe: PendingEntry, fill_price: float, ts: str) -> None:
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
        )
        self.positions[sym] = pos
        self.pending[sym]   = None
        tag = "PAPER" if self.paper else "LIVE"
        LOG.info(
            "[%s OPEN] %s %s qty=%.6g @ %.4f  stop=%.4f  target=%.4f  milestones=%s",
            tag, sym, pe.side, pe.qty, fill_price, pe.stop_price, pe.target_price,
            [f"{m:.4f}" for m in pe.trail_milestones],
        )

    def _paper_close(self, sym: str, pos: OpenPosition, exit_price: float,
                     reason: str, is_partial: bool, ts: str) -> None:
        is_long = pos.side == "LONG"
        qty = pos.qty / 2 if is_partial else pos.qty
        gross = (exit_price - pos.entry_price) * qty * (1 if is_long else -1)
        self.paper_pnl[sym] = self.paper_pnl.get(sym, 0.0) + gross

        rec = {
            "ts": ts, "symbol": sym, "side": pos.side,
            "qty": qty, "entry": pos.entry_price, "exit": exit_price,
            "reason": reason, "gross_pnl": round(gross, 4),
            "cumulative_pnl": round(self.paper_pnl[sym], 4),
            "is_partial": is_partial,
        }
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
        ts = str(datetime.now(timezone.utc).isoformat())

        # ── A. Manage open position ───────────────────────────────────────
        pos = self.positions[sym]
        if pos is not None:
            pos.bars_held += 1
            result = resolve_exit_on_bar(pos, last_row, self.time_exit_bars, self.use_trail)
            if result is not None:
                exit_price, reason = result
                is_partial = (reason == "partial_target")
                self._paper_close(sym, pos, exit_price, reason, is_partial, ts)
            return  # don't look for new entries while in position

        # ── B. Check pending entry ────────────────────────────────────────
        pe = self.pending[sym]
        if pe is not None:
            pe.bars_waiting += 1
            hi, lo = float(last_row["high"]), float(last_row["low"])
            filled = (
                (pe.side == "LONG"  and lo  <= pe.entry_limit) or
                (pe.side == "SHORT" and hi >= pe.entry_limit)
            )
            if filled:
                self._paper_open(sym, pe, pe.entry_limit, ts)
            elif pe.bars_waiting >= pe.max_bars_wait:
                LOG.info("[PAPER CANCEL] %s %s entry order expired (%d bars, not filled)",
                         sym, pe.side, pe.bars_waiting)
                self.pending[sym] = None
            return  # either just filled, or still waiting

        # ── C. Look for new signal (no position, no pending) ─────────────
        if self.strat_cfg.get("target_ext", 1.618) <= 1.0:
            t_exit = 60
        else:
            t_exit = 90
        self.time_exit_bars = t_exit

        signal = get_signal_for_last_bar(df, self.strat_cfg)
        if signal is None:
            return

        qty = self._resolve_qty(df)
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
            max_bars_wait=3,
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
                        df = _fetch_bars_mexc(sym, n=350)
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
