#!/usr/bin/env python3
"""
grid_tick_sim.py — Simulate pct_ladder grid bot on real tick data with dead-time modelling.

Dead time: after each fill, the live bot cancels all orders + re-places new ones.
This takes ~5 seconds during which NO orders are live → crossings are MISSED.
This is the key effect that makes tick simulation different from candle simulation.

Usage:
    cd /root/trading_bot
    python3 scripts/grid_tick_sim.py
    python3 scripts/grid_tick_sim.py --dead-time 3   # 3s dead time
    python3 scripts/grid_tick_sim.py --dir backtest/ticks --from 2026-02-10
"""
from __future__ import annotations

import argparse
import math
import sys
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────────────

BAND_WIDTH   = 100.0
QTY_STEP     = 0.000001
MIN_QTY      = 0.000001
TOTAL_EQUITY = 50_000.0   # USDC starting portfolio (50% cash / 50% ETH)

# Base config: 0.4% / $5000 step — all others scale proportionally
BASE_PCT  = 0.40
BASE_STEP = 5_000.0

# Grid configs to test: (pct %, step $, pro_levels, label)
#
# With real tick data, pro_levels is not needed — price moves tick by tick so
# sequential fills happen naturally with dead time between them.
# We use 1 level per side for all configs and the same $5,000 capital per fill,
# purely comparing the effect of grid spacing on PnL.

# Step scales proportionally with pct so crash coverage is identical for all configs:
#   step = BASE_STEP × (pct / BASE_PCT)
# This means runway = (total_usdc / step) × pct is constant across all configs.

BASE_PCT  = 0.40
BASE_STEP = 5_000.0

GRID_CONFIGS = [
    (pct, BASE_STEP * pct / BASE_PCT, 1, f"{pct:.2f}% / ${BASE_STEP * pct / BASE_PCT:,.0f}")
    for pct in [0.10, 0.20, 0.40, 0.60, 0.80, 1.00]
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _band_mid(price: float) -> float:
    return math.floor(price / BAND_WIDTH) * BAND_WIDTH + BAND_WIDTH / 2.0


def _round_qty(qty: float) -> float:
    q = math.floor(qty / QTY_STEP) * QTY_STEP
    return q if q >= MIN_QTY else 0.0


def _buy_qty(level_price: float, buy_quote: float) -> float:
    return _round_qty(buy_quote / _band_mid(level_price))


def _sell_qty(level_price: float, sell_quote: float) -> float:
    return _round_qty(sell_quote / _band_mid(level_price))


# ── Simulation state ───────────────────────────────────────────────────────────

@dataclass
class GridState:
    pct:        float
    step:       float
    pro_levels: int
    label:      str

    # Portfolio
    cash:     float = 0.0
    eth_qty:  float = 0.0
    ref:      Optional[float] = None

    # Dead-time (milliseconds)
    dead_until_ms: int = 0

    # LIFO stack for cycle PnL: list of [qty, price]
    lifo_stack: List = field(default_factory=list)

    # Metrics
    total_fills:  int   = 0
    buy_fills:    int   = 0
    sell_fills:   int   = 0
    cycles:       int   = 0
    lifo_pnl:     float = 0.0
    dead_skipped: int   = 0   # fill EVENTS missed due to dead time (first crossing only)
    _was_in_dead: bool  = False  # track transitions into dead time

    first_ts: Optional[int] = None
    last_ts:  Optional[int] = None
    n_days:   float = 0.0

    def init_balanced(self, price: float) -> None:
        """Start 50/50 cash/ETH at given price."""
        self.cash    = TOTAL_EQUITY / 2.0
        self.eth_qty = (TOTAL_EQUITY / 2.0) / price
        self.ref     = price
        self.lifo_stack = [[self.eth_qty, price]]

    @property
    def pct_frac(self) -> float:
        return self.pct / 100.0


# ── Core tick processor ────────────────────────────────────────────────────────

def process_tick(s: GridState, ts_ms: int, price: float) -> None:
    if s.first_ts is None:
        s.first_ts = ts_ms
        s.init_balanced(price)
    s.last_ts = ts_ms

    ref = s.ref
    if ref is None:
        return

    # ── Check if in dead time ──────────────────────────────────────────────
    in_dead = ts_ms < s.dead_until_ms
    if not in_dead:
        s._was_in_dead = False  # reset so next dead window counts fresh

    # ── Sell levels (price went UP) ────────────────────────────────────────
    for k in range(s.pro_levels, 0, -1):
        sell_lvl = ref * (1.0 + k * s.pct_frac)
        if price >= sell_lvl:
            if in_dead:
                # Count only the FIRST tick that crosses during this dead window
                if not s._was_in_dead:
                    s.dead_skipped += 1
                    s._was_in_dead = True
                return
            qty = _sell_qty(sell_lvl, s.step)
            qty = min(qty, s.eth_qty)
            if qty <= 0:
                return

            # Apply sell fill
            s.cash    += qty * sell_lvl
            s.eth_qty -= qty

            # LIFO cycle PnL
            remaining = qty
            i = len(s.lifo_stack) - 1
            while i >= 0 and remaining > 0:
                entry = s.lifo_stack[i]
                if sell_lvl > entry[1]:
                    take = min(remaining, entry[0])
                    s.lifo_pnl += take * (sell_lvl - entry[1])
                    s.cycles   += 1
                    entry[0]   -= take
                    remaining  -= take
                    if entry[0] <= 0:
                        s.lifo_stack.pop(i)
                    break   # LIFO: only match one lot per sell event
                i -= 1

            s.ref          = sell_lvl
            s.dead_until_ms = ts_ms + DEAD_TIME_MS
            s.total_fills  += 1
            s.sell_fills   += 1
            return   # one fill per tick

    # ── Buy levels (price went DOWN) ──────────────────────────────────────
    for k in range(s.pro_levels, 0, -1):
        buy_lvl = ref * (1.0 - k * s.pct_frac)
        if price <= buy_lvl:
            if in_dead:
                if not s._was_in_dead:
                    s.dead_skipped += 1
                    s._was_in_dead = True
                return
            qty  = _buy_qty(buy_lvl, s.step)
            cost = qty * buy_lvl
            if cost > s.cash or qty <= 0:
                return

            # Apply buy fill
            s.cash    -= cost
            s.eth_qty += qty
            s.lifo_stack.append([qty, buy_lvl])

            s.ref          = buy_lvl
            s.dead_until_ms = ts_ms + DEAD_TIME_MS
            s.total_fills  += 1
            s.buy_fills    += 1
            return   # one fill per tick


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    global DEAD_TIME_MS

    ap = argparse.ArgumentParser()
    ap.add_argument("--dir",       default="backtest/ticks",
                    help="Directory with tick parquet files")
    ap.add_argument("--from",      dest="date_from", default=None,
                    help="Start date YYYY-MM-DD (default: all files)")
    ap.add_argument("--to",        dest="date_to",   default=None,
                    help="End date YYYY-MM-DD (default: all files)")
    ap.add_argument("--dead-time", type=float, default=0.0,
                    help="Dead time after each fill in seconds (default: 0 = no restriction)")
    ap.add_argument("--source",    default="binance",
                    help="File prefix filter: 'binance' or 'mexc' (default: binance)")
    args = ap.parse_args()

    DEAD_TIME_MS = int(args.dead_time * 1000)

    tick_dir = Path(args.dir)
    files = sorted(f for f in tick_dir.glob(f"{args.source}_ETHUS*.parquet"))

    # Date filter
    if args.date_from:
        files = [f for f in files if str(f.name[len(args.source)+9:len(args.source)+9+10]) >= args.date_from]
    if args.date_to:
        files = [f for f in files if str(f.name[len(args.source)+9:len(args.source)+9+10]) <= args.date_to]

    if not files:
        print("No files found. Check --dir and --source.")
        sys.exit(1)

    print(f"Dead time: {args.dead_time}s ({DEAD_TIME_MS}ms)")
    print(f"Files    : {len(files)} days ({files[0].name} → {files[-1].name})")
    print(f"Equity   : ${TOTAL_EQUITY:,.0f} (50% cash / 50% ETH at first tick price)")
    print(f"Configs  : {len(GRID_CONFIGS)}")
    print()

    # Initialise one GridState per config
    states = [
        GridState(pct=pct, step=step, pro_levels=lvls, label=label)
        for pct, step, lvls, label in GRID_CONFIGS
    ]

    total_ticks = 0
    n_days = 0

    for fpath in files:
        df = pd.read_parquet(fpath, columns=["ts", "price"])
        df["ts"]    = df["ts"].astype("int64")
        df["price"] = df["price"].astype("float64")

        n = len(df)
        total_ticks += n
        n_days += 1

        print(f"  {fpath.name}  {n:>9,} ticks", end="\r")

        # Process ticks — iterate once, fan out to all configs
        ts_arr    = df["ts"].values
        price_arr = df["price"].values

        for i in range(len(ts_arr)):
            ts_ms = int(ts_arr[i])
            price = float(price_arr[i])
            for s in states:
                process_tick(s, ts_ms, price)

    print(f"\n  Done. {total_ticks:,} ticks over {n_days} days.\n")

    # ── Results ────────────────────────────────────────────────────────────
    for s in states:
        days = n_days if n_days > 0 else 1

    # Markdown table
    header = f"{'Grid':>18}  {'Fills/day':>10}  {'Cycles/day':>11}  {'PnL/cycle':>10}  {'PnL/day':>9}  {'Total PnL':>10}  {'Dead-skipped/day':>17}"
    print(header)
    print("─" * len(header))

    best_pnl = max(s.lifo_pnl for s in states)

    for s in states:
        days = n_days if n_days > 0 else 1
        fills_pd  = s.total_fills / days
        cycles_pd = s.cycles / days
        pnl_pd    = s.lifo_pnl / days
        pnl_cyc   = s.lifo_pnl / s.cycles if s.cycles > 0 else 0.0
        skipped_pd = s.dead_skipped / days
        rel       = s.lifo_pnl / best_pnl * 100 if best_pnl > 0 else 0.0
        marker    = " ◄ BEST" if s.lifo_pnl == best_pnl else f" ({rel:.0f}% of best)"

        print(
            f"{s.label:>18}  "
            f"{fills_pd:>10.1f}  "
            f"{cycles_pd:>11.1f}  "
            f"${pnl_cyc:>9.2f}  "
            f"${pnl_pd:>8.2f}  "
            f"${s.lifo_pnl:>9.2f}  "
            f"{skipped_pd:>17.1f}"
            f"{marker}"
        )

    print()
    print("Notes:")
    print(f"  - Dead time = {args.dead_time}s after each fill (cancel + replace API round trip)")
    print(f"  - Step scales with pct: BASE_STEP=${BASE_STEP:,.0f} at {BASE_PCT}% → same crash coverage")
    print(f"  - LIFO cycle PnL: only profitable buy→sell pairs counted")
    print(f"  - No rebalancing modelled (pure grid fills only)")
    print(f"  - 1 order per side, sequential fills via real tick price movement")
    print(f"  - Same ${BASE_STEP:,.0f} capital per fill for all configs (pure pct comparison)")


if __name__ == "__main__":
    main()
