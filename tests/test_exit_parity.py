"""
Drift-guard: the exit logic is implemented twice — once in the research
backtester (`IntradayBacktester._resolve_exit`) and once, hand-written, in the
live bot (`scripts/live_fib_bot.resolve_exit_on_bar`). They MUST stay in lock-step
or live ≠ backtest.

This test drives identical (position, bar) scenarios through BOTH and asserts the
returned (exit_price, reason) AND the state mutations (stop_price, stop_pending,
trail_milestone_idx, partial flag) match.

Notes:
  * The backtester is run with continuous_session=True (24h/crypto), which skips
    the carry-over guard + force-square-off, isolating the shared exit core.
  * The one implementation difference is the time-exit metric: live counts BARS
    (`bars_held`), the backtester counts WALL-CLOCK minutes
    (`bar.timestamp - entry_time`). On contiguous 1-min bars they're equal; the
    test aligns them by setting entry_time = bar_ts - bars_held minutes.
  * trail-stop reason ("trail_stop" vs "stop_loss") is only guarded by use_trail
    in the live copy, but trail_milestone_idx>0 is unreachable with trailing off,
    so every trailing scenario here pairs idx>0 with use_trail=True (realistic).
"""
from __future__ import annotations

import sys
from datetime import time as dt_time, timedelta
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(_ROOT / "src"), str(_ROOT), str(_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from intraday_research.backtester import IntradayBacktester
from intraday_research.types import Position, SignalSide
from live_fib_bot import OpenPosition, resolve_exit_on_bar

TS = pd.Timestamp("2026-06-30 12:00:00", tz="UTC")
T_EXIT = 90  # time-exit threshold (bars == minutes here)


def _sc(name, side, stop, target, milestones, idx, hi, lo, close, *,
        stop_pending=None, partial=False, bars_held=5, use_trail=True):
    return dict(name=name, side=side, stop=stop, target=target, milestones=milestones,
                idx=idx, hi=hi, lo=lo, close=close, stop_pending=stop_pending,
                partial=partial, bars_held=bars_held, use_trail=use_trail)


# stop=stop_price, target=target_price, entry_price fixed at 100.0
SCENARIOS = [
    _sc("long_stop_loss",     "LONG",  98.0, 110.0, [],            0, 101.0, 97.9, 100.0, use_trail=False),
    _sc("short_stop_loss",    "SHORT", 102.0, 90.0, [],            0, 102.1, 99.0, 100.0, use_trail=False),
    _sc("long_trail_stop",    "LONG",  101.0, 120.0, [103.0,105.0], 1, 101.5, 100.9, 101.0),
    _sc("short_trail_stop",   "SHORT",  99.0,  80.0, [97.0,95.0],   1, 99.1,  98.5,  99.0),
    _sc("pending_then_stop",  "LONG",   98.0, 120.0, [103.0],       1, 101.0, 100.4, 100.5, stop_pending=100.5),
    _sc("long_milestone_adv", "LONG",   98.0, 120.0, [102.0,106.0], 0, 102.5,  99.0, 102.0),
    _sc("long_partial_target","LONG",   98.0, 110.0, [105.0,110.0], 1, 110.5, 100.0, 110.0),
    _sc("multi_milestone_bar","LONG",   98.0, 200.0, [102.0,104.0,106.0], 0, 107.0, 99.0, 106.0),
    _sc("long_full_target",   "LONG",   98.0, 110.0, [],            0, 110.5, 100.0, 110.0, use_trail=False),
    _sc("short_full_target",  "SHORT", 102.0,  90.0, [],            0, 101.0,  89.5,  90.0, use_trail=False),
    _sc("time_exit",          "LONG",   90.0, 200.0, [],            0, 105.0,  95.0, 101.0, bars_held=90, use_trail=False),
    _sc("no_exit",            "LONG",   90.0, 200.0, [],            0, 105.0,  95.0, 100.0, use_trail=False),
]


def _live_pos(sc):
    return OpenPosition(
        symbol="X", side=sc["side"], qty=1.0, entry_price=100.0,
        stop_price=sc["stop"], target_price=sc["target"],
        entry_time_utc=str(TS), entry_bar_ts=str(TS), bars_held=sc["bars_held"],
        trail_milestones=list(sc["milestones"]), trail_milestone_idx=sc["idx"],
        stop_pending=sc["stop_pending"], partial_done=sc["partial"],
    )


def _bt_pos(sc):
    return Position(
        symbol="X", side=SignalSide(sc["side"]), quantity=1.0, effective_quantity=1.0,
        lot_size=1, entry_time=TS - timedelta(minutes=sc["bars_held"]), entry_price=100.0,
        stop_price=sc["stop"], target_price=sc["target"], entry_reason="t", strategy_name="t",
        trail_milestones=list(sc["milestones"]), trail_milestone_idx=sc["idx"],
        stop_pending=sc["stop_pending"], partial_exit_done=sc["partial"],
    )


def _row(sc):
    return pd.Series({"high": sc["hi"], "low": sc["lo"], "close": sc["close"], "timestamp": TS})


def _eq(a, b):
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) < 1e-9


@pytest.mark.parametrize("sc", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
def test_exit_parity(sc):
    live_p, bt_p, row = _live_pos(sc), _bt_pos(sc), _row(sc)

    live_out = resolve_exit_on_bar(live_p, row, T_EXIT, sc["use_trail"])
    bt_out = IntradayBacktester._resolve_exit(
        bt_p, row, T_EXIT, timedelta(minutes=1), dt_time(23, 59), sc["use_trail"], True)

    # returned decision
    assert (live_out is None) == (bt_out is None), f"{sc['name']}: one exited, other didn't"
    if live_out is not None:
        assert _eq(live_out[0], bt_out[0]), f"{sc['name']}: exit price {live_out[0]} != {bt_out[0]}"
        assert live_out[1] == bt_out[1], f"{sc['name']}: reason {live_out[1]} != {bt_out[1]}"

    # The backtester sets partial_exit_done in its CALLER (run loop, ~line 190) after a
    # partial_target, whereas the live resolver sets it inline. Both end the bar with the
    # flag True — mirror the backtester's caller so we compare END-OF-BAR state, not
    # intra-function state. (This is the one deliberate structural difference; documented.)
    if bt_out is not None and bt_out[1] == "partial_target":
        bt_p.partial_exit_done = True

    # state mutations
    assert _eq(live_p.stop_price, bt_p.stop_price), f"{sc['name']}: stop_price drift"
    assert _eq(live_p.stop_pending, bt_p.stop_pending), f"{sc['name']}: stop_pending drift"
    assert live_p.trail_milestone_idx == bt_p.trail_milestone_idx, f"{sc['name']}: idx drift"
    assert live_p.partial_done == bt_p.partial_exit_done, f"{sc['name']}: partial flag drift"
