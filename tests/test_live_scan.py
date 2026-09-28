"""Tests for the live-scanner infrastructure (sma44_level1_intraday/live/):
entry-trigger / exit resolution reused from the backtest engine, the
running-day-bar reconstruction helper, config/state I/O, and that the
notebook-config loader can actually parse the real notebook.
"""
from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
import pytest

from sma44_level1_intraday.config.schema import StrategyConfig
from sma44_level1_intraday.live.engine import check_fixed_exit, check_trigger, compute_qty
from sma44_level1_intraday.live.notebook_config import load_live_scan_setup
from sma44_level1_intraday.live.state import (
    append_trade_log, read_open_positions, read_watchlist, write_open_positions, write_watchlist,
)


# ── check_trigger (thin wrapper over ScannerBacktester._resolve_pending_trigger) ──

def test_check_trigger_long_triggers_on_high_breach():
    sig = {"direction": "LONG", "trigger_price": 101.0, "signal_low": 99.0, "signal_high": 101.0}
    triggered, invalidated = check_trigger(sig, {"open": 100.0, "high": 101.5, "low": 100.0, "close": 101.2})
    assert (triggered, invalidated) == (True, False)


def test_check_trigger_long_invalidates_on_low_breach():
    sig = {"direction": "LONG", "trigger_price": 101.0, "signal_low": 99.0, "signal_high": 101.0}
    triggered, invalidated = check_trigger(sig, {"open": 100.0, "high": 100.5, "low": 98.5, "close": 99.0})
    assert (triggered, invalidated) == (False, True)


def test_check_trigger_neither_when_price_still_inside_range():
    sig = {"direction": "LONG", "trigger_price": 101.0, "signal_low": 99.0, "signal_high": 101.0}
    triggered, invalidated = check_trigger(sig, {"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.2})
    assert (triggered, invalidated) == (False, False)


# ── check_fixed_exit ───────────────────────────────────────────────────────

def test_check_fixed_exit_hits_target_long():
    exit_info = check_fixed_exit("LONG", stop_loss=95.0, target=110.0,
                                  running_bar={"open": 100, "high": 111, "low": 99, "close": 110.5},
                                  same_candle_priority="use_ohlc_path")
    assert exit_info == (110.0, "target")


def test_check_fixed_exit_hits_stop_short():
    exit_info = check_fixed_exit("SHORT", stop_loss=105.0, target=90.0,
                                  running_bar={"open": 100, "high": 106, "low": 99, "close": 104},
                                  same_candle_priority="use_ohlc_path")
    assert exit_info == (105.0, "sl")


def test_check_fixed_exit_none_when_neither_touched():
    exit_info = check_fixed_exit("LONG", stop_loss=95.0, target=110.0,
                                  running_bar={"open": 100, "high": 102, "low": 99, "close": 101},
                                  same_candle_priority="use_ohlc_path")
    assert exit_info is None


def test_check_fixed_exit_same_bar_ambiguity_uses_bullish_bar_assumption():
    # Bullish running bar (close >= open) -> assumed low-then-high path ->
    # for LONG that means stop (low side) was hit first.
    exit_info = check_fixed_exit("LONG", stop_loss=95.0, target=105.0,
                                  running_bar={"open": 100, "high": 106, "low": 94, "close": 101},
                                  same_candle_priority="use_ohlc_path")
    assert exit_info == (95.0, "sl")


def test_check_fixed_exit_respects_explicit_priority_override():
    exit_info = check_fixed_exit("LONG", stop_loss=95.0, target=105.0,
                                  running_bar={"open": 100, "high": 106, "low": 94, "close": 101},
                                  same_candle_priority="target_first")
    assert exit_info == (105.0, "target")


# ── compute_qty ────────────────────────────────────────────────────────────

def test_compute_qty_capital_based_ignores_equity_when_amount_is_fixed():
    cfg = StrategyConfig.from_dict({
        "risk": {"position_sizing_mode": "capital_based", "capital_per_trade_amount": 200_000},
    })
    qty = compute_qty(cfg, {"trigger_price": 250.0, "stop_loss": 240.0}, current_equity=999.0)
    assert qty == 800.0  # 200_000 / 250, floored to whole units


# ── state I/O ──────────────────────────────────────────────────────────────

def test_write_and_read_watchlist_roundtrip(tmp_path, monkeypatch):
    import sma44_level1_intraday.live.state as state
    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "WATCHLIST_LATEST", tmp_path / "watchlist_latest.json")
    monkeypatch.setattr(state, "WATCHLIST_HISTORY_DIR", tmp_path / "watchlist_history")
    monkeypatch.setattr(state, "OPEN_POSITIONS", tmp_path / "open_positions.json")
    monkeypatch.setattr(state, "TRADE_LOG_CSV", tmp_path / "live_trade_log.csv")

    rows = [{"symbol": "NSE:TEST-EQ", "direction": "LONG", "trigger_price": 100.0}]
    write_watchlist(rows, generated_for_date="2026-09-28", generated_at="2026-09-27T18:30:00")
    wl = read_watchlist()
    assert wl["generated_for_date"] == "2026-09-28"
    assert wl["setups"] == rows
    assert (tmp_path / "watchlist_history" / "2026-09-28.json").exists()

    write_open_positions({"NSE:TEST-EQ": {"direction": "LONG", "entry_price": 101.0}})
    assert read_open_positions()["NSE:TEST-EQ"]["entry_price"] == 101.0

    append_trade_log({"event": "entry", "symbol": "NSE:TEST-EQ", "direction": "LONG"})
    append_trade_log({"event": "exit", "symbol": "NSE:TEST-EQ", "direction": "LONG", "net_pnl": 123.45})
    logged = pd.read_csv(state.TRADE_LOG_CSV)
    assert list(logged["event"]) == ["entry", "exit"]
    assert logged.iloc[1]["net_pnl"] == 123.45


def test_read_watchlist_empty_when_no_file(tmp_path, monkeypatch):
    import sma44_level1_intraday.live.state as state
    monkeypatch.setattr(state, "WATCHLIST_LATEST", tmp_path / "nope.json")
    wl = read_watchlist()
    assert wl["setups"] == []


# ── live poller's running-bar reconstruction + market-hours gate ───────────

def test_update_running_bar_tracks_running_high_low_close():
    from sma44_level1_intraday.scripts.live_scan_poll import _update_running_bar

    bars: dict = {}
    bar = _update_running_bar(bars, "NSE:TEST-EQ", 100.0)
    assert bar == {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}
    bar = _update_running_bar(bars, "NSE:TEST-EQ", 105.0)
    assert bar == {"open": 100.0, "high": 105.0, "low": 100.0, "close": 105.0}
    bar = _update_running_bar(bars, "NSE:TEST-EQ", 98.0)
    assert bar == {"open": 100.0, "high": 105.0, "low": 98.0, "close": 98.0}


def test_running_bars_persist_across_separate_processes_via_disk(tmp_path, monkeypatch):
    # Simulates two SEPARATE cron-triggered processes polling 5 minutes
    # apart -- state.py's read/write_running_bars is what makes the day's
    # high/low actually accumulate across them (a plain Python variable
    # would not survive between two real `python -m ...` invocations).
    import sma44_level1_intraday.live.state as state
    from sma44_level1_intraday.scripts.live_scan_poll import _update_running_bar

    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "RUNNING_BARS", tmp_path / "running_bars.json")

    bars = state.read_running_bars("2026-09-28")
    _update_running_bar(bars, "NSE:TEST-EQ", 100.0)
    state.write_running_bars("2026-09-28", bars)

    # Fresh read, as a new process would do.
    bars2 = state.read_running_bars("2026-09-28")
    bar = _update_running_bar(bars2, "NSE:TEST-EQ", 116.0)
    assert bar == {"open": 100.0, "high": 116.0, "low": 100.0, "close": 116.0}

    # A new calendar date discards yesterday's state.
    bars3 = state.read_running_bars("2026-09-29")
    assert bars3 == {}


def test_in_market_hours_rejects_weekend_and_outside_window():
    from sma44_level1_intraday.scripts.live_scan_poll import _in_market_hours

    cfg = StrategyConfig.from_dict({}).market
    saturday_in_window = datetime(2026, 9, 26, 10, 0)   # a Saturday
    assert _in_market_hours(saturday_in_window, cfg) is False
    weekday_before_open = datetime(2026, 9, 28, 9, 0)   # Monday, before 09:15
    assert _in_market_hours(weekday_before_open, cfg) is False
    weekday_mid_session = datetime(2026, 9, 28, 11, 0)
    assert _in_market_hours(weekday_mid_session, cfg) is True


def test_in_market_hours_ignores_continuous_session_flag():
    # continuous_session is a BACKTEST bar-granularity flag (daily/weekly
    # bars skip intraday time gating there) -- the live poller must still
    # gate on the real wall clock even when the notebook sets it True.
    from sma44_level1_intraday.scripts.live_scan_poll import _in_market_hours

    cfg = StrategyConfig.from_dict({"market": {"continuous_session": True}}).market
    midnight = datetime(2026, 9, 28, 0, 30)
    assert _in_market_hours(midnight, cfg) is False


# ── notebook_config loader against the REAL notebook ────────────────────────

def test_load_live_scan_setup_parses_the_real_notebook():
    setup = load_live_scan_setup(end_date="2026-09-28")
    assert setup.end_date == "2026-09-28"
    assert setup.timeframe
    assert setup.universe_file.exists()
    assert isinstance(setup.config, StrategyConfig)
    assert setup.config.scanner.ma_lengths()  # at least one signal MA configured


# ── poll_once integration: entry then exit across two separate calls ──────

def test_poll_once_triggers_entry_then_exits_at_target_without_re_entering(tmp_path, monkeypatch):
    import sma44_level1_intraday.live.state as state
    import sma44_level1_intraday.scripts.live_scan_poll as poll

    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "WATCHLIST_LATEST", tmp_path / "watchlist_latest.json")
    monkeypatch.setattr(state, "WATCHLIST_HISTORY_DIR", tmp_path / "watchlist_history")
    monkeypatch.setattr(state, "OPEN_POSITIONS", tmp_path / "open_positions.json")
    monkeypatch.setattr(state, "TRADE_LOG_CSV", tmp_path / "live_trade_log.csv")
    monkeypatch.setattr(state, "RUNNING_BARS", tmp_path / "running_bars.json")
    monkeypatch.setattr("sma44_level1_intraday.live.telegram.send_telegram", lambda *a, **k: True)
    monkeypatch.setattr(poll, "send_telegram", lambda *a, **k: True)

    state.write_watchlist(
        [{"symbol": "NSE:FAKEA-EQ", "direction": "LONG", "trigger_price": 101.0,
          "stop_loss": 95.0, "target": 115.0, "signal_low": 99.0, "signal_high": 101.0,
          "ma_length": 44, "ma_alignment": "A", "setup_quality": "BEST"}],
        generated_for_date="2026-09-28", generated_at="2026-09-27T18:30:00",
    )

    cfg = StrategyConfig.from_dict({"risk": {"position_sizing_mode": "capital_based", "capital_per_trade_amount": 200_000}})

    class FakeSetup:
        config = cfg

    class FakeClient:
        def __init__(self, prices):
            self.prices = prices

        def get_ltps(self, symbols):
            return {s: self.prices[s] for s in symbols if s in self.prices}

    # Poll 1 (10:00): price breaks trigger -> opens a position.
    poll.poll_once(FakeClient({"NSE:FAKEA-EQ": 102.0}), FakeSetup(), now_ist=datetime(2026, 9, 28, 10, 0))
    pos = state.read_open_positions()
    assert pos["NSE:FAKEA-EQ"]["entry_price"] == 101.0
    assert state.read_watchlist()["setups"] == []   # consumed, not re-checkable

    # Poll 2 (10:05), a SEPARATE call (simulating a separate cron process):
    # price now clears target -> should exit, and must NOT re-enter.
    poll.poll_once(FakeClient({"NSE:FAKEA-EQ": 116.0}), FakeSetup(), now_ist=datetime(2026, 9, 28, 10, 5))
    pos = state.read_open_positions()
    assert "NSE:FAKEA-EQ" not in pos

    log = pd.read_csv(state.TRADE_LOG_CSV)
    assert list(log["event"]) == ["entry", "exit"]
    assert log.iloc[1]["exit_reason"] == "target"
    assert log.iloc[1]["exit_price"] == 115.0
