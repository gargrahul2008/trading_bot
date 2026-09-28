"""Tests for the generic infrastructure that survives the 44-SMA scanner
rewrite: OHLC resampling (resample.py), the ATR Chandelier trailing stop
(exits.py), and the backtest execution engine's trade-lifecycle mechanics
(backtest/engine.py) — position sizing, trade limits, same-candle priority,
force-exit, daily-loss breaker, costs. None of this is "44 MA filtering
logic"; it is what happens to a candidate signal once one has fired, and is
unchanged by the scanner rewrite.

The new detection logic itself (scanner.py's touch-cluster state machine) is
tested separately in tests/test_scanner.py.
"""
from __future__ import annotations

from datetime import date, time, timedelta

import numpy as np
import pandas as pd
import pytest

from sma44_level1_intraday.backtest.engine import ScannerBacktester
from sma44_level1_intraday.config.schema import StrategyConfig
from sma44_level1_intraday.resample import resample_ohlc

TZ = "Asia/Kolkata"


def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz=TZ)


# ── resample.py (generic timeframe infrastructure) ──────────────────────────

def _one_min_frame(day: str, n_minutes: int = 375, start_price: float = 100.0) -> pd.DataFrame:
    idx = pd.date_range(f"{day} 09:15", periods=n_minutes, freq="1min", tz=TZ)
    rows = []
    for i, ts in enumerate(idx):
        px = start_price + i * 0.01
        rows.append([ts, "TEST", px, px + 0.05, px - 0.05, px + 0.02, 1000])
    df = pd.DataFrame(rows, columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])
    df["trade_date"] = df["timestamp"].dt.date
    return df


def test_resample_intraday_session_produces_expected_bar_count():
    df = _one_min_frame("2026-06-01")
    bars = resample_ohlc(df, "75min", continuous_session=False, session_start=time(9, 15), session_end=time(15, 30))
    assert len(bars) == 5  # 375 minutes / 75
    assert bars["bar_close_time"].iloc[0] == _ts("2026-06-01 10:30")


def test_resample_weekly_groups_by_iso_week_without_lookahead():
    # Two ISO weeks of daily sessions (Mon-Fri, then Mon-Tue).
    days = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05",  # week 23
            "2026-06-08", "2026-06-09"]                                            # week 24
    frames = []
    for i, d in enumerate(days):
        idx = pd.date_range(f"{d} 09:15", periods=375, freq="1min", tz=TZ)
        base = 100 + i
        frames.append(pd.DataFrame({
            "timestamp": idx, "symbol": "TEST",
            "open": base, "high": base + 1, "low": base - 1, "close": base + 0.5, "volume": 1,
        }))
    raw = pd.concat(frames, ignore_index=True)
    raw["trade_date"] = raw["timestamp"].dt.date

    wk = resample_ohlc(raw, "1W", continuous_session=False, session_start=time(9, 15), session_end=time(15, 30))
    assert len(wk) == 2
    # Week 1 spans Mon..Fri: open from Monday, close from Friday, high/low across all 5 days.
    assert wk.iloc[0]["open"] == pytest.approx(100)
    assert wk.iloc[0]["close"] == pytest.approx(104.5)
    assert wk.iloc[0]["high"] == pytest.approx(105)
    assert wk.iloc[0]["low"] == pytest.approx(99)
    # The weekly bar is only "known" at the session close of its LAST trading day.
    assert wk.iloc[0]["bar_close_time"] == _ts("2026-06-05 15:30")
    assert wk.iloc[1]["bar_close_time"] == _ts("2026-06-09 15:30")
    assert wk.iloc[0]["bar_close_time"] >= raw[raw["trade_date"].astype(str) <= "2026-06-05"]["timestamp"].max()


def test_resample_monthly_groups_by_calendar_month_without_lookahead():
    # Two calendar months of daily sessions.
    days = ["2026-05-28", "2026-05-29",                       # May
            "2026-06-01", "2026-06-02", "2026-06-03"]           # June
    frames = []
    for i, d in enumerate(days):
        idx = pd.date_range(f"{d} 09:15", periods=375, freq="1min", tz=TZ)
        base = 100 + i
        frames.append(pd.DataFrame({
            "timestamp": idx, "symbol": "TEST",
            "open": base, "high": base + 1, "low": base - 1, "close": base + 0.5, "volume": 1,
        }))
    raw = pd.concat(frames, ignore_index=True)
    raw["trade_date"] = raw["timestamp"].dt.date

    mo = resample_ohlc(raw, "1M", continuous_session=False, session_start=time(9, 15), session_end=time(15, 30))
    assert len(mo) == 2
    assert mo.iloc[0]["open"] == pytest.approx(100)     # May 28
    assert mo.iloc[0]["close"] == pytest.approx(101.5)  # May 29's close (last day of May in this data)
    assert mo.iloc[1]["open"] == pytest.approx(102)     # June 1
    # The monthly bar is only "known" at the session close of its LAST trading day.
    assert mo.iloc[0]["bar_close_time"] == _ts("2026-05-29 15:30")
    assert mo.iloc[1]["bar_close_time"] == _ts("2026-06-03 15:30")


def test_is_daily_weekly_and_monthly_timeframe_aliases():
    from sma44_level1_intraday.resample import is_daily_timeframe, is_monthly_timeframe, is_weekly_timeframe
    assert is_daily_timeframe("1D") and is_daily_timeframe("daily") and is_daily_timeframe("day")
    assert not is_daily_timeframe("15min")
    assert is_weekly_timeframe("1W") and is_weekly_timeframe("weekly") and is_weekly_timeframe("wk")
    assert not is_weekly_timeframe("1D")
    assert is_monthly_timeframe("1M") and is_monthly_timeframe("monthly") and is_monthly_timeframe("month")
    assert not is_monthly_timeframe("1W")


# ── bars_lookback_start — pad a fetch's start date so a long rolling
# indicator (e.g. a 200-period MA) is already valid AT the requested start,
# not just n_bars after it ────────────────────────────────────────────────

def test_bars_lookback_start_daily_pads_for_weekends_and_holidays():
    from sma44_level1_intraday.resample import bars_lookback_start
    padded = bars_lookback_start(date(2026, 1, 1), "1D", 200)
    assert padded < date(2026, 1, 1)
    # 200 trading days needs *more* than 200 calendar days back (weekends
    # alone add ~80 days), and comfortably more than a bare 5/7 estimate
    # once NSE holidays are padded in too.
    bare_weekday_estimate = date(2026, 1, 1) - timedelta(days=200 * 7 // 5)
    assert padded < bare_weekday_estimate


def test_bars_lookback_start_weekly_uses_calendar_weeks():
    from sma44_level1_intraday.resample import bars_lookback_start
    padded = bars_lookback_start(date(2026, 1, 1), "1W", 10)
    assert padded == date(2026, 1, 1) - timedelta(weeks=14)  # 10 + the 4-week buffer


def test_bars_lookback_start_rejects_intraday_timeframes():
    from sma44_level1_intraday.resample import bars_lookback_start
    with pytest.raises(ValueError):
        bars_lookback_start(date(2026, 1, 1), "15min", 200)


def test_bars_lookback_start_rejects_negative_n_bars():
    from sma44_level1_intraday.resample import bars_lookback_start
    with pytest.raises(ValueError):
        bars_lookback_start(date(2026, 1, 1), "1D", -1)


# ── backtest/engine.py: candidate/bar test helpers ───────────────────────────
# `_candidate` mirrors the columns backtest/engine.py's `_close_position` reads
# straight off a candidate signal (see signals.CANDIDATE_COLUMNS) — these are
# now the SCANNER's output fields (touch_count, first/previous/current touch
# time, etc.), not the old level/touch_type/trend-timeframe fields.

def _candidate(ts, direction="LONG", trigger=101.0, stop=99.0, target=105.0, symbol="TEST"):
    return {
        "timestamp": ts, "symbol": symbol, "direction": direction,
        "timeframe": "15min",
        "sma_44": 100.0, "close_distance_pct": 1.0, "low_distance_pct": 0.4, "high_distance_pct": 1.6,
        "ma_slope_pct": 0.5, "trend_class": "A",
        "min5_trend_class": None, "min15_trend_class": None, "min30_trend_class": None,
        "hourly_trend_class": None, "daily_trend_class": None,
        "weekly_trend_class": "A", "monthly_trend_class": "A",
        "touch_count": 2, "touch_number": 2,
        "first_interaction_time": ts, "previous_touch_time": ts, "current_touch_time": ts,
        "bars_since_previous_touch": 5, "max_distance_reached_pct": 2.5,
        "wicked_through_sma": False, "is_new_event": True,
        "signal_open": 100.5, "signal_high": 101.0, "signal_low": 99.0, "signal_close": 100.8,
        "daily_volume": 1_000_000,
        "trigger_price": trigger, "stop_loss": stop, "target": target,
    }


def _entry_bar(ts, o, h, l, c, symbol="TEST"):
    return {"timestamp": ts, "symbol": symbol, "trade_date": ts.date(), "open": o, "high": h, "low": l, "close": c}


# ── exits.py: ATR Chandelier trailing stop ─────────────────────────────────

def _chand_cfg(**over):
    """ExecutionConfig for the chandelier, with test-friendly defaults."""
    base = {"exit_mode": "atr_chandelier", "atr_multiplier": 2.0, "trail_after_R": 1.0,
            "use_structure_filter": False, "exit_on_close": False,
            "use_breakeven_after_activation": False}
    base.update(over)
    return StrategyConfig.from_dict({"execution": base}).execution


def _cbar(ts, o, h, l, c, atr=1.0, swing_low=None, swing_high=None):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "atr": atr,
            "recent_swing_low": swing_low, "recent_swing_high": swing_high}


def test_chandelier_defines_R_from_the_initial_stop():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    t = ChandelierTrailingStop(_chand_cfg())
    long = t.open("LONG", entry_price=100.0, initial_stop=98.0)
    assert long.r_value == pytest.approx(2.0) and long.stop == pytest.approx(98.0)
    short = t.open("SHORT", entry_price=100.0, initial_stop=102.0)
    assert short.r_value == pytest.approx(2.0) and short.stop == pytest.approx(102.0)
    with pytest.raises(ValueError):
        t.open("LONG", entry_price=100.0, initial_stop=101.0)  # stop on the wrong side


def test_chandelier_does_not_arm_before_plus_1R_and_loser_exits_at_initial_stop():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    t = ChandelierTrailingStop(_chand_cfg())
    st = t.open("LONG", 100.0, 98.0)   # R = 2 -> arms at 102

    # A bar that goes only +1.0 (below +1R) must NOT arm, and must NOT move the stop.
    t.update(st, _cbar(_ts("2026-06-01 10:00"), 100, 101.0, 99.5, 100.5, atr=1.0))
    assert st.activated is False
    assert st.stop == pytest.approx(98.0)   # still the initial stop

    # Price then collapses -> exits at the INITIAL stop, reason "sl".
    exit_info = t.check_exit(st, _cbar(_ts("2026-06-01 10:15"), 100, 100, 97.0, 97.5))
    assert exit_info == (pytest.approx(98.0), "sl")


def test_chandelier_arms_at_plus_1R_and_trails_by_atr_ratcheting_only_upward():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    t = ChandelierTrailingStop(_chand_cfg(atr_multiplier=2.0))
    st = t.open("LONG", 100.0, 98.0)   # R = 2 -> arms at 102

    # Bar reaches 103 (>= +1R) -> arms. hh=103, atr=1 -> atr_stop = 103 - 2*1 = 101.
    t.update(st, _cbar(_ts("2026-06-01 10:00"), 100, 103.0, 99.9, 102.5, atr=1.0))
    assert st.activated is True
    assert st.stop == pytest.approx(101.0)

    # Higher high 105 -> stop rises to 103.
    t.update(st, _cbar(_ts("2026-06-01 10:15"), 102.5, 105.0, 102.0, 104.5, atr=1.0))
    assert st.stop == pytest.approx(103.0)

    # A pullback bar (lower high) must NOT lower the stop — ratchet only.
    t.update(st, _cbar(_ts("2026-06-01 10:30"), 104.5, 104.6, 103.5, 103.8, atr=1.0))
    assert st.stop == pytest.approx(103.0)


def test_chandelier_short_trails_downward_only():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    t = ChandelierTrailingStop(_chand_cfg(atr_multiplier=2.0))
    st = t.open("SHORT", 100.0, 102.0)   # R = 2 -> arms at 98

    t.update(st, _cbar(_ts("2026-06-01 10:00"), 100, 100.1, 97.0, 97.5, atr=1.0))
    assert st.activated is True
    assert st.stop == pytest.approx(99.0)          # ll=97 + 2*1

    t.update(st, _cbar(_ts("2026-06-01 10:15"), 97.5, 97.6, 95.0, 95.5, atr=1.0))
    assert st.stop == pytest.approx(97.0)          # ll=95 + 2

    t.update(st, _cbar(_ts("2026-06-01 10:30"), 95.5, 96.5, 95.4, 96.2, atr=1.0))
    assert st.stop == pytest.approx(97.0)          # never moves up


def test_chandelier_breakeven_on_activation():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    # ATR is huge, so the chandelier stop would sit below entry; breakeven must win.
    t = ChandelierTrailingStop(_chand_cfg(use_breakeven_after_activation=True,
                                          breakeven_buffer_points=0.5, atr_multiplier=10.0))
    st = t.open("LONG", 100.0, 98.0)
    t.update(st, _cbar(_ts("2026-06-01 10:00"), 100, 102.5, 99.9, 102.0, atr=1.0))
    assert st.activated and st.breakeven_applied
    assert st.stop == pytest.approx(100.5)   # entry + buffer, not 102.5 - 10


def test_chandelier_structure_filter_tightens_stop_to_confirmed_swing():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    bar = _cbar(_ts("2026-06-01 10:00"), 100, 103.0, 99.9, 102.5, atr=1.0, swing_low=101.5)

    off = ChandelierTrailingStop(_chand_cfg(use_structure_filter=False))
    s1 = off.open("LONG", 100.0, 98.0)
    off.update(s1, bar)
    assert s1.stop == pytest.approx(101.0)   # ATR only: 103 - 2

    on = ChandelierTrailingStop(_chand_cfg(use_structure_filter=True))
    s2 = on.open("LONG", 100.0, 98.0)
    on.update(s2, bar)
    assert s2.stop == pytest.approx(101.5)   # swing low is tighter -> max() picks it


def test_chandelier_exit_on_close_vs_touch():
    from sma44_level1_intraday.exits import ChandelierTrailingStop
    # A bar that WICKS below the stop but closes above it.
    wick = _cbar(_ts("2026-06-01 10:15"), 102, 102.5, 100.5, 102.0)

    touch = ChandelierTrailingStop(_chand_cfg(exit_on_close=False))
    s1 = touch.open("LONG", 100.0, 98.0)
    s1.stop = 101.0
    assert touch.check_exit(s1, wick) == (pytest.approx(101.0), "sl")   # touched -> exit at stop

    on_close = ChandelierTrailingStop(_chand_cfg(exit_on_close=True))
    s2 = on_close.open("LONG", 100.0, 98.0)
    s2.stop = 101.0
    assert on_close.check_exit(s2, wick) is None                        # close held -> stay in

    # Now a bar that CLOSES below the stop -> exits at the close.
    breach = _cbar(_ts("2026-06-01 10:30"), 102, 102.1, 100.0, 100.4)
    assert on_close.check_exit(s2, breach) == (pytest.approx(100.4), "sl")


def _chand_entry_bar(ts, o, h, l, c, atr=1.0, sl=None, sh=None, symbol="TEST"):
    b = _entry_bar(ts, o, h, l, c, symbol=symbol)
    b.update({"atr": atr, "recent_swing_low": sl, "recent_swing_high": sh})
    return b


def test_chandelier_end_to_end_lets_winner_run_past_the_fixed_target():
    # Signal gives entry 100 / stop 98 (R=2) and a fixed 2R target of 104.
    # In chandelier mode that target is IGNORED: the trade runs to 106 and only
    # exits when the ratcheted trail (104) is breached.
    config = StrategyConfig.from_dict({
        "execution": {"exit_mode": "atr_chandelier", "atr_multiplier": 2.0,
                      "use_structure_filter": False, "exit_on_close": False,
                      "use_breakeven_after_activation": True},
        "risk": {"fixed_qty": 1},
    })
    sig_ts = _ts("2026-06-01 10:00")
    bars = pd.DataFrame([
        _chand_entry_bar(sig_ts, 99.5, 100.0, 98.0, 99.8),                       # signal bar
        _chand_entry_bar(_ts("2026-06-01 10:15"), 99.8, 100.5, 99.6, 100.4),     # entry @100; +0.5R -> no arm
        _chand_entry_bar(_ts("2026-06-01 10:30"), 100.4, 103.0, 100.2, 102.8),   # +1R -> arm; stop 101
        _chand_entry_bar(_ts("2026-06-01 10:45"), 102.8, 106.0, 102.5, 105.8),   # fixed target 104 NOT taken
        _chand_entry_bar(_ts("2026-06-01 11:00"), 105.8, 106.2, 103.5, 103.8),   # stop 104 breached -> exit
    ])
    candidates = pd.DataFrame([_candidate(sig_ts, trigger=100.0, stop=98.0, target=104.0)])
    result = ScannerBacktester(config).run(bars, candidates)

    trades = result["trades"]
    assert len(trades) == 1
    tr = trades.iloc[0]
    assert tr["exit_reason"] == "trail_stop"
    assert tr["entry_price"] == pytest.approx(100.0)
    assert tr["exit_price"] == pytest.approx(104.0)
    assert pd.isna(tr["target"])          # fixed take-profit is OFF by default

    # The trail log records the stop path + when it armed (for plotting/debug).
    trail = result["trail_log"]
    assert list(trail["activated"]) == [False, True, True]
    assert list(trail["stop"]) == pytest.approx([98.0, 101.0, 104.0])


def test_chandelier_optional_fixed_take_profit_caps_the_winner():
    config = StrategyConfig.from_dict({
        "execution": {"exit_mode": "atr_chandelier", "atr_multiplier": 2.0,
                      "use_structure_filter": False, "exit_on_close": False,
                      "use_fixed_take_profit": True, "fixed_rr_target": 2.0},
        "risk": {"fixed_qty": 1},
    })
    sig_ts = _ts("2026-06-01 10:00")
    bars = pd.DataFrame([
        _chand_entry_bar(sig_ts, 99.5, 100.0, 98.0, 99.8),
        _chand_entry_bar(_ts("2026-06-01 10:15"), 99.8, 100.5, 99.6, 100.4),   # entry @100 (R=2 -> TP 104)
        _chand_entry_bar(_ts("2026-06-01 10:30"), 100.4, 106.0, 100.2, 105.8), # blows past TP 104
    ])
    candidates = pd.DataFrame([_candidate(sig_ts, trigger=100.0, stop=98.0, target=104.0)])
    trades = ScannerBacktester(config).run(bars, candidates)["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "target"
    assert trades.iloc[0]["exit_price"] == pytest.approx(104.0)   # capped at 2R


def test_chandelier_swings_are_confirmed_only_no_lookahead():
    from sma44_level1_intraday.exits import compute_confirmed_swings
    # A clear swing low at index 3 (value 95) with lookback=2 -> confirmed at index 5.
    df = pd.DataFrame({
        "high": [100, 99, 98, 96, 99, 100, 101, 102],
        "low":  [99, 98, 97, 95, 97, 99, 100, 101],
    })
    low, _high = compute_confirmed_swings(df, lookback=2)
    assert np.isnan(low[4])            # not yet confirmed at index 4
    assert low[5] == pytest.approx(95) # confirmed exactly at index 3+2
    assert low[7] == pytest.approx(95) # persists afterwards


# ── backtest/engine.py: trade-lifecycle mechanics ────────────────────────────

def test_engine_triggers_and_hits_target():
    config = StrategyConfig.default()
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),  # triggers (>=101) and hits target 105
    ])
    candidates = pd.DataFrame([_candidate(signal_ts)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    trade = trades.iloc[0]
    assert trade["exit_reason"] == "target"
    assert trade["entry_price"] == pytest.approx(101.0)
    assert trade["exit_price"] == pytest.approx(105.0)
    assert trade["qty"] == pytest.approx(config.risk.fixed_qty)


def test_engine_expires_setup_after_configured_bars():
    config = StrategyConfig.from_dict({"scanner": {"setup_expiry_bars": 2}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.9, 99.0, 100.8),          # signal bar (no trigger yet)
        _entry_bar(_ts("2026-06-01 10:40"), 100.5, 100.9, 100.0, 100.6),  # bars_waited=1, no trigger
        _entry_bar(_ts("2026-06-01 10:45"), 100.5, 100.9, 100.0, 100.6),  # bars_waited=2 -> expires
        _entry_bar(_ts("2026-06-01 10:50"), 100.5, 106.0, 100.0, 105.5),  # would've triggered, but expired
    ])
    candidates = pd.DataFrame([_candidate(signal_ts)])
    result = ScannerBacktester(config).run(bars, candidates)
    assert result["trades"].empty
    assert "setup_expired" in result["rejected"]["reason"].values


# ── Pre-entry invalidation (_resolve_pending_trigger) ────────────────────────
# A pending setup's invalidation level, BEFORE entry, is the signal candle's
# own opposite extreme (sig["signal_low"] for LONG, sig["signal_high"] for
# SHORT) — deliberately NOT sig["stop_loss"] (the rule-2 stop, widened out to
# the lookback window/44-SMA, which only applies once a position is actually
# OPEN). If the invalidation level is breached while still pending, the setup
# is cancelled outright rather than waiting out its expiry. When a single
# bar's range spans BOTH the trigger and the invalidation level, which
# happened first is inferred from the bar's own shape — the same OHLC-path
# assumption _resolve_priority already uses for SL/target ambiguity on an
# OPEN position: a bullish (close>=open) bar dipped to its low before
# rallying to its high; a bearish bar the reverse.

def test_engine_pending_setup_invalidated_on_a_separate_bar_before_trigger():
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        # Breaches the stop (99) but stays well clear of the trigger (101) — unambiguous.
        _entry_bar(_ts("2026-06-01 10:40"), 100.5, 100.6, 98.5, 98.6),
        # Would have triggered (high >= 101), but the setup is already gone.
        _entry_bar(_ts("2026-06-01 10:45"), 98.6, 103.0, 98.5, 102.0),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    assert result["trades"].empty
    assert "stop_breached_before_entry" in result["rejected"]["reason"].values


def test_engine_long_bullish_ambiguous_bar_invalidates_before_entry():
    # Bullish (close >= open): assumed low-then-high -> the stop (99, LOW)
    # is deemed hit BEFORE the trigger (101, HIGH) -> no entry at all.
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        _entry_bar(_ts("2026-06-01 10:40"), 100.0, 102.0, 98.0, 101.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    assert result["trades"].empty
    assert "stop_breached_before_entry" in result["rejected"]["reason"].values


def test_engine_long_bearish_ambiguous_bar_triggers_then_hits_sl_same_candle():
    # Bearish (close < open): assumed high-then-low -> the trigger (101,
    # HIGH) is deemed hit FIRST -> entry fills, and the same bar's low (98)
    # then stops it out via the existing same-candle-exit path.
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        _entry_bar(_ts("2026-06-01 10:40"), 101.8, 102.0, 98.0, 98.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["entry_price"] == pytest.approx(101.0)
    assert trades.iloc[0]["exit_reason"] == "sl"


def test_engine_short_ambiguous_bar_mirrors_long_by_bar_shape_not_direction():
    signal_ts = _ts("2026-06-01 10:35")
    candidates = pd.DataFrame([_candidate(signal_ts, direction="SHORT", trigger=99.0, stop=101.0, target=95.0)])

    # Bullish bar: low-then-high -> for SHORT the trigger IS the low -> hit
    # first -> entry fills, then the same bar's high (102) stops it out.
    bullish = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        _entry_bar(_ts("2026-06-01 10:40"), 98.5, 102.0, 98.0, 101.5),
    ])
    result = ScannerBacktester(StrategyConfig.default()).run(bullish, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "sl"

    # Bearish bar: high-then-low -> for SHORT the stop IS the high -> hit
    # first -> invalidated, no entry at all.
    bearish = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        _entry_bar(_ts("2026-06-01 10:40"), 101.8, 102.0, 98.0, 98.5),
    ])
    result2 = ScannerBacktester(StrategyConfig.default()).run(bearish, candidates)
    assert result2["trades"].empty
    assert "stop_breached_before_entry" in result2["rejected"]["reason"].values


def test_engine_long_invalidates_on_signal_low_breach_even_though_stop_loss_is_wider():
    # The PRE-ENTRY invalidation check uses the signal candle's own low
    # (99.0), NOT the wider rule-2 stop_loss (95.0 here, standing in for a
    # stop pulled down to the 44-SMA/lookback window) -- a dip through
    # signal_low invalidates the setup even though it never got anywhere
    # near the actual stop_loss.
    signal_ts = _ts("2026-06-01 10:35")
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=95.0, target=105.0)])
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        # Dips below signal_low (99.0) but stays well above stop_loss (95.0);
        # never reaches the trigger (101.0) either.
        _entry_bar(_ts("2026-06-01 10:40"), 99.5, 99.8, 98.0, 98.5),
    ])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    assert result["trades"].empty
    assert "stop_breached_before_entry" in result["rejected"]["reason"].values


def test_engine_long_stays_pending_when_only_the_wider_stop_loss_would_be_breached():
    # Mirror sanity check: confirms the new check is genuinely keyed to
    # signal_low, not stop_loss -- a bar that stays ABOVE signal_low never
    # invalidates, no matter how close it gets to the wider stop_loss.
    signal_ts = _ts("2026-06-01 10:35")
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=95.0, target=105.0)])
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        # Stays comfortably above signal_low (99.0) and doesn't trigger either.
        _entry_bar(_ts("2026-06-01 10:40"), 99.5, 99.9, 99.2, 99.6),
    ])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    assert result["trades"].empty
    assert result["rejected"].empty  # still pending -- not invalidated, not expired yet


def test_engine_short_invalidates_on_signal_high_breach_even_though_stop_loss_is_wider():
    # Mirror of the LONG regression above, and the exact shape of the real
    # BPCL 2026-01-23 case that prompted this change: the entry bar's high
    # pokes above the signal candle's own high (the narrow invalidation
    # level) while staying well below the rule-2 stop_loss (widened out to
    # the 44-SMA) -- must still invalidate.
    signal_ts = _ts("2026-06-01 10:35")
    candidates = pd.DataFrame([_candidate(signal_ts, direction="SHORT", trigger=99.0, stop=105.0, target=95.0)])
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 100.8, 100.0, 100.6),
        # Pokes above signal_high (101.0) but stays well below stop_loss (105.0).
        _entry_bar(_ts("2026-06-01 10:40"), 100.5, 101.9, 100.2, 100.8),
    ])
    result = ScannerBacktester(StrategyConfig.default()).run(bars, candidates)
    assert result["trades"].empty
    assert "stop_breached_before_entry" in result["rejected"]["reason"].values


def test_candle_trailing_stop_long_trails_up_and_ignores_target():
    config = StrategyConfig.from_dict({"execution": {"use_candle_trailing_stop": True}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),                 # signal bar -> pending
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 102.0, 100.5, 101.5),  # trigger @101.0; stop trails 99.0 -> 100.5
        _entry_bar(_ts("2026-06-01 10:45"), 101.5, 103.0, 101.0, 102.5),  # high 103 would hit target 102 (IGNORED); stop -> 101.0
        _entry_bar(_ts("2026-06-01 10:50"), 102.5, 103.5, 101.5, 103.0),  # stop -> 101.5
        _entry_bar(_ts("2026-06-01 10:55"), 103.0, 103.2, 101.0, 101.2),  # low 101.0 <= stop 101.5 -> exit @101.5
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=102.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["exit_reason"] == "trail_stop"
    assert t["entry_price"] == pytest.approx(101.0)
    assert t["exit_price"] == pytest.approx(101.5)  # last trailed stop (from 10:50's low)


def test_candle_trailing_stop_short_trails_down():
    config = StrategyConfig.from_dict({"execution": {"use_candle_trailing_stop": True}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.0, 101.0, 99.0, 99.5),                 # signal bar -> pending
        _entry_bar(_ts("2026-06-01 10:40"), 99.5, 99.6, 98.0, 98.5),     # trigger @99.0; stop trails 101.0 -> 99.6
        _entry_bar(_ts("2026-06-01 10:45"), 98.5, 99.0, 97.0, 97.5),     # stop -> 99.0
        _entry_bar(_ts("2026-06-01 10:50"), 97.5, 98.0, 96.5, 97.0),     # stop -> 98.0
        _entry_bar(_ts("2026-06-01 10:55"), 97.0, 99.5, 97.0, 99.0),     # high 99.5 >= stop 98.0 -> exit @98.0
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, direction="SHORT", trigger=99.0, stop=101.0, target=93.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["exit_reason"] == "trail_stop"
    assert t["entry_price"] == pytest.approx(99.0)
    assert t["exit_price"] == pytest.approx(98.0)


def test_candle_trailing_stop_first_bar_stopout_reports_sl_not_trail():
    # If the position is stopped on the entry bar itself (before the stop has
    # trailed anywhere), the reason is the plain initial "sl", not "trail_stop".
    config = StrategyConfig.from_dict({"execution": {"use_candle_trailing_stop": True}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),                 # signal bar -> pending
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 102.0, 98.5, 99.2),    # trigger @101.0 AND low 98.5 <= stop 99.0 same bar
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=110.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["exit_reason"] == "sl"
    assert t["exit_price"] == pytest.approx(99.0)


def test_engine_force_exits_at_configured_time():
    config = StrategyConfig.default()
    signal_ts = _ts("2026-06-01 15:05")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 15:10"), 100.8, 101.2, 100.5, 101.0),  # triggers at 101.0
        _entry_bar(_ts("2026-06-01 15:15"), 101.0, 101.5, 100.8, 101.3),  # force exit time, no SL/target hit
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=110.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "force_exit"
    assert trades.iloc[0]["exit_price"] == pytest.approx(101.3)


def test_engine_sl_first_priority_on_ambiguous_candle():
    config = StrategyConfig.from_dict({"execution": {"same_candle_priority": "sl_first"}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        # A clean, unambiguous trigger — stays clear of the stop this bar —
        # so the position opens cleanly, keeping the ambiguity test below
        # isolated to same_candle_priority (an OPEN position hitting both SL
        # and target in one bar), not entangled with pre-entry invalidation
        # (see _resolve_pending_trigger / test_engine_pending_setup_*).
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 102.0, 100.0, 101.5),
        # NOW the already-open position sees both SL (99) and target (105) in
        # one bar -> sl_first wins.
        _entry_bar(_ts("2026-06-01 10:45"), 101.5, 106.0, 98.0, 102.0),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "sl"


def test_execution_config_default_same_candle_priority_is_use_ohlc_path():
    assert StrategyConfig.default().execution.same_candle_priority == "use_ohlc_path"
    assert StrategyConfig.from_dict({}).execution.same_candle_priority == "use_ohlc_path"


def test_engine_use_ohlc_path_is_the_default_and_infers_from_bar_shape():
    # Regression for BAJAJ-AUTO 2026-01-30: a bearish exit bar (opened near
    # its high, closed near its low) is assumed to have visited its HIGH
    # before its LOW -> for a LONG position that means target (the high
    # side) was hit BEFORE the stop (the low side), even though the bar's
    # range spans both. No explicit same_candle_priority override -> this is
    # exercising the actual default, not an opt-in mode.
    config = StrategyConfig.default()
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 102.0, 100.0, 101.5),  # clean trigger, no ambiguity
        # Bearish (close < open): high-then-low -> target (105) deemed hit
        # before the stop (99), even though this bar's range spans both.
        _entry_bar(_ts("2026-06-01 10:45"), 106.0, 106.0, 98.0, 99.0),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "target"
    assert trades.iloc[0]["exit_price"] == pytest.approx(105.0)


def test_engine_use_ohlc_path_bullish_bar_resolves_to_sl_instead():
    # Mirror: a bullish exit bar (low-then-high) is assumed to have visited
    # its LOW first -> stop wins even though target is also in range.
    config = StrategyConfig.default()
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 102.0, 100.0, 101.5),
        # Bullish (close >= open): low-then-high -> stop (99) deemed hit first.
        _entry_bar(_ts("2026-06-01 10:45"), 98.0, 106.0, 98.0, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "sl"
    assert trades.iloc[0]["exit_price"] == pytest.approx(99.0)


def test_max_pending_setups_per_symbol_gates_overlapping_setups():
    # Two candidates on consecutive bars, neither triggering immediately.
    sig1, sig2 = _ts("2026-06-01 10:35"), _ts("2026-06-01 10:40")
    bars = pd.DataFrame([
        _entry_bar(sig1, 100.0, 100.5, 99.0, 100.2),
        _entry_bar(sig2, 100.2, 100.6, 99.5, 100.3),
        _entry_bar(_ts("2026-06-01 10:45"), 100.3, 108.0, 100.0, 107.5),   # triggers both
    ])
    candidates = pd.DataFrame([
        _candidate(sig1, trigger=101.0, stop=99.0, target=105.0),
        _candidate(sig2, trigger=102.0, stop=99.5, target=106.0),
    ])
    limits = {"one_active_position_per_symbol": False, "max_trades_per_symbol_per_day": 10}

    # Default (1 pending): the 2nd candidate is dropped — and now LOGGED, not silent.
    one = StrategyConfig.from_dict({"trade_limits": {**limits, "max_pending_setups_per_symbol": 1}})
    r1 = ScannerBacktester(one).run(bars, candidates)
    assert len(r1["trades"]) == 1
    assert "max_pending_setups_per_symbol_reached" in set(r1["rejected"]["reason"])

    # Allowing 2 pending setups -> both arm and both trigger.
    two = StrategyConfig.from_dict({"trade_limits": {**limits, "max_pending_setups_per_symbol": 2}})
    r2 = ScannerBacktester(two).run(bars, candidates)
    assert len(r2["trades"]) == 2

    # None = unlimited pending.
    unl = StrategyConfig.from_dict({"trade_limits": {**limits, "max_pending_setups_per_symbol": None}})
    assert len(ScannerBacktester(unl).run(bars, candidates)["trades"]) == 2


def test_engine_one_active_position_per_symbol_blocks_second_setup():
    config = StrategyConfig.default()
    assert config.trade_limits.one_active_position_per_symbol is True
    sig1_ts = _ts("2026-06-01 10:35")
    sig2_ts = _ts("2026-06-01 10:40")
    bars = pd.DataFrame([
        _entry_bar(sig1_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(sig2_ts, 100.8, 101.1, 100.5, 100.9),   # triggers setup 1 (high>=101) — position opens
        _entry_bar(_ts("2026-06-01 10:45"), 100.9, 108.0, 100.0, 107.0),  # target hit for pos 1; also candidate 2 present
    ])
    candidates = pd.DataFrame([
        _candidate(sig1_ts, trigger=101.0, stop=99.0, target=105.0),
        _candidate(sig2_ts, trigger=101.1, stop=100.0, target=110.0),
    ])
    result = ScannerBacktester(config).run(bars, candidates)
    # Setup 2's candidate should be rejected since a pending/open setup already exists for TEST
    assert len(result["trades"]) == 1


def test_engine_risk_per_trade_sizing():
    config = StrategyConfig.from_dict({
        "risk": {"position_sizing_mode": "risk_per_trade", "risk_per_trade_pct": 1.0, "initial_capital": 100000.0},
    })
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    # risk_amount = 100000 * 1% = 1000; stop_distance = 101-99=2 -> qty = 500
    assert trades.iloc[0]["qty"] == pytest.approx(500.0)


def test_engine_risk_per_trade_sizing_uses_fixed_amount_override():
    # "trade a fixed ₹X of risk per trade regardless of total capital" —
    # risk_per_trade_amount overrides risk_per_trade_pct entirely, same
    # fixed-amount-overrides-percentage convention as capital_based.
    config = StrategyConfig.from_dict({
        "risk": {
            "position_sizing_mode": "risk_per_trade",
            "risk_per_trade_amount": 25000.0,
            "risk_per_trade_pct": 1.0,  # would give qty=125 if NOT overridden — must be ignored
            "initial_capital": 2_500_000.0,
        },
    })
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    # stop_distance = 101-99=2 -> qty = floor(25000 / 2) = 12500
    assert trades.iloc[0]["qty"] == pytest.approx(12500.0)


def test_engine_capital_based_sizing_uses_pct_of_equity():
    config = StrategyConfig.from_dict({
        "risk": {"position_sizing_mode": "capital_based", "capital_per_trade_pct": 10.0, "initial_capital": 100000.0},
    })
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    # capital_amount = 100000 * 10% = 10000; qty = floor(10000 / 101) = 99
    assert trades.iloc[0]["qty"] == pytest.approx(99.0)


def test_engine_capital_based_sizing_uses_fixed_amount_override():
    config = StrategyConfig.from_dict({
        "risk": {
            "position_sizing_mode": "capital_based",
            "capital_per_trade_amount": 5000.0,
            "initial_capital": 100000.0,
        },
    })
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    # capital_per_trade_amount overrides pct: qty = floor(5000 / 101) = 49
    assert trades.iloc[0]["qty"] == pytest.approx(49.0)


def test_engine_daily_loss_breaker_blocks_new_trades():
    config = StrategyConfig.from_dict({
        "risk": {"initial_capital": 1000.0, "max_daily_loss_pct": 1.0, "fixed_qty": 100},
        "trade_limits": {"one_active_position_per_symbol": False},
    })
    # First trade loses 100*  (101-99)=200 minus tiny costs -> way past 1% of 1000 (=10)
    sig1_ts = _ts("2026-06-01 10:35")
    sig2_ts = _ts("2026-06-01 10:45")
    bars = pd.DataFrame([
        _entry_bar(sig1_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 101.2, 98.0, 99.0),  # triggers then hits SL same/next bar
        _entry_bar(sig2_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:50"), 100.8, 106.0, 100.0, 105.5),  # would trigger 2nd trade
    ])
    candidates = pd.DataFrame([
        _candidate(sig1_ts, trigger=101.0, stop=99.0, target=105.0),
        _candidate(sig2_ts, trigger=101.0, stop=99.0, target=105.0),
    ])
    result = ScannerBacktester(config).run(bars, candidates)
    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "sl"
    assert "max_daily_loss_hit" in result["rejected"]["reason"].values


def test_engine_daily_loss_breaker_does_not_latch_when_equity_is_negative():
    config = StrategyConfig.from_dict({
        "risk": {"initial_capital": 100.0, "max_daily_loss_pct": 100.0, "fixed_qty": 100},
        "trade_limits": {"one_active_position_per_symbol": False},
    })
    t1, t2 = _ts("2026-06-01 10:35"), _ts("2026-06-02 10:35")
    bars = pd.DataFrame([
        _entry_bar(t1, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 101.2, 98.0, 99.0),
        _entry_bar(t2, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-02 10:40"), 100.8, 106.0, 100.0, 105.5),
    ])
    candidates = pd.DataFrame([
        _candidate(t1, trigger=101.0, stop=99.0, target=105.0),
        _candidate(t2, trigger=101.0, stop=99.0, target=105.0),
    ])
    result = ScannerBacktester(config).run(bars, candidates)
    assert len(result["trades"]) == 2


def test_costs_reduce_net_pnl():
    config = StrategyConfig.from_dict({"costs": {"brokerage_per_order": 5.0, "slippage_points": 0.1}})
    signal_ts = _ts("2026-06-01 10:35")
    bars = pd.DataFrame([
        _entry_bar(signal_ts, 100.5, 101.0, 99.0, 100.8),
        _entry_bar(_ts("2026-06-01 10:40"), 100.8, 106.0, 100.5, 105.5),
    ])
    candidates = pd.DataFrame([_candidate(signal_ts, trigger=101.0, stop=99.0, target=105.0)])
    result = ScannerBacktester(config).run(bars, candidates)
    trade = result["trades"].iloc[0]
    assert trade["costs"] > 0
    assert trade["net_pnl"] == pytest.approx(trade["gross_pnl"] - trade["costs"])
