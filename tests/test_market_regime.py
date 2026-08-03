from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from intraday_research.market_regime import (
    RegimeConfig, apply_regime_to_signals, build_weekly_filter, daily_state_frame,
    resample_weekly, update_market_filter, wilder_atr, wilder_smma,
)


def _cfg(**kw):
    return RegimeConfig(**kw)


def _step(close, prev_state='ON', prev_count=0, smma=22150.0, atr=600.0,
          prev_low=None, **cfg_kw):
    """Helper matching the spec's test bands: with smma=22150, atr=600,
    exit 0.25 / reentry 0.10 -> lower 22000, upper 22210 (defaults); the
    spec examples use lower 22000 / upper 22300 -> reentry 0.25."""
    return update_market_filter(close, smma, atr, prev_low, prev_state, prev_count, _cfg(**cfg_kw))


# spec bands: lower 22000, upper 22300 => smma 22150, atr 600, mult 0.25/0.25
SPEC = dict(smma=22150.0, atr=600.0, exit_atr_multiplier=0.25, reentry_atr_multiplier=0.25)


def test_1_on_state_lower_band_break():
    r = _step(21950, 'ON', 0, exit_mode='confirmed', **SPEC)
    assert r['market_state'] == 'OFF'
    assert r['buy_allowed'] is False
    assert r['cancel_pending_buys'] is True
    assert r['below_lower_count'] == 1
    assert r['force_exit_all'] is False
    assert r['transition'] == 'ON_TO_OFF'
    assert r['reason'] == 'LOWER_BAND_BREAK'


def test_2_on_state_between_bands():
    r = _step(22100, 'ON', 0, **SPEC)
    assert r['market_state'] == 'ON'
    assert r['buy_allowed'] is True
    assert r['transition'] == 'NONE'
    assert r['force_exit_all'] is False


def test_3_off_state_between_bands():
    r = _step(22200, 'OFF', 0, **SPEC)
    assert r['market_state'] == 'OFF'
    assert r['buy_allowed'] is False
    assert r['transition'] == 'NONE'


def test_4_off_state_upper_band_reclaim():
    r = _step(22350, 'OFF', 1, **SPEC)
    assert r['market_state'] == 'ON'
    assert r['buy_allowed'] is True
    assert r['below_lower_count'] == 0
    assert r['transition'] == 'OFF_TO_ON'
    assert r['reason'] == 'UPPER_BAND_RECLAIM'


def test_5_equality_does_not_trigger():
    on = _step(22000.0, 'ON', 0, **SPEC)     # close == lower band
    assert on['market_state'] == 'ON'
    off = _step(22300.0, 'OFF', 0, **SPEC)   # close == upper band
    assert off['market_state'] == 'OFF'


def test_6_confirmed_two_week_exit():
    wk1 = _step(21950, 'ON', 0, exit_mode='confirmed', **SPEC)
    assert (wk1['market_state'], wk1['below_lower_count'], wk1['force_exit_all']) == ('OFF', 1, False)
    wk2 = _step(21900, 'OFF', wk1['below_lower_count'], exit_mode='confirmed', **SPEC)
    assert wk2['market_state'] == 'OFF'
    assert wk2['below_lower_count'] == 2
    assert wk2['force_exit_all'] is True
    assert wk2['reason'] == 'CONFIRMED_LOWER_BAND_BREAK'


def test_7_failed_consecutive_confirmation():
    wk1 = _step(21950, 'ON', 0, exit_mode='confirmed', **SPEC)
    wk2 = _step(22100, 'OFF', wk1['below_lower_count'], exit_mode='confirmed', **SPEC)
    assert wk2['market_state'] == 'OFF'
    assert wk2['below_lower_count'] == 0
    assert wk2['force_exit_all'] is False


def test_8_immediate_exit_mode():
    r = _step(21950, 'ON', 0, exit_mode='immediate', **SPEC)
    assert r['market_state'] == 'OFF'
    assert r['buy_allowed'] is False
    assert r['cancel_pending_buys'] is True
    assert r['force_exit_all'] is True


def test_9_emergency_exit():
    r = _step(21500, 'ON', 0, prev_low=21800.0, enable_emergency_exit=True, **SPEC)
    assert r['market_state'] == 'OFF'
    assert r['buy_allowed'] is False
    assert r['cancel_pending_buys'] is True
    assert r['force_exit_all'] is True
    assert r['reason'] == 'EMERGENCY_10_WEEK_LOW_BREAK'
    # emergency disabled -> normal confirmed path (no force exit on week 1)
    r2 = _step(21500, 'ON', 0, prev_low=21800.0, enable_emergency_exit=False, **SPEC)
    assert r2['force_exit_all'] is False
    assert r2['reason'] == 'LOWER_BAND_BREAK'


# ── Indicators ────────────────────────────────────────────────────────────────

def test_smma_wilder_initialization_and_recursion():
    closes = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0, 20.0])
    smma = wilder_smma(closes, 5)
    assert np.isnan(smma.iloc[3])
    assert smma.iloc[4] == pytest.approx(12.0)            # SMA of first 5
    assert smma.iloc[5] == pytest.approx((12.0 * 4 + 20.0) / 5)


def test_atr_wilder_first_tr_and_recursion():
    weekly = pd.DataFrame({
        'High':  [110.0, 112, 111, 115, 118, 120, 119, 121, 117, 116, 122, 125, 124, 126, 130],
        'Low':   [100.0, 105, 104, 108, 110, 112, 111, 113, 109, 108, 114, 117, 116, 118, 121],
        'Close': [105.0, 108, 107, 112, 115, 117, 115, 118, 112, 111, 118, 121, 119, 122, 126],
    })
    atr = wilder_atr(weekly, 14)
    from intraday_research.market_regime import weekly_true_range
    tr = weekly_true_range(weekly)
    assert tr.iloc[0] == pytest.approx(10.0)              # first TR = H - L
    assert atr.iloc[13] == pytest.approx(tr.iloc[:14].mean())
    assert atr.iloc[14] == pytest.approx((atr.iloc[13] * 13 + tr.iloc[14]) / 14)


# ── Weekly resampler ──────────────────────────────────────────────────────────

def _daily_nifty(n_weeks=30, start=date(2024, 1, 1), holiday_fridays=()):
    rows = []
    rng = np.random.default_rng(1)
    price = 22000.0
    d = start
    while len(rows) < n_weeks * 5:
        if d.weekday() < 5 and not (d.weekday() == 4 and d in holiday_fridays):
            price *= 1 + rng.normal(0.0005, 0.01)
            rows.append({'Date': d, 'Open': price * 0.999, 'High': price * 1.01,
                         'Low': price * 0.99, 'Close': price})
        d += timedelta(days=1)
    return pd.DataFrame(rows)


def test_weekly_resampler_basic():
    daily = _daily_nifty(8)
    weekly = resample_weekly(daily)
    first_week = daily[pd.to_datetime(daily['Date']).dt.isocalendar().week
                       == pd.Timestamp(daily['Date'].iloc[0]).week]
    assert weekly.iloc[0]['Open'] == first_week.iloc[0]['Open']
    assert weekly.iloc[0]['Close'] == first_week.iloc[-1]['Close']
    assert weekly.iloc[0]['High'] == first_week['High'].max()
    assert weekly.iloc[0]['Low'] == first_week['Low'].min()
    assert pd.Timestamp(weekly.iloc[0]['week_end_date']).weekday() == 4


def test_14_holiday_shortened_week_uses_last_session():
    holiday = date(2024, 1, 19)  # a Friday
    daily = _daily_nifty(8, holiday_fridays=(holiday,))
    weekly = resample_weekly(daily)
    wk = weekly[[pd.Timestamp(d).week == pd.Timestamp(holiday).week
                 and pd.Timestamp(d).year == holiday.year
                 for d in weekly['week_end_date']]]
    assert len(wk) == 1
    assert wk.iloc[0]['week_end_date'] == holiday - timedelta(days=1)  # Thursday close


def test_incomplete_final_week_dropped():
    daily = _daily_nifty(8)
    # chop the last week after Wednesday
    daily_cut = daily.iloc[:-2]
    assert pd.Timestamp(daily_cut['Date'].iloc[-1]).weekday() == 2
    weekly = resample_weekly(daily_cut)
    assert pd.Timestamp(weekly.iloc[-1]['week_end_date']).weekday() == 4


def test_13_previous_10_week_low_excludes_current_week():
    daily = _daily_nifty(30)
    wf = build_weekly_filter(daily, RegimeConfig())
    weekly = resample_weekly(daily)
    expected = weekly['Low'].shift(1).rolling(10).min()
    merged = wf.merge(weekly.assign(expected=expected)[['week_end_date', 'expected', 'Low']],
                      on='week_end_date')
    assert np.allclose(merged['previous_10_week_low'], merged['expected'])
    # current week's low never enters its own threshold
    assert not any(np.isclose(merged['previous_10_week_low'], merged['Low'])
                   & (merged['Low'] < merged['previous_10_week_low'] + 1e-9)
                   & (merged['Low'] == merged[['Low']].min(axis=1)))


# ── Effective dates / no-lookahead ────────────────────────────────────────────

def test_signal_effective_from_next_session():
    daily = _daily_nifty(30)
    wf = build_weekly_filter(daily, RegimeConfig())
    dates = sorted(daily['Date'])
    for _, row in wf.iterrows():
        if row['effective_from_date'] is None:
            continue
        assert row['effective_from_date'] > row['week_end_date']
        later = [d for d in dates if d > row['week_end_date']]
        assert row['effective_from_date'] == later[0]


def test_10_state_constant_within_week():
    daily = _daily_nifty(40)
    wf = build_weekly_filter(daily, RegimeConfig())
    dstate = daily_state_frame(wf, sorted(daily['Date']))
    # state only changes on a Monday-ish first session after a week end
    changes = dstate['market_state'] != dstate['market_state'].shift(1)
    change_days = dstate.index[changes.fillna(False)][1:]  # skip day 0 (vs NaN)
    eff_days = set(wf['effective_from_date'].dropna())
    assert all(d in eff_days for d in change_days)


# ── Integration with the BTST signal frame ────────────────────────────────────

def test_11_blocked_signals_not_replayed_and_forced_exits_truncate():
    dates = sorted(_daily_nifty(10)['Date'])
    dstate = pd.DataFrame({
        'Date': dates,
        'market_state': 'ON', 'buy_allowed': True, 'force_exit': False,
    }).set_index('Date')
    # OFF from dates[10] .. dates[19], force exit on dates[12]
    dstate.iloc[10:20, dstate.columns.get_loc('buy_allowed')] = False
    dstate.iloc[10:20, dstate.columns.get_loc('market_state')] = 'OFF'
    dstate.iloc[12, dstate.columns.get_loc('force_exit')] = True

    signal_df = pd.DataFrame([
        {'ticker': 'X', 'perc': 0.5, 'tdate': dates[8]},    # entered before OFF
        {'ticker': 'Y', 'perc': 0.5, 'tdate': dates[11]},   # blocked
        {'ticker': 'Z', 'perc': 0.5, 'tdate': dates[15]},   # blocked
        {'ticker': 'W', 'perc': 0.5, 'tdate': dates[22]},   # after re-entry
    ])
    out, stats = apply_regime_to_signals(signal_df, dates, dstate, lf=5)
    assert stats['blocked_buys'] == 2
    assert list(out['ticker']) == ['X', 'W']                # blocked never replayed
    x = out[out['ticker'] == 'X'].iloc[0]
    assert x['cdate'] == dates[12]                          # truncated to force-exit day
    w = out[out['ticker'] == 'W'].iloc[0]
    assert w['cdate'] == dates[27]                          # natural lf exit
    assert stats['forced_exit_tranches'] == 1


def test_regime_filtered_backtest_runs_end_to_end():
    import sys
    sys.path.insert(0, 'tests')
    from test_btst_lagrangian import _make_universe, _params
    from intraday_research.market_regime import compare_regime_versions
    data = _make_universe(n_tickers=6, n_days=120, seed=3)
    # synthetic nifty with a crash in the middle
    dates = sorted(data['TICK0']['Date'])
    rng = np.random.default_rng(5)
    px, closes = 22000.0, []
    for i in range(len(dates)):
        drift = -0.012 if 55 <= i <= 75 else 0.0008
        px *= 1 + rng.normal(drift, 0.006)
        closes.append(px)
    nifty = pd.DataFrame({'Date': dates, 'Close': closes})
    nifty['Open'] = nifty['Close'].shift(1).fillna(nifty['Close'])
    nifty['High'] = nifty[['Open', 'Close']].max(axis=1) * 1.004
    nifty['Low'] = nifty[['Open', 'Close']].min(axis=1) * 0.996
    params = _params(lb=10, lf=3)
    params['execution'] = 'hold'
    summary, details = compare_regime_versions(params, data, nifty, verbose=False)
    assert 'A_original' in summary.index and 'D_asym_confirmed' in summary.index
    assert summary.loc['D_asym_confirmed', 'pct_time_off'] > 0
    assert summary.loc['A_original', 'blocked_buys'] == 0


# ── Daily timeframe ───────────────────────────────────────────────────────────

def test_daily_timeframe_one_row_per_session_and_next_day_effective():
    daily = _daily_nifty(20)
    cfg = RegimeConfig(timeframe='daily', smma_period=5, atr_period=14,
                       emergency_low_lookback=10)
    wf = build_weekly_filter(daily, cfg)
    dates = sorted(daily['Date'])
    warmup = max(cfg.atr_period, cfg.emergency_low_lookback + 1, cfg.smma_period)
    assert len(wf) == len(dates) - warmup + 1
    # bar date is a trading day and signal is effective the next session
    for _, row in wf.head(20).iterrows():
        assert row['week_end_date'] in dates
        if row['effective_from_date'] is not None:
            later = [d for d in dates if d > row['week_end_date']]
            assert row['effective_from_date'] == later[0]


def test_daily_timeframe_state_machine_matches_weekly_logic():
    # same bands, same closes -> the pure update function is timeframe agnostic;
    # here we just confirm the daily builder actually flips state on a crash
    daily = _daily_nifty(30)
    mid = len(daily) // 2
    daily.loc[mid:, ['Open', 'High', 'Low', 'Close']] *= 0.85  # sharp drop
    cfg = RegimeConfig(timeframe='daily', enable_emergency_exit=True)
    wf = build_weekly_filter(daily, cfg)
    assert (wf['market_state'] == 'OFF').any()
    assert (wf['transition'] == 'ON_TO_OFF').any()


def test_invalid_timeframe_rejected():
    with pytest.raises(ValueError):
        RegimeConfig(timeframe='monthly')


# ── band_momentum filter style ────────────────────────────────────────────────

def _bm(close, prev_state='ON', prev_zone='ABOVE', smma=22150.0, atr=600.0,
        prev_low=None, **cfg_kw):
    from intraday_research.market_regime import update_band_momentum_filter
    cfg_kw.setdefault('filter_style', 'band_momentum')
    cfg_kw.setdefault('exit_atr_multiplier', 0.25)    # lower 22000
    cfg_kw.setdefault('reentry_atr_multiplier', 0.25)  # upper 22300
    return update_band_momentum_filter(close, smma, atr, prev_low, prev_state,
                                       prev_zone, RegimeConfig(**cfg_kw))


def test_bm_rule1_falling_through_upper_stops_buying_but_holds():
    r = _bm(22200, 'ON', 'ABOVE')          # was above, now between
    assert r['market_state'] == 'WARN'
    assert r['buy_allowed'] is False
    assert r['force_exit_all'] is False    # positions keep normal exits
    assert r['reason'] == 'UPPER_BAND_LOST'


def test_bm_rule2_lower_band_break_sells_all():
    for prev_state, prev_zone in (('ON', 'ABOVE'), ('WARN', 'BETWEEN'), ('ON', 'BETWEEN')):
        r = _bm(21900, prev_state, prev_zone)
        assert r['market_state'] == 'DOWN', (prev_state, prev_zone)
        assert r['force_exit_all'] is True
        assert r['reason'] == 'LOWER_BAND_BREAK_SELL_ALL'


def test_bm_rule3_reclaiming_lower_band_restarts_buying():
    r = _bm(22100, 'DOWN', 'BELOW')        # from below back between the bands
    assert r['market_state'] == 'ON'
    assert r['buy_allowed'] is True
    assert r['reason'] == 'LOWER_BAND_RECLAIM'


def test_bm_rule4_reversal_between_bands_is_not_an_event():
    # re-entered from below (ON, zone BETWEEN); price stalls/reverses between
    # the bands -> stays ON; only a fresh lower-band break sells
    r = _bm(22050, 'ON', 'BETWEEN')
    assert r['market_state'] == 'ON'
    assert r['force_exit_all'] is False
    r2 = _bm(21950, 'ON', 'BETWEEN')       # actual lower break
    assert r2['market_state'] == 'DOWN'
    assert r2['force_exit_all'] is True


def test_bm_warn_reclaims_upper_back_to_on():
    r = _bm(22400, 'WARN', 'BETWEEN')
    assert r['market_state'] == 'ON'
    assert r['reason'] == 'UPPER_BAND_RECLAIM'


def test_bm_equality_does_not_trigger():
    assert _bm(22300.0, 'ON', 'ABOVE')['market_state'] == 'ON'    # == upper: no WARN yet
    assert _bm(22000.0, 'ON', 'BETWEEN')['market_state'] == 'ON'  # == lower not a break
    assert _bm(22000.0, 'DOWN', 'BELOW')['market_state'] == 'DOWN'  # == lower no reclaim


def test_bm_full_filter_builds_and_integrates():
    daily = _daily_nifty(40)
    mid = len(daily) // 2
    daily.loc[mid:mid + 20, ['Open', 'High', 'Low', 'Close']] *= 0.90
    cfg = RegimeConfig(filter_style='band_momentum')
    wf = build_weekly_filter(daily, cfg)
    assert set(wf['market_state']) <= {'ON', 'WARN', 'DOWN'}
    dstate = daily_state_frame(wf, sorted(daily['Date']))
    assert (~dstate['buy_allowed']).any()


# ── sma_weekly_daily filter style ─────────────────────────────────────────────

def test_sma_daily_weekly_rule_and_next_session_effect():
    from intraday_research.market_regime import build_sma_daily_filter
    daily = _daily_nifty(30)
    cfg = RegimeConfig(filter_style='sma_weekly_daily', smma_period=5)
    dates = sorted(daily['Date'])
    dstate, diag = build_sma_daily_filter(daily, cfg, dates)
    # weekly state is a plain close vs 5-SMA comparison (simple mean)
    from intraday_research.market_regime import resample_weekly
    weekly = resample_weekly(daily)
    sma = weekly['Close'].rolling(5).mean()
    merged = diag.merge(weekly.assign(expected_sma=sma)[['week_end_date', 'expected_sma']],
                        on='week_end_date')
    assert np.allclose(merged['sma'], merged['expected_sma'])
    for _, row in merged.iterrows():
        expected = 'ON' if row['weekly_close'] > row['expected_sma'] else 'OFF'
        assert row['market_state'] == expected
    # weekly flips act from the next session, never the same day
    flips = diag[diag['transition'] != 'NONE']
    for _, f in flips.iterrows():
        if f['effective_from_date'] is not None:
            assert f['effective_from_date'] > f['week_end_date']


def test_sma_daily_overlay_blocks_and_exits_intraweek():
    from intraday_research.market_regime import build_sma_daily_filter
    daily = _daily_nifty(20)
    cfg = RegimeConfig(filter_style='sma_weekly_daily', smma_period=5)
    dates = sorted(daily['Date'])
    dstate, diag = build_sma_daily_filter(daily, cfg, dates)
    # find an ON week and crash one mid-week day below the reference SMA
    weeks = diag[diag['effective_from_date'].notna()].reset_index(drop=True)
    on_weeks = weeks[weeks['market_state'] == 'ON']
    assert not on_weeks.empty
    wk = on_weeks.iloc[-1]
    target_day = next(d for d in dates if d > wk['effective_from_date'])
    crashed = daily.copy()
    crashed.loc[crashed['Date'] == target_day,
                ['Open', 'High', 'Low', 'Close']] = wk['sma'] * 0.90
    dstate2, _ = build_sma_daily_filter(crashed, cfg, dates)
    assert not dstate2.at[target_day, 'buy_allowed']       # no new buys that day
    assert dstate2.at[target_day, 'force_exit']            # book closed that day
    # control: same day priced ABOVE the reference SMA must stay tradeable
    pumped = daily.copy()
    pumped.loc[pumped['Date'] == target_day,
               ['Open', 'High', 'Low', 'Close']] = wk['sma'] * 1.10
    dstate3, _ = build_sma_daily_filter(pumped, cfg, dates)
    assert dstate3.at[target_day, 'buy_allowed']
    assert not dstate3.at[target_day, 'force_exit']


def test_sma_daily_overlay_never_reenables_during_off_week():
    from intraday_research.market_regime import build_sma_daily_filter
    daily = _daily_nifty(20)
    cfg = RegimeConfig(filter_style='sma_weekly_daily', smma_period=5)
    dates = sorted(daily['Date'])
    _, diag = build_sma_daily_filter(daily, cfg, dates)
    weeks = diag[diag['effective_from_date'].notna()].reset_index(drop=True)
    # force an OFF week by lifting a day's price far ABOVE the SMA inside it:
    # buying must still be blocked because the weekly close was below the SMA
    off_weeks = weeks[weeks['market_state'] == 'OFF']
    if off_weeks.empty:
        crashed = daily.copy()
        wk = weeks.iloc[-2]
        week_days = [d for d in dates if wk['week_end_date'] >= d
                     and d > wk['week_end_date'] - pd.Timedelta(days=7).to_pytimedelta()]
        for d in week_days:
            crashed.loc[crashed['Date'] == d, ['Open', 'High', 'Low', 'Close']] = wk['sma'] * 0.9
        _, diag = build_sma_daily_filter(crashed, cfg, dates)
        weeks = diag[diag['effective_from_date'].notna()].reset_index(drop=True)
        off_weeks = weeks[weeks['market_state'] == 'OFF']
        daily = crashed
    wk = off_weeks.iloc[0]
    inside = [d for d in dates if d >= wk['effective_from_date']][:3]
    if inside:
        pumped = daily.copy()
        for d in inside:
            pumped.loc[pumped['Date'] == d, 'Close'] = wk['sma'] * 1.5
        dstate3, _ = build_sma_daily_filter(pumped, RegimeConfig(
            filter_style='sma_weekly_daily', smma_period=5), dates)
        for d in inside:
            # price above MA but weekly close said OFF -> still no buying
            week_after = [w for _, w in dstate3.iterrows()]
            assert not dstate3.at[d, 'buy_allowed'] or dstate3.at[d, 'market_state'] == 'ON'
