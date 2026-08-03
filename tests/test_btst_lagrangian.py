from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from intraday_research.btst_lagrangian import (
    align_universe, compute_returns, generate_live_signals, get_trade_statistics,
    lagrangian_sig, lagrangian_ranking, run_backtest, trunc_siglo,
)


def _make_universe(n_tickers=8, n_days=60, seed=7):
    rng = np.random.default_rng(seed)
    start = date(2023, 1, 2)
    dates = []
    d = start
    while len(dates) < n_days:
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    data = {}
    for i in range(n_tickers):
        drift = rng.normal(0.0005, 0.001)
        close = 100 * np.cumprod(1 + rng.normal(drift, 0.01, n_days))
        gap = rng.normal(0, 0.005, n_days)
        open_ = close * (1 + gap)
        data[f'TICK{i}'] = pd.DataFrame({
            'Date': dates, 'Open': open_, 'High': np.maximum(open_, close) * 1.01,
            'Low': np.minimum(open_, close) * 0.99, 'Close': close,
            'Volume': 1000,
        })
    return data


def test_trunc_siglo_keeps_top_positive_weights():
    signals = np.array([0.4, -0.1, 0.3, 0.05, 0.2, 0.05])
    out = trunc_siglo(signals, trades=3)
    assert np.count_nonzero(out) == 3
    assert set(np.flatnonzero(out)) == {0, 2, 4}
    assert (out >= 0).all()


def test_lagrangian_sig_long_only_weights():
    rng = np.random.default_rng(3)
    returns = rng.normal(0.001, 0.01, size=(10, 20))
    weights = lagrangian_sig(returns, lagrangian_ranking, trades=5, long_only=1)
    assert weights.shape == (10,)
    assert (weights >= 0).all()
    assert np.count_nonzero(weights) <= 5
    assert np.isclose(np.abs(weights).sum(), 1.0)


def _params(lb=10, lf=3):
    return {
        'strategy_id': '1lg0',
        'frequency_type': 'close to open',
        'tot_capital': 10_00_000,
        'lb': lb,
        'trades': 3,
        'lf': lf,
        'risk_free_rate': 0.02,
        'long_only': 1,
    }


def test_run_backtest_trade_frame_consistency():
    data = _make_universe()
    params = _params()
    trade_df = run_backtest(params, data, verbose=False)
    assert not trade_df.empty
    delivery = trade_df[trade_df['perc'] != 0]
    intra = trade_df[trade_df['perc'] == 0]
    # every delivery leg entered at tdate close, exits lf trading days later at open
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    row = delivery[delivery['cdate'].notna()].iloc[0]
    assert dates.index(row['cdate']) - dates.index(row['tdate']) == params['lf']
    assert row['open_price'] == pytest.approx(closes.at[row['tdate'], row['ticker']])
    assert row['close_price'] == pytest.approx(opens.at[row['cdate'], row['ticker']])
    # delivery pnl = qty * (exit open - entry close)
    assert row['pnl'] == pytest.approx(row['quantity'] * (row['close_price'] - row['open_price']))
    # intraday flat legs are priced open -> close on the same date
    if not intra.empty:
        leg = intra.iloc[0]
        assert leg['tdate'] == leg['cdate']
        assert leg['pnl'] == pytest.approx(leg['quantity'] * (leg['open_price'] - leg['close_price']))
    # costs populated on the right rows
    assert (delivery['del_cost'] > 0).all()
    assert (delivery['intra_cost'] == 0).all()
    if not intra.empty:
        assert (intra['intra_cost'] > 0).all()
        assert (intra['del_cost'] == 0).all()


def test_net_pnl_equals_sum_of_overnight_moves():
    """Delivery pnl minus intraday flat legs must equal holding overnight only."""
    data = _make_universe(n_tickers=6, n_days=40, seed=11)
    params = _params(lb=10, lf=3)
    trade_df = run_backtest(params, data, verbose=False)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    completed = trade_df[(trade_df['perc'] != 0) & (trade_df['cdate'].notna())]
    expected = 0.0
    for _, row in completed.iterrows():
        i, j = dates.index(row['tdate']), dates.index(row['cdate'])
        overnight = sum(
            opens.at[dates[k + 1], row['ticker']] - closes.at[dates[k], row['ticker']]
            for k in range(i, j)
        )
        expected += row['quantity'] * overnight
    # intraday legs of still-open positions don't net against a completed exit,
    # so only compare when every position completed
    open_positions = trade_df[(trade_df['perc'] != 0) & (trade_df['cdate'].isna())]
    if open_positions.empty:
        assert trade_df['pnl'].sum() == pytest.approx(expected, rel=1e-6)


def test_get_trade_statistics_shapes():
    data = _make_universe()
    trade_df = run_backtest(_params(), data, verbose=False)
    daily, metrics = get_trade_statistics(trade_df, _params())
    assert {'pnl_pct', 'act_pnl_pct', 'act_pnl_pct2'} <= set(daily.columns)
    assert list(metrics.index) == ['gross', 'net (discount broker)', 'net (full-service broker)']


def test_generate_live_signals():
    data = _make_universe()
    params = _params()
    orders = generate_live_signals(params, data)
    assert not orders.empty
    assert len(orders) <= params['trades']
    assert (orders['perc'] > 0).all()
    assert np.isclose(orders['perc'].sum(), 1.0)
    # quantity matches capital tranche
    row = orders.iloc[0]
    assert row['quantity'] == int(row['perc'] * params['tot_capital'] / (params['lf'] + 1) / row['buy_price'])


def test_all_objectives_produce_valid_weights():
    from intraday_research.btst_lagrangian import OBJECTIVES
    rng = np.random.default_rng(5)
    returns = rng.normal(0.001, 0.01, size=(8, 15))
    for name, foo in OBJECTIVES.items():
        weights = lagrangian_sig(returns, foo, trades=4, long_only=1)
        assert np.count_nonzero(weights) <= 4, name
        assert (weights >= 0).all(), name
        assert np.isclose(np.abs(weights).sum(), 1.0), name


def test_hold_mode_has_no_flat_legs():
    data = _make_universe()
    params = {**_params(), 'execution': 'hold'}
    trade_df = run_backtest(params, data, verbose=False)
    assert (trade_df['perc'] != 0).all()
    assert (trade_df['intra_cost'] == 0).all()


def test_delta_gross_pnl_matches_hold_for_single_tranche():
    from intraday_research.btst_lagrangian import backtest_daily_from_signals
    data = _make_universe(n_tickers=4, n_days=30, seed=2)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    signal_df = pd.DataFrame([{'ticker': 'TICK0', 'perc': 1.0, 'tdate': dates[5]}])
    params = {**_params(lb=5, lf=4), 'trades': 1}
    hold_daily, _ = backtest_daily_from_signals({**params, 'execution': 'hold'},
                                                signal_df, opens, closes)
    delta_daily, _ = backtest_daily_from_signals({**params, 'execution': 'delta'},
                                                 signal_df, opens, closes)
    assert delta_daily['pnl'].sum() == pytest.approx(hold_daily['pnl'].sum(), rel=1e-9)
    # single tranche: one buy at entry close + one sell at exit open, no churn between
    mid = delta_daily.loc[[d for d in delta_daily.index if dates[5] < d < dates[9]]]
    assert (mid['turnover'] == 0).all()


def test_delta_costs_below_flat_legs():
    data = _make_universe(n_tickers=6, n_days=50, seed=4)
    from intraday_research.btst_lagrangian import backtest_daily
    params = _params(lb=10, lf=5)
    flat_daily, _ = backtest_daily({**params, 'execution': 'flat_legs'}, data, verbose=False)
    delta_daily, _ = backtest_daily({**params, 'execution': 'delta'}, data, verbose=False)
    flat_cost = (flat_daily['intra_cost'] + flat_daily['del_cost']).sum()
    delta_cost = delta_daily['del_cost'].sum()
    assert delta_cost < flat_cost


def test_run_sweep_leaderboard():
    from intraday_research.btst_sweeps import run_sweep
    data = _make_universe(n_tickers=6, n_days=40, seed=9)
    grid = {'lb': [8], 'trades': [3], 'lf': [1, 3], 'execution': ['flat_legs', 'hold', 'delta']}
    board = run_sweep(data, base_params=_params(), grid=grid, verbose=False)
    assert len(board) == 6
    assert {'net_sharpe', 'net_pct', 'gross_pct', 'execution', 'lf'} <= set(board.columns)
    assert board['net_sharpe'].is_monotonic_decreasing


def test_session_daily_from_minute_captures_configured_times():
    from intraday_research.btst_lagrangian import session_daily_from_minute
    ts = pd.date_range('2025-01-01 09:15', '2025-01-01 15:29', freq='min', tz='Asia/Kolkata')
    ts = ts.append(pd.date_range('2025-01-02 09:15', '2025-01-02 15:29', freq='min', tz='Asia/Kolkata'))
    n = len(ts)
    minute_df = pd.DataFrame({
        'timestamp': ts,
        'open': np.arange(n, dtype=float),
        'high': np.arange(n, dtype=float) + 0.5,
        'low': np.arange(n, dtype=float) - 0.5,
        'close': np.arange(n, dtype=float) + 0.25,
        'volume': 10,
    })
    daily = session_daily_from_minute(minute_df, entry_price_time='15:20', exit_price_time='09:20')
    assert len(daily) == 2
    lookup = minute_df.assign(hm=minute_df['timestamp'].dt.strftime('%H:%M'),
                              d=minute_df['timestamp'].dt.date)
    for _, row in daily.iterrows():
        day = lookup[lookup['d'] == row['Date']]
        assert row['Open'] == day[day['hm'] == '09:20']['open'].iloc[0]
        assert row['Close'] == day[day['hm'] == '15:20']['open'].iloc[0]
    # different configured times capture different prices
    daily2 = session_daily_from_minute(minute_df, entry_price_time='15:25', exit_price_time='09:25')
    assert (daily2['Open'] > daily['Open']).all()
    assert (daily2['Close'] > daily['Close']).all()


def test_session_daily_falls_back_to_nearest_candle_in_window():
    from intraday_research.btst_lagrangian import session_daily_from_minute
    ts = pd.date_range('2025-01-01 09:15', '2025-01-01 15:29', freq='min', tz='Asia/Kolkata')
    minute_df = pd.DataFrame({
        'timestamp': ts, 'open': np.arange(len(ts), dtype=float),
        'high': 1.0, 'low': 0.0, 'close': 1.0, 'volume': 1,
    })
    hm = minute_df['timestamp'].dt.strftime('%H:%M')
    gappy = minute_df[~hm.isin(['09:20', '15:20'])]  # exact minutes missing
    daily = session_daily_from_minute(gappy, entry_price_time='15:20', exit_price_time='09:20')
    lookup = gappy.assign(hm=gappy['timestamp'].dt.strftime('%H:%M'))
    assert daily['Open'].iloc[0] == lookup[lookup['hm'] == '09:21']['open'].iloc[0]
    assert daily['Close'].iloc[0] == lookup[lookup['hm'] == '15:19']['open'].iloc[0]


def test_all_modes_equal_for_single_overnight_trade():
    """lf=1, one tranche: flat_legs, hold and delta must produce the exact
    same gross pnl = qty * (next open - entry close)."""
    from intraday_research.btst_lagrangian import backtest_daily_from_signals
    data = _make_universe(n_tickers=4, n_days=20, seed=13)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    signal_df = pd.DataFrame([{'ticker': 'TICK1', 'perc': 1.0, 'tdate': dates[8]}])
    params = {**_params(lb=5, lf=1), 'trades': 1}
    expected_qty = int(params['tot_capital'] / 2 / closes.at[dates[8], 'TICK1'])
    expected = expected_qty * (opens.at[dates[9], 'TICK1'] - closes.at[dates[8], 'TICK1'])
    for execution in ['flat_legs', 'hold', 'delta']:
        daily_x, _ = backtest_daily_from_signals({**params, 'execution': execution},
                                                 signal_df, opens, closes)
        assert daily_x['pnl'].sum() == pytest.approx(expected, rel=1e-9), execution


def test_no_lookahead_backtest_signal_reproducible_from_truncated_data():
    """The backtest signal for day T must be identical to what the live path
    produces when it can only see data up to day T."""
    from intraday_research.btst_lagrangian import generate_signal_history
    data = _make_universe(n_tickers=6, n_days=40, seed=21)
    params = _params(lb=10, lf=3)
    opens, closes = align_universe(data, verbose=False)
    signal_df = generate_signal_history(params, opens, closes, verbose=False)
    tdate = sorted(signal_df['tdate'].unique())[5]
    backtest_day = signal_df[signal_df['tdate'] == tdate].set_index('ticker')['perc']

    truncated = {t: df[df['Date'] <= tdate].reset_index(drop=True) for t, df in data.items()}
    live = generate_live_signals(params, truncated).set_index('ticker')['perc']

    assert set(live.index) == set(backtest_day.index)
    for ticker in live.index:
        assert live[ticker] == pytest.approx(backtest_day[ticker], rel=1e-9)


def test_upper_bound_caps_weights_and_forces_diversification():
    rng = np.random.default_rng(6)
    returns = rng.normal(0.001, 0.01, size=(12, 25))
    tight = lagrangian_sig(returns, lagrangian_ranking, trades=12, long_only=1, upper_bound=0.2)
    assert (tight <= 0.2 + 1e-8).all()
    assert np.count_nonzero(tight > 1e-6) >= 5  # sum-to-1 with 0.2 cap needs >= 5 names
    loose = lagrangian_sig(returns, lagrangian_ranking, trades=12, long_only=1, upper_bound=0.5)
    assert np.count_nonzero(loose > 1e-6) <= np.count_nonzero(tight > 1e-6)


def test_delta_pnl_reconciles_with_cash_ledger():
    """Independent verification: simulate a plain cash ledger (money out on
    buys, money in on sells, final holdings marked at last close). Must equal
    the engine's summed daily pnl exactly."""
    from intraday_research.btst_lagrangian import backtest_daily, align_universe, \
        generate_signal_history, _size_signals
    data = _make_universe(n_tickers=6, n_days=45, seed=17)
    params = {**_params(lb=10, lf=4), 'execution': 'delta'}
    opens, closes = align_universe(data, verbose=False)
    signal_df = generate_signal_history(params, opens, closes, verbose=False)
    daily, _ = backtest_daily(params, data, verbose=False, signal_df=signal_df)

    sized = _size_signals(signal_df, params, opens, closes)
    dates = list(closes.index)
    pos = {d: i for i, d in enumerate(dates)}
    # independent target book
    import collections
    book = {d: collections.defaultdict(float) for d in dates}
    for _, r in sized.iterrows():
        end = pos[r['cdate']] if r['cdate'] is not None else len(dates)
        for k in range(pos[r['tdate']], end):
            book[dates[k]][r['ticker']] += r['quantity']
    cash = 0.0
    prev = collections.defaultdict(float)
    for d in dates:
        cur = book[d]
        for t in set(prev) | set(cur):
            diff = cur.get(t, 0) - prev.get(t, 0)
            if diff < 0:
                cash += -diff * opens.at[d, t]      # net sells at the open
            elif diff > 0:
                cash -= diff * closes.at[d, t]      # net buys at the close
        prev = cur
    final_value = sum(q * closes.at[dates[-1], t] for t, q in prev.items())
    assert cash + final_value == pytest.approx(daily['pnl'].sum(), rel=1e-9)


def test_delta_close_single_tranche_is_close_to_close():
    from intraday_research.btst_lagrangian import backtest_daily_from_signals
    data = _make_universe(n_tickers=4, n_days=30, seed=2)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    signal_df = pd.DataFrame([{'ticker': 'TICK0', 'perc': 1.0, 'tdate': dates[5]}])
    params = {**_params(lb=5, lf=4), 'trades': 1, 'execution': 'delta_close'}
    daily_dc, _ = backtest_daily_from_signals(params, signal_df, opens, closes)
    qty = int(params['tot_capital'] / 5 / closes.at[dates[5], 'TICK0'])
    expected = qty * (closes.at[dates[9], 'TICK0'] - closes.at[dates[5], 'TICK0'])
    assert daily_dc['pnl'].sum() == pytest.approx(expected, rel=1e-9)
    # only two trading days: entry buy and exit sell, both at the close
    assert (daily_dc['turnover'] > 0).sum() == 2


def test_backtest_start_aligns_all_lookbacks():
    """With backtest_start set, every lb variant must produce its first signal
    on the same date, using warm-up data for the lookback window."""
    from intraday_research.btst_lagrangian import generate_signal_history
    data = _make_universe(n_tickers=6, n_days=80, seed=8)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    start = dates[40]
    firsts = {}
    for lb in (10, 20, 30):
        sig = generate_signal_history({**_params(lb=lb), 'backtest_start': start},
                                      opens, closes, verbose=False)
        firsts[lb] = sig['tdate'].min()
        assert (sig['tdate'] >= start).all()
    assert len(set(firsts.values())) == 1
    assert firsts[10] == start
    # string dates work too
    sig = generate_signal_history({**_params(lb=10), 'backtest_start': str(start)},
                                  opens, closes, verbose=False)
    assert sig['tdate'].min() == start


def test_backtest_start_uses_warmup_returns():
    """Signals from a warmed-up run must be identical to the unrestricted run
    on the overlapping dates (lookback reaches into warm-up data)."""
    from intraday_research.btst_lagrangian import generate_signal_history
    data = _make_universe(n_tickers=5, n_days=60, seed=15)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    start = dates[30]
    params = _params(lb=12)
    full = generate_signal_history(params, opens, closes, verbose=False)
    full = full[full['tdate'] >= start].reset_index(drop=True)
    warmed = generate_signal_history({**params, 'backtest_start': start},
                                     opens, closes, verbose=False)
    pd.testing.assert_frame_equal(full, warmed)


def test_backtest_start_insufficient_warmup_raises():
    from intraday_research.btst_lagrangian import generate_signal_history
    data = _make_universe(n_tickers=5, n_days=40, seed=15)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    with pytest.raises(ValueError, match='warm-up'):
        generate_signal_history({**_params(lb=20), 'backtest_start': dates[5]},
                                opens, closes, verbose=False)


def test_log_returns_option():
    data = _make_universe(n_tickers=4, n_days=20, seed=1)
    opens, closes = align_universe(data, verbose=False)
    simple = compute_returns(opens, closes, 'close to open', return_type='simple')
    logr = compute_returns(opens, closes, 'close to open', return_type='log')
    assert np.allclose(logr.values, np.log(1 + simple.values))
    with pytest.raises(ValueError):
        compute_returns(opens, closes, 'close to open', return_type='weird')


def test_day_netted_same_day_pair_charged_intraday():
    """Exit sell at 09:20 matched by a same-day 15:20 entry buy in the same
    ticker must be charged intraday, not delivery, on the matched quantity."""
    from intraday_research.btst_lagrangian import (
        backtest_daily_from_signals, day_netted_costs, _trade_ledger_from_trades,
        STT_DELIVERY, STT_INTRADAY)
    data = _make_universe(n_tickers=4, n_days=30, seed=2)
    opens, closes = align_universe(data, verbose=False)
    dates = list(closes.index)
    # tranche 1 exits on dates[9] (lf=4), tranche 2 enters the same day
    signal_df = pd.DataFrame([
        {'ticker': 'TICK0', 'perc': 1.0, 'tdate': dates[5]},
        {'ticker': 'TICK0', 'perc': 1.0, 'tdate': dates[9]},
    ])
    params = {**_params(lb=5, lf=4), 'trades': 1, 'execution': 'hold'}
    netted, _ = backtest_daily_from_signals({**params, 'cost_model': 'day_netted'},
                                            signal_df, opens, closes)
    legacy, _ = backtest_daily_from_signals({**params, 'cost_model': 'per_leg'},
                                            signal_df, opens, closes)
    total_netted = (netted['intra_cost'] + netted['del_cost']).sum()
    total_legacy = (legacy['intra_cost'] + legacy['del_cost']).sum()
    assert total_netted < total_legacy      # delivery STT avoided on the pair
    # gross pnl identical — only cost attribution changes
    assert netted['pnl'].sum() == pytest.approx(legacy['pnl'].sum())
    # the overlap day carries intraday cost in the netted model
    assert netted.at[dates[9], 'intra_cost'] > 0


def test_day_netted_residual_charged_delivery():
    from intraday_research.btst_lagrangian import day_netted_costs
    # 100 bought, 40 sold same day at flat prices -> 40 matched intraday,
    # 60 residual delivery buy (stamp, no delivery STT)
    ledger = pd.DataFrame([{
        'Date': date(2024, 1, 1), 'ticker': 'X',
        'buy_qty': 100.0, 'buy_value': 100.0 * 500,
        'sell_qty': 40.0, 'sell_value': 40.0 * 505,
    }])
    costs = day_netted_costs(ledger)
    from intraday_research.btst_lagrangian import (
        BROKERAGE_INTRADAY_DISCBROK, MAX_BROKERAGE_INTRADAY_DISCBROK,
        TRANSACTION_CHARGE, SEBI_CHARGE, GST, STT_INTRADAY, STAMP_INTRADAY,
        STAMP_DELIVERY)
    mbv, msv = 40 * 500.0, 40 * 505.0
    brok = min(mbv * BROKERAGE_INTRADAY_DISCBROK, 20) + min(msv * BROKERAGE_INTRADAY_DISCBROK, 20)
    expected_intra = ((brok + (mbv + msv) * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
                      + msv * STT_INTRADAY + mbv * STAMP_INTRADAY)
    rbv = 60 * 500.0
    expected_del = (rbv * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST) + rbv * STAMP_DELIVERY
    assert costs['intra_cost'].iloc[0] == pytest.approx(expected_intra)
    assert costs['del_cost'].iloc[0] == pytest.approx(expected_del)


def test_day_netted_default_and_delta_unaffected():
    from intraday_research.btst_lagrangian import backtest_daily_from_signals
    data = _make_universe(n_tickers=5, n_days=40, seed=3)
    opens, closes = align_universe(data, verbose=False)
    from intraday_research.btst_lagrangian import generate_signal_history
    signal_df = generate_signal_history(_params(lb=8, lf=4), opens, closes, verbose=False)
    params = _params(lb=8, lf=4)
    # flat_legs day-netted (default) costs strictly below the per-leg model
    d_net, _ = backtest_daily_from_signals({**params, 'execution': 'flat_legs'},
                                           signal_df, opens, closes)
    d_leg, _ = backtest_daily_from_signals({**params, 'execution': 'flat_legs',
                                            'cost_model': 'per_leg'}, signal_df, opens, closes)
    assert (d_net['intra_cost'] + d_net['del_cost']).sum() < (d_leg['intra_cost'] + d_leg['del_cost']).sum()
    # delta modes place one net trade per ticker/day -> cost_model irrelevant
    for ex in ('delta', 'delta_close'):
        a, _ = backtest_daily_from_signals({**params, 'execution': ex}, signal_df, opens, closes)
        b, _ = backtest_daily_from_signals({**params, 'execution': ex, 'cost_model': 'per_leg'},
                                           signal_df, opens, closes)
        assert a['del_cost'].sum() == pytest.approx(b['del_cost'].sum())
