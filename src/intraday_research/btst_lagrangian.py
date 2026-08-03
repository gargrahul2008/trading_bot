"""
BTST Lagrangian portfolio strategy ('1lg0') — ported from traderealm.

Live behaviour (traderealm/nse_order_creation.py + backtest/runner.py):
  Every trading day near the close (~15:24 IST) the strategy takes the last
  `lb` trading days of close→open returns for every ticker in the universe,
  maximises a Sharpe-like Lagrangian objective under long-only weight bounds
  (0..0.5, weights sum to 1), keeps only the top `trades` weights
  (re-normalised to sum to 1), and buys each selected name at the close with
  capital  perc * tot_capital / (lf + 1).

  The book is sold at the next open and the still-pending net position is
  re-bought at the close every day, so market exposure is overnight-only
  (BTST) for `lf` consecutive nights per signal.

Execution modes (params['execution']):
  'flat_legs' (default) — the original live behaviour above: overnight-only
      exposure, with perc=0 rows modelling the daily sell-open/re-buy-close
      round trips while a position is pending.
  'hold' — buy at entry close, hold continuously, sell at exit open. No flat
      legs, so delivery costs only, but exposure includes intraday moves.
  'delta' — maintain the net target book and trade only daily differences
      (sells at open, buys at close). This is what the live consolidated
      orders effectively did; overlapping tranches in the same ticker are
      netted so churn (and cost) drops sharply versus 'flat_legs'.

Backtest trade frame columns match the original traderealm runner:
  ticker, perc, tdate, cdate, open_price (entry close), close_price
  (exit open), quantity, pnl, buy_value, sell_value, trade_value,
  intra_cost / del_cost   (discount broker profile),
  intra_cost2 / del_cost2 (full-service broker profile).
Rows with perc == 0 are the intermediate intraday flat legs.
"""
from __future__ import annotations

from functools import partial
from math import exp, sqrt

import numpy as np
import pandas as pd
from scipy.optimize import minimize

# ── Cost constants (traderealm/backtest/cost.py) ─────────────────────────────
BROKERAGE_INTRADAY_DISCBROK = 0.03 / 100
MAX_BROKERAGE_INTRADAY_DISCBROK = 20
BROKERAGE_DELIVERY_DISCBROK = 0
BROKERAGE_INTRADAY_KS = 0
MIN_BROKERAGE_DELIVERY_KS = 20
BROKERAGE_DELIVERY_KS = 0.25 / 100
TRANSACTION_CHARGE = 0.00335 / 100
SEBI_CHARGE = 0.0001 / 100
GST = 18 / 100
STT_INTRADAY = 0.025 / 100
STT_DELIVERY = 0.1 / 100
STAMP_INTRADAY = 0.003 / 100
STAMP_DELIVERY = 0.015 / 100
SLIPPAGE = 0.0 / 100

# optimiser noise below this weight is treated as zero (would round to 0 quantity)
WEIGHT_EPS = 1e-6


# ── Optimisation objectives (traderealm/strategy/utils.py) ───────────────────

def lagrangian_ranking(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix,
                       penalty=0.05):
    portfolio_return = np.dot(weights, returns_mean)
    portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(covariance_matrix, weights)))
    objective = (portfolio_return - risk_free_rate) / portfolio_volatility
    return -objective + penalty * np.sum(weights ** 2)


def sortino_ratio(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix):
    port_return = weights.dot(returns_mean)
    downside_mask = returns_mean - risk_free_rate < 0
    downside_deviation = np.sqrt(((returns_mean[downside_mask] - risk_free_rate) ** 2).mean())
    return -(port_return - risk_free_rate) / downside_deviation


def calmar_ratio(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix):
    port_series = weights.dot(returns_matrix)
    cumulative = np.cumprod(1 + port_series)
    rolling_max = np.maximum.accumulate(cumulative)
    max_dd = np.max((rolling_max - cumulative) / rolling_max)
    avg_return = port_series.mean()
    return -avg_return / max_dd


def mean_variance_utility(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix,
                          risk_aversion=1):
    port_return = weights.dot(returns_mean)
    port_var = weights.dot(covariance_matrix).dot(weights)
    return -(port_return - 0.5 * risk_aversion * port_var)


def cvar_objective(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix,
                   alpha=0.05):
    port_rets = weights.dot(returns_matrix)
    var = np.percentile(port_rets, 100 * alpha)
    cvar = port_rets[port_rets <= var].mean()
    return cvar


def omega_ratio(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix,
                threshold=0.0):
    port_rets = weights.dot(returns_matrix)
    gains = np.sum(np.maximum(port_rets - threshold, 0))
    losses = np.sum(np.maximum(threshold - port_rets, 0))
    return -gains / losses


def min_variance(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix):
    return weights.dot(covariance_matrix).dot(weights)


def combined_all(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix,
                 w_sharpe=1.0, w_cvar=1.0, w_minvar=1.0):
    Ln = lagrangian_ranking(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix)
    Sn = sortino_ratio(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix)
    Cn = cvar_objective(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix)
    mvn = mean_variance_utility(weights, returns_mean, covariance_matrix, risk_free_rate, returns_matrix)
    comp = w_sharpe * Ln + w_sharpe * Sn + w_cvar * Cn + w_minvar * mvn
    return -comp


OBJECTIVES = {
    'lagrangian': lagrangian_ranking,
    'sortino': sortino_ratio,
    'calmar': calmar_ratio,
    'mean_variance': mean_variance_utility,
    'cvar': cvar_objective,
    'omega': omega_ratio,
    'min_variance': min_variance,
    'combined': combined_all,
}


def resolve_objective(params):
    """Objective from params: 'objective' name string, or a 'foo' callable."""
    foo = params.get('foo')
    if foo is None:
        foo = OBJECTIVES[params.get('objective', 'lagrangian')]
    if foo is lagrangian_ranking and 'l2_penalty' in params:
        foo = partial(lagrangian_ranking, penalty=params['l2_penalty'])
    return foo


# ── Signal truncation helpers (traderealm/strategy/helper.py) ────────────────

def trunc_siglo(signals, trades=5):
    top_indices = np.argpartition(-signals, trades)[:trades]
    out = np.zeros_like(signals)
    out[top_indices] = np.maximum(signals[top_indices], 0)
    return out


def trunc_sigso(signals, trades=5):
    top_indices = np.argpartition(signals, trades)[:trades]
    out = np.zeros_like(signals)
    out[top_indices] = np.minimum(signals[top_indices], 0)
    return out


def trunc_sig(signals, trades=5):
    top_indices = np.argsort(np.abs(signals))[-trades:]
    out = np.zeros_like(signals)
    out[top_indices] = signals[top_indices]
    return out


def truncate_weights(weights, trades, long_only=1):
    """Keep the top `trades` weights, re-normalised to sum(|w|)=1."""
    weights = np.asarray(weights, dtype=float)
    if trades >= len(weights):
        signals = weights.copy()
        if long_only == 1:
            signals = np.maximum(signals, 0)
        elif long_only == -1:
            signals = np.minimum(signals, 0)
    elif long_only == 1:
        signals = trunc_siglo(weights, trades)
    elif long_only == -1:
        signals = trunc_sigso(weights, trades)
    else:
        signals = trunc_sig(weights, trades)
    total = np.sum(np.abs(signals))
    return signals / total if total else signals


# ── Portfolio optimiser (traderealm/strategy/utils.py::lagrangian_sig) ───────

def optimize_weights(returns, foo, risk_free_rate=0.02, method='SLSQP', rmean='sma', long_only=1,
                     lower_bound=-0.5, upper_bound=0.5):
    """returns: (n_tickers, lb) matrix. Returns raw optimised weights
    (before top-N truncation). upper_bound is the per-name weight cap: with
    long-only weights summing to 1, a cap of 0.2 forces at least 5 names.
    (traderealm hardcoded 0.5 and ignored its upper_bound param.)"""
    if rmean == 'wma':
        ma_weight = np.array(list(range(1, returns.shape[1] + 1)))
        returns_mean = np.dot(np.dot(returns, ma_weight), 1 / np.sum(ma_weight))
    elif rmean == 'ewma':
        ma_weight = np.array([sqrt(sqrt(exp(i))) for i in range(1, returns.shape[1] + 1)])
        returns_mean = np.dot(np.dot(returns, ma_weight), 1 / np.sum(ma_weight))
    else:  # 'sma'
        returns_mean = np.mean(returns, axis=1)
    covariance_matrix = np.cov(returns)
    n = returns.shape[0]
    if long_only == 1:
        bounds = [(0, upper_bound)] * n
        constraints = ({'type': 'eq', 'fun': lambda weights: np.sum(weights) - 1})
        initial_weights = np.full(n, 1.0 / n)
    elif long_only == -1:
        bounds = [(lower_bound, 0)] * n
        constraints = ({'type': 'eq', 'fun': lambda weights: np.sum(weights) + 1})
        initial_weights = np.full(n, -1.0 / n)
    else:
        bounds = [(lower_bound, upper_bound)] * n
        constraints = ({'type': 'eq', 'fun': lambda weights: np.sum(weights) - 1})
        initial_weights = np.full(n, 1.0 / n)
    result = minimize(
        foo,
        initial_weights,
        args=(returns_mean, covariance_matrix, risk_free_rate, returns),
        method=method,
        bounds=bounds,
        constraints=constraints,
    )
    return result.x


def lagrangian_sig(returns, foo, risk_free_rate=0.02, trades=5, method='SLSQP',
                   lower_bound=-0.5, upper_bound=0.5, rmean='sma', long_only=1):
    weights = optimize_weights(returns, foo, risk_free_rate=risk_free_rate, method=method,
                               rmean=rmean, long_only=long_only,
                               lower_bound=lower_bound, upper_bound=upper_bound)
    return truncate_weights(weights, trades, long_only)


def get_strategy_signals(params, returns):
    trades = params.get('trades', 5)
    if params['strategy_id'] == '1lg0':
        return lagrangian_sig(
            returns,
            resolve_objective(params),
            risk_free_rate=params.get('risk_free_rate', 0.02),
            trades=trades,
            method=params.get('method', 'SLSQP'),
            lower_bound=params.get('lower_bound', -0.5),
            upper_bound=params.get('upper_bound', 0.5),
            rmean=params.get('rmean', 'sma'),
            long_only=params.get('long_only', 1),
        )
    raise ValueError(f"Unknown strategy_id: {params['strategy_id']}")


# ── Session bars from minute data ────────────────────────────────────────────

def session_daily_from_minute(minute_df: pd.DataFrame, entry_price_time: str = '15:20',
                              exit_price_time: str = '09:20') -> pd.DataFrame:
    """Collapse 1-minute bars into one row per day with prices captured at the
    strategy's actual execution times (mirrors traderealm's daystartendtime
    resampling, which used a 09:20–15:20 session):

      Close = traded price at entry_price_time  (entry/signal near market close)
      Open  = traded price at exit_price_time   (exit shortly after next open)

    The price at a minute mark is that candle's open. If the exact minute is
    missing (halt, short session), the nearest candle inside the
    [exit_price_time, entry_price_time] window is used: first at/after
    exit_price_time for Open, last at/before entry_price_time for Close.
    Days with no candle in the window are dropped.
    """
    df = minute_df.copy()
    ts = pd.to_datetime(df['timestamp'])
    try:
        ts = ts.dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
    except TypeError:
        pass  # already tz-naive
    df['Date'] = ts.dt.date
    times = ts.dt.strftime('%H:%M')
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                            'close': 'Close', 'volume': 'Volume'})
    window = df[(times >= exit_price_time) & (times <= entry_price_time)]
    if window.empty:
        return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    window = window.sort_values('timestamp')
    daily = window.groupby('Date').agg(
        open_px=('Open', 'first'),   # price at exit_price_time
        close_px=('Open', 'last'),   # price at entry_price_time (candle open = price at the mark)
        High=('High', 'max'),
        Low=('Low', 'min'),
        Volume=('Volume', 'sum'),
    ).reset_index()
    daily = daily.rename(columns={'open_px': 'Open', 'close_px': 'Close'})
    return daily[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]


# ── Data alignment and returns ───────────────────────────────────────────────

def align_universe(data: dict[str, pd.DataFrame], verbose: bool = True):
    """Build wide Open/Close frames indexed by trade date.

    Mirrors traderealm getleneqt: tickers without full history over the union
    of dates are dropped.
    """
    closes = pd.DataFrame({t: df.set_index('Date')['Close'] for t, df in data.items()}).sort_index()
    opens = pd.DataFrame({t: df.set_index('Date')['Open'] for t, df in data.items()}).sort_index()
    full = [t for t in closes.columns if closes[t].notna().all() and opens[t].notna().all()]
    dropped = sorted(set(closes.columns) - set(full))
    if dropped and verbose:
        print('Dropped tickers (incomplete history):', dropped)
    return opens[full], closes[full]


def compute_returns(opens: pd.DataFrame, closes: pd.DataFrame, frequency_type: str,
                    return_type: str = 'simple') -> pd.DataFrame:
    """With session bars, 'close to open' is the 15:20 -> next-day 09:20 move.

    return_type 'simple' (traderealm's active code) or 'log' (the variant its
    commented-out line suggests ran live in 2023: log(Open / prev Close))."""
    if frequency_type == 'close to open':
        ratio = opens / closes.shift(1)
    elif frequency_type == 'close to close':
        ratio = closes / closes.shift(1)
    elif frequency_type == 'open to close':
        ratio = closes / opens
    else:
        raise ValueError(f'Unknown frequency_type: {frequency_type}')
    if return_type == 'simple':
        rets = ratio - 1
    elif return_type == 'log':
        rets = np.log(ratio)
    else:
        raise ValueError(f"return_type must be 'simple' or 'log', got {return_type!r}")
    return rets.dropna(how='all')


# ── Signal history ───────────────────────────────────────────────────────────

def generate_weight_history(params, opens, closes, verbose=True) -> pd.DataFrame:
    """Raw optimiser weights for every rolling window (dates x tickers).

    Independent of `trades`, `lf`, `execution` and capital — sweeps reuse one
    weight history across all of those.

    params['backtest_start'] (date or 'YYYY-MM-DD'): emit signals only from
    this date onward. Load data with enough warm-up history before it so the
    lookback is always full — then every variant, whatever its `lb`, trades
    the exact same evaluation period.
    """
    lb = params['lb']
    backtest_start = params.get('backtest_start')
    if backtest_start is not None:
        backtest_start = pd.Timestamp(backtest_start).date()
    rets = compute_returns(opens, closes, params.get('frequency_type', 'close to open'),
                           return_type=params.get('return_type', 'simple'))
    foo = resolve_objective(params)
    rows, index = [], []
    dates = list(rets.index)
    if backtest_start is not None and len(dates) >= lb and dates[lb - 1] > backtest_start:
        raise ValueError(
            f"Not enough warm-up data: first possible signal is {dates[lb - 1]} "
            f"but backtest_start is {backtest_start}. Load data at least "
            f"{lb + 1} trading days before backtest_start.")
    for i in range(lb, len(dates) + 1):
        window = rets.iloc[i - lb:i]
        if backtest_start is not None and window.index[-1] < backtest_start:
            continue
        rows.append(optimize_weights(
            window.values.T, foo,
            risk_free_rate=params.get('risk_free_rate', 0.02),
            method=params.get('method', 'SLSQP'),
            rmean=params.get('rmean', 'sma'),
            long_only=params.get('long_only', 1),
            lower_bound=params.get('lower_bound', -0.5),
            upper_bound=params.get('upper_bound', 0.5),
        ))
        index.append(window.index[-1])
        if verbose and (i == lb or i % 50 == 0):
            print(f'  {i}/{len(dates)}  {index[-1]}')
    return pd.DataFrame(rows, index=index, columns=rets.columns)


def signals_from_weights(weights_df: pd.DataFrame, trades: int, long_only: int = 1) -> pd.DataFrame:
    """Apply top-N truncation to a weight history. Returns signal_df
    (ticker, perc, tdate)."""
    signals = []
    tickers = list(weights_df.columns)
    for tdate, row in weights_df.iterrows():
        percs = truncate_weights(row.values, trades, long_only)
        for ticker, perc in zip(tickers, percs):
            if abs(perc) > WEIGHT_EPS:
                signals.append({'ticker': ticker, 'perc': perc, 'tdate': tdate})
    return pd.DataFrame(signals)


def generate_signal_history(params, opens, closes, verbose=True):
    """Run the optimiser over every rolling window. Returns signal_df
    (ticker, perc, tdate)."""
    weights_df = generate_weight_history(params, opens, closes, verbose=verbose)
    return signals_from_weights(weights_df, params.get('trades', 5), params.get('long_only', 1))


# ── Trade construction ───────────────────────────────────────────────────────

def _size_signals(signal_df, params, opens, closes):
    """Attach cdate, entry/exit prices, quantity and delivery pnl.

    A pre-assigned cdate column is respected (the market-regime filter uses
    this to truncate exits on force-exit days)."""
    lf = params['lf']
    capital = params['tot_capital']
    dates = list(closes.index)
    pos = {d: i for i, d in enumerate(dates)}
    signal_df = signal_df.copy()
    if 'cdate' not in signal_df.columns:
        signal_df['cdate'] = signal_df['tdate'].map(
            lambda d: dates[pos[d] + lf] if pos[d] + lf < len(dates) else None)
    signal_df['open_price'] = signal_df.apply(lambda r: closes.at[r['tdate'], r['ticker']], axis=1)
    signal_df['close_price'] = signal_df.apply(
        lambda r: opens.at[r['cdate'], r['ticker']] if r['cdate'] is not None else 0.0, axis=1)
    signal_df['quantity'] = (signal_df['perc'] * (capital / (lf + 1)) / signal_df['open_price']).astype(int)
    signal_df['pnl'] = np.where(signal_df['close_price'] != 0,
                                signal_df['quantity'] * (signal_df['close_price'] - signal_df['open_price']), 0)
    return signal_df


def _intraday_flat_legs(signal_df, opens, closes):
    """For every day a position is held (tdate < d < cdate) the live system is
    flat intraday: it sells at the open and re-buys at the close. Model those
    legs as perc=0 rows with pnl = qty * (open - close)."""
    legs = []
    dates = list(closes.index)
    open_positions = signal_df[signal_df['cdate'].notna()]
    for d in dates:
        held = open_positions[(open_positions['tdate'] < d) & (open_positions['cdate'] > d)]
        if held.empty:
            continue
        qty = held.groupby('ticker')['quantity'].sum()
        for ticker, quantity in qty.items():
            if quantity == 0:
                continue
            legs.append({
                'ticker': ticker, 'perc': 0.0, 'tdate': d, 'cdate': d,
                'open_price': opens.at[d, ticker], 'close_price': closes.at[d, ticker],
                'quantity': quantity,
            })
    intra = pd.DataFrame(legs)
    if not intra.empty:
        intra['pnl'] = intra['quantity'] * (intra['open_price'] - intra['close_price'])
    return intra


def add_trade_costs(trade_df: pd.DataFrame) -> pd.DataFrame:
    df = trade_df
    df['buy_value'] = np.where(df['perc'] != 0, df['quantity'] * df['open_price'],
                               df['quantity'] * df['close_price'])
    df['sell_value'] = np.where(df['perc'] != 0, df['quantity'] * df['close_price'],
                                df['quantity'] * df['open_price'])
    df['trade_value'] = df['buy_value'] + df['sell_value']

    brokerage_intra = (
        np.minimum(df['buy_value'] * BROKERAGE_INTRADAY_DISCBROK, MAX_BROKERAGE_INTRADAY_DISCBROK)
        + np.minimum(df['sell_value'] * BROKERAGE_INTRADAY_DISCBROK, MAX_BROKERAGE_INTRADAY_DISCBROK)
    )
    intra_cost = (
        (brokerage_intra + df['trade_value'] * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + df['sell_value'] * STT_INTRADAY + df['buy_value'] * STAMP_INTRADAY
        + df['trade_value'] * SLIPPAGE
    )
    del_cost = (
        (df['trade_value'] * (BROKERAGE_DELIVERY_DISCBROK + TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + df['sell_value'] * STT_DELIVERY + df['buy_value'] * STAMP_DELIVERY
        + df['trade_value'] * SLIPPAGE
    )
    intra_cost2 = (
        (df['trade_value'] * (BROKERAGE_INTRADAY_KS + TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + df['sell_value'] * STT_INTRADAY + df['buy_value'] * STAMP_INTRADAY
        + df['trade_value'] * SLIPPAGE
    )
    brokerage_del2 = (
        np.maximum(df['buy_value'] * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS)
        + np.minimum(df['sell_value'] * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS)
    )
    del_cost2 = (
        (brokerage_del2 + df['trade_value'] * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + df['sell_value'] * STT_DELIVERY + df['buy_value'] * STAMP_DELIVERY
        + df['trade_value'] * SLIPPAGE
    )
    df['intra_cost'] = np.where(df['perc'] == 0, intra_cost, 0)
    df['del_cost'] = np.where(df['perc'] != 0, del_cost, 0)
    df['intra_cost2'] = np.where(df['perc'] == 0, intra_cost2, 0)
    df['del_cost2'] = np.where(df['perc'] != 0, del_cost2, 0)
    return df


# ── Day-netted cost model ─────────────────────────────────────────────────────
# Broker/exchange classification is per ticker per day: quantity bought AND
# sold the same day is an intraday round trip (intraday brokerage/STT/stamp);
# only the day's NET quantity change is a delivery trade (delivery STT on the
# sell side, delivery stamp on the buy side). The original traderealm model
# charged every tranche entry/exit at delivery rates even when a same-day
# opposite trade in the same ticker made it an intraday pair — overcharging
# flat_legs and hold. delta / delta_close place at most one net trade per
# ticker per day, so they were already correct.

def _trade_ledger_from_trades(trade_df) -> pd.DataFrame:
    """Per (date, ticker) buy/sell quantities and values from a flat_legs or
    hold trade frame (entries buy at tdate close, exits sell at cdate open,
    perc=0 flat legs sell at open and re-buy at close on their day)."""
    legs = []
    for _, r in trade_df.iterrows():
        qty = r['quantity']
        if qty == 0:
            continue
        if r['perc'] != 0:
            legs.append({'Date': r['tdate'], 'ticker': r['ticker'],
                         'buy_qty': qty, 'buy_value': qty * r['open_price'],
                         'sell_qty': 0.0, 'sell_value': 0.0})
            if r['cdate'] is not None and r['close_price'] != 0:
                legs.append({'Date': r['cdate'], 'ticker': r['ticker'],
                             'buy_qty': 0.0, 'buy_value': 0.0,
                             'sell_qty': qty, 'sell_value': qty * r['close_price']})
        else:
            legs.append({'Date': r['tdate'], 'ticker': r['ticker'],
                         'buy_qty': qty, 'buy_value': qty * r['close_price'],
                         'sell_qty': qty, 'sell_value': qty * r['open_price']})
    if not legs:
        return pd.DataFrame(columns=['Date', 'ticker', 'buy_qty', 'buy_value',
                                     'sell_qty', 'sell_value'])
    return pd.DataFrame(legs).groupby(['Date', 'ticker'], as_index=False).sum()


def day_netted_costs(ledger: pd.DataFrame) -> pd.DataFrame:
    """Per-day costs with same-day buy/sell netting per ticker.

    matched quantity -> intraday rates on both sides;
    net residual     -> delivery rates on its side only.
    Returns a frame indexed by date: intra_cost, del_cost, intra_cost2,
    del_cost2 (discount / full-service broker profiles)."""
    if ledger.empty:
        return pd.DataFrame(columns=['intra_cost', 'del_cost', 'intra_cost2', 'del_cost2'])
    led = ledger.copy()
    matched = np.minimum(led['buy_qty'], led['sell_qty'])
    avg_buy = np.where(led['buy_qty'] > 0, led['buy_value'] / led['buy_qty'], 0.0)
    avg_sell = np.where(led['sell_qty'] > 0, led['sell_value'] / led['sell_qty'], 0.0)
    mbv = matched * avg_buy    # matched (intraday) buy value
    msv = matched * avg_sell   # matched (intraday) sell value
    rbv = led['buy_value'] - mbv    # residual delivery buy value
    rsv = led['sell_value'] - msv   # residual delivery sell value

    brok_i = (np.minimum(mbv * BROKERAGE_INTRADAY_DISCBROK, MAX_BROKERAGE_INTRADAY_DISCBROK)
              + np.minimum(msv * BROKERAGE_INTRADAY_DISCBROK, MAX_BROKERAGE_INTRADAY_DISCBROK))
    led['intra_cost'] = np.where(matched > 0,
                                 (brok_i + (mbv + msv) * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
                                 + msv * STT_INTRADAY + mbv * STAMP_INTRADAY
                                 + (mbv + msv) * SLIPPAGE, 0.0)
    led['del_cost'] = (
        ((rbv + rsv) * (BROKERAGE_DELIVERY_DISCBROK + TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + rsv * STT_DELIVERY + rbv * STAMP_DELIVERY + (rbv + rsv) * SLIPPAGE
    )
    led['intra_cost2'] = np.where(matched > 0,
                                  ((mbv + msv) * (BROKERAGE_INTRADAY_KS + TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
                                  + msv * STT_INTRADAY + mbv * STAMP_INTRADAY
                                  + (mbv + msv) * SLIPPAGE, 0.0)
    brok_d2 = (np.where(rbv > 0, np.maximum(rbv * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0.0)
               + np.where(rsv > 0, np.minimum(rsv * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0.0))
    led['del_cost2'] = (
        (brok_d2 + (rbv + rsv) * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
        + rsv * STT_DELIVERY + rbv * STAMP_DELIVERY + (rbv + rsv) * SLIPPAGE
    )
    return led.groupby('Date')[['intra_cost', 'del_cost', 'intra_cost2', 'del_cost2']].sum()


# ── Delta (netted) execution ─────────────────────────────────────────────────

def _delta_daily_book(signal_df, opens, closes):
    """Daily P&L of the netted book: hold the target position, sell decreases
    at the open, buy increases at the close. Returns a daily frame with the
    same cost columns as get_trade_statistics."""
    dates = list(closes.index)
    pos = {d: i for i, d in enumerate(dates)}
    target = pd.DataFrame(0.0, index=closes.index, columns=closes.columns)
    for _, row in signal_df.iterrows():
        start = pos[row['tdate']]
        end = pos[row['cdate']] if row['cdate'] is not None else len(dates)
        target.iloc[start:end, target.columns.get_loc(row['ticker'])] += row['quantity']

    prev = target.shift(1).fillna(0.0)
    sells = (prev - target).clip(lower=0)   # executed at the open
    buys = (target - prev).clip(lower=0)    # executed at the close
    held = prev - sells                      # kept through the day

    prev_close = closes.shift(1)
    overnight = (prev * (opens - prev_close)).sum(axis=1).fillna(0.0)
    intraday = (held * (closes - opens)).sum(axis=1)
    pnl = overnight + intraday

    sell_value = sells * opens
    buy_value = buys * closes
    turnover = sell_value.sum(axis=1) + buy_value.sum(axis=1)

    base_charges = (turnover * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
    duties = sell_value.sum(axis=1) * STT_DELIVERY + buy_value.sum(axis=1) * STAMP_DELIVERY
    del_cost = base_charges + duties + turnover * SLIPPAGE

    brokerage2 = (
        np.where(buy_value > 0, np.maximum(buy_value * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0).sum(axis=1)
        + np.where(sell_value > 0, np.minimum(sell_value * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0).sum(axis=1)
    )
    del_cost2 = ((brokerage2 + turnover * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
                 + duties + turnover * SLIPPAGE)

    daily = pd.DataFrame({
        'pnl': pnl, 'intra_cost': 0.0, 'del_cost': del_cost,
        'intra_cost2': 0.0, 'del_cost2': del_cost2,
        'turnover': turnover,
    })
    daily.index.name = 'cdate'
    # days before the first signal carry no book
    return daily[(target.abs().sum(axis=1) > 0) | (prev.abs().sum(axis=1) > 0)]


def _delta_close_daily_book(signal_df, opens, closes):
    """Fully causal netted execution: ALL net trades (buys and sells) execute
    at the close (~15:20), when the day's signal is known. Tranche exits
    therefore happen at the close of cdate, not the open, and the book is
    marked close-to-close. This is the variant you can actually run live:
    one net order list per day at 15:20."""
    dates = list(closes.index)
    pos = {d: i for i, d in enumerate(dates)}
    target = pd.DataFrame(0.0, index=closes.index, columns=closes.columns)
    for _, row in signal_df.iterrows():
        start = pos[row['tdate']]
        end = pos[row['cdate']] if row['cdate'] is not None else len(dates)
        target.iloc[start:end, target.columns.get_loc(row['ticker'])] += row['quantity']

    prev = target.shift(1).fillna(0.0)
    change = target - prev                      # executed at the close of d
    buy_value = change.clip(lower=0) * closes
    sell_value = -change.clip(upper=0) * closes

    pnl = (prev * (closes - closes.shift(1))).sum(axis=1).fillna(0.0)

    turnover = sell_value.sum(axis=1) + buy_value.sum(axis=1)
    base_charges = (turnover * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
    duties = sell_value.sum(axis=1) * STT_DELIVERY + buy_value.sum(axis=1) * STAMP_DELIVERY
    del_cost = base_charges + duties + turnover * SLIPPAGE
    brokerage2 = (
        np.where(buy_value > 0, np.maximum(buy_value * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0).sum(axis=1)
        + np.where(sell_value > 0, np.minimum(sell_value * BROKERAGE_DELIVERY_KS, MIN_BROKERAGE_DELIVERY_KS), 0).sum(axis=1)
    )
    del_cost2 = ((brokerage2 + turnover * (TRANSACTION_CHARGE + SEBI_CHARGE)) * (1 + GST)
                 + duties + turnover * SLIPPAGE)

    daily = pd.DataFrame({
        'pnl': pnl, 'intra_cost': 0.0, 'del_cost': del_cost,
        'intra_cost2': 0.0, 'del_cost2': del_cost2,
        'turnover': turnover,
    })
    daily.index.name = 'cdate'
    return daily[(target.abs().sum(axis=1) > 0) | (prev.abs().sum(axis=1) > 0)]


# ── Backtest entry points ────────────────────────────────────────────────────

def run_backtest(params: dict, data: dict[str, pd.DataFrame], verbose: bool = True,
                 signal_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Historical backtest for 'flat_legs' (default) or 'hold' execution.
    data: ticker -> daily DataFrame with Date/Open/High/Low/Close columns.
    Pass a precomputed signal_df (ticker, perc, tdate) to skip optimisation."""
    execution = params.get('execution', 'flat_legs')
    if execution in ('delta', 'delta_close'):
        raise ValueError(f"{execution} execution has daily-book accounting; use backtest_daily()")
    opens, closes = align_universe(data, verbose=verbose)
    if signal_df is None:
        signal_df = generate_signal_history(params, opens, closes, verbose=verbose)
    if signal_df.empty:
        return signal_df
    if verbose:
        print('Signals generated:', len(signal_df))
    signal_df = _size_signals(signal_df, params, opens, closes)
    if execution == 'flat_legs':
        intra = _intraday_flat_legs(signal_df, opens, closes)
        trade_df = pd.concat([signal_df, intra], ignore_index=True)
    else:  # hold
        trade_df = signal_df
    return add_trade_costs(trade_df)


def backtest_daily_from_signals(params: dict, signal_df: pd.DataFrame,
                                opens: pd.DataFrame, closes: pd.DataFrame):
    """(daily_df, metrics_df) for any execution mode from a precomputed
    signal_df (ticker, perc, tdate) and aligned open/close frames."""
    execution = params.get('execution', 'flat_legs')
    cost_model = params.get('cost_model', 'day_netted')
    sized = _size_signals(signal_df, params, opens, closes)
    if execution in ('flat_legs', 'hold'):
        if execution == 'flat_legs':
            intra = _intraday_flat_legs(sized, opens, closes)
            trade_df = add_trade_costs(pd.concat([sized, intra], ignore_index=True))
        else:
            trade_df = add_trade_costs(sized)
        if cost_model == 'per_leg':      # original traderealm attribution
            return get_trade_statistics(trade_df, params)
        if cost_model != 'day_netted':
            raise ValueError(f"cost_model must be 'day_netted' or 'per_leg', got {cost_model!r}")
        pnl_by_day = trade_df[trade_df['quantity'] != 0].groupby('cdate')['pnl'].sum()
        costs = day_netted_costs(_trade_ledger_from_trades(trade_df))
        fdf = pd.concat([pnl_by_day.rename('pnl'), costs], axis=1).fillna(0.0)
        fdf.index.name = 'cdate'
        return _finalize_daily(fdf.sort_index(), params)
    if execution == 'delta':
        daily = _delta_daily_book(sized, opens, closes)
        return _finalize_daily(daily, params)
    if execution == 'delta_close':
        daily = _delta_close_daily_book(sized, opens, closes)
        return _finalize_daily(daily, params)
    raise ValueError(f'Unknown execution mode: {execution}')


def backtest_daily(params: dict, data: dict[str, pd.DataFrame], verbose: bool = True,
                   signal_df: pd.DataFrame | None = None):
    """Unified entry for every execution mode. Returns (daily_df, metrics_df)."""
    opens, closes = align_universe(data, verbose=verbose)
    if signal_df is None:
        signal_df = generate_signal_history(params, opens, closes, verbose=verbose)
    return backtest_daily_from_signals(params, signal_df, opens, closes)


# ── Statistics (traderealm/backtest/stats.py) ────────────────────────────────

def get_sharpe_ratio(return_series):
    return return_series.mean() * 255 / (return_series.std() * np.sqrt(255))


def get_sortino_ratio(return_series):
    std_neg = return_series[return_series < 0].std() * np.sqrt(255)
    return return_series.mean() * 255 / std_neg


def get_max_drawdown(return_series):
    drawdowns = return_series.cumsum() - return_series.cumsum().cummax()
    return drawdowns.min()


def get_winning_days_pct(return_series):
    return len(return_series[return_series > 0]) / len(return_series) * 100


def _finalize_daily(fdf, params):
    fdf = fdf.copy()
    fdf['act_pnl'] = fdf['pnl'] - fdf['intra_cost'] - fdf['del_cost']
    fdf['act_pnl2'] = fdf['pnl'] - fdf['intra_cost2'] - fdf['del_cost2']
    capital = params.get('tot_capital', 10_00_000)
    fdf['pnl_pct'] = fdf['pnl'] / capital * 100
    fdf['act_pnl_pct'] = fdf['act_pnl'] / capital * 100
    fdf['act_pnl_pct2'] = fdf['act_pnl2'] / capital * 100
    metrics = pd.DataFrame([
        {
            'series': name,
            'total_return_pct': fdf[col].sum(),
            'sharpe': get_sharpe_ratio(fdf[col]),
            'sortino': get_sortino_ratio(fdf[col]),
            'max_drawdown_pct': get_max_drawdown(fdf[col]),
            'winning_days_pct': get_winning_days_pct(fdf[col]),
        }
        for name, col in [('gross', 'pnl_pct'),
                          ('net (discount broker)', 'act_pnl_pct'),
                          ('net (full-service broker)', 'act_pnl_pct2')]
    ]).set_index('series')
    return fdf, metrics


def get_trade_statistics(trade_df: pd.DataFrame, params: dict):
    """Aggregate per exit date. Returns (daily_df, metrics_df)."""
    fdf = trade_df[trade_df['quantity'] != 0].groupby('cdate').agg(
        {'pnl': 'sum', 'intra_cost': 'sum', 'del_cost': 'sum',
         'intra_cost2': 'sum', 'del_cost2': 'sum'})
    return _finalize_daily(fdf, params)


# ── Live signal generation (nse_order_creation.py live path) ─────────────────

def generate_live_signals(params: dict, data: dict[str, pd.DataFrame],
                          ltp: dict[str, float] | None = None) -> pd.DataFrame:
    """Generate today's BTST orders from the last `lb` returns.

    data must include today's bar up to signal time (~15:20 IST close proxy).
    ltp overrides the buy price per ticker (live uses the 15:24 LTP);
    otherwise the last close in data is used.
    """
    lb = params['lb']
    lf = params['lf']
    capital = params['tot_capital']
    opens, closes = align_universe(data)
    rets = compute_returns(opens, closes, params.get('frequency_type', 'close to open'),
                           return_type=params.get('return_type', 'simple'))
    window = rets.iloc[-lb:]
    tdate = window.index[-1]
    percs = get_strategy_signals(params, window.values.T)
    rows = []
    for ticker, perc in zip(rets.columns, percs):
        if abs(perc) <= WEIGHT_EPS:
            continue
        buy_price = (ltp or {}).get(ticker, closes[ticker].iloc[-1])
        rows.append({
            'ticker': ticker, 'perc': perc, 'tdate': tdate,
            'buy_price': buy_price,
            'quantity': int(perc * (capital / (lf + 1)) / buy_price),
            'capital': perc * capital / (lf + 1),
        })
    return pd.DataFrame(rows).sort_values('perc', ascending=False).reset_index(drop=True)
