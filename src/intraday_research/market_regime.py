"""
Weekly NIFTY market-regime filter for the BTST Lagrangian strategy.

Sits ABOVE the stock strategy and only decides:
  1. whether new buy tranches are allowed (state ON), and
  2. whether all open tranches must be force-exited (confirmed / immediate /
     emergency rules).
It never changes stock selection, weights, sizing or normal exits.

Logic (completed weekly NIFTY candles only, IST):
  SMMA5   = 5-week Wilder smoothed moving average of weekly closes
  ATR14   = 14-week Wilder ATR of weekly OHLC
  Lower   = SMMA5 - exit_atr_multiplier    * ATR14   (risk-off threshold)
  Upper   = SMMA5 + reentry_atr_multiplier * ATR14   (risk-on threshold)

  ON  -> OFF  when weekly close <  Lower          (strict; block new buys)
  OFF -> ON   when weekly close >  Upper          (strict; allow new buys)
  between the bands the previous state is retained (hysteresis).

  exit_mode 'confirmed': force-exit all positions after `confirmation_weeks`
      consecutive weekly closes below the (current) lower band.
  exit_mode 'immediate': force-exit on the first lower-band break.
  Emergency: weekly close < previous `emergency_low_lookback`-week low
      (current week excluded) -> immediate OFF + force-exit, overrides both.

A signal computed on the weekly close becomes effective from the NEXT trading
session — never at the close that generated it.

Integration with the BTST backtest is signal-frame based (`apply_regime_to_
signals`): tranches whose tdate falls on a buy-blocked day are dropped
(never replayed) and open tranches get their cdate truncated to the force-exit
effective date, so every execution mode (flat_legs / hold / delta /
delta_close) inherits the filter without engine changes.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

STATE_ON = 'ON'
STATE_OFF = 'OFF'
_VALID_STATES = (STATE_ON, STATE_OFF)
_VALID_EXIT_MODES = ('confirmed', 'immediate')


_VALID_TIMEFRAMES = ('weekly', 'daily')
_VALID_FILTER_STYLES = ('threshold', 'band_momentum', 'sma_weekly_daily')


@dataclass
class RegimeConfig:
    """timeframe='weekly' runs on completed weekly candles (the spec default);
    timeframe='daily' runs the identical band state machine on daily NIFTY
    candles — every *_period / confirmation / lookback value is then in DAYS
    (confirmation_weeks means consecutive bars of the configured timeframe).

    filter_style:
      'threshold' (default) — the spec machine: OFF below the lower band,
          ON above the upper band, state retained between the bands.
      'band_momentum' — direction-aware 3-state machine:
          ON   (buying allowed)  — price above the upper band, or recovered
                                   above the lower band from below;
          WARN (buying blocked, positions keep their normal exits) — price
                                   fell out of the top through the upper band;
          DOWN (sold out, buying blocked) — price broke the lower band
                                   (force-exit everything, immediately).
          Reversals between the bands are not events: only band crossings
          change state. confirmation_weeks is unused in this style."""
    smma_period: int = 5
    atr_period: int = 14
    exit_atr_multiplier: float = 0.25
    reentry_atr_multiplier: float = 0.10
    confirmation_weeks: int = 2
    emergency_low_lookback: int = 10
    enable_emergency_exit: bool = True
    exit_mode: str = 'confirmed'
    default_initial_state: str = 'ON'
    timeframe: str = 'weekly'
    filter_style: str = 'threshold'

    def __post_init__(self) -> None:
        if self.exit_mode not in _VALID_EXIT_MODES:
            raise ValueError(f"exit_mode must be one of {_VALID_EXIT_MODES}, got {self.exit_mode!r}")
        if self.default_initial_state not in _VALID_STATES:
            raise ValueError(f"default_initial_state must be one of {_VALID_STATES}")
        if self.timeframe not in _VALID_TIMEFRAMES:
            raise ValueError(f"timeframe must be one of {_VALID_TIMEFRAMES}, got {self.timeframe!r}")
        if self.filter_style not in _VALID_FILTER_STYLES:
            raise ValueError(f"filter_style must be one of {_VALID_FILTER_STYLES}, got {self.filter_style!r}")


# ── Weekly resampling ─────────────────────────────────────────────────────────

def _validate_daily(daily: pd.DataFrame) -> pd.DataFrame:
    df = daily.copy()
    required = {'Date', 'Open', 'High', 'Low', 'Close'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'daily data missing columns: {sorted(missing)}')
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
    if df['Date'].duplicated().any():
        raise ValueError('duplicate dates in daily data')
    if (df['High'] < df['Low']).any():
        raise ValueError('found rows with High < Low')
    if (df[['Open', 'High', 'Low', 'Close']] <= 0).any().any():
        raise ValueError('OHLC values must be positive')
    return df.sort_values('Date').reset_index(drop=True)


def daily_candles(daily: pd.DataFrame) -> pd.DataFrame:
    """Daily OHLC passthrough in the same schema as resample_weekly
    (week_end_date is the bar's own date; every daily bar is complete)."""
    df = _validate_daily(daily)
    return pd.DataFrame({
        'week_end_date': df['Date'], 'Open': df['Open'], 'High': df['High'],
        'Low': df['Low'], 'Close': df['Close'], 'n_days': 1,
    })


def resample_weekly(daily: pd.DataFrame) -> pd.DataFrame:
    """Daily OHLC (Date, Open, High, Low, Close) -> completed weekly candles.

    Weeks end on Friday (or the final NSE session of the week). The last week
    in the data is kept only if its final session is a Friday; otherwise it is
    treated as incomplete and dropped. Returns columns:
    week_end_date, Open, High, Low, Close, n_days.
    """
    df = _validate_daily(daily)
    ts = pd.to_datetime(df['Date'])
    grouped = df.groupby(ts.dt.to_period('W-FRI'))
    weekly = grouped.agg(
        week_end_date=('Date', 'last'),
        Open=('Open', 'first'),
        High=('High', 'max'),
        Low=('Low', 'min'),
        Close=('Close', 'last'),
        n_days=('Date', 'count'),
    ).reset_index(drop=True)
    # the final week is complete only if its last session is a Friday
    last_end = pd.Timestamp(weekly.iloc[-1]['week_end_date'])
    if last_end.weekday() != 4:
        weekly = weekly.iloc[:-1].reset_index(drop=True)
    return weekly


# ── Indicators (Wilder) ───────────────────────────────────────────────────────

def wilder_smma(closes: pd.Series, period: int) -> pd.Series:
    """Wilder smoothed MA (RMA): SMA of the first `period` values, then
    SMMA[t] = (SMMA[t-1]*(period-1) + close[t]) / period. NaN during warm-up."""
    values = closes.to_numpy(dtype=float)
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return pd.Series(out, index=closes.index)
    out[period - 1] = values[:period].mean()
    for i in range(period, len(values)):
        out[i] = (out[i - 1] * (period - 1) + values[i]) / period
    return pd.Series(out, index=closes.index)


def weekly_true_range(weekly: pd.DataFrame) -> pd.Series:
    prev_close = weekly['Close'].shift(1)
    tr = pd.concat([
        weekly['High'] - weekly['Low'],
        (weekly['High'] - prev_close).abs(),
        (weekly['Low'] - prev_close).abs(),
    ], axis=1).max(axis=1)
    tr.iloc[0] = weekly['High'].iloc[0] - weekly['Low'].iloc[0]
    return tr


def wilder_atr(weekly: pd.DataFrame, period: int) -> pd.Series:
    """Wilder ATR on weekly candles: SMA of first `period` TRs, then
    ATR[t] = (ATR[t-1]*(period-1) + TR[t]) / period."""
    tr = weekly_true_range(weekly).to_numpy(dtype=float)
    out = np.full(len(tr), np.nan)
    if len(tr) < period:
        return pd.Series(out, index=weekly.index)
    out[period - 1] = tr[:period].mean()
    for i in range(period, len(tr)):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return pd.Series(out, index=weekly.index)


# ── State machine (pure) ──────────────────────────────────────────────────────

def update_market_filter(
    weekly_close: float,
    smma: float,
    atr: float,
    previous_10_week_low: float | None,
    previous_state: str,
    previous_below_lower_count: int,
    config: RegimeConfig,
) -> dict:
    """One weekly state-machine step. Pure function, no side effects."""
    if previous_state not in _VALID_STATES:
        raise ValueError(f'invalid previous_state: {previous_state!r}')
    lower_band = smma - config.exit_atr_multiplier * atr
    upper_band = smma + config.reentry_atr_multiplier * atr

    state = previous_state
    below_lower_count = previous_below_lower_count
    force_exit_all = False
    cancel_pending_buys = False
    transition = 'NONE'
    reason = 'NO_CHANGE'

    emergency_breakdown = (
        config.enable_emergency_exit
        and previous_10_week_low is not None
        and not pd.isna(previous_10_week_low)
        and weekly_close < previous_10_week_low
    )

    if emergency_breakdown:
        state = STATE_OFF
        force_exit_all = True
        cancel_pending_buys = True
        reason = 'EMERGENCY_10_WEEK_LOW_BREAK'
        transition = 'ON_TO_OFF' if previous_state == STATE_ON else 'NONE'
        below_lower_count = previous_below_lower_count + 1 if weekly_close < lower_band else 0
    elif previous_state == STATE_ON:
        if weekly_close < lower_band:
            state = STATE_OFF
            below_lower_count = 1
            cancel_pending_buys = True
            transition = 'ON_TO_OFF'
            reason = 'LOWER_BAND_BREAK'
            if config.exit_mode == 'immediate':
                force_exit_all = True
        else:
            state = STATE_ON
            below_lower_count = 0
    else:  # previous OFF
        if weekly_close > upper_band:
            state = STATE_ON
            below_lower_count = 0
            transition = 'OFF_TO_ON'
            reason = 'UPPER_BAND_RECLAIM'
        else:
            state = STATE_OFF
            below_lower_count = previous_below_lower_count + 1 if weekly_close < lower_band else 0
            if config.exit_mode == 'confirmed' and below_lower_count >= config.confirmation_weeks:
                force_exit_all = True
                reason = 'CONFIRMED_LOWER_BAND_BREAK'

    return {
        'market_state': state,
        'buy_allowed': state == STATE_ON,
        'force_exit_all': force_exit_all,
        'cancel_pending_buys': cancel_pending_buys,
        'below_lower_count': below_lower_count,
        'lower_band': lower_band,
        'upper_band': upper_band,
        'transition': transition,
        'reason': reason,
    }


def _band_zone(close: float, lower_band: float, upper_band: float) -> str:
    if close > upper_band:
        return 'ABOVE'
    if close < lower_band:
        return 'BELOW'
    return 'BETWEEN'


def update_band_momentum_filter(
    weekly_close: float,
    smma: float,
    atr: float,
    previous_10_week_low: float | None,
    previous_state: str,
    previous_zone: str,
    config: RegimeConfig,
) -> dict:
    """One step of the direction-aware 3-state machine (filter_style
    'band_momentum'). States: ON / WARN / DOWN. Pure function.

      ON:   close breaks lower band        -> DOWN + force-exit everything
            close falls below upper band
            after being above it           -> WARN (block new buys, hold rest)
      WARN: close breaks lower band        -> DOWN + force-exit everything
            close reclaims the upper band  -> ON
      DOWN: close reclaims the lower band  -> ON (buy again; reversals between
                                              the bands are not events)
    Equality with a band never triggers. Emergency low-break (if enabled)
    overrides everything -> DOWN + force-exit.
    """
    if previous_state not in ('ON', 'WARN', 'DOWN'):
        raise ValueError(f'invalid previous_state: {previous_state!r}')
    lower_band = smma - config.exit_atr_multiplier * atr
    upper_band = smma + config.reentry_atr_multiplier * atr
    zone = _band_zone(weekly_close, lower_band, upper_band)

    state = previous_state
    force_exit_all = False
    cancel_pending_buys = False
    reason = 'NO_CHANGE'

    emergency_breakdown = (
        config.enable_emergency_exit
        and previous_10_week_low is not None
        and not pd.isna(previous_10_week_low)
        and weekly_close < previous_10_week_low
    )

    if emergency_breakdown:
        state = 'DOWN'
        force_exit_all = True
        cancel_pending_buys = True
        reason = 'EMERGENCY_10_WEEK_LOW_BREAK'
    elif previous_state == 'ON':
        if weekly_close < lower_band:
            state, force_exit_all, cancel_pending_buys = 'DOWN', True, True
            reason = 'LOWER_BAND_BREAK_SELL_ALL'
        elif previous_zone == 'ABOVE' and weekly_close < upper_band:
            state, cancel_pending_buys = 'WARN', True
            reason = 'UPPER_BAND_LOST'
    elif previous_state == 'WARN':
        if weekly_close < lower_band:
            state, force_exit_all = 'DOWN', True
            reason = 'LOWER_BAND_BREAK_SELL_ALL'
        elif weekly_close > upper_band:
            state = 'ON'
            reason = 'UPPER_BAND_RECLAIM'
    else:  # DOWN
        if weekly_close > lower_band:
            state = 'ON'
            reason = 'LOWER_BAND_RECLAIM'

    transition = f'{previous_state}_TO_{state}' if state != previous_state else 'NONE'
    return {
        'market_state': state,
        'buy_allowed': state == 'ON',
        'force_exit_all': force_exit_all,
        'cancel_pending_buys': cancel_pending_buys,
        'below_lower_count': 0,
        'lower_band': lower_band,
        'upper_band': upper_band,
        'zone': zone,
        'transition': transition,
        'reason': reason,
    }


# ── Weekly filter builder ─────────────────────────────────────────────────────

def build_weekly_filter(daily_nifty: pd.DataFrame, config: RegimeConfig | None = None) -> pd.DataFrame:
    """Run the state machine over completed weekly candles.

    Returns the diagnostic frame (one row per completed candle from the first
    candle where SMMA, ATR and the previous N-bar low all exist).
    effective_from_date is the next trading session after week_end_date
    (None for the final candle when no later session exists in the data).
    With config.timeframe='daily' the same state machine runs on daily
    candles; week_end_date is then simply the bar date.
    """
    config = config or RegimeConfig()
    weekly = resample_weekly(daily_nifty) if config.timeframe == 'weekly' else daily_candles(daily_nifty)
    weekly['smma'] = wilder_smma(weekly['Close'], config.smma_period)
    weekly['atr'] = wilder_atr(weekly, config.atr_period)
    weekly['previous_10_week_low'] = (
        weekly['Low'].shift(1).rolling(config.emergency_low_lookback).min()
    )

    trading_dates = sorted(daily_nifty['Date'])

    def next_session(after) -> object:
        for d in trading_dates:
            if d > after:
                return d
        return None

    rows = []
    state = None
    below = 0
    zone = 'BETWEEN'
    for _, wk in weekly.iterrows():
        if pd.isna(wk['smma']) or pd.isna(wk['atr']) or pd.isna(wk['previous_10_week_low']):
            continue
        lower = wk['smma'] - config.exit_atr_multiplier * wk['atr']
        upper = wk['smma'] + config.reentry_atr_multiplier * wk['atr']
        if state is None:
            # initial state (section 18): banded, else default
            zone = _band_zone(wk['Close'], lower, upper)
            if config.filter_style == 'band_momentum':
                if zone == 'ABOVE':
                    state = 'ON'
                elif zone == 'BELOW':
                    state = 'DOWN'
                else:
                    state = 'ON' if config.default_initial_state == STATE_ON else 'WARN'
                below = 0
            else:
                if zone == 'ABOVE':
                    state = STATE_ON
                elif zone == 'BELOW':
                    state = STATE_OFF
                else:
                    state = config.default_initial_state
                below = 1 if (state == STATE_OFF and zone == 'BELOW') else 0
            result = {
                'market_state': state, 'buy_allowed': state == STATE_ON,
                'force_exit_all': False, 'cancel_pending_buys': False,
                'below_lower_count': below, 'lower_band': lower, 'upper_band': upper,
                'transition': 'NONE', 'reason': 'INITIAL_STATE',
            }
            prev_state = state
        elif config.filter_style == 'band_momentum':
            prev_state = state
            result = update_band_momentum_filter(
                wk['Close'], wk['smma'], wk['atr'], wk['previous_10_week_low'],
                state, zone, config,
            )
            state = result['market_state']
            zone = result['zone']
        else:
            prev_state = state
            result = update_market_filter(
                wk['Close'], wk['smma'], wk['atr'], wk['previous_10_week_low'],
                state, below, config,
            )
            state = result['market_state']
            below = result['below_lower_count']
        rows.append({
            'week_end_date': wk['week_end_date'],
            'effective_from_date': next_session(wk['week_end_date']),
            'weekly_open': wk['Open'], 'weekly_high': wk['High'],
            'weekly_low': wk['Low'], 'weekly_close': wk['Close'],
            'smma_5': wk['smma'], 'atr_14': wk['atr'],
            'previous_10_week_low': wk['previous_10_week_low'],
            'previous_market_state': prev_state,
            **result,
        })
    return pd.DataFrame(rows)


# general-purpose alias: works for both weekly and daily timeframes
build_regime_filter = build_weekly_filter


def build_sma_daily_filter(daily_nifty: pd.DataFrame, config: RegimeConfig,
                           trading_dates: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """filter_style 'sma_weekly_daily': plain 5-week SMA rule + intra-week
    daily overlay. No bands, no hysteresis.

    Weekly (completed candles, effective next session):
      weekly close >  SMA(smma_period)  -> ON  (trade normally)
      weekly close <= SMA               -> OFF (force-exit everything, block
                                          buys until a weekly close > SMA)
    Daily overlay (inside the running week, vs the LAST completed week's SMA —
    the only value known mid-week):
      day's close < last week's SMA -> that day: block new buys AND force-exit
      the book. The overlay only restricts; it never re-enables buying during
      an OFF week. Decision uses the day's close (15:20 session price), which
      is simultaneous with delta_close execution; for open-exit executions the
      modelled exit is ~6h earlier than the decision.

    Returns (daily_state indexed by Date with market_state / buy_allowed /
    force_exit, weekly diagnostic frame).
    """
    weekly = resample_weekly(daily_nifty)
    weekly['sma'] = weekly['Close'].rolling(config.smma_period).mean()
    all_dates = sorted(daily_nifty['Date'])

    def next_session(after):
        for d in all_dates:
            if d > after:
                return d
        return None

    diag_rows = []
    prev_state = None
    for _, wk in weekly.iterrows():
        if pd.isna(wk['sma']):
            continue
        state = STATE_ON if wk['Close'] > wk['sma'] else STATE_OFF
        transition = 'NONE'
        if prev_state is not None and state != prev_state:
            transition = f'{prev_state}_TO_{state}'
        diag_rows.append({
            'week_end_date': wk['week_end_date'],
            'effective_from_date': next_session(wk['week_end_date']),
            'weekly_close': wk['Close'], 'sma': wk['sma'],
            'market_state': state, 'transition': transition,
            'force_exit_all': transition == 'ON_TO_OFF',
            'buy_allowed': state == STATE_ON,
            'reason': 'WEEKLY_CLOSE_VS_SMA' if transition != 'NONE' else 'NO_CHANGE',
        })
        prev_state = state
    diag = pd.DataFrame(diag_rows)

    nifty_close = daily_nifty.set_index('Date')['Close']
    weeks = diag[diag['effective_from_date'].notna()].reset_index(drop=True)
    rows = []
    j = -1
    for d in sorted(trading_dates):
        while j + 1 < len(weeks) and weeks.loc[j + 1, 'effective_from_date'] <= d:
            j += 1
        if j < 0:
            rows.append({'Date': d, 'market_state': STATE_ON, 'buy_allowed': True,
                         'force_exit': False})
            continue
        base_state = weeks.loc[j, 'market_state']
        ref_sma = weeks.loc[j, 'sma']
        weekly_flip_exit = bool(weeks.loc[j, 'force_exit_all']
                                and weeks.loc[j, 'effective_from_date'] == d)
        price = nifty_close.get(d)
        daily_blocked = bool(price is not None and not pd.isna(price) and price < ref_sma)
        buy_allowed = (base_state == STATE_ON) and not daily_blocked
        force_exit = weekly_flip_exit or (base_state == STATE_ON and daily_blocked)
        rows.append({
            'Date': d,
            'market_state': base_state if not daily_blocked else STATE_OFF,
            'buy_allowed': buy_allowed,
            'force_exit': force_exit,
        })
    return pd.DataFrame(rows).set_index('Date'), diag


def regime_event_log(weekly_filter: pd.DataFrame) -> pd.DataFrame:
    """Every transition / force-exit event with its context."""
    events = weekly_filter[
        (weekly_filter['transition'] != 'NONE') | weekly_filter['force_exit_all']
    ].copy()
    events['action_taken'] = np.where(
        events['force_exit_all'], 'Force-exit all open positions; block new buys',
        np.where(events['transition'] == 'ON_TO_OFF',
                 'Block new purchases and cancel pending buys',
                 'Allow fresh buy signals'))
    return events.reset_index(drop=True)


# ── Daily effective state and BTST integration ────────────────────────────────

def daily_state_frame(weekly_filter: pd.DataFrame, trading_dates: list) -> pd.DataFrame:
    """Map the weekly filter onto trading days.

    For each trading day the state of the most recent week whose
    effective_from_date <= day applies (i.e. signals act from the next session
    after the weekly close, never intra-week). force_exit is True only on the
    single effective date of a force-exit week. Days before the first
    effective week default to buy_allowed=True (unfiltered warm-up).
    """
    wf = weekly_filter[weekly_filter['effective_from_date'].notna()].sort_values('effective_from_date')
    eff_dates = list(wf['effective_from_date'])
    states = list(wf['market_state'])
    force = list(wf['force_exit_all'])
    force_days = {d for d, f in zip(eff_dates, force) if f}
    rows = []
    j = -1
    for d in sorted(trading_dates):
        while j + 1 < len(eff_dates) and eff_dates[j + 1] <= d:
            j += 1
        state = states[j] if j >= 0 else STATE_ON
        rows.append({
            'Date': d,
            'market_state': state,
            'buy_allowed': state == STATE_ON,
            'force_exit': d in force_days,
        })
    return pd.DataFrame(rows).set_index('Date')


def apply_regime_to_signals(signal_df: pd.DataFrame, dates: list, daily_state: pd.DataFrame,
                            lf: int) -> tuple[pd.DataFrame, dict]:
    """Filter the BTST signal frame with the regime filter.

    - drops tranches whose tdate is a buy-blocked day (never replayed later);
    - assigns cdate = tdate + lf sessions, truncated to the first force-exit
      day after entry (exit executes at that day's normal exit slot: the
      09:20 open for flat_legs/hold/delta, the 15:20 close for delta_close).

    Returns (filtered signal_df with explicit cdate, stats dict).
    """
    dates = sorted(dates)
    pos = {d: i for i, d in enumerate(dates)}
    force_days = sorted(daily_state.index[daily_state['force_exit']])

    allowed = signal_df['tdate'].map(lambda d: bool(daily_state.at[d, 'buy_allowed'])
                                     if d in daily_state.index else True)
    blocked = int((~allowed).sum())
    out = signal_df[allowed].copy().reset_index(drop=True)

    def exit_date(tdate):
        natural = dates[pos[tdate] + lf] if pos[tdate] + lf < len(dates) else None
        for f in force_days:
            if f > tdate and (natural is None or f < natural):
                return f
        return natural

    out['cdate'] = out['tdate'].map(exit_date)
    forced = int(sum(
        1 for _, r in out.iterrows()
        if r['cdate'] is not None and pos.get(r['cdate'], 0) - pos[r['tdate']] < lf
    ))
    stats = {'blocked_buys': blocked, 'forced_exit_tranches': forced,
             'force_exit_days': force_days}
    return out, stats


# ── Version comparison (spec section 22) ─────────────────────────────────────

def regime_versions() -> dict:
    return {
        'A_original': None,
        'B_smma_only': RegimeConfig(exit_atr_multiplier=0.0, reentry_atr_multiplier=0.0),
        'C_symmetric_atr': RegimeConfig(reentry_atr_multiplier=0.25),
        'D_asym_confirmed': RegimeConfig(),
        'E_asym_immediate': RegimeConfig(exit_mode='immediate'),
        'F_no_emergency': RegimeConfig(enable_emergency_exit=False),
    }


def compare_regime_versions(params: dict, data: dict, nifty_daily: pd.DataFrame,
                            versions: dict | None = None, verbose: bool = True,
                            signal_df: pd.DataFrame | None = None):
    """Backtest the strategy under every filter version.

    Returns (summary DataFrame, {name: (daily, metrics, weekly_filter)}).
    Signals are generated once (or passed in) and reused; only the filter
    differs. signal_df must be the raw (ticker, perc, tdate) frame without a
    cdate column.
    """
    from .btst_lagrangian import (align_universe, backtest_daily_from_signals,
                                  generate_signal_history)
    versions = versions or regime_versions()
    opens, closes = align_universe(data, verbose=verbose)
    if signal_df is None:
        signal_df = generate_signal_history(params, opens, closes, verbose=verbose)
    elif 'cdate' in signal_df.columns:
        raise ValueError('pass the raw signal frame (no cdate column)')
    dates = list(closes.index)
    # filter-state metrics are measured over the evaluation window only, not
    # any warm-up days loaded before params['backtest_start']
    backtest_start = params.get('backtest_start')
    if backtest_start is not None:
        backtest_start = pd.Timestamp(backtest_start).date()
        eval_dates = [d for d in dates if d >= backtest_start]
    else:
        eval_dates = dates
    nifty_close = nifty_daily.set_index('Date')['Close']

    rows, details = [], {}
    for name, config in versions.items():
        if config is None:
            sig, stats, wf, dstate = signal_df, {'blocked_buys': 0, 'forced_exit_tranches': 0}, None, None
        elif config.filter_style == 'sma_weekly_daily':
            dstate, wf = build_sma_daily_filter(nifty_daily, config, eval_dates)
            sig, stats = apply_regime_to_signals(signal_df, dates, dstate, params['lf'])
        else:
            wf = build_weekly_filter(nifty_daily, config)
            dstate = daily_state_frame(wf, eval_dates)
            sig, stats = apply_regime_to_signals(signal_df, dates, dstate, params['lf'])
        daily, metrics = backtest_daily_from_signals(params, sig, opens, closes)
        net = metrics.loc['net (discount broker)']
        row = {
            'version': name,
            'net_pct': net['total_return_pct'],
            'net_sharpe': net['sharpe'],
            'net_maxdd_pct': net['max_drawdown_pct'],
            'win_days_pct': net['winning_days_pct'],
            'gross_pct': metrics.loc['gross', 'total_return_pct'],
            'blocked_buys': stats['blocked_buys'],
            'forced_exit_tranches': stats['forced_exit_tranches'],
        }
        if dstate is not None:
            off_days = dstate[~dstate['buy_allowed']].index
            row['pct_time_off'] = len(off_days) / len(dstate) * 100
            row['on_to_off'] = int((wf['transition'] == 'ON_TO_OFF').sum())
            row['off_to_on'] = int((wf['transition'] == 'OFF_TO_ON').sum())
            nifty_off = nifty_close.reindex(off_days).dropna()
            if len(nifty_off) > 1:
                row['nifty_ret_off_pct'] = (nifty_off.pct_change().fillna(0) + 1).prod() * 100 - 100
            strat_off = daily.reindex(off_days)['act_pnl_pct'].dropna()
            row['strat_ret_off_pct'] = strat_off.sum()
        else:
            row['pct_time_off'] = 0.0
        rows.append(row)
        details[name] = (daily, metrics, wf)
        if verbose:
            print(f"  {name}: net {row['net_pct']:.2f}% sharpe {row['net_sharpe']:.2f} "
                  f"maxdd {row['net_maxdd_pct']:.2f}% off {row['pct_time_off']:.1f}%")
    return pd.DataFrame(rows).set_index('version'), details


def off_period_report(weekly_filter: pd.DataFrame, nifty_daily: pd.DataFrame,
                      strategy_daily: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per completed OFF period (spec section 24 core fields)."""
    nifty_close = nifty_daily.set_index('Date')['Close']
    wf = weekly_filter.reset_index(drop=True)
    periods = []
    off_start = None
    for _, row in wf.iterrows():
        if row['transition'] == 'ON_TO_OFF' and off_start is None:
            off_start = row
        elif row['transition'] == 'OFF_TO_ON' and off_start is not None:
            start_eff, end_eff = off_start['effective_from_date'], row['effective_from_date']
            seg = nifty_close[(nifty_close.index >= start_eff)]
            seg = seg[seg.index <= (end_eff or seg.index.max())]
            entry = {
                'off_signal_date': off_start['week_end_date'],
                'off_effective_date': start_eff,
                'reentry_signal_date': row['week_end_date'],
                'reentry_effective_date': end_eff,
                'trading_days_off': len(seg),
                'nifty_return_off_pct': (seg.iloc[-1] / seg.iloc[0] - 1) * 100 if len(seg) > 1 else 0.0,
                'max_nifty_decline_off_pct': (seg.min() / seg.iloc[0] - 1) * 100 if len(seg) > 1 else 0.0,
            }
            if strategy_daily is not None and len(seg):
                mask = [d in set(seg.index) for d in strategy_daily.index]
                entry['strategy_return_off_pct'] = strategy_daily.loc[mask, 'act_pnl_pct'].sum()
            periods.append(entry)
            off_start = None
    return pd.DataFrame(periods)
