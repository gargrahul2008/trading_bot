"""Generic OHLC resampling for the 44-SMA scanner strategy.

Turns 1-minute OHLCV data into any configured timeframe (e.g. "1min", "5min",
"15min", "60min", "1D"/"daily", "1W"/"weekly", "1M"/"monthly") — the scanner
itself is timeframe-independent (see scanner.py); this module is purely the
"turn raw 1-minute bars into candles of the configured size" infrastructure.
"""
from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta

import pandas as pd

_DAILY_ALIASES = {"1d", "d", "day", "daily"}
_WEEKLY_ALIASES = {"1w", "w", "wk", "week", "weekly"}
_MONTHLY_ALIASES = {"1m", "mo", "mon", "month", "monthly"}

OHLC_AGG = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}


def is_daily_timeframe(timeframe: str) -> bool:
    return timeframe.strip().lower() in _DAILY_ALIASES


def is_weekly_timeframe(timeframe: str) -> bool:
    return timeframe.strip().lower() in _WEEKLY_ALIASES


def is_monthly_timeframe(timeframe: str) -> bool:
    # "1m" is deliberately NOT treated as 1-minute here (that's "1min") —
    # see the module docstring's own examples; a bare "1m" is ambiguous
    # enough elsewhere that callers needing 1-minute should always spell it
    # "1min", never "1m".
    return timeframe.strip().lower() in _MONTHLY_ALIASES


def bars_lookback_start(reference_date: date, timeframe: str, n_bars: int) -> date:
    """The calendar date `n_bars` bars-of-`timeframe` before `reference_date`,
    padded for holidays — used to fetch enough history that a rolling
    indicator (e.g. a 200-period MA) is already valid AT `reference_date`
    itself, not just n_bars afterward. Without this, a scan/backtest that
    sets START_DATE to a recent date would need hundreds of bars to elapse
    before config.ma200_filter (or any other long lookback) has anything to
    say, silently doing nothing for that whole opening stretch.

    Daily: ~5 trading days/week, plus NSE's ~12-16 holidays/year (~1 in 16
    trading days) padded in on top of the weekday estimate.
    Weekly: ~1 bar/week, plus a small buffer for the rare skipped week.
    Intraday timeframes aren't supported here (bars/day depends on session
    length in a way this helper doesn't attempt) — pad START_DATE by hand
    for those.
    """
    if n_bars < 0:
        raise ValueError(f"n_bars must be >= 0, got {n_bars}")
    if is_weekly_timeframe(timeframe):
        return reference_date - timedelta(weeks=n_bars + 4)
    if is_daily_timeframe(timeframe):
        calendar_days = math.ceil(n_bars * 7 / 5) + math.ceil(n_bars / 15) + 5
        return reference_date - timedelta(days=calendar_days)
    raise ValueError(f"bars_lookback_start doesn't support intraday timeframe {timeframe!r}")


def resample_ohlc(
    frame: pd.DataFrame,
    timeframe: str,
    *,
    continuous_session: bool,
    session_start: time,
    session_end: time,
) -> pd.DataFrame:
    """Resample a single symbol's 1-minute OHLCV frame to `timeframe`.

    Returns a frame sorted by bar_open_time with columns:
      bar_open_time, bar_close_time, open, high, low, close, volume
    `bar_close_time` is the instant the bar becomes fully known (its right edge) —
    this is what downstream code must compare against to avoid using a still-forming
    candle.
    """
    if frame.empty:
        return pd.DataFrame(columns=["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"])

    if is_monthly_timeframe(timeframe):
        return _resample_monthly(frame, continuous_session=continuous_session, session_end=session_end)
    if is_weekly_timeframe(timeframe):
        return _resample_weekly(frame, continuous_session=continuous_session, session_end=session_end)
    if is_daily_timeframe(timeframe):
        return _resample_daily(frame, continuous_session=continuous_session, session_end=session_end)
    if continuous_session:
        return _resample_intraday_continuous(frame, timeframe)
    return _resample_intraday_session(frame, timeframe, session_start=session_start)


def _resample_weekly(frame: pd.DataFrame, *, continuous_session: bool, session_end: time) -> pd.DataFrame:
    """Weekly OHLC, grouped by ISO week. Needed for a positional 'weekly trend +
    daily entry' setup. The bar becomes known only at the session close of the
    LAST trading day of that week — that is its bar_close_time, so the as-of merge
    can never see a still-forming weekly candle."""
    working = frame.sort_values("timestamp")
    tz = working["timestamp"].dt.tz
    iso = working["timestamp"].dt.isocalendar()
    week_key = iso["year"].astype(int) * 100 + iso["week"].astype(int)
    working = working.assign(_week=week_key.to_numpy())

    weekly = working.groupby("_week", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        bar_open_time=("timestamp", "first"),
        _last_ts=("timestamp", "last"),
    ).reset_index(drop=True)

    if continuous_session:
        weekly["bar_close_time"] = weekly["_last_ts"] + pd.Timedelta(minutes=1)
    else:
        weekly["bar_close_time"] = weekly["_last_ts"].apply(
            lambda ts: pd.Timestamp(datetime.combine(ts.date(), session_end), tz=tz)
        )
    return weekly[["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"]]


def _resample_monthly(frame: pd.DataFrame, *, continuous_session: bool, session_end: time) -> pd.DataFrame:
    """Monthly OHLC, grouped by calendar year+month — same no-lookahead
    convention as _resample_weekly: the bar becomes known only at the
    session close of the LAST trading day of that month."""
    working = frame.sort_values("timestamp")
    tz = working["timestamp"].dt.tz
    month_key = working["timestamp"].dt.year.astype(int) * 100 + working["timestamp"].dt.month.astype(int)
    working = working.assign(_month=month_key.to_numpy())

    monthly = working.groupby("_month", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        bar_open_time=("timestamp", "first"),
        _last_ts=("timestamp", "last"),
    ).reset_index(drop=True)

    if continuous_session:
        monthly["bar_close_time"] = monthly["_last_ts"] + pd.Timedelta(minutes=1)
    else:
        monthly["bar_close_time"] = monthly["_last_ts"].apply(
            lambda ts: pd.Timestamp(datetime.combine(ts.date(), session_end), tz=tz)
        )
    return monthly[["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"]]


def _resample_daily(frame: pd.DataFrame, *, continuous_session: bool, session_end: time) -> pd.DataFrame:
    working = frame.sort_values("timestamp")
    daily = working.groupby("trade_date", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        bar_open_time=("timestamp", "first"),
    ).reset_index(drop=True)
    tz = working["timestamp"].dt.tz
    if continuous_session:
        # No fixed session close for 24/7 instruments: the daily bar completes at
        # the start of the next calendar day (midnight in the working timezone).
        bar_close_time = daily["bar_open_time"].apply(
            lambda ts: pd.Timestamp(datetime.combine(ts.date(), time(0, 0)), tz=tz) + pd.Timedelta(days=1)
        )
    else:
        bar_close_time = daily["bar_open_time"].apply(
            lambda ts: pd.Timestamp(datetime.combine(ts.date(), session_end), tz=tz)
        )
    daily["bar_close_time"] = bar_close_time
    return daily[["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"]]


def _resample_intraday_session(frame: pd.DataFrame, timeframe: str, *, session_start: time) -> pd.DataFrame:
    freq = pd.Timedelta(timeframe)
    bars: list[pd.DataFrame] = []
    for trade_date, day_frame in frame.groupby("trade_date", sort=True):
        day_frame = day_frame.sort_values("timestamp").set_index("timestamp")
        tz = day_frame.index.tz
        day_date = trade_date if isinstance(trade_date, date) else pd.Timestamp(trade_date).date()
        origin = pd.Timestamp(datetime.combine(day_date, session_start), tz=tz)
        resampled = day_frame.resample(freq, origin=origin, closed="left", label="left").agg(OHLC_AGG).dropna(
            subset=["open", "high", "low", "close"]
        )
        if resampled.empty:
            continue
        resampled = resampled.reset_index().rename(columns={"timestamp": "bar_open_time"})
        resampled["bar_close_time"] = resampled["bar_open_time"] + freq
        bars.append(resampled)
    if not bars:
        return pd.DataFrame(columns=["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"])
    return pd.concat(bars, ignore_index=True).sort_values("bar_open_time").reset_index(drop=True)


def _resample_intraday_continuous(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    freq = pd.Timedelta(timeframe)
    working = frame.sort_values("timestamp").set_index("timestamp")
    resampled = working.resample(freq, origin="epoch", closed="left", label="left").agg(OHLC_AGG).dropna(
        subset=["open", "high", "low", "close"]
    )
    resampled = resampled.reset_index().rename(columns={"timestamp": "bar_open_time"})
    resampled["bar_close_time"] = resampled["bar_open_time"] + freq
    return resampled[["bar_open_time", "bar_close_time", "open", "high", "low", "close", "volume"]]
