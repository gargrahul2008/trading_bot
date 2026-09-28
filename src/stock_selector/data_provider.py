"""Data-provider interface for the stock selector plus an in-memory
implementation used by backtests and unit tests.

Contract highlights (look-ahead control lives here):
  * ``get_daily_history`` returns COMPLETED sessions strictly before the
    as-of date, corporate-action adjusted on the same basis as intraday data.
  * ``get_preclose_snapshot`` must be built only from trades/bars whose
    timestamp is at or before the cutoff.
  * ``get_same_time_cumulative_volumes`` returns the cumulative volume AT THE
    SAME CUTOFF TIME for prior sessions — never full-day volume.
  * Event / restriction data must not include information published after
    the cutoff; return ``None`` where a dataset is unavailable.
"""
from __future__ import annotations

import abc
from datetime import date, datetime, time

import numpy as np
import pandas as pd

from .models import PreCloseSnapshot, UniverseEntry

DAILY_COLUMNS = ('date', 'open', 'high', 'low', 'close', 'volume')


class DataProvider(abc.ABC):
    """Abstract point-in-time data source."""

    @abc.abstractmethod
    def get_universe(self, as_of_date: date) -> list[UniverseEntry]:
        """Point-in-time universe: only symbols listed/approved on that date."""

    @abc.abstractmethod
    def get_daily_history(self, symbol: str, as_of_date: date) -> pd.DataFrame | None:
        """Adjusted daily OHLCV of completed sessions strictly before as_of_date."""

    @abc.abstractmethod
    def get_preclose_snapshot(self, symbol: str, cutoff: datetime) -> PreCloseSnapshot | None:
        """Current-session state built only from data through the cutoff."""

    @abc.abstractmethod
    def get_same_time_cumulative_volumes(
        self, symbol: str, as_of_date: date, cutoff_time: time, lookback: int,
    ) -> pd.Series | None:
        """Cumulative volume at cutoff_time for each of the previous
        ``lookback`` sessions. ``None`` when unavailable (never substitute
        full-day volume here)."""

    @abc.abstractmethod
    def get_index_history(self, index_symbol: str, as_of_date: date) -> pd.DataFrame | None:
        """Daily closes of a benchmark or sector index, completed sessions only."""

    @abc.abstractmethod
    def get_index_last_price(self, index_symbol: str, cutoff: datetime) -> float | None:
        """Index level at the cutoff; ``None`` if intraday index data is missing."""

    @abc.abstractmethod
    def get_sector_index_symbol(self, sector: str | None) -> str | None:
        """Map a stock's sector name to its sector-index symbol."""

    @abc.abstractmethod
    def get_next_session_date(self, as_of_date: date) -> date | None:
        """Next trading session after as_of_date; ``None`` if calendar unknown."""


# ---------------------------------------------------------------------------
# validation helpers (spec section 33)
# ---------------------------------------------------------------------------

def validate_daily_history(df: pd.DataFrame, as_of_date: date) -> list[str]:
    """Return a list of validation problems (empty list == valid)."""
    problems: list[str] = []
    if df is None or df.empty:
        return ['EMPTY_HISTORY']
    missing = [c for c in DAILY_COLUMNS if c not in df.columns]
    if missing:
        return [f'MISSING_COLUMNS:{",".join(missing)}']
    dates = pd.to_datetime(df['date'])
    if not dates.is_monotonic_increasing:
        problems.append('DATES_NOT_SORTED')
    if dates.duplicated().any():
        problems.append('DUPLICATE_DATES')
    if (dates.dt.date >= as_of_date).any():
        problems.append('FUTURE_DATES_IN_HISTORY')
    ohlc = df[['open', 'high', 'low', 'close']]
    if not (ohlc > 0).all().all():
        problems.append('NON_POSITIVE_PRICES')
    if (df['high'] + 1e-9 < df[['open', 'low', 'close']].max(axis=1)).any():
        problems.append('HIGH_BELOW_OTHER_PRICES')
    if (df['low'] - 1e-9 > df[['open', 'high', 'close']].min(axis=1)).any():
        problems.append('LOW_ABOVE_OTHER_PRICES')
    if (df['volume'] < 0).any():
        problems.append('NEGATIVE_VOLUME')
    return problems


def validate_snapshot(snap: PreCloseSnapshot, cutoff: datetime) -> list[str]:
    """Return validation problems for the pre-close snapshot."""
    problems: list[str] = []
    snap_ts = pd.Timestamp(snap.timestamp)
    cutoff_ts = pd.Timestamp(cutoff)
    if snap_ts.tzinfo is None and cutoff_ts.tzinfo is not None:
        cutoff_ts = cutoff_ts.tz_localize(None)
    elif snap_ts.tzinfo is not None and cutoff_ts.tzinfo is None:
        snap_ts = snap_ts.tz_localize(None)
    if snap_ts > cutoff_ts:
        problems.append('SNAPSHOT_AFTER_CUTOFF')
    prices = (snap.session_open, snap.high_to_cutoff, snap.low_to_cutoff, snap.last_price)
    if any(p is None or not np.isfinite(p) or p <= 0 for p in prices):
        problems.append('NON_POSITIVE_SNAPSHOT_PRICES')
        return problems
    if snap.high_to_cutoff + 1e-9 < max(snap.session_open, snap.last_price, snap.low_to_cutoff):
        problems.append('SNAPSHOT_HIGH_INCONSISTENT')
    if snap.low_to_cutoff - 1e-9 > min(snap.session_open, snap.last_price, snap.high_to_cutoff):
        problems.append('SNAPSHOT_LOW_INCONSISTENT')
    if snap.cumulative_volume is None or snap.cumulative_volume < 0:
        problems.append('NEGATIVE_SNAPSHOT_VOLUME')
    if snap.vwap is not None and snap.vwap <= 0:
        problems.append('NON_POSITIVE_VWAP')
    return problems


# ---------------------------------------------------------------------------
# in-memory provider
# ---------------------------------------------------------------------------

class InMemoryDataProvider(DataProvider):
    """Provider backed by in-memory frames; used for backtests and tests.

    Parameters
    ----------
    daily : dict[symbol, DataFrame(date, open, high, low, close, volume)]
        Adjusted daily bars (any date range; truncated point-in-time on read).
    universe : list[UniverseEntry] | dict[date, list[UniverseEntry]]
        Static universe or a point-in-time mapping (largest key <= as_of wins).
        Entries with ``listing_date`` after the as-of date are always dropped.
    intraday : dict[symbol, DataFrame(timestamp, open, high, low, close, volume)]
        One- or five-minute bars; used to build pre-close snapshots and
        same-time volume history by truncating at the cutoff.
    index_daily / index_intraday : same shapes, keyed by index symbol.
    sector_index_map : dict[sector_name, index_symbol]
    same_time_volumes : dict[symbol, Series]
        Optional explicit same-time cumulative-volume history (overrides
        computation from ``intraday``).
    trading_calendar : iterable of session dates (for next-session lookup).
    """

    def __init__(
        self,
        daily: dict[str, pd.DataFrame],
        universe: list[UniverseEntry] | dict[date, list[UniverseEntry]],
        intraday: dict[str, pd.DataFrame] | None = None,
        index_daily: dict[str, pd.DataFrame] | None = None,
        index_intraday: dict[str, pd.DataFrame] | None = None,
        sector_index_map: dict[str, str] | None = None,
        same_time_volumes: dict[str, pd.Series] | None = None,
        trading_calendar: list[date] | None = None,
        circuit_locked: dict[str, bool] | None = None,
    ) -> None:
        self._daily = daily
        self._universe = universe
        self._intraday = intraday or {}
        self._index_daily = index_daily or {}
        self._index_intraday = index_intraday or {}
        self._sector_index_map = sector_index_map or {}
        self._same_time_volumes = same_time_volumes or {}
        self._calendar = sorted(trading_calendar) if trading_calendar else None
        self._circuit_locked = circuit_locked or {}

    # -- universe ----------------------------------------------------------
    def get_universe(self, as_of_date: date) -> list[UniverseEntry]:
        if isinstance(self._universe, dict):
            keys = [k for k in self._universe if k <= as_of_date]
            entries = self._universe[max(keys)] if keys else []
        else:
            entries = self._universe
        return [
            e for e in entries
            if e.listing_date is None or e.listing_date <= as_of_date
        ]

    # -- daily -----------------------------------------------------------
    def get_daily_history(self, symbol: str, as_of_date: date) -> pd.DataFrame | None:
        df = self._daily.get(symbol)
        if df is None:
            return None
        dates = pd.to_datetime(df['date']).dt.date
        return df.loc[dates < as_of_date].reset_index(drop=True)

    # -- intraday snapshot -------------------------------------------------
    def _intraday_to_cutoff(self, frame: pd.DataFrame, cutoff: datetime) -> pd.DataFrame:
        ts = pd.to_datetime(frame['timestamp'])
        cutoff_ts = pd.Timestamp(cutoff)
        # reconcile tz-awareness: a naive cutoff means local (exchange) time
        if ts.dt.tz is None and cutoff_ts.tzinfo is not None:
            cutoff_ts = cutoff_ts.tz_localize(None)
        elif ts.dt.tz is not None and cutoff_ts.tzinfo is None:
            cutoff_ts = cutoff_ts.tz_localize(ts.dt.tz)
        same_day = ts.dt.date == cutoff_ts.date()
        return frame.loc[same_day & (ts <= cutoff_ts)]

    def get_preclose_snapshot(self, symbol: str, cutoff: datetime) -> PreCloseSnapshot | None:
        frame = self._intraday.get(symbol)
        if frame is None or frame.empty:
            return None
        day = self._intraday_to_cutoff(frame, cutoff)
        if day.empty:
            return None
        typical = (day['high'] + day['low'] + day['close']) / 3.0
        total_volume = float(day['volume'].sum())
        vwap = float((typical * day['volume']).sum() / total_volume) if total_volume > 0 else None
        return PreCloseSnapshot(
            timestamp=pd.to_datetime(day['timestamp'].iloc[-1]).to_pydatetime(),
            session_open=float(day['open'].iloc[0]),
            high_to_cutoff=float(day['high'].max()),
            low_to_cutoff=float(day['low'].min()),
            last_price=float(day['close'].iloc[-1]),
            cumulative_volume=total_volume,
            vwap=vwap,
            circuit_locked=self._circuit_locked.get(symbol),
        )

    # -- same-time volume ------------------------------------------------------
    def get_same_time_cumulative_volumes(
        self, symbol: str, as_of_date: date, cutoff_time: time, lookback: int,
    ) -> pd.Series | None:
        explicit = self._same_time_volumes.get(symbol)
        if explicit is not None:
            return explicit.tail(lookback)
        frame = self._intraday.get(symbol)
        if frame is None or frame.empty:
            return None
        ts = pd.to_datetime(frame['timestamp'])
        past = frame.loc[(ts.dt.date < as_of_date) & (ts.dt.time <= cutoff_time)]
        if past.empty:
            return None
        past_dates = pd.to_datetime(past['timestamp']).dt.date
        per_day = past.groupby(past_dates)['volume'].sum().sort_index()
        if len(per_day) < lookback:
            return None
        return per_day.tail(lookback)

    # -- indices ----------------------------------------------------------------
    def get_index_history(self, index_symbol: str, as_of_date: date) -> pd.DataFrame | None:
        df = self._index_daily.get(index_symbol)
        if df is None:
            return None
        dates = pd.to_datetime(df['date']).dt.date
        return df.loc[dates < as_of_date].reset_index(drop=True)

    def get_index_last_price(self, index_symbol: str, cutoff: datetime) -> float | None:
        frame = self._index_intraday.get(index_symbol)
        if frame is None or frame.empty:
            return None
        day = self._intraday_to_cutoff(frame, cutoff)
        if day.empty:
            return None
        return float(day['close'].iloc[-1])

    def get_sector_index_symbol(self, sector: str | None) -> str | None:
        if sector is None:
            return None
        return self._sector_index_map.get(sector)

    def get_next_session_date(self, as_of_date: date) -> date | None:
        if not self._calendar:
            return None
        upcoming = [d for d in self._calendar if d > as_of_date]
        return upcoming[0] if upcoming else None
