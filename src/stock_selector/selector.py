"""BullishStockSelector — orchestrates the full selection pipeline
(spec sections 31-32).

Flow: validate timestamp -> point-in-time universe -> per-stock indicator
rows (built only from data through the cutoff) -> cross-sectional RS
percentiles -> sector strength -> hard filters -> scores -> eligibility ->
status -> deterministic ranking -> reports.
"""
from __future__ import annotations

import math
from datetime import date, datetime, time

import numpy as np
import pandas as pd

from .config import (
    DATA_MODE_PRE_CLOSE_SNAPSHOT,
    DATA_MODE_PREVIOUS_COMPLETED_DAY,
    VALID_MARKET_STATES,
    SelectorConfig,
)
from .classifier import classify_stock
from .daily_indicators import compute_daily_indicators
from .data_provider import DataProvider, validate_daily_history, validate_snapshot
from .exclusions import evaluate_exclusions
from .liquidity import compute_liquidity
from .models import PreCloseSnapshot, SelectionResult, UniverseEntry
from .provisional_bar import (
    append_provisional_bar,
    compute_preclose_metrics,
    same_time_volume_ratio,
    snapshot_to_provisional_bar,
)
from .ranking import assign_ranks, continuation_candidates, entry_candidates
from .relative_strength import (
    assign_cross_sectional_percentiles,
    compute_benchmark_returns,
    compute_stock_relative_strength,
)
from .reports import build_diagnostic_summary, order_columns, save_reports
from .scoring import compute_scores
from .sector_strength import UNAVAILABLE_SECTOR_METRICS, compute_sector_strength
from .weekly_indicators import compute_weekly_indicators

NAN = float('nan')


class BullishStockSelector:
    """Ranks structurally bullish stocks for a BTST strategy.

    Selection only — capital allocation, slots, ramping and order placement
    belong to the separate execution module.
    """

    def __init__(self, config: SelectorConfig, data_provider: DataProvider) -> None:
        config.validate()
        self.config = config
        self.provider = data_provider

    # ------------------------------------------------------------------ api
    def run(
        self,
        as_of_timestamp: datetime | str,
        market_state: str,
        universe: list[UniverseEntry] | None = None,
        save_reports_to_disk: bool = True,
    ) -> SelectionResult:
        cfg = self.config
        if market_state not in VALID_MARKET_STATES:
            raise ValueError(f'invalid market_state: {market_state!r}')
        cutoff = self._resolve_cutoff(as_of_timestamp)
        as_of_date = cutoff.date()

        if universe is None:
            universe = self.provider.get_universe(as_of_date)

        benchmark_closes = self._index_closes_full(cfg.benchmark_symbol, as_of_date, cutoff)
        if benchmark_closes is None or len(benchmark_closes) <= cfg.rs_long_period:
            raise ValueError(
                f'benchmark {cfg.benchmark_symbol!r} history unavailable or too short — '
                'relative strength cannot be calculated'
            )
        benchmark_returns = compute_benchmark_returns(benchmark_closes, cfg)

        calendar_gap_ok = self._calendar_gap_ok(as_of_date)
        sector_cache: dict[str, dict] = {}

        rows: list[dict] = []
        for entry in universe:
            try:
                row = self._build_stock_row(
                    entry, as_of_date, cutoff, benchmark_returns, calendar_gap_ok,
                )
            except Exception as exc:  # never let one symbol kill the batch
                row = self._error_row(entry, str(exc))
            sector_metrics = self._sector_metrics(
                entry.sector, as_of_date, cutoff, benchmark_returns, sector_cache,
            )
            row.update(sector_metrics)
            if not sector_metrics.get('sector_data_available'):
                row.setdefault('_warnings', []).append('SECTOR_DATA_UNAVAILABLE')
            row['as_of_date'] = as_of_date
            row['cutoff_timestamp'] = cutoff
            row['data_mode'] = cfg.data_mode
            rows.append(row)

        assign_cross_sectional_percentiles(rows)

        for row in rows:
            row.update(compute_scores(row, cfg))
            classify_stock(row, cfg, market_state)
            row['warning_reasons'] = row.pop('_warnings', [])

        snapshot = order_columns(pd.DataFrame(rows)) if rows else order_columns(pd.DataFrame())
        snapshot = assign_ranks(snapshot, cfg)

        entry_df = entry_candidates(snapshot, cfg)
        continuation_df = continuation_candidates(snapshot)
        rejected_df = snapshot.loc[
            snapshot['status'].isin(['REJECTED', 'HARD_FAIL'])
        ].reset_index(drop=True)

        report_paths: dict[str, str] = {}
        if save_reports_to_disk:
            report_paths = save_reports(snapshot, entry_df, as_of_date, cfg)

        return SelectionResult(
            full_snapshot_dataframe=snapshot,
            entry_candidates_dataframe=entry_df,
            continuation_candidates_dataframe=continuation_df,
            rejected_dataframe=rejected_df,
            diagnostic_summary=build_diagnostic_summary(snapshot, market_state, as_of_date, cfg),
            report_paths=report_paths,
        )

    # ------------------------------------------------------------- internals
    def _resolve_cutoff(self, as_of_timestamp: datetime | str) -> pd.Timestamp:
        ts = pd.Timestamp(as_of_timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(self.config.timezone)
        else:
            ts = ts.tz_convert(self.config.timezone)
        hh, mm, ss = (int(p) for p in self.config.selection_cutoff_time.split(':'))
        cutoff = ts.normalize() + pd.Timedelta(hours=hh, minutes=mm, seconds=ss)
        if ts < cutoff:
            raise ValueError(
                f'as_of_timestamp {ts} is before the selection cutoff {cutoff}; '
                'the pre-close snapshot does not exist yet'
            )
        return cutoff

    def _cutoff_naive(self, cutoff: pd.Timestamp) -> datetime:
        return cutoff.tz_localize(None).to_pydatetime()

    def _calendar_gap_ok(self, as_of_date: date) -> bool | None:
        next_session = self.provider.get_next_session_date(as_of_date)
        if next_session is None:
            return None
        return (next_session - as_of_date).days <= self.config.max_calendar_gap_days

    def _index_closes_full(
        self, index_symbol: str, as_of_date: date, cutoff: pd.Timestamp,
    ) -> np.ndarray | None:
        """Completed index closes plus the index level at the cutoff.

        In PREVIOUS_COMPLETED_DAY mode the last completed close IS the
        current value. In snapshot mode a missing intraday index level falls
        back to the previous close (benchmark assumed unchanged today).
        """
        hist = self.provider.get_index_history(index_symbol, as_of_date)
        if hist is None or hist.empty:
            return None
        closes = hist['close'].to_numpy(dtype=float)
        if self.config.data_mode == DATA_MODE_PREVIOUS_COMPLETED_DAY:
            return closes
        last = self.provider.get_index_last_price(index_symbol, self._cutoff_naive(cutoff))
        if last is None:
            last = closes[-1]
        return np.append(closes, float(last))

    def _sector_metrics(
        self,
        sector: str | None,
        as_of_date: date,
        cutoff: pd.Timestamp,
        benchmark_returns: dict[str, float],
        cache: dict[str, dict],
    ) -> dict:
        index_symbol = self.provider.get_sector_index_symbol(sector)
        if index_symbol is None:
            return dict(UNAVAILABLE_SECTOR_METRICS)
        if index_symbol not in cache:
            closes = self._index_closes_full(index_symbol, as_of_date, cutoff)
            cache[index_symbol] = compute_sector_strength(
                closes, benchmark_returns['benchmark_return_63'], self.config,
            )
        return dict(cache[index_symbol])

    def _base_row(self, entry: UniverseEntry) -> dict:
        return {
            'symbol': entry.symbol,
            'exchange': entry.exchange,
            'sector': entry.sector,
            'industry': entry.industry,
            'universe_name': entry.universe_name,
            '_warnings': [],
        }

    def _error_row(self, entry: UniverseEntry, message: str) -> dict:
        row = self._base_row(entry)
        row.update({
            'data_valid': False,
            'snapshot_valid': False,
            'daily_history_sessions': 0,
            'weekly_bars': 0,
            'tradable': entry.tradable if entry.tradable is not None else True,
            'liquidity_pass': False,
            'required_turnover': self.config.required_turnover,
        })
        row['_warnings'].append(f'PROCESSING_ERROR:{message}')
        return row

    def _build_stock_row(
        self,
        entry: UniverseEntry,
        as_of_date: date,
        cutoff: pd.Timestamp,
        benchmark_returns: dict[str, float],
        calendar_gap_ok: bool | None,
    ) -> dict:
        cfg = self.config
        row = self._base_row(entry)
        warnings: list[str] = row['_warnings']
        if calendar_gap_ok is None:
            warnings.append('CALENDAR_DATA_UNAVAILABLE')
        row['calendar_gap_ok'] = calendar_gap_ok

        history = self.provider.get_daily_history(entry.symbol, as_of_date)
        history_problems = validate_daily_history(history, as_of_date)
        data_valid = not any(
            p for p in history_problems if p not in ('EMPTY_HISTORY',)
        )
        if history_problems and history_problems != ['EMPTY_HISTORY']:
            warnings.extend(f'HISTORY:{p}' for p in history_problems)
        history = history if history is not None else pd.DataFrame(
            columns=['date', 'open', 'high', 'low', 'close', 'volume']
        )

        snapshot: PreCloseSnapshot | None = None
        snapshot_valid = True
        snapshot_reject_reason = 'MISSING_CURRENT_SNAPSHOT'
        vwap: float | None = None

        if cfg.data_mode == DATA_MODE_PRE_CLOSE_SNAPSHOT:
            snapshot = self.provider.get_preclose_snapshot(
                entry.symbol, self._cutoff_naive(cutoff)
            )
            if snapshot is None:
                snapshot_valid = False
            else:
                snap_problems = validate_snapshot(snapshot, self._cutoff_naive(cutoff))
                if snap_problems:
                    snapshot_valid = False
                    snapshot_reject_reason = 'INVALID_DATA'
                    warnings.extend(f'SNAPSHOT:{p}' for p in snap_problems)

        if cfg.data_mode == DATA_MODE_PRE_CLOSE_SNAPSHOT and snapshot_valid:
            completed = history
            current_bar = snapshot_to_provisional_bar(snapshot)
            vwap = snapshot.vwap
            full = append_provisional_bar(completed, current_bar, as_of_date)
            bid, ask = snapshot.bid_price, snapshot.ask_price
        else:
            # PREVIOUS_COMPLETED_DAY mode, or a degraded diagnostics row when
            # the snapshot is missing: the last completed session is "current".
            if len(history) < 2:
                row.update(self._insufficient_history_fields(entry, len(history)))
                row['snapshot_valid'] = snapshot_valid
                row['snapshot_reject_reason'] = snapshot_reject_reason
                row['data_valid'] = data_valid and len(history) > 0
                return row
            completed = history.iloc[:-1].reset_index(drop=True)
            last = history.iloc[-1]
            current_bar = {
                'open': float(last['open']), 'high': float(last['high']),
                'low': float(last['low']), 'close': float(last['close']),
                'volume': float(last['volume']),
            }
            full = history
            bid = ask = None
            if cfg.data_mode == DATA_MODE_PREVIOUS_COMPLETED_DAY:
                warnings.append('PREVIOUS_DAY_MODE_NO_INTRADAY_CONFIRMATION')

        previous_close = float(completed['close'].iloc[-1]) if len(completed) else NAN
        if vwap is None and cfg.data_mode == DATA_MODE_PRE_CLOSE_SNAPSHOT and snapshot_valid:
            warnings.append('VWAP_UNAVAILABLE')

        row.update(compute_daily_indicators(full, cfg))
        row.update(compute_weekly_indicators(history, as_of_date, cfg))
        row.update(compute_liquidity(completed, cfg))
        row.update(compute_preclose_metrics(current_bar, previous_close, vwap, bid, ask))
        row.update(
            compute_stock_relative_strength(
                full['close'].to_numpy(dtype=float), benchmark_returns, cfg,
            )
        )
        row.update(benchmark_returns)
        row.update(evaluate_exclusions(entry, snapshot, cfg, warnings))

        # same-time volume ratio
        if cfg.data_mode == DATA_MODE_PRE_CLOSE_SNAPSHOT:
            same_time_history = self.provider.get_same_time_cumulative_volumes(
                entry.symbol, as_of_date, self._cutoff_time(), cfg.same_time_volume_lookback,
            )
            ratio = same_time_volume_ratio(current_bar['volume'], same_time_history)
            available = math.isfinite(ratio)
            if not available:
                warnings.append('SAME_TIME_VOLUME_UNAVAILABLE')
        else:
            # full-day vs full-day median: consistent semantics for this mode
            tail = completed['volume'].tail(cfg.same_time_volume_lookback)
            ratio = same_time_volume_ratio(current_bar['volume'], tail)
            available = math.isfinite(ratio)
            warnings.append('FULL_DAY_VOLUME_RATIO_MODE')
        row['same_time_volume_ratio'] = ratio
        row['same_time_volume_available'] = available

        row.update({
            'daily_history_sessions': int(len(history)),
            'data_valid': data_valid,
            'snapshot_valid': snapshot_valid,
            'snapshot_reject_reason': snapshot_reject_reason,
            'previous_close': previous_close,
            'session_open': current_bar['open'],
            'session_high_to_cutoff': current_bar['high'],
            'session_low_to_cutoff': current_bar['low'],
            'cumulative_volume_to_cutoff': current_bar['volume'],
            'vwap_to_cutoff': vwap if vwap is not None else NAN,
            'vwap_available': vwap is not None,
        })
        return row

    def _insufficient_history_fields(self, entry: UniverseEntry, sessions: int) -> dict:
        fields = {
            'daily_history_sessions': sessions,
            'weekly_bars': 0,
            'tradable': entry.tradable if entry.tradable is not None else True,
            'btst_eligible': bool(entry.btst_eligible) if entry.btst_eligible is not None else True,
            'restricted_security': bool(entry.restricted_security or False),
            'event_risk': bool(entry.event_risk or False),
            'corporate_action_risk': bool(entry.corporate_action_risk or False),
            'circuit_locked': False,
            'liquidity_pass': False,
            'required_turnover': self.config.required_turnover,
            'median_traded_value_20': NAN,
        }
        return fields

    def _cutoff_time(self) -> time:
        hh, mm, ss = (int(p) for p in self.config.selection_cutoff_time.split(':'))
        return time(hh, mm, ss)
