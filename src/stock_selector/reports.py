"""CSV reports and the diagnostic summary (spec section 30)."""
from __future__ import annotations

import os
from datetime import date
from typing import Any

import pandas as pd

from .config import SelectorConfig
from .models import (
    STATUS_HARD_FAIL,
    STATUS_REJECTED,
)

# exact output column order (spec section 30)
REQUIRED_COLUMNS = [
    'as_of_date', 'cutoff_timestamp', 'data_mode',
    'symbol', 'exchange', 'sector', 'industry', 'universe_name',
    'market_state', 'market_entry_allowed',
    'last_price', 'previous_close', 'session_open',
    'session_high_to_cutoff', 'session_low_to_cutoff',
    'cumulative_volume_to_cutoff', 'vwap_to_cutoff',
    'day_return', 'opening_gap', 'close_location_value', 'vwap_distance',
    'same_time_volume_ratio', 'drawdown_from_intraday_high', 'bid_ask_spread_percent',
    'ema_20', 'sma_50', 'sma_200', 'ema_20_rising', 'sma_50_rising', 'sma_200_rising',
    'atr_14', 'atr_percent', 'extension_from_ema20_atr',
    'return_5d', 'return_20d', 'return_63d', 'return_126d',
    'previous_20_day_high', 'breakout_ratio', 'high_52_week', 'high_52_week_proximity',
    'weekly_close', 'weekly_ema_10', 'weekly_sma_30',
    'weekly_ema_10_rising', 'weekly_sma_30_rising', 'weekly_bullish_alignment',
    'benchmark_return_20', 'benchmark_return_63', 'benchmark_return_126',
    'excess_return_20', 'excess_return_63', 'excess_return_126',
    'rs_20_percentile', 'rs_63_percentile', 'rs_126_percentile', 'rs_composite_percentile',
    'sector_data_available', 'sector_close', 'sector_sma_50', 'sector_sma_50_rising',
    'sector_excess_return_63', 'sector_trend_bullish', 'sector_rs_bullish',
    'median_traded_value_20', 'required_turnover', 'liquidity_pass',
    'tradable', 'btst_eligible', 'restricted_security', 'event_risk',
    'corporate_action_risk', 'circuit_locked',
    'data_valid', 'stock_hard_filter_pass', 'hard_fail',
    'trend_score', 'relative_strength_score', 'momentum_score',
    'preclose_score', 'risk_sector_score', 'bullish_score',
    'entry_eligible', 'continue_eligible', 'status',
    'raw_universe_rank', 'entry_candidate_rank', 'diversified_entry_candidate_rank',
    'acceptance_reasons', 'rejection_reasons', 'warning_reasons',
]

REASON_COLUMNS = ('acceptance_reasons', 'rejection_reasons', 'warning_reasons')


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Required columns first (creating any that are missing), extras after."""
    out = df.copy()
    for column in REQUIRED_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    extras = [c for c in out.columns if c not in REQUIRED_COLUMNS]
    return out[REQUIRED_COLUMNS + extras]


def serialize_reasons(df: pd.DataFrame, delimiter: str) -> pd.DataFrame:
    """Reason lists -> delimiter-joined strings for CSV output."""
    out = df.copy()
    for column in REASON_COLUMNS:
        if column in out.columns:
            out[column] = out[column].map(
                lambda v: delimiter.join(v) if isinstance(v, (list, tuple)) else (v or '')
            )
    return out


def save_reports(
    full_snapshot: pd.DataFrame,
    entry_candidates: pd.DataFrame,
    as_of_date: date,
    config: SelectorConfig,
) -> dict[str, str]:
    """Write the three CSV reports; returns {report_name: path}."""
    os.makedirs(config.output_dir, exist_ok=True)
    stamp = as_of_date.strftime('%Y%m%d')
    rejected = full_snapshot.loc[
        full_snapshot['status'].isin([STATUS_REJECTED, STATUS_HARD_FAIL])
    ]
    paths = {
        'full_snapshot': os.path.join(config.output_dir, f'stock_filter_full_snapshot_{stamp}.csv'),
        'entry_candidates': os.path.join(config.output_dir, f'stock_filter_entry_candidates_{stamp}.csv'),
        'rejections': os.path.join(config.output_dir, f'stock_filter_rejections_{stamp}.csv'),
    }
    delimiter = config.reason_delimiter
    serialize_reasons(full_snapshot, delimiter).to_csv(paths['full_snapshot'], index=False)
    serialize_reasons(entry_candidates, delimiter).to_csv(paths['entry_candidates'], index=False)
    serialize_reasons(rejected, delimiter).to_csv(paths['rejections'], index=False)
    return paths


def build_diagnostic_summary(
    full_snapshot: pd.DataFrame,
    market_state: str,
    as_of_date: date,
    config: SelectorConfig,
) -> dict[str, Any]:
    status_counts = full_snapshot['status'].value_counts().to_dict() if len(full_snapshot) else {}
    eligible = full_snapshot.loc[full_snapshot['entry_eligible'] == True] if len(full_snapshot) else pd.DataFrame()  # noqa: E712
    scores = full_snapshot['bullish_score'].dropna() if len(full_snapshot) else pd.Series(dtype=float)
    warning_rows = 0
    if 'warning_reasons' in full_snapshot.columns:
        warning_rows = int(full_snapshot['warning_reasons'].map(
            lambda v: bool(v) if isinstance(v, list) else bool(v)
        ).sum())
    return {
        'as_of_date': str(as_of_date),
        'data_mode': config.data_mode,
        'market_state': market_state,
        'universe_size': int(len(full_snapshot)),
        'status_counts': {str(k): int(v) for k, v in status_counts.items()},
        'entry_candidates': int(len(eligible)),
        'continuation_candidates': int(full_snapshot['continue_eligible'].sum()) if len(full_snapshot) else 0,
        'mean_bullish_score': float(scores.mean()) if len(scores) else None,
        'max_bullish_score': float(scores.max()) if len(scores) else None,
        'rows_with_warnings': warning_rows,
    }
