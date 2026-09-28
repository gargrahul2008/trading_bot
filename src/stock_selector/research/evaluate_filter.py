"""Filter-validation report (spec section 36).

Measures, for each historical selection, what happened AFTER the cutoff:
next-open return, next-morning VWAP returns (5/10/15 minutes), first-15-minute
MFE/MAE, overnight gap and cost-adjusted profitability. Strictly one-way:
selection frames go in, outcome frames come out.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

NAN = float('nan')


@dataclass
class CostModel:
    """Round-trip transaction costs as a fraction of traded value."""
    buy_cost_pct: float = 0.0005
    sell_cost_pct: float = 0.0005

    @property
    def round_trip_pct(self) -> float:
        return self.buy_cost_pct + self.sell_cost_pct


def _window_vwap(bars: pd.DataFrame, minutes: int) -> float:
    window = bars.head(minutes)
    volume = window['volume'].sum()
    if volume <= 0:
        return NAN
    typical = (window['high'] + window['low'] + window['close']) / 3.0
    return float((typical * window['volume']).sum() / volume)


def evaluate_selection_outcomes(
    selections: pd.DataFrame,
    next_session_intraday: dict[str, pd.DataFrame],
    cost_model: CostModel | None = None,
    exit_window_minutes: int = 15,
) -> pd.DataFrame:
    """Per-selection outcome metrics.

    ``selections`` needs at least: symbol, as_of_date, last_price plus any
    grouping columns (bullish_score, entry_candidate_rank, sector, ...).
    ``next_session_intraday`` maps ``"SYMBOL|YYYY-MM-DD"`` (the SELECTION
    date) to the next session's minute bars sorted by timestamp.
    """
    costs = cost_model or CostModel()
    records: list[dict] = []
    for _, sel in selections.iterrows():
        key = f"{sel['symbol']}|{pd.Timestamp(sel['as_of_date']).date()}"
        bars = next_session_intraday.get(key)
        entry_price = float(sel['last_price'])
        record = dict(sel)
        if bars is None or bars.empty or not np.isfinite(entry_price) or entry_price <= 0:
            record.update({
                'next_open': NAN, 'return_to_open': NAN, 'overnight_gap': NAN,
                'return_to_vwap_5m': NAN, 'return_to_vwap_10m': NAN,
                'return_to_vwap_15m': NAN, 'mfe_15m': NAN, 'mae_15m': NAN,
                'gross_profitable': None, 'net_return_15m_vwap': NAN,
                'net_profitable': None, 'transaction_cost_pct': costs.round_trip_pct,
            })
            records.append(record)
            continue
        next_open = float(bars['open'].iloc[0])
        window = bars.head(exit_window_minutes)
        vwap_15 = _window_vwap(bars, exit_window_minutes)
        return_to_vwap_15 = vwap_15 / entry_price - 1.0 if np.isfinite(vwap_15) else NAN
        gross = return_to_vwap_15
        net = gross - costs.round_trip_pct if np.isfinite(gross) else NAN
        record.update({
            'next_open': next_open,
            'return_to_open': next_open / entry_price - 1.0,
            'overnight_gap': next_open / entry_price - 1.0,
            'return_to_vwap_5m': (_window_vwap(bars, 5) / entry_price - 1.0)
                                 if np.isfinite(_window_vwap(bars, 5)) else NAN,
            'return_to_vwap_10m': (_window_vwap(bars, 10) / entry_price - 1.0)
                                  if np.isfinite(_window_vwap(bars, 10)) else NAN,
            'return_to_vwap_15m': return_to_vwap_15,
            'mfe_15m': float(window['high'].max()) / entry_price - 1.0,
            'mae_15m': float(window['low'].min()) / entry_price - 1.0,
            'gross_profitable': bool(gross > 0) if np.isfinite(gross) else None,
            'transaction_cost_pct': costs.round_trip_pct,
            'net_return_15m_vwap': net,
            'net_profitable': bool(net > 0) if np.isfinite(net) else None,
        })
        records.append(record)
    return pd.DataFrame(records)


def summarize_outcomes(
    outcomes: pd.DataFrame,
    by: str | list[str] | None = None,
    return_column: str = 'return_to_vwap_15m',
) -> pd.DataFrame:
    """Aggregate outcome statistics, optionally grouped (by score decile,
    rank, RS group, sector, regime, day of week, gap group, ...)."""
    def _stats(group: pd.DataFrame) -> pd.Series:
        returns = group[return_column].dropna()
        net = group['net_return_15m_vwap'].dropna()
        return pd.Series({
            'n_candidates': len(group),
            'avg_return': returns.mean() if len(returns) else NAN,
            'median_return': returns.median() if len(returns) else NAN,
            'win_rate': (returns > 0).mean() if len(returns) else NAN,
            'std_return': returns.std() if len(returns) > 1 else NAN,
            'worst_return': returns.min() if len(returns) else NAN,
            'p5_return': returns.quantile(0.05) if len(returns) else NAN,
            'avg_cost': group['transaction_cost_pct'].mean(),
            'avg_net_return': net.mean() if len(net) else NAN,
            'net_win_rate': (net > 0).mean() if len(net) else NAN,
        })

    if by is None:
        return _stats(outcomes).to_frame('all').T
    grouped = outcomes.groupby(by, dropna=False, observed=True)
    try:
        return grouped.apply(_stats, include_groups=False)
    except TypeError:  # pandas < 2.2 has no include_groups
        return grouped.apply(_stats)


def daily_coverage_report(outcomes: pd.DataFrame) -> pd.DataFrame:
    """Candidate count per day and share of days with no qualifying stocks."""
    per_day = outcomes.groupby('as_of_date').size().rename('candidates')
    summary = pd.DataFrame({
        'candidates_per_day_mean': [per_day.mean()],
        'candidates_per_day_median': [per_day.median()],
        'days': [len(per_day)],
        'zero_candidate_days_pct': [(per_day == 0).mean() * 100.0],
    })
    return summary


def sector_concentration(outcomes: pd.DataFrame) -> pd.Series:
    """Share of selections per sector."""
    return outcomes['sector'].value_counts(normalize=True, dropna=False)
