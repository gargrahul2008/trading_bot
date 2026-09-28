"""Limited, predefined parameter-robustness testing (spec section 37).

This deliberately does NOT optimize hundreds of combinations: it sweeps a
small explicit grid, reports stability metrics per combination, and provides
walk-forward window splits for out-of-sample checks. Prefer a stable
parameter region over the single highest historical return.
"""
from __future__ import annotations

import itertools
from dataclasses import replace
from datetime import datetime

import pandas as pd

from ..config import SelectorConfig
from ..data_provider import DataProvider
from ..selector import BullishStockSelector
from .evaluate_filter import CostModel, evaluate_selection_outcomes

# spec-suggested sweep values
DEFAULT_GRID: dict[str, list] = {
    'min_entry_score': [70.0, 75.0, 80.0],
    'min_entry_rs_percentile': [65.0, 70.0, 75.0, 80.0],
    'min_close_location_value': [0.55, 0.60, 0.65, 0.70],
    'min_same_time_volume_ratio': [0.80, 0.90, 1.00, 1.20],
    'max_extension_from_ema20_atr': [2.0, 2.5, 3.0],
    'min_52_week_high_proximity': [0.80, 0.85, 0.90],
}


def walk_forward_windows(
    timestamps: list[datetime], n_folds: int = 4,
) -> list[tuple[list[datetime], list[datetime]]]:
    """Expanding-window (train, test) splits over selection timestamps."""
    ordered = sorted(timestamps)
    fold_size = max(1, len(ordered) // (n_folds + 1))
    windows = []
    for fold in range(1, n_folds + 1):
        split = fold * fold_size
        test_end = min(len(ordered), split + fold_size)
        if split >= len(ordered):
            break
        windows.append((ordered[:split], ordered[split:test_end]))
    return windows


def run_parameter_grid(
    provider: DataProvider,
    timestamps: list[datetime],
    market_states: dict[datetime, str],
    base_config: SelectorConfig,
    grid: dict[str, list] | None = None,
    next_session_intraday: dict[str, pd.DataFrame] | None = None,
    cost_model: CostModel | None = None,
) -> pd.DataFrame:
    """Run the selector over every timestamp for each parameter combination.

    Returns one row per combination with candidate-count stability and (when
    ``next_session_intraday`` outcome data is supplied) net-return statistics.
    Sweep one or two parameters at a time — the default grid is only a menu.
    """
    grid = grid or {k: DEFAULT_GRID[k] for k in ('min_entry_score', 'min_entry_rs_percentile')}
    names = list(grid)
    results: list[dict] = []
    for combo in itertools.product(*(grid[name] for name in names)):
        config = replace(base_config, **dict(zip(names, combo)))
        selector = BullishStockSelector(config, provider)
        all_candidates: list[pd.DataFrame] = []
        counts: list[int] = []
        for ts in timestamps:
            result = selector.run(
                ts, market_states.get(ts, 'ON'), save_reports_to_disk=False,
            )
            candidates = result.entry_candidates_dataframe
            counts.append(len(candidates))
            if len(candidates):
                all_candidates.append(candidates)
        row: dict = dict(zip(names, combo))
        counts_series = pd.Series(counts, dtype=float)
        row.update({
            'runs': len(timestamps),
            'mean_candidates': counts_series.mean(),
            'std_candidates': counts_series.std(),
            'zero_candidate_days_pct': (counts_series == 0).mean() * 100.0,
        })
        if next_session_intraday is not None and all_candidates:
            outcomes = evaluate_selection_outcomes(
                pd.concat(all_candidates, ignore_index=True),
                next_session_intraday,
                cost_model,
            )
            net = outcomes['net_return_15m_vwap'].dropna()
            row.update({
                'avg_net_return': net.mean() if len(net) else float('nan'),
                'net_win_rate': (net > 0).mean() if len(net) else float('nan'),
                'p5_net_return': net.quantile(0.05) if len(net) else float('nan'),
            })
        results.append(row)
    return pd.DataFrame(results)
