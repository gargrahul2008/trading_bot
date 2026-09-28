"""Deterministic candidate ranking (spec sections 28-29)."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .config import SelectorConfig

RANK_SORT_COLUMNS = [
    'bullish_score',
    'rs_composite_percentile',
    'relative_strength_score',
    'preclose_score',
    'same_time_volume_ratio',
    'median_traded_value_20',
    'symbol',
]
RANK_SORT_ASCENDING = [False, False, False, False, False, False, True]


def _deterministic_sort(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        by=RANK_SORT_COLUMNS,
        ascending=RANK_SORT_ASCENDING,
        kind='mergesort',
        na_position='last',
    )


def assign_ranks(snapshot: pd.DataFrame, config: SelectorConfig) -> pd.DataFrame:
    """Add raw_universe_rank, entry_candidate_rank and (optionally)
    diversified_entry_candidate_rank columns."""
    df = snapshot.copy()
    df['raw_universe_rank'] = np.nan
    df['entry_candidate_rank'] = np.nan
    df['diversified_entry_candidate_rank'] = np.nan
    if df.empty:
        return df

    # rank every stock with a valid score, strongest first
    scored = df.loc[df['bullish_score'].notna()]
    ordered = _deterministic_sort(scored)
    df.loc[ordered.index, 'raw_universe_rank'] = range(1, len(ordered) + 1)

    eligible = df.loc[df['entry_eligible'] == True]  # noqa: E712
    ordered_eligible = _deterministic_sort(eligible)
    df.loc[ordered_eligible.index, 'entry_candidate_rank'] = range(1, len(ordered_eligible) + 1)

    if config.enable_diversified_output:
        sector_counts: dict[object, int] = {}
        rank = 0
        for idx in ordered_eligible.index:
            if rank >= config.max_output_candidates:
                break
            sector = df.at[idx, 'sector']
            key = sector if isinstance(sector, str) and sector else '_UNKNOWN_'
            if sector_counts.get(key, 0) >= config.max_top_candidates_per_sector:
                continue
            sector_counts[key] = sector_counts.get(key, 0) + 1
            rank += 1
            df.at[idx, 'diversified_entry_candidate_rank'] = rank

    return df


def entry_candidates(snapshot: pd.DataFrame, config: SelectorConfig) -> pd.DataFrame:
    """Entry-eligible rows ordered by rank, capped at max_output_candidates."""
    ranked = snapshot.loc[snapshot['entry_candidate_rank'].notna()]
    ranked = ranked.sort_values('entry_candidate_rank', kind='mergesort')
    return ranked.head(config.max_output_candidates).reset_index(drop=True)


def continuation_candidates(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Continuation-eligible rows, strongest first (deterministic)."""
    rows = snapshot.loc[snapshot['continue_eligible'] == True]  # noqa: E712
    return _deterministic_sort(rows).reset_index(drop=True)
