"""Performance by bullish-score decile and by the spec's score buckets
(60-69, 70-74, 75-79, 80-89, 90-100)."""
from __future__ import annotations

import pandas as pd

from .evaluate_filter import summarize_outcomes

SCORE_BUCKETS = [
    (60.0, 70.0, '60-69'),
    (70.0, 75.0, '70-74'),
    (75.0, 80.0, '75-79'),
    (80.0, 90.0, '80-89'),
    (90.0, 100.0001, '90-100'),
]


def score_decile_report(
    outcomes: pd.DataFrame, return_column: str = 'return_to_vwap_15m',
) -> pd.DataFrame:
    """Outcome statistics by bullish-score decile (0-10, 10-20, ..., 90-100)."""
    df = outcomes.copy()
    df['score_decile'] = pd.cut(
        df['bullish_score'],
        bins=[i * 10.0 for i in range(11)],
        labels=[f'{i * 10}-{(i + 1) * 10}' for i in range(10)],
        include_lowest=True,
    )
    return summarize_outcomes(df, by='score_decile', return_column=return_column)


def score_bucket_report(
    outcomes: pd.DataFrame, return_column: str = 'return_to_vwap_15m',
) -> pd.DataFrame:
    """Outcome statistics for the spec's five score buckets."""
    df = outcomes.copy()

    def bucket(score: float) -> str | None:
        for low, high, label in SCORE_BUCKETS:
            if low <= score < high:
                return label
        return None

    df['score_bucket'] = df['bullish_score'].map(bucket)
    df = df.loc[df['score_bucket'].notna()]
    report = summarize_outcomes(df, by='score_bucket', return_column=return_column)
    order = [label for _, _, label in SCORE_BUCKETS if label in report.index]
    return report.loc[order]
