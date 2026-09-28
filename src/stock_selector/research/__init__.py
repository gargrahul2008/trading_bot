"""Research and validation for the stock selector.

These modules may use FUTURE returns to evaluate historical selections.
Future returns must never be fed back into the filter itself.
"""
from .evaluate_filter import CostModel, evaluate_selection_outcomes, summarize_outcomes
from .score_decile_analysis import score_bucket_report, score_decile_report
from .parameter_robustness import run_parameter_grid, walk_forward_windows

__all__ = [
    'CostModel',
    'evaluate_selection_outcomes',
    'summarize_outcomes',
    'score_bucket_report',
    'score_decile_report',
    'run_parameter_grid',
    'walk_forward_windows',
]
