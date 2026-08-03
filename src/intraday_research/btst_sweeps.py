"""
Parameter sweeps for the BTST Lagrangian ('1lg0') strategy.

The expensive step is the rolling SLSQP optimisation, which depends only on
(lb, frequency_type, rmean, objective, risk_free_rate, l2_penalty, method,
long_only) — NOT on trades / lf / execution / capital. The sweep therefore
computes one raw weight history per signal-parameter combo (optionally in
parallel processes) and reuses it across every truncation, holding-period and
execution-mode variant.

Usage (notebook):
    from intraday_research.btst_sweeps import run_sweep

    leaderboard = run_sweep(data, base_params=PARAMS, grid={
        "lb": [10, 20, 30],
        "trades": [3, 5, 10],
        "lf": [1, 3, 5],
        "execution": ["flat_legs", "hold", "delta"],
        "objective": ["lagrangian", "sortino", "min_variance"],
    }, max_workers=4)

Leaderboard is sorted by net (discount-broker) Sharpe.
"""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from itertools import product

import pandas as pd

from .btst_lagrangian import (
    align_universe,
    backtest_daily_from_signals,
    generate_weight_history,
    signals_from_weights,
)

# Parameters that change the raw optimiser weights (one weight history each).
SIGNAL_KEYS = (
    "lb", "frequency_type", "rmean", "objective", "risk_free_rate",
    "l2_penalty", "method", "long_only", "upper_bound", "lower_bound",
    "return_type",
)
# Parameters applied after optimisation (cheap to sweep).
DOWNSTREAM_KEYS = ("trades", "lf", "execution", "tot_capital")

DEFAULT_GRID: dict[str, list] = {
    "lb": [10, 20, 30],
    "trades": [3, 5, 10],
    "lf": [1, 3, 5],
    "execution": ["flat_legs", "hold", "delta"],
}


def _iter_combos(grid: dict[str, list], keys: tuple[str, ...], base: dict):
    active = [k for k in keys if k in grid]
    values = [grid[k] for k in active]
    for combo in product(*values) if active else [()]:
        yield {**{k: base[k] for k in keys if k in base and k not in active},
               **dict(zip(active, combo))}


def _weight_history_task(args):
    sig_params, opens, closes = args
    return sig_params, generate_weight_history(sig_params, opens, closes, verbose=False)


def run_sweep(
    data: dict[str, pd.DataFrame],
    base_params: dict,
    grid: dict[str, list] | None = None,
    max_workers: int | None = None,
    sort_by: str = "net_sharpe",
    verbose: bool = True,
) -> pd.DataFrame:
    """Grid sweep. Returns a leaderboard DataFrame, one row per combo."""
    grid = dict(DEFAULT_GRID if grid is None else grid)
    unknown = set(grid) - set(SIGNAL_KEYS) - set(DOWNSTREAM_KEYS)
    if unknown:
        raise ValueError(f"Unsweepable parameters: {sorted(unknown)}")
    if "foo" in base_params:
        raise ValueError("Sweeps need objectives by name: use params['objective'] "
                         f"(one of {list(_objective_names())}), not a 'foo' callable.")

    opens, closes = align_universe(data, verbose=verbose)
    signal_combos = list(_iter_combos(grid, SIGNAL_KEYS, base_params))
    if verbose:
        n_down = 1
        for k in DOWNSTREAM_KEYS:
            n_down *= len(grid.get(k, [None]))
        print(f"{len(signal_combos)} weight histories x {n_down} downstream variants")

    tasks = [({**base_params, **combo}, opens, closes) for combo in signal_combos]
    histories: list[tuple[dict, pd.DataFrame]] = []
    if max_workers and max_workers > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            for i, result in enumerate(pool.map(_weight_history_task, tasks), 1):
                histories.append(result)
                if verbose:
                    print(f"  weight history {i}/{len(tasks)} done")
    else:
        for i, task in enumerate(tasks, 1):
            histories.append(_weight_history_task(task))
            if verbose:
                print(f"  weight history {i}/{len(tasks)} done")

    rows = []
    for sig_params, weights_df in histories:
        for down in _iter_combos(grid, DOWNSTREAM_KEYS, base_params):
            params = {**sig_params, **down}
            signal_df = signals_from_weights(
                weights_df, params.get("trades", 5), params.get("long_only", 1))
            if signal_df.empty:
                continue
            daily, metrics = backtest_daily_from_signals(params, signal_df, opens, closes)
            gross = metrics.loc["gross"]
            net = metrics.loc["net (discount broker)"]
            net2 = metrics.loc["net (full-service broker)"]
            defaults = {"execution": "flat_legs", "objective": "lagrangian",
                        "rmean": "sma", "frequency_type": "close to open", "trades": 5}
            display_keys = ["lb", "trades", "lf", "execution", "objective", "rmean",
                            "frequency_type"]
            # every swept dimension must be visible in the leaderboard
            display_keys += [k for k in grid if k not in display_keys and k != "tot_capital"]
            rows.append({
                **{k: params.get(k, defaults.get(k)) for k in display_keys},
                "gross_pct": gross["total_return_pct"],
                "gross_sharpe": gross["sharpe"],
                "net_pct": net["total_return_pct"],
                "net_sharpe": net["sharpe"],
                "net_sortino": net["sortino"],
                "net_maxdd_pct": net["max_drawdown_pct"],
                "net_win_days_pct": net["winning_days_pct"],
                "net2_pct": net2["total_return_pct"],
                "days": len(daily),
            })
    leaderboard = pd.DataFrame(rows)
    if not leaderboard.empty:
        leaderboard = leaderboard.sort_values(sort_by, ascending=False).reset_index(drop=True)
    return leaderboard


def _objective_names():
    from .btst_lagrangian import OBJECTIVES
    return OBJECTIVES.keys()
