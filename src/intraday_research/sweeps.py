from __future__ import annotations

from dataclasses import replace
from itertools import product
from typing import Iterable

import pandas as pd

from .backtester import BacktestConfig, IntradayBacktester
from .costs import TransactionCostModel
from .features import FeatureEngine
from .risk import RiskManager
from .strategies import OpeningRangeBreakoutStrategy, TrendPullbackStrategy, VWAPMeanReversionStrategy


DEFAULT_PARAMETER_GRID: dict[str, list[object]] = {
    "first_trade_time": ["09:20", "09:25", "09:30"],
    "last_entry_time": ["14:30", "14:45"],
    "max_trades_per_symbol_per_day": [2, 3, 4],
    "cooldown_minutes_after_exit": [10, 15, 30],
    "opening_range_minutes": [15, 30, 45],
    "orb_breakout_buffer_atr": [0.1, 0.2, 0.3],
    "orb_stop_atr_multiple": [0.75, 1.0, 1.25],
    "orb_target_atr_multiple": [1.0, 1.5, 2.0],
    "vwap_reversion_band_atr": [0.75, 1.0, 1.25, 1.5],
    "vwap_stop_atr_multiple": [0.75, 1.0, 1.25],
    "vwap_target_atr_multiple": [0.75, 1.0, 1.25],
    "pullback_touch_buffer_atr": [0.1],
    "pullback_min_depth_atr": [0.2],
    "pullback_stop_atr_multiple": [1.0],
    "pullback_target_atr_multiple": [1.5],
    "pullback_min_trend_gap_atr": [0.2],
    "pullback_min_reclaim_body_pct": [0.3],
    "pullback_min_bars": [2],
    "pullback_min_close_location_pct": [0.6],
    "pullback_min_rejection_wick_to_body_ratio": [1.5],
    "pullback_min_signal_score": [65.0],
    "pullback_volume_lookback": [5],
    "pullback_min_volume_expansion_ratio": [1.25],
    "pullback_swing_lookback": [5],
    "pullback_structural_stop_buffer_atr": [0.1],
    "pullback_max_consumed_target_pct": [0.4],
    "pullback_require_vwap_alignment": [True],
    "pullback_require_break_of_prior_candle": [True],
    "pullback_allow_intrabar_rejection_entry": [True],
    "pullback_max_trades_per_side_per_day": [1],
}


def _iter_parameter_sets(grid: dict[str, list[object]]) -> Iterable[dict[str, object]]:
    keys = list(grid.keys())
    for values in product(*(grid[key] for key in keys)):
        yield dict(zip(keys, values))


def _compute_max_drawdown(net_pnl: pd.Series) -> float:
    if net_pnl.empty:
        return 0.0
    equity_curve = net_pnl.cumsum()
    running_peak = equity_curve.cummax()
    drawdown = equity_curve - running_peak
    return float(drawdown.min())


def _strategy_param(params: dict[str, object], key: str, default: object) -> object:
    return params.get(key, default)


def run_parameter_sweep(
    market_data: pd.DataFrame,
    *,
    base_config: BacktestConfig,
    grid: dict[str, list[object]] | None = None,
    strategy_modes: Iterable[str] = ("orb_only", "vwap_only", "combined", "trend_pullback_only", "orb_vwap_pullback"),
    risk_manager: RiskManager | None = None,
    cost_model: TransactionCostModel | None = None,
) -> pd.DataFrame:
    effective_grid = grid or DEFAULT_PARAMETER_GRID
    rows: list[dict[str, object]] = []

    for params in _iter_parameter_sets(effective_grid):
        for strategy_mode in strategy_modes:
            opening_range_minutes = int(params["opening_range_minutes"])
            orb = OpeningRangeBreakoutStrategy(
                opening_range_minutes=opening_range_minutes,
                breakout_buffer_atr=float(params["orb_breakout_buffer_atr"]),
                stop_atr_multiple=float(params["orb_stop_atr_multiple"]),
                target_atr_multiple=float(params["orb_target_atr_multiple"]),
            )
            vwap = VWAPMeanReversionStrategy(
                opening_range_minutes=opening_range_minutes,
                reversion_band_atr=float(params["vwap_reversion_band_atr"]),
                stop_atr_multiple=float(params["vwap_stop_atr_multiple"]),
                target_atr_multiple=float(params["vwap_target_atr_multiple"]),
            )
            trend_pullback = TrendPullbackStrategy(
                opening_range_minutes=opening_range_minutes,
                pullback_touch_buffer_atr=float(_strategy_param(params, "pullback_touch_buffer_atr", 0.1)),
                minimum_pullback_depth_atr=float(_strategy_param(params, "pullback_min_depth_atr", 0.2)),
                minimum_pullback_bars=int(_strategy_param(params, "pullback_min_bars", 2)),
                stop_atr_multiple=float(_strategy_param(params, "pullback_stop_atr_multiple", 1.0)),
                target_atr_multiple=float(_strategy_param(params, "pullback_target_atr_multiple", 1.5)),
                minimum_trend_gap_atr=float(_strategy_param(params, "pullback_min_trend_gap_atr", 0.2)),
                minimum_reclaim_body_pct=float(_strategy_param(params, "pullback_min_reclaim_body_pct", 0.3)),
                minimum_close_location_pct=float(_strategy_param(params, "pullback_min_close_location_pct", 0.6)),
                minimum_rejection_wick_to_body_ratio=float(
                    _strategy_param(params, "pullback_min_rejection_wick_to_body_ratio", 1.5)
                ),
                minimum_signal_score=float(_strategy_param(params, "pullback_min_signal_score", 65.0)),
                minimum_volume_expansion_ratio=float(
                    _strategy_param(params, "pullback_min_volume_expansion_ratio", 1.25)
                ),
                volume_expansion_lookback=int(_strategy_param(params, "pullback_volume_lookback", 5)),
                swing_lookback=int(_strategy_param(params, "pullback_swing_lookback", 5)),
                structural_stop_buffer_atr=float(
                    _strategy_param(params, "pullback_structural_stop_buffer_atr", 0.1)
                ),
                maximum_consumed_target_pct=float(
                    _strategy_param(params, "pullback_max_consumed_target_pct", 0.4)
                ),
                require_vwap_alignment=bool(_strategy_param(params, "pullback_require_vwap_alignment", True)),
                require_break_of_prior_candle=bool(
                    _strategy_param(params, "pullback_require_break_of_prior_candle", True)
                ),
                allow_intrabar_rejection_entry=bool(
                    _strategy_param(params, "pullback_allow_intrabar_rejection_entry", True)
                ),
                max_trades_per_side_per_day=int(
                    _strategy_param(params, "pullback_max_trades_per_side_per_day", 1)
                ),
            )

            if strategy_mode == "orb_only":
                strategies = [orb]
            elif strategy_mode == "vwap_only":
                strategies = [vwap]
            elif strategy_mode == "combined":
                strategies = [orb, vwap]
            elif strategy_mode == "trend_pullback_only":
                strategies = [trend_pullback]
            elif strategy_mode == "orb_vwap_pullback":
                strategies = [orb, vwap, trend_pullback]
            else:
                raise ValueError(f"Unsupported strategy mode: {strategy_mode}")

            engine = IntradayBacktester(
                strategies=strategies,
                feature_engine=FeatureEngine(opening_range_minutes=opening_range_minutes),
                risk_manager=replace(risk_manager, max_positions=risk_manager.max_positions) if risk_manager else RiskManager(),
                cost_model=cost_model or TransactionCostModel(),
            )
            config = replace(
                base_config,
                first_trade_time=pd.Timestamp(str(params["first_trade_time"])).time(),
                last_entry_time=pd.Timestamp(str(params["last_entry_time"])).time(),
                max_trades_per_symbol_per_day=int(params["max_trades_per_symbol_per_day"]),
                cooldown_minutes_after_exit=int(params["cooldown_minutes_after_exit"]),
            )
            result = engine.run(market_data, config)
            trades = result["trades"]

            if trades.empty:
                row = {
                    "strategy_mode": strategy_mode,
                    **params,
                    "trades": 0,
                    "gross_pnl": 0.0,
                    "net_pnl": 0.0,
                    "total_cost": 0.0,
                    "net_expectancy": 0.0,
                    "avg_cost_per_trade": 0.0,
                    "avg_gross_per_trade": 0.0,
                    "avg_cost_bps": 0.0,
                    "avg_gross_bps": 0.0,
                    "max_drawdown": 0.0,
                    "profit_factor": 0.0,
                    "gross_win_rate": 0.0,
                    "net_win_rate": 0.0,
                }
            else:
                gross_wins = trades.loc[trades["gross_pnl"] > 0, "gross_pnl"]
                gross_losses = trades.loc[trades["gross_pnl"] < 0, "gross_pnl"]
                net_wins = trades.loc[trades["net_pnl"] > 0, "net_pnl"]
                net_losses = trades.loc[trades["net_pnl"] < 0, "net_pnl"]
                profit_factor = float(net_wins.sum() / abs(net_losses.sum())) if not net_losses.empty else 0.0
                row = {
                    "strategy_mode": strategy_mode,
                    **params,
                    "trades": int(len(trades)),
                    "gross_pnl": float(trades["gross_pnl"].sum()),
                    "net_pnl": float(trades["net_pnl"].sum()),
                    "total_cost": float(trades["total_cost"].sum()),
                    "net_expectancy": float(trades["net_pnl"].mean()),
                    "avg_cost_per_trade": float(trades["total_cost"].mean()),
                    "avg_gross_per_trade": float(trades["gross_pnl"].mean()),
                    "avg_cost_bps": float(trades["cost_bps"].mean()),
                    "avg_gross_bps": float(trades["gross_bps"].mean()),
                    "max_drawdown": _compute_max_drawdown(trades["net_pnl"]),
                    "profit_factor": profit_factor,
                    "gross_win_rate": float((trades["gross_pnl"] > 0).mean() * 100.0),
                    "net_win_rate": float((trades["net_pnl"] > 0).mean() * 100.0),
                }
            rows.append(row)

    return pd.DataFrame(rows)
