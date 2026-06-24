from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def strategy_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["strategy_name", "trades", "gross_pnl", "net_pnl", "win_rate"])
    summary = (
        trades.assign(is_win=lambda df: df["net_pnl"] > 0)
        .groupby("strategy_name", as_index=False)
        .agg(
            trades=("symbol", "count"),
            gross_pnl=("gross_pnl", "sum"),
            net_pnl=("net_pnl", "sum"),
            win_rate=("is_win", "mean"),
        )
    )
    summary["win_rate"] = summary["win_rate"] * 100.0
    return summary


def symbol_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["symbol", "trades", "gross_pnl", "net_pnl", "win_rate"])
    summary = (
        trades.assign(is_win=lambda df: df["net_pnl"] > 0)
        .groupby("symbol", as_index=False)
        .agg(
            trades=("symbol", "count"),
            gross_pnl=("gross_pnl", "sum"),
            net_pnl=("net_pnl", "sum"),
            win_rate=("is_win", "mean"),
        )
    )
    summary["win_rate"] = summary["win_rate"] * 100.0
    return summary


def hourly_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["entry_hour", "trades", "gross_pnl", "net_pnl", "win_rate"])
    summary = (
        trades.assign(
            entry_hour=lambda df: pd.to_datetime(df["entry_time"]).dt.strftime("%H:00"),
            is_win=lambda df: df["net_pnl"] > 0,
        )
        .groupby("entry_hour", as_index=False)
        .agg(
            trades=("symbol", "count"),
            gross_pnl=("gross_pnl", "sum"),
            net_pnl=("net_pnl", "sum"),
            win_rate=("is_win", "mean"),
        )
    )
    summary["win_rate"] = summary["win_rate"] * 100.0
    return summary


def build_backtest_summary(trades: pd.DataFrame, daily_pnl: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "total_trades": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "win_rate": 0.0,
            "gross_win_rate": 0.0,
            "net_win_rate": 0.0,
            "average_win": 0.0,
            "average_loss": 0.0,
            "average_gross_pnl_per_trade": 0.0,
            "average_cost_per_trade": 0.0,
            "average_cost_bps": 0.0,
            "average_gross_edge_bps": 0.0,
            "profit_factor": 0.0,
            "expectancy_per_trade": 0.0,
            "max_drawdown": 0.0,
            "max_daily_loss": 0.0,
            "total_turnover": 0.0,
            "brokerage_as_pct_of_total_cost": 0.0,
            "slippage_as_pct_of_total_cost": 0.0,
            "best_day": None,
            "worst_day": None,
            "long_vs_short_performance": [],
            "strategy_wise_performance": [],
            "symbol_wise_performance": [],
            "time_of_day_performance": [],
        }

    net_pnl = trades["net_pnl"]
    gross_pnl = trades["gross_pnl"]
    wins = net_pnl[net_pnl > 0]
    losses = net_pnl[net_pnl < 0]
    equity_curve = net_pnl.cumsum()
    running_peak = equity_curve.cummax()
    drawdown = equity_curve - running_peak
    total_cost = trades["total_cost"].sum()
    total_turnover = trades["buy_turnover"].sum() + trades["sell_turnover"].sum()

    long_short = (
        trades.groupby("side", as_index=False)
        .agg(trades=("symbol", "count"), gross_pnl=("gross_pnl", "sum"), net_pnl=("net_pnl", "sum"))
        .to_dict("records")
    )
    best_day_row = daily_pnl.loc[daily_pnl["net_pnl"].idxmax()].to_dict()
    worst_day_row = daily_pnl.loc[daily_pnl["net_pnl"].idxmin()].to_dict()

    return {
        "total_trades": int(len(trades)),
        "gross_pnl": float(gross_pnl.sum()),
        "net_pnl": float(net_pnl.sum()),
        "win_rate": float((len(wins) / len(trades)) * 100.0),
        "gross_win_rate": float((trades["gross_pnl"] > 0).mean() * 100.0),
        "net_win_rate": float((trades["net_pnl"] > 0).mean() * 100.0),
        "average_win": float(wins.mean()) if not wins.empty else 0.0,
        "average_loss": float(losses.mean()) if not losses.empty else 0.0,
        "average_gross_pnl_per_trade": float(gross_pnl.mean()),
        "average_cost_per_trade": float(trades["total_cost"].mean()),
        "average_cost_bps": float(trades["cost_bps"].mean()),
        "average_gross_edge_bps": float(trades["gross_bps"].mean()),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if not losses.empty else 0.0,
        "expectancy_per_trade": float(net_pnl.mean()),
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "max_daily_loss": float(daily_pnl["net_pnl"].min()) if not daily_pnl.empty else 0.0,
        "total_turnover": float(total_turnover),
        "brokerage_as_pct_of_total_cost": float((trades["brokerage"].sum() / total_cost) * 100.0) if total_cost else 0.0,
        "slippage_as_pct_of_total_cost": float((trades["slippage"].sum() / total_cost) * 100.0) if total_cost else 0.0,
        "best_day": best_day_row,
        "worst_day": worst_day_row,
        "long_vs_short_performance": long_short,
        "strategy_wise_performance": strategy_summary(trades).to_dict("records"),
        "symbol_wise_performance": symbol_summary(trades).to_dict("records"),
        "time_of_day_performance": hourly_summary(trades).to_dict("records"),
    }


def write_backtest_outputs(
    trades: pd.DataFrame,
    daily_pnl: pd.DataFrame,
    output_dir: str | Path,
    rejected_trades: pd.DataFrame | None = None,
) -> dict[str, Path]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)

    trade_path = target / "trade_journal.csv"
    daily_path = target / "daily_pnl.csv"
    strategy_path = target / "strategy_summary.csv"
    symbol_path = target / "symbol_summary.csv"
    hourly_path = target / "hourly_summary.csv"
    summary_path = target / "backtest_summary.json"
    rejected_path = target / "rejected_trades.csv"

    trades.to_csv(trade_path, index=False)
    daily_pnl.to_csv(daily_path, index=False)
    strategy_summary(trades).to_csv(strategy_path, index=False)
    symbol_summary(trades).to_csv(symbol_path, index=False)
    hourly_summary(trades).to_csv(hourly_path, index=False)
    (rejected_trades if rejected_trades is not None else pd.DataFrame()).to_csv(rejected_path, index=False)
    summary = build_backtest_summary(trades, daily_pnl)
    summary_path.write_text(json.dumps(summary, default=str, indent=2))

    return {
        "trade_journal": trade_path,
        "daily_pnl": daily_path,
        "strategy_summary": strategy_path,
        "symbol_summary": symbol_path,
        "hourly_summary": hourly_path,
        "rejected_trades": rejected_path,
        "backtest_summary": summary_path,
    }
