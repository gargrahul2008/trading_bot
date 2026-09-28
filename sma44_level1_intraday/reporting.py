"""Trade journal CSV + summary-stats reporting for the 44-SMA scanner strategy."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _max_drawdown(trades: pd.DataFrame) -> tuple[float, float]:
    """Return (max_drawdown_amount, max_drawdown_pct_of_peak_equity) from the
    running equity curve built by sorting trades on exit_time."""
    if trades.empty:
        return 0.0, 0.0
    ordered = trades.sort_values("exit_time")
    equity_curve = ordered["net_pnl"].cumsum()
    running_peak = equity_curve.cummax()
    drawdown = running_peak - equity_curve
    max_dd = float(drawdown.max())
    peak_at_max_dd = float(running_peak.loc[drawdown.idxmax()]) if max_dd > 0 else 0.0
    max_dd_pct = (max_dd / peak_at_max_dd * 100.0) if peak_at_max_dd > 0 else 0.0
    return max_dd, max_dd_pct


def _side_performance(trades: pd.DataFrame, direction: str) -> dict[str, float]:
    side = trades[trades["direction"] == direction]
    wins = side[side["net_pnl"] > 0]
    return {
        "trades": int(len(side)),
        "win_rate": float(len(wins) / len(side) * 100.0) if len(side) else 0.0,
        "net_pnl": float(side["net_pnl"].sum()) if len(side) else 0.0,
        "avg_r_multiple": float(side["r_multiple"].mean()) if len(side) else 0.0,
    }


def build_summary(trades: pd.DataFrame) -> dict[str, object]:
    if trades.empty:
        return {
            "total_trades": 0, "winning_trades": 0, "losing_trades": 0, "win_rate": 0.0,
            "gross_pnl": 0.0, "net_pnl": 0.0, "average_pnl": 0.0, "average_r_multiple": 0.0,
            "max_drawdown": 0.0, "max_drawdown_pct": 0.0, "profit_factor": 0.0,
            "trades_per_symbol": {}, "trades_per_day": {},
            "long_performance": _side_performance(trades, "LONG"),
            "short_performance": _side_performance(trades, "SHORT"),
        }

    wins = trades[trades["net_pnl"] > 0]
    losses = trades[trades["net_pnl"] < 0]
    gross_wins = float(wins["net_pnl"].sum())
    gross_losses = float(losses["net_pnl"].sum())
    max_dd, max_dd_pct = _max_drawdown(trades)

    trades_per_symbol = trades.groupby("symbol").size().to_dict()
    trades_per_day = trades.groupby("trade_date").size()
    trades_per_day.index = trades_per_day.index.astype(str)

    return {
        "total_trades": int(len(trades)),
        "winning_trades": int(len(wins)),
        "losing_trades": int(len(losses)),
        "win_rate": float(len(wins) / len(trades) * 100.0),
        "gross_pnl": float(trades["gross_pnl"].sum()),
        "net_pnl": float(trades["net_pnl"].sum()),
        "average_pnl": float(trades["net_pnl"].mean()),
        "average_r_multiple": float(trades["r_multiple"].mean()),
        "max_drawdown": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "profit_factor": float(gross_wins / abs(gross_losses)) if gross_losses < 0 else float("inf"),
        "trades_per_symbol": {str(k): int(v) for k, v in trades_per_symbol.items()},
        "trades_per_day": trades_per_day.to_dict(),
        "long_performance": _side_performance(trades, "LONG"),
        "short_performance": _side_performance(trades, "SHORT"),
    }


def write_outputs(
    trades: pd.DataFrame,
    output_dir: str | Path,
    *,
    rejected: pd.DataFrame | None = None,
) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades_path = out_dir / "trade_journal.csv"
    trades.to_csv(trades_path, index=False)

    summary = build_summary(trades)
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2, default=str)

    paths = {"trade_journal": str(trades_path), "summary": str(summary_path)}

    if rejected is not None and not rejected.empty:
        rejected_path = out_dir / "rejected_signals.csv"
        rejected.to_csv(rejected_path, index=False)
        paths["rejected_signals"] = str(rejected_path)

    return paths
