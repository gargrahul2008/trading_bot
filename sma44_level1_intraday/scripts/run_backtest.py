#!/usr/bin/env python3
"""
run_backtest.py — Run the 44-SMA uptrend-pullback scanner strategy on any ticker(s).

Supports:
  • NSE equities  (data/fyers/NSE_<SYM>_EQ.parquet, auto-fetched from Fyers if missing)
  • Crypto        (data/binance/<SYM>_1m.parquet, auto-fetched from Binance if missing)

All strategy parameters come from a JSON config (see config/default_config.json).
Override the whole file with --config, or nothing for the shipped defaults.

Usage:
    # Indian equities, default 75min/5min config
    python -m sma44_level1_intraday.scripts.run_backtest --symbols RELIANCE HDFCBANK \\
        --start 2026-01-01 --end 2026-06-01

    # Crypto, with a custom config (e.g. continuous_session=true, 15min/1min)
    python -m sma44_level1_intraday.scripts.run_backtest --symbols BTCUSDT --crypto \\
        --config sma44_level1_intraday/config/crypto_config.json \\
        --start 2026-01-01 --end 2026-06-01
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(0, str(REPO_ROOT))

from sma44_level1_intraday.config.schema import StrategyConfig  # noqa: E402
from sma44_level1_intraday.data.loader import load_symbol_data  # noqa: E402
from sma44_level1_intraday.pipeline import run_backtest  # noqa: E402
from sma44_level1_intraday.reporting import build_summary, write_outputs  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="44-SMA uptrend-pullback scanner backtest")
    ap.add_argument("--symbols", nargs="+", required=True, help="Ticker name(s): RELIANCE, BTCUSDT, etc.")
    ap.add_argument("--crypto", action="store_true", help="Symbols are crypto (loads from data/binance/)")
    ap.add_argument("--start", default="2026-01-01", help="Start date YYYY-MM-DD")
    ap.add_argument("--end", default="2026-06-01", help="End date YYYY-MM-DD")
    ap.add_argument("--config", default=None, help="Path to a JSON config (default: shipped default_config.json)")
    ap.add_argument("--output-dir", default="artifacts/sma44_level1", help="Directory for trade_journal.csv / summary.json")
    args = ap.parse_args()

    config = StrategyConfig.from_json(args.config) if args.config else StrategyConfig.default()
    asset_type = "crypto" if args.crypto else None

    symbol_raw = {}
    for sym in args.symbols:
        print(f"Loading {sym} ({args.start} → {args.end}) ...")
        frame = load_symbol_data(sym, args.start, args.end, asset_type=asset_type, repo_root=REPO_ROOT, verbose=True)
        if frame.empty:
            print(f"  {sym}: no data in range — skipping")
            continue
        symbol_key = frame["symbol"].iloc[0]
        symbol_raw[symbol_key] = frame

    if not symbol_raw:
        print("No data loaded for any symbol — nothing to backtest.")
        return

    result = run_backtest(symbol_raw, config)
    trades = result["trades"]
    rejected = result["rejected"]

    paths = write_outputs(trades, args.output_dir, rejected=rejected)
    summary = build_summary(trades)

    print(f"\n{config.strategy_name}  {args.start} → {args.end}  symbols={list(symbol_raw)}")
    print(f"timeframe={config.timeframes.timeframe}")
    print("─" * 70)
    print(f"Total trades:     {summary['total_trades']}")
    print(f"Win rate:         {summary['win_rate']:.1f}%")
    print(f"Gross P&L:        {summary['gross_pnl']:,.2f}")
    print(f"Net P&L:          {summary['net_pnl']:,.2f}")
    print(f"Avg R multiple:   {summary['average_r_multiple']:.2f}")
    print(f"Profit factor:    {summary['profit_factor']:.2f}")
    print(f"Max drawdown:     {summary['max_drawdown']:,.2f} ({summary['max_drawdown_pct']:.1f}%)")
    print(f"\nWritten to: {paths}")


if __name__ == "__main__":
    main()
