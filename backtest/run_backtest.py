"""
Backtest runner for pct_ladder strategy.

Usage:
    python backtest/run_backtest.py \
        --config strategies/pct_ladder/config.mexc.json \
        --start 2022-01-01 \
        --end 2026-01-01 \
        --initial-cash 25000 \
        --initial-eth 5 \
        --interval 1m

Options:
    --config            Path to strategy JSON config
    --symbol            Override symbol (default: first symbol in config)
    --start             Start date YYYY-MM-DD  (default: 4 years ago)
    --end               End date YYYY-MM-DD    (default: today)
    --interval          Candle interval: 1m 5m 15m 1h 4h 1d  (default: 1m)
    --initial-cash      Starting USDC balance  (default: from config or 25000)
    --initial-eth       Starting ETH holding   (default: 0)
    --initial-eth-cost  Avg cost basis for initial ETH  (default: 0)
    --cache-dir         Directory to cache downloaded candles (default: backtest/cache)
    --save-trades       Save all simulated trades to CSV path (optional)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from backtest.fetch_data import fetch_klines, dt_to_ms
from backtest.engine import BacktestEngine
from strategies.pct_ladder.strategy import create_strategy


# ── helpers ───────────────────────────────────────────────────────────────────

def _dec(x) -> Decimal:
    return Decimal(str(x))


def _fmt(v: float, decimals: int = 2) -> str:
    return f"{v:,.{decimals}f}"


def _monthly_breakdown(trades: list, equity_curve: list) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()

    df = pd.DataFrame(trades)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["month"] = df["ts"].dt.to_period("M")

    ladder = df[~df["reason"].str.startswith("rebalance_")]
    buys   = ladder[ladder["side"] == "BUY"].groupby("month").size().rename("buys")
    sells  = ladder[ladder["side"] == "SELL"].groupby("month").size().rename("sells")

    # Realized PnL gained each month = diff in cumulative realized_pnl at last sell of month
    sells_df = ladder[ladder["side"] == "SELL"].copy()
    sells_df = sells_df.sort_values("ts")
    monthly_pnl = (
        sells_df.groupby("month")["realized_pnl_cumulative"]
        .last()
        .diff()
        .fillna(sells_df.groupby("month")["realized_pnl_cumulative"].last().iloc[0])
        .rename("realized_pnl")
    )

    monthly = pd.concat([buys, sells, monthly_pnl], axis=1).fillna(0)
    monthly["cycles"] = monthly[["buys", "sells"]].min(axis=1).astype(int)
    monthly["buys"]   = monthly["buys"].astype(int)
    monthly["sells"]  = monthly["sells"].astype(int)
    monthly["realized_pnl"] = monthly["realized_pnl"].round(2)
    monthly = monthly.reset_index()
    monthly["month"] = monthly["month"].astype(str)

    return monthly[["month", "buys", "sells", "cycles", "realized_pnl"]]


def _yearly_breakdown(monthly_df: pd.DataFrame) -> pd.DataFrame:
    if monthly_df.empty:
        return pd.DataFrame()
    df = monthly_df.copy()
    df["year"] = df["month"].str[:4]
    return (
        df.groupby("year")
        .agg(buys=("buys", "sum"), sells=("sells", "sum"),
             cycles=("cycles", "sum"), realized_pnl=("realized_pnl", "sum"))
        .reset_index()
        .assign(realized_pnl=lambda d: d["realized_pnl"].round(2))
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="pct_ladder backtester")
    p.add_argument("--config",            required=True)
    p.add_argument("--symbol",            default=None)
    p.add_argument("--start",             default=None)
    p.add_argument("--end",               default=None)
    p.add_argument("--interval",          default="1m")
    p.add_argument("--initial-cash",      type=float, default=None)
    p.add_argument("--initial-eth",       type=float, default=0.0)
    p.add_argument("--initial-eth-cost",  type=float, default=0.0)
    p.add_argument("--cache-dir",         default="backtest/cache")
    p.add_argument("--save-trades",       default=None)
    p.add_argument("--source",            default="binance",
                   help="Data source: 'binance' (default, 4yr history) or 'mexc'")
    p.add_argument("--symbol-fetch",      default=None,
                   help="Symbol to fetch from data source (e.g. ETHUSDT). "
                        "Defaults to config symbol, with USDC→USDT substitution for Binance.")
    return p.parse_args()


def main():
    args = parse_args()

    # ── Load config ────────────────────────────────────────────────────────────
    with open(args.config) as f:
        cfg = json.load(f)

    strategy_cfg = cfg.get("strategy", {})
    exec_cfg     = cfg.get("execution", {})

    symbol = args.symbol or strategy_cfg["symbols"][0]

    # Determine fetch symbol: Binance uses USDT pairs, MEXC uses USDC
    if args.symbol_fetch:
        symbol_fetch = args.symbol_fetch
    elif args.source == "binance":
        symbol_fetch = symbol.replace("USDC", "USDT")
    else:
        symbol_fetch = symbol

    quote_reserve = Decimal(str(exec_cfg.get("quote_reserve_usdt", exec_cfg.get("quote_reserve", 0)) or 0))
    use_buffer    = bool(exec_cfg.get("use_inventory_buffer", True))

    # Initial capital
    initial_cash = Decimal(str(
        args.initial_cash if args.initial_cash is not None
        else strategy_cfg.get("fixed_capital", 25000)
    ))
    initial_eth      = Decimal(str(args.initial_eth))
    initial_eth_cost = Decimal(str(args.initial_eth_cost))

    # Dates
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    four_years_ago = (datetime.now(timezone.utc) - timedelta(days=4*365)).strftime("%Y-%m-%d")
    start_str = args.start or four_years_ago
    end_str   = args.end   or today

    start_ms = dt_to_ms(start_str)
    end_ms   = dt_to_ms(end_str)

    print(f"\n{'='*60}")
    print(f"  pct_ladder Backtest")
    print(f"  Symbol   : {symbol}  (data: {args.source.upper()} {symbol_fetch})")
    print(f"  Period   : {start_str}  →  {end_str}")
    print(f"  Interval : {args.interval}")
    print(f"  Cash     : ${_fmt(float(initial_cash))}")
    print(f"  ETH      : {float(initial_eth)} @ ${float(initial_eth_cost):.2f}")
    print(f"  Config   : upper={strategy_cfg.get('upper_pct')}% / lower={strategy_cfg.get('lower_pct')}%  "
          f"quote=${strategy_cfg.get('buy_quote', strategy_cfg.get('buy_quote_usdt', '?'))}")
    rebal_thr = strategy_cfg.get("rebalance_threshold_steps", 0)
    if rebal_thr:
        print(f"  Rebalance: threshold={rebal_thr} steps → target={strategy_cfg.get('rebalance_target_steps', 8)} steps")
    print(f"{'='*60}\n")

    # ── Fetch data ─────────────────────────────────────────────────────────────
    df = fetch_klines(symbol_fetch, args.interval, start_ms, end_ms,
                      cache_dir=args.cache_dir, source=args.source)
    if df.empty:
        print("ERROR: No candle data returned.")
        sys.exit(1)

    candles = df.to_dict("records")
    print(f"\n[backtest] Running on {len(candles):,} candles...\n")

    # ── Build strategy ────────────────────────────────────────────────────────
    strategy = create_strategy(strategy_cfg)

    # ── Run engine ────────────────────────────────────────────────────────────
    engine = BacktestEngine(
        strategy=strategy,
        symbol=symbol,
        initial_cash=initial_cash,
        initial_eth=initial_eth,
        initial_eth_cost=initial_eth_cost,
        quote_reserve=quote_reserve,
        use_inventory_buffer=use_buffer,
    )

    summary = engine.run(candles)

    # ── Print results ─────────────────────────────────────────────────────────

    print(f"\n{'='*60}")
    print(f"  BACKTEST RESULTS  ({start_str} → {end_str})")
    print(f"{'='*60}")
    print(f"  Initial equity    : ${_fmt(summary['initial_equity'])}")
    print(f"  Final equity      : ${_fmt(summary['final_equity'])}")
    pnl_total = summary['final_equity'] - summary['initial_equity']
    pnl_pct   = (pnl_total / summary['initial_equity'] * 100) if summary['initial_equity'] else 0
    print(f"  Total PnL         : ${_fmt(pnl_total)}  ({pnl_pct:.1f}%)")
    print(f"  Realized PnL      : ${_fmt(summary['realized_pnl'])}  (ladder trades only)")
    print(f"  Max Drawdown      : {summary['max_drawdown_pct']:.2f}%")
    print(f"")
    print(f"  Total candles     : {len(candles):,}")
    print(f"  Ladder buys       : {summary['ladder_buys']:,}")
    print(f"  Ladder sells      : {summary['ladder_sells']:,}")
    print(f"  Cycles (min B/S)  : {summary['cycles']:,}")
    if summary['rebalance_trades']:
        print(f"  Rebalance trades  : {summary['rebalance_trades']:,}")
    # Annualised metrics
    days = (pd.Timestamp(end_str) - pd.Timestamp(start_str)).days
    if days > 0 and summary['initial_equity'] > 0:
        years = days / 365
        ann_return = ((summary['final_equity'] / summary['initial_equity']) ** (1 / years) - 1) * 100
        daily_cycles = summary['cycles'] / days
        print(f"")
        print(f"  Period (days)     : {days}")
        print(f"  Annualised return : {ann_return:.1f}%")
        print(f"  Cycles / day      : {daily_cycles:.2f}")
        print(f"  Avg PnL / cycle   : ${summary['realized_pnl'] / summary['cycles']:.2f}" if summary['cycles'] else "")
    print(f"{'='*60}\n")

    # Monthly table
    monthly_df = _monthly_breakdown(engine.trades, engine.equity_curve)
    if not monthly_df.empty:
        print("Monthly breakdown:")
        print(monthly_df.to_string(index=False))
        print()

    # Yearly table
    yearly_df = _yearly_breakdown(monthly_df)
    if not yearly_df.empty:
        print("Yearly summary:")
        print(yearly_df.to_string(index=False))
        print()

    # Optional trade CSV
    if args.save_trades:
        pd.DataFrame(engine.trades).to_csv(args.save_trades, index=False)
        print(f"[backtest] Trades saved to {args.save_trades}")


if __name__ == "__main__":
    main()
