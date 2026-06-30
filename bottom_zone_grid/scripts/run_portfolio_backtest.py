#!/usr/bin/env python3
"""
Portfolio-level rolling backtest for bottom-zone grid strategy.

Simulates: universe refresh → scan → entry → grid trade → exit → capital rotation.
No look-ahead bias: all data sliced to simulation date.

Usage:
    python -m bottom_zone_grid.scripts.run_portfolio_backtest \
        --config bottom_zone_grid/config/strategy_config.json --days 60
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.broker.fyers_client import FyersClient
from common.broker.auth_json import get_fyers_creds_from_json

from bottom_zone_grid.data.universe import fetch_index_constituents, apply_basic_filters
from bottom_zone_grid.data.historical import fetch_bulk_daily_ohlcv, fetch_bulk_intraday_ohlcv
from bottom_zone_grid.backtest.portfolio_sim import PortfolioSimulator, PortfolioResult
from bottom_zone_grid.storage.json_store import atomic_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("bzg.portbt")


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def build_broker(config: dict, config_path: str) -> FyersClient:
    bcfg = config.get("broker", {})
    base_dir = os.path.dirname(os.path.abspath(config_path))
    auth_file = bcfg.get("auth_file", "../../fyers_auth.json")
    if not os.path.isabs(auth_file):
        auth_file = os.path.normpath(os.path.join(base_dir, auth_file))
    user_key = bcfg.get("user_key", "user1")
    client_id, access_token = get_fyers_creds_from_json(auth_file, user_key=user_key)
    return FyersClient(client_id=client_id, access_token=access_token, log_path="")


def print_result(r: PortfolioResult):
    print(f"\n{'='*120}")
    print(f"  PORTFOLIO BACKTEST — BOTTOM-ZONE GRID STRATEGY")
    print(f"{'='*120}")
    print(f"  Period:              {r.start_date} → {r.end_date} ({r.trading_days} trading days)")
    print(f"  Total capital:       ₹{r.total_capital:,.0f}")
    print(f"  Capital per slot:    ₹{r.capital_per_slot:,.0f}")
    print(f"  Max slots:           {r.max_slots}")
    print()
    print(f"  Net PnL:             ₹{r.net_pnl:,.2f}")
    print(f"  Gross PnL:           ₹{r.gross_pnl:,.2f}")
    print(f"  Total costs:         ₹{r.total_cost:,.2f}")
    print(f"  Return on capital:   {r.return_pct:.2f}%")
    print(f"  Annualized return:   {r.annualized_return_pct:.1f}%")
    print(f"  Max drawdown:        ₹{r.max_drawdown:,.0f} ({r.max_drawdown_pct:.2f}%)")
    print()
    print(f"  Total campaigns:     {r.total_campaigns}")
    print(f"  Total cycles:        {r.total_cycles}")
    print(f"  Avg cycles/day:      {r.avg_cycles_per_day:.1f}")
    print(f"  Avg slots used:      {r.avg_slots_used:.1f}")

    # Campaign details
    if r.campaigns:
        print(f"\n  {'—'*130}")
        print(f"  CAMPAIGN DETAILS ({len(r.campaigns)} campaigns)")
        print(f"  {'—'*130}")
        print(f"  {'#':>3}  {'Symbol':12s} {'Entry':>12s} {'Exit':>12s} {'Days':>5s} "
              f"{'GridCyc':>8s} {'GridPnL':>12s} {'ExitPnL':>10s} {'TotalPnL':>12s} "
              f"{'AvgBR':>8s} {'Gap':>8s} {'Qty':>5s} {'ExitReason':>25s}")
        print(f"  {'—'*3}  {'—'*12} {'—'*12} {'—'*12} {'—'*5} "
              f"{'—'*8} {'—'*12} {'—'*10} {'—'*12} "
              f"{'—'*8} {'—'*8} {'—'*5} {'—'*25}")

        for i, c in enumerate(r.campaigns):
            days = c.days_active
            grid_pnl = c.net_pnl - c.exit_pnl
            print(f"  {i+1:3d}  {c.symbol:12s} {c.entry_date:>12s} {(c.exit_date or ''):>12s} "
                  f"{days:5d} "
                  f"{c.grid_cycles:8d} ₹{grid_pnl:11,.2f} ₹{c.exit_pnl:9,.2f} ₹{c.net_pnl:11,.2f} "
                  f"{c.avg_bar_range:8.2f} {c.grid_gap:8.2f} {c.order_qty:5d} {(c.exit_reason or ''):>25s}")

        total_grid_pnl = sum(c.net_pnl - c.exit_pnl for c in r.campaigns)
        total_exit_pnl = sum(c.exit_pnl for c in r.campaigns)
        total_pnl = sum(c.net_pnl for c in r.campaigns)
        print(f"\n  Grid PnL: ₹{total_grid_pnl:,.2f}  |  Exit PnL: ₹{total_exit_pnl:,.2f}  |  Total: ₹{total_pnl:,.2f}")

    # Daily equity snapshot (every 5 days)
    if r.daily_equity:
        print(f"\n  {'—'*90}")
        print(f"  DAILY EQUITY (sampled)")
        print(f"  {'—'*90}")
        print(f"  {'Date':>12s} {'Equity':>14s} {'Slots':>6s} {'DayCycles':>10s} {'Active':>30s}")
        print(f"  {'—'*12} {'—'*14} {'—'*6} {'—'*10} {'—'*30}")

        step = max(1, len(r.daily_equity) // 20)
        for i in range(0, len(r.daily_equity), step):
            d = r.daily_equity[i]
            print(f"  {d['date']:>12s} ₹{d['equity']:13,.0f} {d['active_slots']:6d} "
                  f"{d['day_cycles']:10d} {d.get('active_symbols', ''):>30s}")
        if len(r.daily_equity) % step != 0:
            d = r.daily_equity[-1]
            print(f"  {d['date']:>12s} ₹{d['equity']:13,.0f} {d['active_slots']:6d} "
                  f"{d['day_cycles']:10d} {d.get('active_symbols', ''):>30s}")

    print(f"\n{'='*120}")


def save_result(result: PortfolioResult, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    summary = {
        "start_date": result.start_date,
        "end_date": result.end_date,
        "trading_days": result.trading_days,
        "total_capital": result.total_capital,
        "capital_per_slot": result.capital_per_slot,
        "max_slots": result.max_slots,
        "total_campaigns": result.total_campaigns,
        "total_cycles": result.total_cycles,
        "gross_pnl": result.gross_pnl,
        "total_cost": result.total_cost,
        "net_pnl": result.net_pnl,
        "return_pct": result.return_pct,
        "annualized_return_pct": result.annualized_return_pct,
        "max_drawdown": result.max_drawdown,
        "max_drawdown_pct": result.max_drawdown_pct,
        "avg_slots_used": result.avg_slots_used,
        "avg_cycles_per_day": result.avg_cycles_per_day,
    }
    atomic_write(os.path.join(output_dir, f"portfolio_{timestamp}_summary.json"), summary, backup=False)

    if result.campaigns:
        campaigns = []
        for c in result.campaigns:
            days = c.days_active.days if hasattr(c.days_active, 'days') else c.days_active
            campaigns.append({
                "symbol": c.symbol, "entry_date": c.entry_date,
                "exit_date": c.exit_date, "exit_reason": c.exit_reason,
                "days_active": days, "range_low": c.range_low,
                "range_high": c.range_high, "avg_bar_range": c.avg_bar_range,
                "grid_gap": c.grid_gap,
                "order_qty": c.order_qty, "capital": c.capital_allocated,
                "grid_cycles": c.grid_cycles, "exit_cycles": c.exit_cycles,
                "gross_pnl": c.gross_pnl,
                "total_cost": c.total_cost, "net_pnl": c.net_pnl,
                "exit_pnl": c.exit_pnl, "max_drawdown": c.max_drawdown,
            })
        with open(os.path.join(output_dir, f"portfolio_{timestamp}_campaigns.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(campaigns[0].keys()))
            writer.writeheader()
            writer.writerows(campaigns)

    if result.daily_equity:
        with open(os.path.join(output_dir, f"portfolio_{timestamp}_equity.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(result.daily_equity[0].keys()))
            writer.writeheader()
            writer.writerows(result.daily_equity)

    LOG.info("Saved portfolio backtest to %s/portfolio_%s_*", output_dir, timestamp)


def main():
    ap = argparse.ArgumentParser(description="Portfolio Backtest — Bottom-Zone Grid")
    ap.add_argument("--config", required=True, help="Path to strategy_config.json")
    ap.add_argument("--days", type=int, default=60, help="Simulation days (default: 60)")
    ap.add_argument("--output-dir", default=None, help="Override output directory")
    args = ap.parse_args()

    config = load_config(args.config)
    base_dir = os.path.dirname(os.path.abspath(args.config))
    output_dir = args.output_dir or os.path.join(base_dir, "..", "strategy_data", "backtest")

    brcfg = config.get("bar_range", {})
    primary_tf = brcfg.get("primary_timeframe", "15")
    bar_range_warmup = brcfg.get("intraday_lookback_days", 10)

    # 1. Fetch universe
    source = config.get("universe", {}).get("source", "nifty500")
    LOG.info("Fetching universe: %s", source)
    raw_universe = fetch_index_constituents(source)

    # 2. Build broker
    broker = build_broker(config, args.config)

    # 3. Fetch daily OHLCV (range lookback + sim days)
    range_lookback = max(
        config.get("range", {}).get("lookback_days", 60),
        config.get("range", {}).get("alternative_lookback_days", 120),
    )
    daily_lookback = range_lookback + args.days + 30
    fyers_symbols = [s["fyers_symbol"] for s in raw_universe]
    LOG.info("Fetching %d-day daily OHLCV for %d symbols...", daily_lookback, len(fyers_symbols))
    daily_data = fetch_bulk_daily_ohlcv(broker, fyers_symbols, lookback_days=daily_lookback, throttle_seconds=0.3)

    # 4. Basic filters
    filtered = apply_basic_filters(raw_universe, daily_data, config)
    filtered_syms = [s["fyers_symbol"] for s in filtered]
    LOG.info("Filtered to %d symbols", len(filtered_syms))

    # 5. Fetch intraday candles (warmup + sim days)
    intraday_lookback = bar_range_warmup + args.days + 10
    LOG.info("Fetching %smin candles for %d symbols (%d days)...",
             primary_tf, len(filtered_syms), intraday_lookback)
    intraday_data = fetch_bulk_intraday_ohlcv(
        broker, filtered_syms,
        resolution=primary_tf,
        lookback_days=intraday_lookback,
        throttle_seconds=0.3,
    )

    # 6. Run portfolio simulation
    LOG.info("Starting portfolio simulation (%d days)...", args.days)
    sim = PortfolioSimulator(
        config=config,
        universe=filtered,
        daily_data=daily_data,
        intraday_data=intraday_data,
    )
    result = sim.run()

    # 7. Print and save
    print_result(result)
    save_result(result, output_dir)


if __name__ == "__main__":
    main()
