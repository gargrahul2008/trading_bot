#!/usr/bin/env python3
"""
Bottom-zone grid backtester.

Usage:
    # Backtest a specific stock:
    python -m bottom_zone_grid.scripts.run_backtest --config bottom_zone_grid/config/strategy_config.json --symbol TCS

    # Backtest all TRADE_READY from latest scan:
    python -m bottom_zone_grid.scripts.run_backtest --config bottom_zone_grid/config/strategy_config.json --from-scan

    # Backtest with custom lookback:
    python -m bottom_zone_grid.scripts.run_backtest --config bottom_zone_grid/config/strategy_config.json --symbol TCS --days 30
"""

import argparse
import csv
import json
import logging
import math
import os
import sys
from dataclasses import asdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.broker.fyers_client import FyersClient
from common.broker.auth_json import get_fyers_creds_from_json

from bottom_zone_grid.data.historical import (
    fetch_daily_ohlcv,
    fetch_intraday_ohlcv,
    compute_avg_bar_range,
)
from bottom_zone_grid.scanner.range_calculator import validate_range
from bottom_zone_grid.scanner.grid_calculator import (
    calculate_grid_gap_from_bar_range,
    _derive_order_qty_from_capital,
    build_grid_ladder,
    round_to_tick,
)
from bottom_zone_grid.backtest.engine import GridBacktester, BacktestResult
from bottom_zone_grid.storage.json_store import atomic_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("bzg.bt")


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


def resolve_fyers_symbol(symbol: str) -> str:
    sym = symbol.upper().strip()
    if sym.startswith("NSE:"):
        return sym
    return f"NSE:{sym}-EQ"


def backtest_stock(
    broker,
    fyers_symbol: str,
    config: dict,
    bt_days: int = 60,
) -> BacktestResult:
    """Run full backtest for one stock."""

    nse_symbol = fyers_symbol.replace("NSE:", "").replace("-EQ", "")
    brcfg = config.get("bar_range", {})
    primary_tf = brcfg.get("primary_timeframe", "15")
    gap_multiplier = brcfg.get("gap_multiplier", 0.75)
    capital_per_slot = config.get("portfolio", {}).get("capital_per_slot", 1000000)
    reserve_buffer_pct = config.get("grid", {}).get("reserve_charges_buffer_pct", 0.25)
    max_grids_to_bottom = config.get("grid", {}).get("max_grids_to_bottom", 5)

    # 1. Fetch daily OHLCV for range detection
    lookback = max(
        config.get("range", {}).get("lookback_days", 60),
        config.get("range", {}).get("alternative_lookback_days", 120),
    )
    LOG.info("[%s] Fetching %d-day daily OHLCV...", nse_symbol, lookback)
    daily_df = fetch_daily_ohlcv(broker, fyers_symbol, lookback_days=lookback)
    if daily_df.empty:
        raise ValueError(f"No daily data for {fyers_symbol}")

    # 2. Validate range
    range_info = validate_range(daily_df, config)
    if not range_info.valid:
        LOG.warning("[%s] Range invalid: %s — proceeding anyway for backtest", nse_symbol, range_info.reject_reason)
        if range_info.low <= 0 or range_info.high <= range_info.low:
            raise ValueError(f"Cannot backtest {nse_symbol}: {range_info.reject_reason}")

    LOG.info("[%s] Range: %.2f - %.2f (%.1f%%)", nse_symbol,
             range_info.low, range_info.high, range_info.width_pct)

    # 3. Fetch intraday bar range for gap calculation
    intraday_lookback = brcfg.get("intraday_lookback_days", 10)
    LOG.info("[%s] Fetching %smin candles for bar range (%d days)...", nse_symbol, primary_tf, intraday_lookback)
    bar_range_df = fetch_intraday_ohlcv(broker, fyers_symbol, resolution=primary_tf, lookback_days=intraday_lookback)
    avg_br = compute_avg_bar_range(bar_range_df)
    if not avg_br:
        raise ValueError(f"No intraday bar range data for {fyers_symbol}")

    # Also get 5min bar range for display
    bar_range_5m_df = fetch_intraday_ohlcv(broker, fyers_symbol, resolution="5", lookback_days=intraday_lookback)
    avg_br_5m = compute_avg_bar_range(bar_range_5m_df)

    LOG.info("[%s] Avg %smin bar range: %.2f, gap: %.2f", nse_symbol, primary_tf, avg_br,
             calculate_grid_gap_from_bar_range(avg_br, gap_multiplier))

    # 4. Compute grid parameters
    grid_gap = calculate_grid_gap_from_bar_range(avg_br, gap_multiplier)
    if grid_gap <= 0:
        raise ValueError(f"Grid gap is zero for {fyers_symbol}")

    ladder = build_grid_ladder(range_info.low, range_info.high, grid_gap)
    cmp = float(daily_df["close"].iloc[-1])
    grids_to_bottom = min(math.ceil((cmp - range_info.low) / grid_gap), max_grids_to_bottom)

    order_qty = _derive_order_qty_from_capital(
        cmp, grids_to_bottom, ladder, capital_per_slot, reserve_buffer_pct,
    )

    LOG.info("[%s] Grid: gap=%.2f, levels=%d, g2b=%d, qty=%d, order_value=₹%.0f",
             nse_symbol, grid_gap, len(ladder), grids_to_bottom, order_qty, order_qty * cmp)

    # 5. Fetch backtest candles (15min for simulation)
    LOG.info("[%s] Fetching %smin candles for %d-day backtest...", nse_symbol, primary_tf, bt_days)
    bt_df = fetch_intraday_ohlcv(broker, fyers_symbol, resolution=primary_tf, lookback_days=bt_days)
    if bt_df.empty:
        raise ValueError(f"No intraday backtest data for {fyers_symbol}")

    candles = bt_df.to_dict("records")
    LOG.info("[%s] Got %d candles for backtest", nse_symbol, len(candles))

    # 6. Run backtest
    engine = GridBacktester(
        range_low=range_info.low,
        range_high=range_info.high,
        grid_gap=grid_gap,
        order_qty=order_qty,
        capital_per_slot=capital_per_slot,
        config=config,
    )
    result = engine.run(candles)
    result.symbol = nse_symbol
    result.avg_bar_range_15m = round(avg_br, 4)
    result.avg_bar_range_5m = round(avg_br_5m, 4) if avg_br_5m else None

    return result


def print_result(r: BacktestResult):
    """Pretty-print a single backtest result."""
    win_rate = (r.winning_cycles / r.total_cycles * 100) if r.total_cycles > 0 else 0

    print(f"\n{'='*100}")
    print(f"  BACKTEST RESULT: {r.symbol}")
    print(f"{'='*100}")
    print(f"  Period:            {r.start_date} → {r.end_date} ({r.trading_days} trading days, {r.total_candles} candles)")
    print(f"  Range:             ₹{r.range_low:.2f} — ₹{r.range_high:.2f}")
    print(f"  Grid gap:          ₹{r.grid_gap:.2f} ({r.total_levels} levels)")
    print(f"  Order qty:         {r.order_qty} shares (₹{r.order_qty * r.range_low:,.0f} per order)")
    print(f"  Bar range (15m):   ₹{r.avg_bar_range_15m:.2f}")
    if r.avg_bar_range_5m:
        print(f"  Bar range (5m):    ₹{r.avg_bar_range_5m:.2f}")
    print()
    print(f"  Capital:           ₹{r.capital_per_slot:,.0f}")
    print(f"  Gross PnL:         ₹{r.gross_pnl:,.2f}")
    print(f"  Total costs:       ₹{r.total_cost:,.2f}")
    print(f"  Net PnL:           ₹{r.net_pnl:,.2f}")
    print(f"  Return on capital: {r.return_on_capital_pct:.2f}%")
    print(f"  Annualized return: {r.annualized_return_pct:.1f}%")
    print()
    print(f"  Total cycles:      {r.total_cycles}")
    print(f"  Winning cycles:    {r.winning_cycles} ({win_rate:.1f}%)")
    print(f"  Losing cycles:     {r.losing_cycles}")
    print(f"  Avg cycle PnL:     ₹{r.avg_cycle_pnl:.2f}")
    print(f"  Cycles per day:    {r.cycles_per_day:.1f}")
    print()
    print(f"  Total buys:        {r.total_buys}")
    print(f"  Total sells:       {r.total_sells}")
    print(f"  Max inventory:     {r.max_inventory_qty} shares (₹{r.max_inventory_value:,.0f})")
    print(f"  Avg inventory:     ₹{r.avg_inventory_value:,.0f}")
    print()
    print(f"  Max drawdown:      ₹{r.max_drawdown:,.0f} ({r.max_drawdown_pct:.2f}%)")
    print(f"{'='*100}")

    # Print last 20 cycles
    if r.cycles:
        n = min(len(r.cycles), 20)
        print(f"\n  Last {n} cycles:")
        print(f"  {'#':>3}  {'Buy@':>10}  {'Sell@':>10}  {'Gross':>10}  {'Cost':>8}  {'Net':>10}  {'BuyTime':>20}  {'SellTime':>20}")
        print(f"  {'—'*3}  {'—'*10}  {'—'*10}  {'—'*10}  {'—'*8}  {'—'*10}  {'—'*20}  {'—'*20}")
        for i, cy in enumerate(r.cycles[-n:]):
            idx = len(r.cycles) - n + i + 1
            print(f"  {idx:3d}  {cy.buy_trade.price:10.2f}  {cy.sell_trade.price:10.2f}  "
                  f"{cy.gross_pnl:10.2f}  {cy.cost:8.2f}  {cy.net_pnl:10.2f}  "
                  f"{cy.buy_trade.timestamp[:19]:>20s}  {cy.sell_trade.timestamp[:19]:>20s}")
    print()


def save_result(result: BacktestResult, output_dir: str):
    """Save backtest result to files."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{result.symbol}_{timestamp}"

    summary = {
        "symbol": result.symbol,
        "range_low": result.range_low,
        "range_high": result.range_high,
        "grid_gap": result.grid_gap,
        "total_levels": result.total_levels,
        "order_qty": result.order_qty,
        "avg_bar_range_15m": result.avg_bar_range_15m,
        "avg_bar_range_5m": result.avg_bar_range_5m,
        "start_date": result.start_date,
        "end_date": result.end_date,
        "total_candles": result.total_candles,
        "trading_days": result.trading_days,
        "capital_per_slot": result.capital_per_slot,
        "total_cycles": result.total_cycles,
        "winning_cycles": result.winning_cycles,
        "losing_cycles": result.losing_cycles,
        "gross_pnl": result.gross_pnl,
        "total_cost": result.total_cost,
        "net_pnl": result.net_pnl,
        "return_on_capital_pct": result.return_on_capital_pct,
        "annualized_return_pct": result.annualized_return_pct,
        "avg_cycle_pnl": result.avg_cycle_pnl,
        "cycles_per_day": result.cycles_per_day,
        "total_buys": result.total_buys,
        "total_sells": result.total_sells,
        "max_inventory_qty": result.max_inventory_qty,
        "max_inventory_value": result.max_inventory_value,
        "avg_inventory_value": result.avg_inventory_value,
        "max_drawdown": result.max_drawdown,
        "max_drawdown_pct": result.max_drawdown_pct,
    }
    atomic_write(os.path.join(output_dir, f"{prefix}_summary.json"), summary, backup=False)

    # Cycles CSV
    if result.cycles:
        cycles_rows = []
        for i, cy in enumerate(result.cycles):
            cycles_rows.append({
                "cycle_num": i + 1,
                "buy_time": cy.buy_trade.timestamp,
                "buy_price": cy.buy_trade.price,
                "sell_time": cy.sell_trade.timestamp,
                "sell_price": cy.sell_trade.price,
                "qty": cy.buy_trade.qty,
                "gross_pnl": cy.gross_pnl,
                "cost": cy.cost,
                "net_pnl": cy.net_pnl,
            })
        with open(os.path.join(output_dir, f"{prefix}_cycles.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(cycles_rows[0].keys()))
            writer.writeheader()
            writer.writerows(cycles_rows)

    # Equity curve CSV
    if result.equity_curve:
        with open(os.path.join(output_dir, f"{prefix}_equity.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(result.equity_curve[0].keys()))
            writer.writeheader()
            writer.writerows(result.equity_curve)

    # Trades CSV
    if result.trades:
        trades_rows = []
        for t in result.trades:
            trades_rows.append({
                "timestamp": t.timestamp,
                "side": t.side,
                "price": t.price,
                "qty": t.qty,
                "grid_level": t.grid_level,
                "cost": t.cost,
                "pnl": t.pnl,
            })
        with open(os.path.join(output_dir, f"{prefix}_trades.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(trades_rows[0].keys()))
            writer.writeheader()
            writer.writerows(trades_rows)

    LOG.info("Saved backtest results to %s/%s_*", output_dir, prefix)


def main():
    ap = argparse.ArgumentParser(description="Bottom-Zone Grid Backtester")
    ap.add_argument("--config", required=True, help="Path to strategy_config.json")
    ap.add_argument("--symbol", help="NSE symbol to backtest (e.g. TCS, RELIANCE)")
    ap.add_argument("--from-scan", action="store_true", help="Backtest all TRADE_READY from latest scan")
    ap.add_argument("--days", type=int, default=60, help="Backtest lookback days (default: 60)")
    ap.add_argument("--output-dir", default=None, help="Override output directory")
    args = ap.parse_args()

    config = load_config(args.config)
    base_dir = os.path.dirname(os.path.abspath(args.config))
    output_dir = args.output_dir or os.path.join(base_dir, "..", "strategy_data", "backtest")
    os.makedirs(output_dir, exist_ok=True)

    broker = build_broker(config, args.config)

    symbols = []
    if args.symbol:
        symbols = [resolve_fyers_symbol(args.symbol)]
    elif args.from_scan:
        scan_dir = os.path.join(base_dir, "..", "strategy_data", "scanner")
        scan_file = os.path.join(scan_dir, "full_scan.json")
        if not os.path.exists(scan_file):
            LOG.error("No scan file found at %s. Run scanner first.", scan_file)
            sys.exit(1)
        with open(scan_file) as f:
            scan_data = json.load(f)
        symbols = [r["fyers_symbol"] for r in scan_data if r.get("final_status") == "TRADE_READY"]
        if not symbols:
            LOG.error("No TRADE_READY stocks in scan results.")
            sys.exit(1)
        LOG.info("Found %d TRADE_READY stocks: %s", len(symbols),
                 ", ".join(s.replace("NSE:", "").replace("-EQ", "") for s in symbols))
    else:
        LOG.error("Provide --symbol or --from-scan")
        sys.exit(1)

    all_results = []
    for fsym in symbols:
        try:
            result = backtest_stock(broker, fsym, config, bt_days=args.days)
            all_results.append(result)
            print_result(result)
            save_result(result, output_dir)
        except Exception as e:
            LOG.error("Backtest failed for %s: %s", fsym, e, exc_info=True)

    # Summary table if multiple stocks
    if len(all_results) > 1:
        print(f"\n{'='*120}")
        print("  PORTFOLIO BACKTEST SUMMARY")
        print(f"{'='*120}")
        print(f"  {'Symbol':15s} {'Days':>5s} {'Cycles':>7s} {'NetPnL':>12s} {'ROC%':>8s} {'Ann%':>8s} "
              f"{'WinRate':>8s} {'Cyc/Day':>8s} {'MaxDD':>12s} {'DD%':>6s}")
        print(f"  {'—'*15} {'—'*5} {'—'*7} {'—'*12} {'—'*8} {'—'*8} "
              f"{'—'*8} {'—'*8} {'—'*12} {'—'*6}")

        total_pnl = 0
        for r in all_results:
            wr = (r.winning_cycles / r.total_cycles * 100) if r.total_cycles > 0 else 0
            print(f"  {r.symbol:15s} {r.trading_days:5d} {r.total_cycles:7d} ₹{r.net_pnl:11,.2f} "
                  f"{r.return_on_capital_pct:7.2f}% {r.annualized_return_pct:7.1f}% "
                  f"{wr:7.1f}% {r.cycles_per_day:8.1f} ₹{r.max_drawdown:11,.0f} {r.max_drawdown_pct:5.1f}%")
            total_pnl += r.net_pnl

        total_capital = sum(r.capital_per_slot for r in all_results)
        total_roc = (total_pnl / total_capital * 100) if total_capital > 0 else 0
        print(f"\n  Total net PnL: ₹{total_pnl:,.2f} on ₹{total_capital:,.0f} capital ({total_roc:.2f}%)")
        print()


if __name__ == "__main__":
    main()
