#!/usr/bin/env python3
"""
Detailed scanner — evaluates ALL stocks with full pipeline and saves split output files.

Usage:
    python -m bottom_zone_grid.scripts.run_scanner_detail --config bottom_zone_grid/config/strategy_config.json
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
from bottom_zone_grid.data.historical import fetch_bulk_daily_ohlcv, get_bulk_ltp, fetch_bulk_intraday_bar_range
from bottom_zone_grid.scanner.daily_scanner import scan_all, split_results, build_scan_report
from bottom_zone_grid.storage.json_store import atomic_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("bzg.detail")


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


def save_csv(path: str, rows: list):
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_table(title: str, rows: list, max_rows: int = 30):
    if not rows:
        print(f"\n  {title}: None\n")
        return

    print(f"\n  {title} ({len(rows)} stocks):")
    hdr = (f"  {'#':>3}  {'Symbol':15s} {'CMP':>10s} {'RangeLow':>10s} {'RangeHigh':>10s} "
           f"{'Width%':>7s} {'Pos':>7s} {'Sup':>4s} {'Res':>4s} {'Cont%':>6s} "
           f"{'BR15m':>8s} {'BR5m':>8s} {'Gap':>8s} {'G2B':>4s} {'Lvls':>5s} "
           f"{'OrdQty':>7s} {'OrdVal':>9s} {'NetPft':>8s} {'Score':>6s} "
           f"{'EntryStatus':>22s} {'GridStatus':>22s} {'FinalStatus':>22s}")
    sep = (f"  {'—'*3}  {'—'*15} {'—'*10} {'—'*10} {'—'*10} "
           f"{'—'*7} {'—'*7} {'—'*4} {'—'*4} {'—'*6} "
           f"{'—'*8} {'—'*8} {'—'*8} {'—'*4} {'—'*5} "
           f"{'—'*7} {'—'*9} {'—'*8} {'—'*6} "
           f"{'—'*22} {'—'*22} {'—'*22}")
    print(hdr)
    print(sep)

    for i, r in enumerate(rows[:max_rows]):
        rp = r.get("range_position")
        rp_str = f"{rp:.1%}" if rp is not None else "N/A"
        br15 = r.get("avg_bar_range_15m")
        br5 = r.get("avg_bar_range_5m")
        br15_str = f"{br15:.2f}" if br15 else "N/A"
        br5_str = f"{br5:.2f}" if br5 else "N/A"
        print(
            f"  {i+1:3d}  {r['symbol']:15s} {r['cmp']:10.2f} {r['range_low']:10.2f} {r['range_high']:10.2f} "
            f"{r['range_width_pct']:6.1f}% {rp_str:>7s} {r['support_touches']:4d} {r['resistance_touches']:4d} {r['containment_pct']:5.1f}% "
            f"{br15_str:>8s} {br5_str:>8s} {r['grid_gap_pts']:8.2f} {r['grids_to_bottom']:4d} {r['total_grid_levels']:5d} "
            f"{r['order_qty']:7d} {r['order_value']:9.0f} {r['net_cycle_profit']:8.0f} {r.get('candidate_score', 0):6.1f} "
            f"{r['entry_status']:>22s} {r['grid_status']:>22s} {r['final_status']:>22s}"
        )

    if len(rows) > max_rows:
        print(f"\n  ... and {len(rows) - max_rows} more (see CSV)")


def main():
    ap = argparse.ArgumentParser(description="Bottom-Zone Grid Scanner — Detailed")
    ap.add_argument("--config", required=True, help="Path to strategy_config.json")
    ap.add_argument("--output-dir", default=None, help="Override output directory")
    args = ap.parse_args()

    config = load_config(args.config)
    base_dir = os.path.dirname(os.path.abspath(args.config))
    output_dir = args.output_dir or os.path.join(base_dir, "..", "strategy_data", "scanner")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "history"), exist_ok=True)

    # 1. Fetch universe
    source = config.get("universe", {}).get("source", "nifty500")
    LOG.info("Fetching universe: %s", source)
    raw_universe = fetch_index_constituents(source)

    # 2. Build broker
    LOG.info("Connecting to Fyers...")
    broker = build_broker(config, args.config)

    # 3. Fetch daily OHLCV
    lookback = max(
        config.get("range", {}).get("lookback_days", 60),
        config.get("range", {}).get("alternative_lookback_days", 120),
    )
    fyers_symbols = [s["fyers_symbol"] for s in raw_universe]
    LOG.info("Fetching %d-day daily OHLCV for %d symbols...", lookback, len(fyers_symbols))
    ohlcv_data = fetch_bulk_daily_ohlcv(broker, fyers_symbols, lookback_days=lookback, throttle_seconds=0.3)

    # 4. Basic filters
    filtered = apply_basic_filters(raw_universe, ohlcv_data, config)

    # 5. LTPs
    filtered_syms = [s["fyers_symbol"] for s in filtered]
    LOG.info("Fetching LTPs for %d symbols...", len(filtered_syms))
    ltp_prices = get_bulk_ltp(broker, filtered_syms)

    # 6. Fetch intraday bar range (5min + 15min)
    brcfg = config.get("bar_range", {})
    intraday_lookback = brcfg.get("intraday_lookback_days", 10)
    timeframes = brcfg.get("timeframes", ["15", "5"])
    LOG.info("Fetching intraday bar range (%s min) for %d symbols (%d days)...",
             "/".join(timeframes), len(filtered_syms), intraday_lookback)
    bar_range_all = fetch_bulk_intraday_bar_range(
        broker, filtered_syms,
        resolutions=timeframes,
        lookback_days=intraday_lookback,
        throttle_seconds=0.3,
    )

    # 7. Full scan
    LOG.info("Running full pipeline on %d symbols...", len(filtered))
    rows = scan_all(filtered, ohlcv_data, ltp_prices, config, bar_range_all=bar_range_all)

    # 8. Split results
    splits = split_results(rows)
    report = build_scan_report(rows, config)

    # 9. Save files
    datestamp = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_csv(os.path.join(output_dir, "full_scan.csv"), rows)
    atomic_write(os.path.join(output_dir, "full_scan.json"), rows, backup=False)

    for name, subset in splits.items():
        if subset:
            save_csv(os.path.join(output_dir, f"{name}_{datestamp}.csv"), subset)
            atomic_write(os.path.join(output_dir, f"{name}_{datestamp}.json"), subset, backup=False)

    report["files_generated"] = [f"{k}_{datestamp}" for k, v in splits.items() if v]
    atomic_write(os.path.join(output_dir, "latest_scan.json"), report, backup=True)
    atomic_write(os.path.join(output_dir, "history", f"scan_{timestamp}.json"), report, backup=False)

    LOG.info("Saved all output files to %s", output_dir)

    # 10. Print summary
    print(f"\n{'='*200}")
    print(f"  BOTTOM-ZONE GRID SCANNER — {report['scan_date']} {report['scan_time']}")
    print(f"  Universe: {len(raw_universe)} → Filtered: {len(filtered)} → Analyzed: {report['total_analyzed']}")
    print(f"  Capital per slot: ₹{config.get('portfolio', {}).get('capital_per_slot', 1000000):,.0f}")
    print(f"  Grid gap = {brcfg.get('gap_multiplier', 0.75)*100:.0f}% of avg {brcfg.get('primary_timeframe', '15')}min bar range")
    print(f"{'='*200}")

    print("\n  Status breakdown:")
    for st, cnt in sorted(report["status_breakdown"].items(), key=lambda x: -x[1]):
        print(f"    {st:40s}: {cnt:4d}")

    print_table("TRADE READY", splits["trade_ready"])
    print_table("NEAR READY (almost viable / range watch)", splits["near_ready"])

    valid_range = [r for r in rows if r.get("range_position") is not None and r["range_valid"]]
    valid_range.sort(key=lambda r: r["range_position"])
    print_table("TOP 30 — Closest to bottom (valid range only)", valid_range[:30])

    if splits["range_watch"]:
        print_table("RANGE WATCH (width slightly above max, tracking)", splits["range_watch"])

    print(f"\n  Output files:")
    print(f"    Full scan CSV:  {os.path.join(output_dir, 'full_scan.csv')}")
    print(f"    Full scan JSON: {os.path.join(output_dir, 'full_scan.json')}")
    for name, subset in splits.items():
        if subset:
            print(f"    {name}: {os.path.join(output_dir, f'{name}_{datestamp}.csv')} ({len(subset)} rows)")
    print()


if __name__ == "__main__":
    main()
