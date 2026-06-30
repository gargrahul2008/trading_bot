#!/usr/bin/env python3
"""
Daily scanner for Bottom-Zone Grid Strategy.

Usage:
    python -m bottom_zone_grid.scripts.run_scanner --config bottom_zone_grid/config/strategy_config.json
    python -m bottom_zone_grid.scripts.run_scanner --config bottom_zone_grid/config/strategy_config.json --dry-run
"""

import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.broker.fyers_client import FyersClient
from common.broker.auth_json import get_fyers_creds_from_json

from bottom_zone_grid.data.universe import fetch_index_constituents, apply_basic_filters
from bottom_zone_grid.data.historical import fetch_bulk_daily_ohlcv, get_bulk_ltp
from bottom_zone_grid.scanner.daily_scanner import scan_candidates, build_scan_report
from bottom_zone_grid.storage.json_store import atomic_write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("bzg.main")


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def build_broker(config: dict) -> FyersClient:
    bcfg = config.get("broker", {})
    base_dir = os.path.dirname(os.path.abspath(args.config))
    auth_file = bcfg.get("auth_file", "../../fyers_auth.json")
    if not os.path.isabs(auth_file):
        auth_file = os.path.normpath(os.path.join(base_dir, auth_file))
    user_key = bcfg.get("user_key", "user1")
    client_id, access_token = get_fyers_creds_from_json(auth_file, user_key=user_key)
    return FyersClient(client_id=client_id, access_token=access_token, log_path="")


def main():
    global args
    ap = argparse.ArgumentParser(description="Bottom-Zone Grid Daily Scanner")
    ap.add_argument("--config", required=True, help="Path to strategy_config.json")
    ap.add_argument("--dry-run", action="store_true", help="Fetch universe only, skip OHLCV/scan")
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
    LOG.info("Universe: %d symbols from %s", len(raw_universe), source)

    if args.dry_run:
        print(f"\n--- DRY RUN: {len(raw_universe)} symbols in {source} ---")
        for s in raw_universe[:10]:
            print(f"  {s['fyers_symbol']:25s} {s['company']}")
        print(f"  ... and {len(raw_universe) - 10} more")
        return

    # 2. Build broker
    LOG.info("Connecting to Fyers...")
    broker = build_broker(config)

    # 3. Fetch historical OHLCV for all symbols
    lookback = max(
        config.get("range", {}).get("lookback_days", 60),
        config.get("range", {}).get("alternative_lookback_days", 120),
    )
    fyers_symbols = [s["fyers_symbol"] for s in raw_universe]
    LOG.info("Fetching %d-day OHLCV for %d symbols (this will take a few minutes)...", lookback, len(fyers_symbols))
    ohlcv_data = fetch_bulk_daily_ohlcv(broker, fyers_symbols, lookback_days=lookback, throttle_seconds=0.3)

    # 4. Apply basic filters
    LOG.info("Applying basic filters...")
    filtered_universe = apply_basic_filters(raw_universe, ohlcv_data, config)

    # 5. Get LTPs for filtered symbols
    filtered_fyers = [s["fyers_symbol"] for s in filtered_universe]
    LOG.info("Fetching LTPs for %d filtered symbols...", len(filtered_fyers))
    ltp_prices = get_bulk_ltp(broker, filtered_fyers)

    # 6. Run scanner
    LOG.info("Running scanner...")
    candidates = scan_candidates(filtered_universe, ohlcv_data, ltp_prices, config)

    rejected_count = len(filtered_universe) - len(candidates)
    report = build_scan_report(candidates, rejected_count, config)

    # 7. Save results
    latest_path = os.path.join(output_dir, "latest_scan.json")
    atomic_write(latest_path, report, backup=True)
    LOG.info("Saved scan to %s", latest_path)

    from datetime import datetime
    history_path = os.path.join(output_dir, "history", f"scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    atomic_write(history_path, report, backup=False)

    # 8. Print summary
    print(f"\n{'='*80}")
    print(f"  BOTTOM-ZONE GRID SCANNER — {report['scan_date']} {report['scan_time']}")
    print(f"{'='*80}")
    print(f"  Universe: {report['universe_size']} symbols | Filtered: {len(filtered_universe)} | Eligible: {report['eligible_count']}")
    print(f"  Capital per slot: ₹{config.get('portfolio', {}).get('capital_per_slot', 200000):,.0f}")
    print(f"{'='*80}")

    if not candidates:
        print("\n  No eligible candidates found.\n")
        return

    print(f"\n  {'#':>3}  {'Symbol':15s} {'CMP':>10s} {'Range':>20s} {'Pos':>6s} {'Gap':>8s} {'G2B':>4s} {'NetProfit':>10s} {'Score':>7s}")
    print(f"  {'—'*3}  {'—'*15} {'—'*10} {'—'*20} {'—'*6} {'—'*8} {'—'*4} {'—'*10} {'—'*7}")

    for i, c in enumerate(candidates[:20]):
        rng = f"{c['range_low']:.1f}-{c['range_high']:.1f}"
        print(f"  {i+1:3d}  {c['nse_symbol']:15s} {c['cmp']:10.2f} {rng:>20s} {c['range_position']:5.1%} {c['grid_gap_points']:8.2f} {c['grids_to_bottom']:4d} {c['net_cycle_profit']:10.0f} {c['score']:7.1f}")

    if len(candidates) > 20:
        print(f"\n  ... and {len(candidates) - 20} more candidates in latest_scan.json")

    print()


if __name__ == "__main__":
    main()
