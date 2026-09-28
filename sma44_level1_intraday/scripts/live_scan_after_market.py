#!/usr/bin/env python3
"""Cron job: run AFTER the nightly data top-up
(scripts/fetch_universe_topup_cron.sh, currently `0 18 * * *`) — schedule
this at e.g. 18:30 IST on weekdays. Uses the same cache-first, gap-aware
provider the notebook itself uses (prepare_symbol_bars_for_scan) — normally
a no-op network-wise since the top-up job already ran, but it WILL still
top up a handful of missing days itself if the cache is behind (e.g. a
manual run before that cron fires), same as the notebook would.

Recomputes the FULL actionable-signal scan — identical rules to the
notebook's own "live check" cell (every trade-taking filter, one-signal-
per-symbol across all configured MAs, only setups still genuinely
actionable per signals.pending_setup_status) — using the EXACT config the
research notebook currently has set (see live.notebook_config: no manual
sync, no drift). One deliberate difference from the notebook's live-check
cell: a SHORT candidate on a symbol with no F&O market to borrow via is
dropped here (same filter the BACKTEST section already applies) — sending a
live alert for a trade you literally cannot place would be actively
misleading.

Writes data/live_scan/watchlist_latest.json for
sma44_level1_intraday.scripts.live_scan_poll to track during the next
session, and sends one Telegram summary of the day's actionable list.

Usage:
    python -m sma44_level1_intraday.scripts.live_scan_after_market
    python -m sma44_level1_intraday.scripts.live_scan_after_market --as-of 2026-09-25 --no-notify
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

import pandas as pd  # noqa: E402

from intraday_research.universe import load_universe  # noqa: E402
from sma44_level1_intraday.live.notebook_config import load_live_scan_setup  # noqa: E402
from sma44_level1_intraday.live.state import write_watchlist  # noqa: E402
from sma44_level1_intraday.live.telegram import send_telegram  # noqa: E402
from sma44_level1_intraday.pipeline import prepare_symbol_bars_for_scan  # noqa: E402
from sma44_level1_intraday.setup_quality import enrich_touches_with_setup_quality  # noqa: E402
from sma44_level1_intraday.signals import generate_scan_candidates, pending_setup_status  # noqa: E402


def _sizing_warning(cfg) -> "str | None":
    risk = cfg.risk
    if risk.position_sizing_mode == "capital_based" and risk.capital_per_trade_amount is None:
        return ("risk.position_sizing_mode='capital_based' with capital_per_trade_pct "
                "(percentage of running equity) -- the live poller does NOT track real running "
                "equity across live trades, so live position sizing will drift from what the "
                "backtest would compute. Set capital_per_trade_amount to a fixed rupee amount "
                "for live use.")
    if risk.position_sizing_mode == "risk_based":
        return ("risk.position_sizing_mode='risk_based' -- same running-equity caveat as above "
                "if risk_per_trade_amount is not set.")
    return None


def run(as_of: "date | None" = None, *, notify: bool = True,
        start_date: "date | None" = None) -> list[dict]:
    t0 = time.time()
    as_of = as_of or date.today()
    setup = load_live_scan_setup(end_date=as_of, start_date=start_date)
    cfg = setup.config

    warnings = []
    if cfg.execution.exit_mode != "fixed":
        warnings.append(
            f"config.execution.exit_mode={cfg.execution.exit_mode!r} -- live_scan_poll.py only "
            f"tracks fixed-mode exits (a fixed stop_loss/target). Entries will still alert; exits "
            f"in this mode will NOT be tracked live."
        )
    sizing_warning = _sizing_warning(cfg)
    if sizing_warning:
        warnings.append(sizing_warning)
    for w in warnings:
        print(f"WARNING: {w}")

    universe = load_universe(setup.universe_file)
    etf_symbols = set(load_universe(setup.etf_universe_file).symbols) if setup.etf_universe_file.exists() else set()
    fno_symbols = (
        {s["symbol"] for s in json.loads(setup.fno_universe_file.read_text())["symbols"]}
        if setup.fno_universe_file.exists() else set()
    )
    expiry = cfg.scanner.setup_expiry_bars

    rows: list[dict] = []
    actionable_symbols: set[str] = set()
    skipped = 0
    for sym in universe.symbols:
        try:
            bars = prepare_symbol_bars_for_scan(
                sym, setup.fetch_start_date, setup.end_date, cfg,
                asset_type="equity", repo_root=setup.repo_root, verbose=False,
            )
        except Exception:
            skipped += 1
            continue
        if bars.empty:
            skipped += 1
            continue

        candidates = generate_scan_candidates(bars, cfg)
        if candidates.empty:
            continue
        if sym not in fno_symbols:
            candidates = candidates[candidates["direction"] != "SHORT"]
            if candidates.empty:
                continue

        ts = bars["timestamp"]
        cutoff = ts.iloc[-(expiry + 1)] if len(ts) > expiry else ts.iloc[0]
        for _, cand in candidates[candidates["timestamp"] >= cutoff].iterrows():
            if sym in actionable_symbols:
                break  # one open signal per symbol, across every configured MA
            bars_after = bars[bars["timestamp"] > cand["timestamp"]]
            status = pending_setup_status(cand, bars_after, expiry)
            if status != "actionable":
                continue
            actionable_symbols.add(sym)

            event_row = pd.DataFrame([{
                "symbol": sym, "timeframe": cand["timeframe"], "direction": cand["direction"],
                "timestamp": cand["current_touch_time"],
                "open": cand["signal_open"], "high": cand["signal_high"],
                "low": cand["signal_low"], "close": cand["signal_close"],
                # enrich_touches_with_setup_quality reads the touch-sequence
                # times off this row (see its docstring: it expects
                # scan_history's own qualifying-touch columns), so pass them
                # through rather than only the renamed "timestamp".
                "current_touch_time": cand["current_touch_time"],
                "previous_touch_time": cand["previous_touch_time"],
                "first_interaction_time": cand["first_interaction_time"],
                "bars_since_previous_touch": cand["bars_since_previous_touch"],
            }])
            enriched = enrich_touches_with_setup_quality(
                bars, event_row,
                dataclasses.replace(cfg.scanner, sma_length=int(cand["ma_length"])), cfg.setup_quality,
            )
            rows.append({
                "symbol": sym, "is_etf": sym in etf_symbols, "direction": cand["direction"],
                "ma_length": int(cand["ma_length"]), "ma_alignment": cand["ma_alignment"],
                "confluence_gap_skipped": bool(cand["confluence_gap_skipped"]),
                "confluence_gap_ma": cand["confluence_gap_ma"],
                "confluence_gap_atr": None if pd.isna(cand["confluence_gap_atr"]) else round(float(cand["confluence_gap_atr"]), 3),
                "touch_count": int(cand["touch_count"]), "trend_class": cand["trend_class"],
                "ma_slope_pct": round(float(cand["ma_slope_pct"]), 3),
                "signal_candle_time": str(cand["current_touch_time"]),
                "trigger_price": float(cand["trigger_price"]), "stop_loss": float(cand["stop_loss"]),
                "target": float(cand["target"]),
                "signal_low": float(cand["signal_low"]), "signal_high": float(cand["signal_high"]),
                "setup_quality": enriched["setup_quality"].iloc[0],
                "daily_volume": float(cand["daily_volume"]),
            })

    generated_for = as_of.isoformat()
    write_watchlist(rows, generated_for_date=generated_for, generated_at=datetime.now().isoformat())
    print(f"live scan: {len(rows)} actionable setups across {len(universe.symbols) - skipped} symbols "
          f"({skipped} skipped, no cached data) in {time.time() - t0:.0f}s")

    if notify:
        header = f"44-SMA scan {generated_for}: {len(rows)} actionable setup(s) for the next session"
        if warnings:
            header += "\n[config warning] " + " | ".join(warnings)
        if not rows:
            send_telegram(header)
        else:
            lines = [header]
            for r in sorted(rows, key=lambda r: (r["direction"], -r["touch_count"])):
                gap = f" gap~{r['confluence_gap_ma']}" if r["confluence_gap_skipped"] else ""
                lines.append(
                    f"{r['direction']} {r['symbol']} MA{r['ma_length']} [{r['ma_alignment']}] "
                    f"trig {r['trigger_price']:.2f} sl {r['stop_loss']:.2f} tgt {r['target']:.2f} "
                    f"{r['setup_quality']}{gap}"
                )
            send_telegram("\n".join(lines))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Post-market 44-SMA actionable-signal scan + Telegram summary")
    ap.add_argument("--as-of", default=None, help="ISO date (default: today)")
    ap.add_argument("--start-date", default=None,
                    help="override the notebook's START_DATE (default: whatever the notebook has). "
                         "Shortens the scan window -- note the touch-sequence counter then starts "
                         "from this date, so touch_count can differ from the notebook's.")
    ap.add_argument("--no-notify", action="store_true", help="skip sending the Telegram summary")
    args = ap.parse_args()
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    run(as_of, notify=not args.no_notify, start_date=start_date)


if __name__ == "__main__":
    main()
