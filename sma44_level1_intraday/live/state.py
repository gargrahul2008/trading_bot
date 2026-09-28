"""Persistent state for the live scanner — plain JSON/CSV files under
data/live_scan/, so everything is human-inspectable (the user explicitly
wants to be able to check this by hand):

  watchlist_latest.json      — today's actionable setups, rebuilt from
                                scratch every evening by
                                sma44_level1_intraday.scripts.live_scan_after_market
                                (self-healing: a setup that's still pending
                                naturally reappears here every day until it
                                triggers/invalidates/expires — no carry-forward
                                bookkeeping needed).
  watchlist_history/<date>.json — an archival copy of the above, one per
                                scan date, never overwritten.
  open_positions.json        — setups the live poller has actually triggered
                                and is tracking to an exit. Keyed by symbol
                                (config.trade_limits.one_active_position_per_symbol
                                is the notebook's default, so this is safe).
  live_trade_log.csv         — append-only entry/exit event log, mirroring
                                the backtest trade journal's own columns
                                where they apply.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

STATE_DIR = Path(__file__).resolve().parents[2] / "data" / "live_scan"
WATCHLIST_LATEST = STATE_DIR / "watchlist_latest.json"
WATCHLIST_HISTORY_DIR = STATE_DIR / "watchlist_history"
OPEN_POSITIONS = STATE_DIR / "open_positions.json"
TRADE_LOG_CSV = STATE_DIR / "live_trade_log.csv"
RUNNING_BARS = STATE_DIR / "running_bars.json"

TRADE_LOG_COLUMNS = [
    "event",  # "entry" (written at trigger) or "exit" (written at close)
    "symbol", "direction", "ma_length", "ma_alignment",
    "confluence_gap_skipped", "confluence_gap_ma", "confluence_gap_atr",
    "trend_class", "touch_count", "setup_quality",
    "signal_candle_time", "trigger_price", "stop_loss", "target",
    "entry_time", "entry_price", "qty",
    "exit_time", "exit_price", "exit_reason", "net_pnl", "r_multiple",
]


def _ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    WATCHLIST_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def write_watchlist(rows: list[dict], *, generated_for_date: str, generated_at: str) -> None:
    _ensure_dirs()
    payload = {"generated_for_date": generated_for_date, "generated_at": generated_at, "setups": rows}
    text = json.dumps(payload, indent=2, default=str)
    WATCHLIST_LATEST.write_text(text)
    (WATCHLIST_HISTORY_DIR / f"{generated_for_date}.json").write_text(text)


def read_watchlist() -> dict:
    if not WATCHLIST_LATEST.exists():
        return {"generated_for_date": None, "generated_at": None, "setups": []}
    return json.loads(WATCHLIST_LATEST.read_text())


def remove_from_watchlist(symbol: str) -> None:
    """Drop `symbol`'s row(s) from watchlist_latest.json the moment its
    setup actually triggers — WITHOUT this, a setup that triggers and then
    exits (e.g. hits target) within the same poll would immediately
    re-trigger a phantom re-entry off the very same, now-stale, watchlist
    row on the next poll (or even later in the same one), since nothing
    else marks a watchlist entry as "already acted on". The archival copy
    in watchlist_history/ is left untouched — it's a record of what was
    scanned, not of what's still pending."""
    if not WATCHLIST_LATEST.exists():
        return
    payload = json.loads(WATCHLIST_LATEST.read_text())
    payload["setups"] = [r for r in payload.get("setups", []) if r.get("symbol") != symbol]
    WATCHLIST_LATEST.write_text(json.dumps(payload, indent=2, default=str))


def read_open_positions() -> dict[str, dict]:
    if not OPEN_POSITIONS.exists():
        return {}
    return json.loads(OPEN_POSITIONS.read_text())


def write_open_positions(positions: dict[str, dict]) -> None:
    _ensure_dirs()
    OPEN_POSITIONS.write_text(json.dumps(positions, indent=2, default=str))


def read_running_bars(today: str) -> dict[str, dict]:
    """Per-symbol reconstructed day bar (open/high/low/close from repeated
    LTP polls) for calendar date `today` (ISO string) — persisted to disk
    because the poller runs as a SEPARATE process every cron tick, so a
    plain in-memory dict would never actually accumulate across polls; see
    scripts/live_scan_poll.py's own docstring. Any state on file for a
    DIFFERENT date is discarded (a new trading day starts fresh)."""
    if not RUNNING_BARS.exists():
        return {}
    payload = json.loads(RUNNING_BARS.read_text())
    if payload.get("date") != today:
        return {}
    return payload.get("bars", {})


def write_running_bars(today: str, bars: dict[str, dict]) -> None:
    _ensure_dirs()
    RUNNING_BARS.write_text(json.dumps({"date": today, "bars": bars}, indent=2, default=str))


def append_trade_log(row: dict) -> None:
    _ensure_dirs()
    is_new = not TRADE_LOG_CSV.exists()
    with open(TRADE_LOG_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS)
        if is_new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in TRADE_LOG_COLUMNS})
