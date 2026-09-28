#!/usr/bin/env python3
"""Cron job, every 5 minutes on weekdays during market hours — e.g.
`*/5 9-15 * * 1-5` (9:15 lands on a 5-minute boundary; the internal
`_in_market_hours` check below does the real gating, so a run outside
09:15–15:30 IST or on a weekend is a silent no-op).

Tracks data/live_scan/watchlist_latest.json's pending setups (written by
live_scan_after_market.py the previous evening) against LIVE Fyers LTP.
Fyers' quotes call here only returns `lp` (see live.quotes / FyersClient.
get_ltps), not the day's own OHLC, so each symbol's running day
open/high/low/close is rebuilt from repeated LTP polls, persisted to
data/live_scan/running_bars.json between polls (each cron tick is a
SEPARATE process, so this can't just live in a Python variable — see
live.state.read_running_bars/write_running_bars) — meaning a trigger or
invalidation that happens AND reverts entirely between two 5-minute polls
can still be missed. This is the tradeoff of a 5-minute LTP-polling design,
not a bug.

Sends a Telegram alert the moment a pending setup's entry actually
triggers, and tracks it through to exit (SL / target / force_exit) with a
second alert including realised P&L — using the IDENTICAL trigger /
invalidation / exit rules backtest/engine.py itself uses (see live.engine),
so a live alert can never disagree with what the backtest would have done
on the same price path. Every entry/exit is also appended to
data/live_scan/live_trade_log.csv for manual review.

Only config.execution.exit_mode == "fixed" is supported for exit tracking —
live_scan_after_market.py already warns loudly if the notebook's config
uses atr_chandelier / candle_trail instead.

Usage:
    python -m sma44_level1_intraday.scripts.live_scan_poll
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - py<3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from common.broker.auth_json import get_fyers_creds_from_json  # noqa: E402
from common.broker.fyers_client import FyersClient  # noqa: E402
from sma44_level1_intraday.backtest.costs import compute_trade_costs  # noqa: E402
from sma44_level1_intraday.live.engine import check_fixed_exit, check_trigger, compute_qty  # noqa: E402
from sma44_level1_intraday.live.notebook_config import LiveScanSetup, load_live_scan_setup  # noqa: E402
from sma44_level1_intraday.live.quotes import fetch_ltps  # noqa: E402
from sma44_level1_intraday.live.state import (  # noqa: E402
    append_trade_log, read_open_positions, read_running_bars, read_watchlist, remove_from_watchlist,
    write_open_positions, write_running_bars,
)
from sma44_level1_intraday.live.telegram import send_telegram  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
AUTH_FILE = REPO_ROOT / "fyers_auth.json"
USER_KEY = "user1"


def _update_running_bar(bars: dict[str, dict], symbol: str, ltp: float) -> dict:
    """Mutates and returns `bars[symbol]` in place — `bars` is the whole
    day's dict, loaded once per poll via live.state.read_running_bars and
    persisted back at the end of poll_once (see this module's docstring for
    why it can't just be an in-memory variable)."""
    bar = bars.get(symbol)
    if bar is None:
        bar = {"open": ltp, "high": ltp, "low": ltp, "close": ltp}
        bars[symbol] = bar
    else:
        bar["high"] = max(bar["high"], ltp)
        bar["low"] = min(bar["low"], ltp)
        bar["close"] = ltp
    return bar


def _in_market_hours(now_ist: datetime, market_cfg) -> bool:
    # Deliberately ignores market_cfg.continuous_session: that flag only
    # tells the BACKTEST "this is a daily/weekly bar, don't apply intraday
    # time gating to it" (see the notebook's own config-build cell) — it is
    # not a statement that trading happens around the clock, and the live
    # poller must always gate on the real wall-clock session regardless.
    if now_ist.weekday() >= 5:
        return False
    t = now_ist.time()
    return market_cfg.market_start_time <= t <= market_cfg.market_end_time


def poll_once(client: FyersClient, setup: LiveScanSetup, *, now_ist: "datetime | None" = None) -> None:
    cfg = setup.config
    now_ist = now_ist or datetime.now(IST)
    today = now_ist.date()
    if not _in_market_hours(now_ist, cfg.market):
        return

    wl = read_watchlist()
    open_positions = read_open_positions()
    running_bars = read_running_bars(today.isoformat())

    watch_symbols = {r["symbol"] for r in wl.get("setups", [])} | set(open_positions.keys())
    if not watch_symbols:
        return
    ltps = fetch_ltps(client, sorted(watch_symbols))

    # ── 1. Existing open positions: check for exit first ──────────────────
    force_exit = now_ist.time() >= cfg.market.force_exit_time
    for symbol, pos in list(open_positions.items()):
        ltp = ltps.get(symbol)
        if ltp is None:
            continue
        bar = _update_running_bar(running_bars, symbol, ltp)
        exit_info = None
        if cfg.execution.exit_mode == "fixed":
            exit_info = check_fixed_exit(
                pos["direction"], pos["stop_loss"], pos["target"], bar, cfg.execution.same_candle_priority,
            )
        if exit_info is None and force_exit:
            exit_info = (ltp, "force_exit")
        if exit_info is None:
            continue

        exit_price, exit_reason = exit_info
        direction_sign = 1.0 if pos["direction"] == "LONG" else -1.0
        gross = (exit_price - pos["entry_price"]) * pos["qty"] * direction_sign
        costs = compute_trade_costs(
            entry_price=pos["entry_price"], exit_price=exit_price, qty=pos["qty"], costs_cfg=cfg.costs,
        )
        net_pnl = gross - costs
        risk_per_unit = abs(pos["trigger_price"] - pos["stop_loss"])
        r_mult = net_pnl / (risk_per_unit * pos["qty"]) if risk_per_unit > 0 and pos["qty"] > 0 else 0.0

        append_trade_log({
            **pos, "event": "exit", "exit_time": now_ist.isoformat(),
            "exit_price": exit_price, "exit_reason": exit_reason,
            "net_pnl": round(net_pnl, 2), "r_multiple": round(r_mult, 3),
        })
        send_telegram(
            f"EXIT {pos['direction']} {symbol} MA{pos['ma_length']} @ {exit_price:.2f} ({exit_reason}) "
            f"entry {pos['entry_price']:.2f} qty {pos['qty']:.0f} net P&L {net_pnl:,.2f} (R {r_mult:.2f})"
        )
        open_positions.pop(symbol, None)
        # Also stop tracking this symbol's running bar past its exit for
        # today -- a closed position has nothing left to check against.
        running_bars.pop(symbol, None)

    # ── 2. Watchlist setups still pending: check for trigger ──────────────
    no_new_entries = now_ist.time() > cfg.market.no_new_entry_after
    for row in wl.get("setups", []):
        symbol = row["symbol"]
        if symbol in open_positions:
            continue  # one open position per symbol (config.trade_limits default)
        ltp = ltps.get(symbol)
        if ltp is None:
            continue
        bar = _update_running_bar(running_bars, symbol, ltp)
        if no_new_entries:
            continue  # too late in the session to open a new position

        # A mid-day invalidation isn't acted on here (no "invalidated" alert
        # is sent, matching what was asked for) — a genuinely invalidated
        # setup simply stops reappearing in tomorrow's watchlist once the
        # actual daily bar closes with that breach; until then this row is
        # re-checked every poll, which is harmless (just an unused LTP check).
        triggered, _invalidated = check_trigger(
            {"direction": row["direction"], "trigger_price": row["trigger_price"],
             "signal_low": row["signal_low"], "signal_high": row["signal_high"]},
            bar,
        )
        if not triggered:
            continue

        qty = compute_qty(
            cfg, {"trigger_price": row["trigger_price"], "stop_loss": row["stop_loss"]}, cfg.risk.initial_capital,
        )
        entry_price = float(row["trigger_price"])
        pos = {**row, "entry_time": now_ist.isoformat(), "entry_price": entry_price, "qty": qty}
        open_positions[symbol] = pos
        # Persisted immediately (not just held in the local `wl` list) so a
        # LATER poll -- a fresh cron process, with no memory of this one --
        # can never re-trigger off the same now-consumed signal, including
        # if this very position has already exited by the time that later
        # poll runs (see remove_from_watchlist's own docstring).
        remove_from_watchlist(symbol)
        append_trade_log({**pos, "event": "entry"})
        send_telegram(
            f"ENTRY {row['direction']} {symbol} MA{row['ma_length']} [{row['ma_alignment']}] "
            f"@ {entry_price:.2f} qty {qty:.0f} sl {row['stop_loss']:.2f} tgt {row['target']:.2f} "
            f"{row['setup_quality']}"
        )

    write_open_positions(open_positions)
    write_running_bars(today.isoformat(), running_bars)


def main() -> None:
    client_id, access_token = get_fyers_creds_from_json(str(AUTH_FILE), user_key=USER_KEY)
    client = FyersClient(client_id=client_id, access_token=access_token)
    setup = load_live_scan_setup()
    poll_once(client, setup)


if __name__ == "__main__":
    main()
