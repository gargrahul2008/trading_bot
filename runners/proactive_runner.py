from __future__ import annotations

"""
Proactive runner (template)

Use when a strategy:
- pre-places a grid/ladder of pending LIMIT orders, and
- reacts to fills by placing/cancelling/adjusting other orders.

This file provides a minimal scaffold; you can extend it per strategy with:
- list_open_orders()
- cancel_order()
- place_limit_order()
- reconcile() logic (desired vs actual orders)
"""

import datetime as dt
import time
from typing import Any, Dict

from zoneinfo import ZoneInfo

from common.fyers_broker import FyersBroker
from common.logger import LOG
from common.timeutils import iso_utc, now_utc, parse_hhmm, parse_hhmmss

def run_live_proactive(strategy, broker: FyersBroker, cfg: Dict[str, Any]) -> None:
    exec_cfg = cfg.get("execution", {})
    market_tz = exec_cfg.get("market_tz", "Asia/Kolkata")
    market_open = exec_cfg.get("market_open", "09:15")
    market_close = exec_cfg.get("market_close", "15:30")
    eod_cancel = exec_cfg.get("eod_cancel", "15:29:30")
    poll_seconds = int(exec_cfg.get("poll_seconds", 5))
    closed_poll_seconds = int(exec_cfg.get("closed_poll_seconds", 30))

    tz = ZoneInfo(market_tz)
    open_t = parse_hhmm(market_open)
    close_t = parse_hhmm(market_close)
    eod_t = parse_hhmmss(eod_cancel)

    LOG.info("LIVE (proactive) started strategy=%s symbols=%s", type(strategy).__name__, ",".join(strategy.symbols))

    while True:
        try:
            now_local = dt.datetime.now(tz)
            open_dt = dt.datetime(now_local.year, now_local.month, now_local.day, open_t.hour, open_t.minute, open_t.second, tzinfo=tz)
            close_dt = dt.datetime(now_local.year, now_local.month, now_local.day, close_t.hour, close_t.minute, close_t.second, tzinfo=tz)
            eod_dt = dt.datetime(now_local.year, now_local.month, now_local.day, eod_t.hour, eod_t.minute, eod_t.second, tzinfo=tz)
            in_trading = (now_local >= open_dt) and (now_local < close_dt)
            allow = (now_local >= open_dt) and (now_local < eod_dt)

            if allow and hasattr(strategy, "step"):
                strategy.step(broker, ts_utc=iso_utc(now_utc()))  # type: ignore[attr-defined]

            if hasattr(strategy, "persist"):
                strategy.persist()  # type: ignore[attr-defined]

            sleep_s = poll_seconds if in_trading else closed_poll_seconds
            time.sleep(max(int(sleep_s), 1))

        except KeyboardInterrupt:
            LOG.info("Stopped by user.")
            break
        except Exception as e:
            LOG.exception("Loop error: %s", e)
            time.sleep(2)
