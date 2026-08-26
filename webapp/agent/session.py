"""Which market phase we are in, and how fast to poll during it.

Cadence follows the session for the same reason the bots' `closed_poll_seconds`
does: an account that cannot trade cannot change, and spending quota on a
holiday is quota not available on Monday. The intervals below are what fits the
budget on the busiest app — see budget.py and docs in webapp/agent/README.md.
"""
from __future__ import annotations

import datetime as dt
from typing import Dict, Optional, Set

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.8 fallback, unused here
    ZoneInfo = None  # type: ignore

from common.engine.market_calendar import is_trading_day, load_holidays

MARKET_TZ = "Asia/Kolkata"

LIVE = "live"
PREOPEN = "preopen"
POSTCLOSE = "postclose"
CLOSED = "closed"
HOLIDAY = "holiday"

# Per-phase poll interval in seconds, by section.
#
# `live` is the one that matters: positions and orders at 3s cost ~40 req/min,
# which is what fits alongside four bot runs on one app. Everything else is
# either cheap or barely moves intraday.
PROFILES: Dict[str, Dict[str, float]] = {
    LIVE:      {"positions": 3.0,  "orders": 3.0,  "funds": 30.0,  "holdings": 60.0},
    PREOPEN:   {"positions": 10.0, "orders": 10.0, "funds": 60.0,  "holdings": 120.0},
    POSTCLOSE: {"positions": 15.0, "orders": 15.0, "funds": 60.0,  "holdings": 120.0},
    CLOSED:    {"positions": 60.0, "orders": 60.0, "funds": 300.0, "holdings": 300.0},
    # Nothing can move, so this is a heartbeat that keeps the book warm and
    # surfaces a dead token before Monday morning rather than during it.
    HOLIDAY:   {"positions": 900.0, "orders": 900.0, "funds": 900.0, "holdings": 900.0},
}

_PREOPEN_FROM = dt.time(9, 0)
_OPEN = dt.time(9, 15)
_CLOSE = dt.time(15, 30)
# Positions and charges keep settling after the bell; stop polling once they
# have stopped changing.
_POSTCLOSE_UNTIL = dt.time(16, 15)


class Session:
    def __init__(self, tz_name: str = MARKET_TZ, holidays: Optional[Set[str]] = None) -> None:
        self._tz = ZoneInfo(tz_name) if ZoneInfo else None
        self._holidays = load_holidays() if holidays is None else holidays

    def now(self) -> dt.datetime:
        return dt.datetime.now(self._tz) if self._tz else dt.datetime.now()

    def phase(self, now: Optional[dt.datetime] = None) -> str:
        now = now or self.now()
        if not is_trading_day(now.date(), self._holidays):
            return HOLIDAY
        clock = now.time()
        if clock < _PREOPEN_FROM:
            return CLOSED
        if clock < _OPEN:
            return PREOPEN
        if clock < _CLOSE:
            return LIVE
        if clock < _POSTCLOSE_UNTIL:
            return POSTCLOSE
        return CLOSED

    def intervals(self, now: Optional[dt.datetime] = None) -> Dict[str, float]:
        return PROFILES[self.phase(now)]
