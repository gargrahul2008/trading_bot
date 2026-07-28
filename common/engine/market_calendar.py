"""
Minimal NSE trading-calendar helper for the equity session guard.

Used only when a config sets `execution.equity_session_guard: true` (equity bots). Crypto /
always-on bots never load this, so their behavior is unchanged.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Optional, Set

_DEFAULT_HOLIDAYS = Path(__file__).with_name("nse_holidays.json")


def load_holidays(path: Optional[str] = None) -> Set[str]:
    """Return the set of ISO holiday dates. Falls back to the bundled nse_holidays.json.
    Any read/parse error degrades to an empty set (weekends still handled)."""
    p = Path(path) if path else _DEFAULT_HOLIDAYS
    try:
        data = json.loads(p.read_text())
    except Exception:
        return set()
    if isinstance(data, dict):
        items = data.get("holidays") or []
    elif isinstance(data, list):
        items = data
    else:
        items = []
    return {str(x).strip() for x in items if str(x).strip()}


def is_trading_day(d: date, holidays: Set[str]) -> bool:
    """A weekday that is not an NSE holiday."""
    if d.weekday() >= 5:            # Saturday=5, Sunday=6
        return False
    return d.isoformat() not in holidays
