"""Broker timestamps, made comparable.

Fyers stamps a trade with `orderDateTime` in day-month-year form —
"31-Aug-2026 10:15:23". That is unreadable to a sort: as text, "31-Aug-2026"
comes after "03-Sep-2026", so any list ordered on it puts the end of August
above the start of September. The pad's Recent list showed exactly that, with
one block of closed trades above the open positions and another below them.

Parsed here into ISO instead, which sorts as it reads. Anything unparseable
comes back as the empty string rather than raising: a trade with an odd stamp
still belongs in the list, at the bottom, where an unknown time belongs.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Optional

#: Every shape a Fyers timestamp has arrived in. Day-first first, because that
#: is what the orderbook and tradebook actually send.
FORMATS = (
    "%d-%b-%Y %H:%M:%S",
    "%d-%B-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
)

#: Below this, a number is not seconds since the epoch — it is a price, a
#: quantity, or a year someone stored as an integer.
EPOCH_FLOOR = 10_000_000


def to_iso(value: Any) -> str:
    """A sortable timestamp, or "" if there is not one.

    Already-ISO text is returned unchanged, so a store written by a later
    version costs nothing to read.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < EPOCH_FLOOR:
            return ""
        return dt.datetime.fromtimestamp(float(value)).isoformat(sep=" ")

    text = str(value).strip()
    if not text:
        return ""

    for fmt in FORMATS:
        try:
            return dt.datetime.strptime(text, fmt).isoformat(sep=" ")
        except ValueError:
            continue

    # An epoch as a string, which is how it survives a round trip through JSON
    # and then through SQLite as TEXT.
    try:
        number = float(text)
    except ValueError:
        return ""
    return to_iso(number) if number >= EPOCH_FLOOR else ""


def sort_key(value: Any, fallback: Optional[str] = None) -> str:
    """What to order by: the parsed time, or the day it belongs to.

    A trade whose stamp cannot be read still has a trading day, and ordering it
    by that is right to within a session — far better than dropping it to the
    bottom of the list under a stamp nobody can compare.
    """
    return to_iso(value) or (fallback or "")
