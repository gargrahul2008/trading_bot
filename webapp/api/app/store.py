"""The API's read-only view of the store.

Kept behind these three functions so the rest of the API never holds a database
handle, and so a missing or unreadable store degrades to "no stored data"
rather than to a 500. The dashboard being a little emptier is recoverable; the
dashboard being down is not.
"""
from __future__ import annotations

import logging
import sys
from typing import Any, Dict, Optional

from app.config import REPO

if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

LOG = logging.getLogger("api.store")

try:
    from webapp.history.importer import realised_total
    from webapp.pnl import charges as charges_mod
    from webapp.pnl import portfolio as portfolio_mod
    from webapp.pnl.realised import by_scrip as realised_by_scrip
    from webapp.pnl.service import matches as matched_trades
    from webapp.store.reader import Reader
    from webapp.store.schema import connect
except Exception as exc:  # pragma: no cover - only if the tree is broken
    Reader = None  # type: ignore
    realised_total = None  # type: ignore
    portfolio_mod = None  # type: ignore
    charges_mod = None  # type: ignore
    matched_trades = None  # type: ignore
    realised_by_scrip = None  # type: ignore
    LOG.warning("store unavailable: %s", exc)


def _reader() -> Optional["Reader"]:
    if Reader is None:
        return None
    try:
        # A fresh read-only connection per request: sqlite3 objects are not safe
        # to share across threads, and FastAPI serves on many.
        return Reader(connect(read_only=True))
    except Exception as exc:
        LOG.debug("cannot open store: %s", exc)
        return None


def store_book(account: str) -> Optional[Dict[str, Any]]:
    reader = _reader()
    if reader is None:
        return None
    try:
        return reader.book(account)
    except Exception as exc:
        LOG.warning("store read failed for %s: %s", account, exc)
        return None
    finally:
        reader.conn.close()


def store_status(account: str) -> Optional[Dict[str, Any]]:
    reader = _reader()
    if reader is None:
        return None
    try:
        return reader.agent_status(account)
    except Exception:
        return None
    finally:
        reader.conn.close()


def store_counts() -> Dict[str, Any]:
    reader = _reader()
    if reader is None:
        return {"available": False}
    try:
        counts = reader.counts()
        counts["available"] = True
        return counts
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    finally:
        reader.conn.close()


def store_capital(account: str) -> str:
    """Net capital put into an account, as a decimal string.

    Zero when nothing has been imported — which is different from an account
    with no capital, so the caller decides how to render it. A return figure
    computed against an unimported base would be wildly wrong and look precise.
    """
    reader = _reader()
    if reader is None:
        return "0"
    try:
        return reader.capital_in(account)
    except Exception as exc:
        LOG.warning("capital read failed for %s: %s", account, exc)
        return "0"
    finally:
        reader.conn.close()


def store_realised(account: str, from_date: Optional[str] = None) -> Dict[str, Any]:
    """The broker's own realised P&L, net of charges.

    This is the headline figure; our FIFO matching supplies per-trade detail and
    is never added to it, so the two cannot double-count.
    """
    reader = _reader()
    if reader is None or realised_total is None:
        return {"gross": "0", "charges": "0", "net": "0", "available": False}
    try:
        totals = realised_total(reader.conn, account, from_date=from_date)
        totals["available"] = True
        return totals
    except Exception as exc:
        LOG.warning("realised read failed for %s: %s", account, exc)
        return {"gross": "0", "charges": "0", "net": "0", "available": False}
    finally:
        reader.conn.close()


def store_trades(account: Optional[str] = None, day: Optional[str] = None,
                 limit: int = 500) -> Dict[str, Any]:
    """Closed round trips with per-trade P&L, net of apportioned charges.

    Matching always replays every fill and filters the result, so a position
    opened last week and closed today is a today trade. `day` narrows what is
    shown, never what is matched.
    """
    empty = {"trades": [], "totals": charges_mod.summarise_net([]) if charges_mod else {},
             "available": False}
    reader = _reader()
    if reader is None or matched_trades is None or charges_mod is None:
        return empty
    try:
        found = matched_trades(reader.conn, account, day)
        by_day = charges_mod.load_day_charges(reader.conn, account)
        rows = charges_mod.net_matches(found, by_day)
        # Newest first: the trades someone wants are the recent ones.
        rows.sort(key=lambda r: (r.get("closed_day") or "", r.get("closed_at") or ""),
                  reverse=True)
        return {
            "trades": rows[:limit],
            "totals": charges_mod.summarise_net(rows),
            "shown": min(len(rows), limit),
            "available": True,
        }
    except Exception as exc:
        LOG.warning("trade read failed: %s", exc)
        return empty
    finally:
        reader.conn.close()


def store_realised_scrips(account: Optional[str] = None,
                          from_date: Optional[str] = None) -> Dict[str, Any]:
    """Realised P&L per scrip for the year, from the broker's own history.

    Complete where our matched trades are not: it covers shares bought years ago
    and sold in May, and everything bought and sold within the year.
    """
    reader = _reader()
    if reader is None or realised_by_scrip is None:
        return {"scrips": [], "totals": {}, "available": False}
    try:
        return realised_by_scrip(reader.conn, account, from_date=from_date)
    except Exception as exc:
        LOG.warning("realised-by-scrip read failed: %s", exc)
        return {"scrips": [], "totals": {}, "available": False}
    finally:
        reader.conn.close()
