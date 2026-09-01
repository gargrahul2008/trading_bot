"""The API's read-only view of the store.

Kept behind these three functions so the rest of the API never holds a database
handle, and so a missing or unreadable store degrades to "no stored data"
rather than to a 500. The dashboard being a little emptier is recoverable; the
dashboard being down is not.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional

from app.config import REPO

if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

LOG = logging.getLogger("api.store")

try:
    from webapp.history.importer import realised_total
    from webapp.pnl import charges as charges_mod
    from webapp.pnl import portfolio as portfolio_mod
    from webapp.pnl import rms as rms_mod
    from webapp.history import symbols as symbols_mod
    from webapp.pnl.realised import by_scrip as realised_by_scrip
    from webapp.pnl.service import matches as matched_trades
    from webapp.store.reader import Reader
    from webapp.store.schema import connect
except Exception as exc:  # pragma: no cover - only if the tree is broken
    Reader = None  # type: ignore
    realised_total = None  # type: ignore
    portfolio_mod = None  # type: ignore
    rms_mod = None  # type: ignore
    charges_mod = None  # type: ignore
    matched_trades = None  # type: ignore
    realised_by_scrip = None  # type: ignore
    symbols_mod = None  # type: ignore
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


def store_limits() -> List[Dict[str, Any]]:
    """Every risk limit row, defaults included. Empty means the built-ins."""
    reader = _reader()
    if reader is None:
        return []
    try:
        return [dict(r) for r in reader.conn.execute(
            "SELECT account, name, value FROM rms_limits")]
    except Exception as exc:
        LOG.debug("rms limits unavailable: %s", exc)
        return []
    finally:
        reader.conn.close()


def store_exclusions(account: Optional[str] = None) -> Dict[str, Any]:
    """Scrips set aside per account, as {account: {symbol: reason}}."""
    reader = _reader()
    if reader is None:
        return {}
    try:
        sql = "SELECT * FROM exclusions"
        params: List[Any] = []
        if account:
            sql += " WHERE account = ?"
            params.append(account)
        out: Dict[str, Any] = {}
        for row in reader.conn.execute(sql + " ORDER BY account, symbol", params):
            out.setdefault(row["account"], {})[row["symbol"]] = {
                "reason": row["reason"], "at": row["at"],
            }
        return out
    except Exception as exc:
        # Before any agent has migrated to v9 the table is simply absent. That
        # means "nothing excluded", which is the correct answer, not an error.
        LOG.debug("exclusions unavailable: %s", exc)
        return {}
    finally:
        reader.conn.close()


def store_activity(account: Optional[str] = None, day: Optional[str] = None,
                   limit: int = 200) -> Dict[str, Any]:
    """What actually happened, newest first — one stream, all accounts.

    Two kinds of thing move without anyone watching: an order changes status,
    and a position closes. The first is recorded as it happens; the second is
    derived, because a close is not an event the broker reports — it is the
    absence of a position that was there before. Both belong in one list, or
    you are still reading two pages to answer "what changed?".
    """
    empty = {"events": [], "available": False}
    reader = _reader()
    if reader is None:
        return empty
    try:
        sql = "SELECT * FROM order_events WHERE 1=1"
        params: List[Any] = []
        if account:
            sql += " AND account = ?"
            params.append(account)
        if day:
            sql += " AND trading_day = ?"
            params.append(day)
        sql += " ORDER BY at DESC LIMIT ?"
        params.append(limit)

        events = []
        for row in reader.conn.execute(sql, params):
            # `event` is claimed before the reject classifier runs: it also
            # writes a `kind`, and letting it land first replaced every event
            # name with the reject taxonomy's.
            row = dict(row)
            row["event"] = row.pop("kind")
            row.update(classify_reject(row.get("message")))
            events.append(row)
    except Exception as exc:
        # A missing table means an agent has not written since the migration,
        # not a broken page: closures below still stand on their own.
        LOG.warning("activity read failed: %s", exc)
        events = []
    finally:
        reader.conn.close()

    # A matched round trip names its figures gross/net; the stream calls them
    # pnl/net_pnl so a close reads the same way as everything beside it. Money
    # stays a decimal string, as everywhere else — the browser must not round it.
    for trade in store_trades(account, day, limit).get("trades", []):
        events.append({
            "at": trade.get("closed_at"), "account": trade.get("account"),
            "symbol": trade.get("symbol"), "side": trade.get("direction"),
            "event": "closed", "qty": trade.get("qty"),
            "price": trade.get("exit_price"), "entry_price": trade.get("entry_price"),
            "pnl": trade.get("gross"), "net_pnl": trade.get("net"),
            "charges": trade.get("charges"),
            "product_type": trade.get("product_type"),
            "trading_day": trade.get("closed_day"),
            "order_id": trade.get("exit_order_id"),
        })

    events.sort(key=lambda e: _when(e), reverse=True)
    return {"events": events[:limit], "available": True}


def _when(event: Dict[str, Any]) -> float:
    """Seconds since the epoch, however the source spelled its timestamp.

    Order events carry a float; a matched trade carries whatever the broker's
    fill did, which may be a string. An unparseable time sorts oldest rather
    than raising — a badly stamped row is still worth showing.
    """
    at = event.get("at")
    if isinstance(at, (int, float)):
        return float(at)
    for text in (at, event.get("trading_day")):
        if not isinstance(text, str) or not text:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text[:len(datetime.now().strftime(fmt))],
                                         fmt).timestamp()
            except ValueError:
                continue
    return 0.0


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


def classify_reject(message: Optional[str]) -> Dict[str, Any]:
    """Turn a broker reject message into a cause.

    Reuses the parser the live bots already rely on, so the dashboard names a
    reject the same way the bot that hit it does — margin shortfall, circuit
    limit, closing-auction session, disclosed-quantity, TPIN authorisation.
    Anything it does not recognise stays unclassified rather than being filed
    under a wrong cause.
    """
    if not message:
        return {"kind": None, "reason": None}
    try:
        from common.broker.reject_parser import parse_reject

        action = parse_reject(message)
        return {"kind": action.kind, "reason": action.reason or message}
    except Exception:
        return {"kind": None, "reason": message}


def store_orders(account: Optional[str] = None, day: Optional[str] = None,
                 limit: int = 1000) -> Dict[str, Any]:
    """Every order the store holds, newest first."""
    reader = _reader()
    if reader is None:
        return {"orders": [], "available": False}
    try:
        sql = "SELECT * FROM orders WHERE 1=1"
        params: List[Any] = []
        if account:
            sql += " AND account = ?"
            params.append(account)
        if day:
            sql += " AND trading_day = ?"
            params.append(day)
        sql += " ORDER BY trading_day DESC, order_id DESC LIMIT ?"
        params.append(limit)
        rows = [dict(r) for r in reader.conn.execute(sql, params)]
        for row in rows:
            row.pop("raw", None)
            row.update(classify_reject(row.get("message")))
        return {"orders": rows, "available": True}
    except Exception as exc:
        LOG.warning("order read failed: %s", exc)
        return {"orders": [], "available": False}
    finally:
        reader.conn.close()


def search_symbols(query: str, limit: int = 12) -> Dict[str, Any]:
    """Instruments matching a fragment, from the exchanges' own list."""
    reader = _reader()
    if reader is None or symbols_mod is None:
        return {"matches": [], "available": False}
    try:
        return {
            "matches": symbols_mod.search(reader.conn, query, limit),
            "available": symbols_mod.counts(reader.conn)["symbols"] > 0,
        }
    except Exception as exc:
        LOG.warning("symbol search failed: %s", exc)
        return {"matches": [], "available": False}
    finally:
        reader.conn.close()


def lookup_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """One instrument, with its tick and lot size. None means the exchanges do
    not list it — which for the order pad means it cannot be traded."""
    reader = _reader()
    if reader is None or symbols_mod is None:
        return None
    try:
        return symbols_mod.lookup(reader.conn, symbol)
    except Exception:
        return None
    finally:
        reader.conn.close()


def store_symbols() -> List[str]:
    """Every symbol this dashboard has seen, for the order pad's suggestions.

    Drawn from what has actually been traded and held rather than from a symbol
    master: it is exactly the list someone reaches for, it needs no extra data
    source to keep current, and a free-typed symbol still works for anything new.
    """
    reader = _reader()
    if reader is None:
        return []
    try:
        found = set()
        for sql in (
            "SELECT DISTINCT symbol FROM orders",
            "SELECT DISTINCT symbol FROM fills",
            "SELECT DISTINCT symbol FROM realised_history",
            "SELECT DISTINCT symbol FROM opening_positions",
        ):
            try:
                found.update(row[0] for row in reader.conn.execute(sql) if row[0])
            except Exception:
                continue
        return sorted(found)
    except Exception as exc:
        LOG.warning("symbol read failed: %s", exc)
        return []
    finally:
        reader.conn.close()
