"""What each account held before our fill history begins.

A share bought two years ago and sold today has no matching buy in the store, so
a FIFO pass opens a *short* lot rather than closing a long one — and the trade's
real P&L, the sale against what the holding cost, never appears. Worse, the
broker reports that phantom short's mark-to-market as `unrealized_profit`, which
is neither the right number nor the right kind of number: pratibha's SHRINGARMS
sale showed +3,130 unrealised where the trade was a −6,660 realised loss.

So the matcher is given an opening position: one synthetic buy per holding, at
the holding's cost, dated before the first fill we recorded. Every real fill from
then on applies on top of it.

Two honest limits, both recorded on the row:

* The cost is the broker's **average** for that holding, not a per-lot basis. So
  a partial sale is matched at the average — which is what the broker itself
  does for delivery stock, and the same figure its own realised history uses.
* Anything the account held and sold **before** our first snapshot is not here
  and cannot be. For those, the broker's realised history is the only source,
  and it is already the headline figure on the Portfolio page.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any, Dict, List, Optional

LOG = logging.getLogger("pnl.opening")

SNAPSHOT = "holdings_snapshot"
MANUAL = "manual"


def earliest_fill_day(conn: sqlite3.Connection, account: str) -> Optional[str]:
    row = conn.execute(
        "SELECT MIN(trading_day) FROM fills WHERE account = ?", (account,)
    ).fetchone()
    return row[0] if row and row[0] else None


def earliest_holdings(conn: sqlite3.Connection, account: str) -> List[Dict[str, Any]]:
    """The first holdings snapshot we ever stored for this account."""
    row = conn.execute(
        "SELECT payload FROM snapshots WHERE account = ? AND kind = 'holdings'"
        " ORDER BY taken_at ASC LIMIT 1", (account,)
    ).fetchone()
    if row is None:
        return []
    try:
        data = json.loads(row["payload"])
    except (ValueError, TypeError):
        return []
    return [h for h in data if isinstance(h, dict)] if isinstance(data, list) else []


def seed_from_holdings(conn: sqlite3.Connection, account: str,
                       as_of_day: Optional[str] = None) -> int:
    """Record an opening position for each holding in the earliest snapshot.

    Dated the day *before* the first fill we have, so a fill recorded on that
    first day applies on top rather than being counted twice.
    """
    holdings = [h for h in earliest_holdings(conn, account)
                if h.get("is_open") and float(h.get("qty") or 0) > 0]
    if not holdings:
        return 0

    if as_of_day is None:
        first = earliest_fill_day(conn, account)
        if first:
            import datetime as dt

            as_of_day = (dt.date.fromisoformat(first) - dt.timedelta(days=1)).isoformat()
        else:
            as_of_day = "2026-04-01"

    now = time.time()
    conn.executemany(
        "INSERT INTO opening_positions"
        " (account, symbol, product_type, qty, cost_price, as_of_day, source, note, recorded_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(account, symbol, product_type) DO UPDATE SET"
        # A manual entry is deliberate and outranks a re-seed from a snapshot.
        "   qty = CASE WHEN opening_positions.source = 'manual'"
        "              THEN opening_positions.qty ELSE excluded.qty END,"
        "   cost_price = CASE WHEN opening_positions.source = 'manual'"
        "              THEN opening_positions.cost_price ELSE excluded.cost_price END,"
        "   recorded_at = excluded.recorded_at",
        [
            (account, str(h.get("symbol") or ""), "CNC",
             float(h.get("qty") or 0), float(h.get("cost_price") or 0),
             as_of_day, SNAPSHOT,
             "from the earliest holdings snapshot", now)
            for h in holdings if h.get("symbol")
        ],
    )
    conn.commit()
    return len(holdings)


def record_manual(conn: sqlite3.Connection, account: str, symbol: str, qty: float,
                  cost_price: float, as_of_day: str, product_type: str = "CNC",
                  note: Optional[str] = None) -> None:
    """An opening position entered by hand, for anything older than any snapshot."""
    conn.execute(
        "INSERT INTO opening_positions"
        " (account, symbol, product_type, qty, cost_price, as_of_day, source, note, recorded_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(account, symbol, product_type) DO UPDATE SET"
        "   qty = excluded.qty, cost_price = excluded.cost_price,"
        "   as_of_day = excluded.as_of_day, source = excluded.source,"
        "   note = excluded.note, recorded_at = excluded.recorded_at",
        (account, symbol, product_type, float(qty), float(cost_price), as_of_day,
         MANUAL, note, time.time()),
    )
    conn.commit()


def as_fills(conn: sqlite3.Connection, account: Optional[str] = None) -> List[Dict[str, Any]]:
    """Opening positions as synthetic buys, for the matcher to consume.

    Their trade ids are prefixed `opening:` so a matched round trip can always be
    traced back to an assumed cost rather than a recorded fill.
    """
    sql = ("SELECT account, symbol, product_type, qty, cost_price, as_of_day"
           " FROM opening_positions WHERE qty > 0")
    params: List[Any] = []
    if account:
        sql += " AND account = ?"
        params.append(account)
    return [
        {
            "account": row["account"],
            "symbol": row["symbol"],
            "product_type": row["product_type"],
            "trade_id": "opening:%s:%s" % (row["account"], row["symbol"]),
            "order_id": "",
            "side": "BUY",
            "qty": row["qty"],
            "price": row["cost_price"],
            "trading_day": row["as_of_day"],
            # Sorts before any real fill on the same day.
            "traded_at": "00:00:00",
        }
        for row in conn.execute(sql, params)
    ]
