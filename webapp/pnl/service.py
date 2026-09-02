"""Matching what the store holds.

One rule governs every query here: **match over all history, then filter the
result.** A position opened last week and closed today is a today trade, and its
entry is only findable by replaying the fills that came before. Matching a
single day's fills in isolation would leave that exit unmatched and report a
day's P&L that is simply missing its positional trades.
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Dict, List, Optional

from webapp.pnl.matcher import Match, match_fills, open_position, summarise
from webapp.pnl.opening import as_fills as opening_fills
from webapp.store.reader import Reader


def _fills(conn: sqlite3.Connection, account: Optional[str] = None) -> List[Dict[str, Any]]:
    """Recorded fills, preceded by whatever the account already held.

    Without the opening positions a delivery sale has no buy to match against:
    FIFO opens a short lot, the trade's real P&L never appears, and what the
    screen shows instead is the broker's mark-to-market of a position that does
    not exist.
    """
    sql = ("SELECT account, trade_id, order_id, symbol, side, qty, price, product_type,"
           " traded_at, trading_day FROM fills")
    params: List[Any] = []
    if account:
        sql += " WHERE account = ?"
        params.append(account)
    return opening_fills(conn, account) + [dict(row) for row in conn.execute(sql, params)]


def matches(conn: sqlite3.Connection, account: Optional[str] = None,
            day: Optional[str] = None) -> List[Match]:
    """Closed round trips, optionally narrowed to one day's closes.

    `day` filters on `closed_day`, not on the fills read — see the module note.
    """
    closed, _ = match_fills(_fills(conn, account))
    if day:
        closed = [m for m in closed if m.closed_day == day]
    return closed


def report(conn: sqlite3.Connection, account: Optional[str] = None,
           day: Optional[str] = None) -> Dict[str, Any]:
    """Everything a P&L view needs: the trades, the totals, and what is open."""
    all_fills = _fills(conn, account)
    closed, books = match_fills(all_fills)

    shown = [m for m in closed if not day or m.closed_day == day]
    positions = []
    for (acct, symbol, product), lots in sorted(books.items()):
        position = open_position(lots)
        if position["qty"] == 0:
            continue
        positions.append({
            "account": acct, "symbol": symbol, "product_type": product,
            "qty": str(position["qty"]),
            "avg_price": str(position["avg_price"]),
            "direction": "LONG" if position["qty"] > 0 else "SHORT",
            # The oldest open parcel: how long this position has actually been
            # carried, which the broker's own view does not tell you.
            "opened_day": min(lot.day for lot in lots),
        })

    totals = summarise(shown)
    return {
        "account": account,
        "day": day,
        "fills_considered": len(all_fills),
        "trades": [m.as_dict() for m in shown],
        "totals": {
            "trades": totals["trades"],
            "gross": str(totals["gross"]),
            "intraday": str(totals["by_kind"]["intraday"]),
            "positional": str(totals["by_kind"]["positional"]),
            "long": str(totals["by_direction"]["LONG"]),
            "short": str(totals["by_direction"]["SHORT"]),
            "wins": totals["wins"],
            "losses": totals["losses"],
            "gross_wins": str(totals["gross_wins"]),
            "gross_losses": str(totals["gross_losses"]),
            "by_symbol": {k: str(v) for k, v in sorted(totals["by_symbol"].items())},
        },
        "open_positions": positions,
    }


def open_lots_by_position(conn: sqlite3.Connection,
                          account: Optional[str] = None) -> Dict[tuple, Dict[str, Any]]:
    """What is still open, keyed (account, symbol), with when it was entered.

    Keyed without the product type on purpose: the caller is joining this onto
    the broker's positions and holdings, which are separate books for the same
    scrip. The oldest parcel wins, so a name held in both is dated from whenever
    it was first bought.
    """
    _, books = match_fills(_fills(conn, account))
    out: Dict[tuple, Dict[str, Any]] = {}
    for (acct, symbol, _product), lots in books.items():
        if not lots or open_position(lots)["qty"] == 0:
            continue
        oldest = min(lots, key=lambda lot: (lot.day, lot.at or "", lot.trade_id))
        key = (acct, symbol)
        previous = out.get(key)
        if previous is None or (oldest.day, oldest.at or "") < (previous["opened_day"],
                                                               previous["opened_at"] or ""):
            out[key] = {"opened_day": oldest.day, "opened_at": oldest.at}
    return out


def days_available(conn: sqlite3.Connection, account: Optional[str] = None) -> List[str]:
    sql = "SELECT DISTINCT trading_day FROM fills"
    params: List[Any] = []
    if account:
        sql += " WHERE account = ?"
        params.append(account)
    sql += " ORDER BY trading_day DESC"
    return [row[0] for row in conn.execute(sql, params)]
