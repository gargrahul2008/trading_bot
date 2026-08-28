"""Turning agents' raw sections into the Overview screen.

Pure functions over the agents' JSON — no HTTP, no I/O — so the arithmetic that
decides what a number on the dashboard means is testable on its own.

Two things this file is careful about, both learned from the real payloads:

* A negative CNC equity position is stock **sold out of holdings**, not a short.
  Counting it as a short would show open risk that does not exist.
* A position's realised P&L covers the life of the trade, while the account
  level figure from funds is today's mark-to-market. They are reported
  separately and never added together.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

# Section ages worth surfacing on the account row.
WATCHED_SECTIONS = ("positions", "orders", "funds", "holdings")


def _rows(book: Dict[str, Any], section: str) -> List[Dict[str, Any]]:
    data = ((book or {}).get("sections") or {}).get(section, {}).get("data")
    return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []


def _section_meta(book: Dict[str, Any], section: str) -> Dict[str, Any]:
    return ((book or {}).get("sections") or {}).get(section) or {}


def _sum(rows: Iterable[Dict[str, Any]], key: str) -> float:
    return round(sum(float(row.get(key) or 0.0) for row in rows), 2)


def summarise_positions(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    open_rows = [row for row in rows if float(row.get("net_qty") or 0.0) != 0.0]
    # A delivery sale is stock on its way out of the holdings book, not a short
    # someone has to buy back.
    shorts = [
        row for row in open_rows
        if float(row.get("net_qty") or 0.0) < 0 and not row.get("delivery_sale")
    ]
    longs = [row for row in open_rows if float(row.get("net_qty") or 0.0) > 0]
    return {
        "open": len(open_rows),
        "long": len(longs),
        "short": len(shorts),
        "delivery_sales": len([row for row in open_rows if row.get("delivery_sale")]),
        "carried": len([row for row in open_rows if row.get("carried")]),
        "opened_today": len([row for row in open_rows if row.get("opened_today")]),
        "derivatives": len([row for row in open_rows if row.get("is_derivative")]),
        # Unrealised covers open rows only; realised includes rows closed today,
        # which is exactly where a flat row's remaining value lives.
        "unrealised": _sum(open_rows, "unrealised"),
        "realised": _sum(rows, "realised"),
    }


def summarise_holdings(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    held = [row for row in rows if row.get("is_open")]
    return {
        "count": len(held),
        "invested": _sum(held, "invested"),
        "market_value": _sum(held, "market_value"),
        "unrealised": _sum(held, "unrealised"),
        "sold_today": len(rows) - len(held),
    }


def summarise_orders(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_source: Dict[str, int] = {}
    for row in rows:
        by_source[str(row.get("source") or "unknown")] = (
            by_source.get(str(row.get("source") or "unknown"), 0) + 1
        )
    return {
        "total": len(rows),
        "open": len([row for row in rows if row.get("is_open")]),
        "rejected": len([row for row in rows if row.get("status") == "REJECTED"]),
        "by_source": by_source,
    }


def account_summary(
    account: str,
    book: Optional[Dict[str, Any]],
    health: Optional[Dict[str, Any]],
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """One row of the Overview table.

    An unreachable account still produces a row — named, flagged, with whatever
    is known. Dropping it would make a broken agent look like a closed account.

    `error` describes the *agent*, not the data: when the agent is unreachable
    but the store has its last book, the row is populated from that and the
    error rides along. Treating any error as "no data" threw away the fallback
    the store exists to provide.
    """
    if book is None:
        return {
            "account": account,
            "reachable": False,
            "live": False,
            "error": error or "no data from agent",
            "auth_ok": True,
            "from_store": False,
            "funds": None,
            "positions": None,
            "holdings": None,
            "orders": None,
            "sections": {},
        }

    positions = _rows(book, "positions")
    holdings = _rows(book, "holdings")
    orders = _rows(book, "orders")
    funds = _section_meta(book, "funds").get("data") or {}

    # Reachable means the agent answered. A row rebuilt from the store has data
    # but no live agent, and the screen must not imply otherwise.
    from_store = str((book or {}).get("source")) == "store"

    return {
        "account": account,
        "reachable": not from_store,
        # Always present, never optional: the UI decides how to render a row on
        # this, and a key that is sometimes absent is a branch someone forgets.
        "from_store": from_store,
        "live": bool((health or {}).get("live")) and not from_store,
        # An expired token makes every section fail identically. It needs its
        # own signal, because the fix is "refresh the token", not "wait".
        "auth_ok": bool((health or {}).get("auth_ok", True)),
        "phase": ((health or {}).get("poller") or {}).get("phase"),
        "allow_trading": bool((health or {}).get("allow_trading")),
        "error": None,
        "funds": {
            "available": float(funds.get("available") or 0.0),
            "utilised": float(funds.get("utilised") or 0.0),
            "total": float(funds.get("total") or 0.0),
            # Today's mark-to-market, per the broker. Not the same as the sum of
            # positions' realised, which covers each trade's whole life.
            "realised_today": float(funds.get("realised_pnl") or 0.0),
        },
        "positions": summarise_positions(positions),
        "holdings": summarise_holdings(holdings),
        "orders": summarise_orders(orders),
        "sections": {
            name: {
                "age_s": _section_meta(book, name).get("age_s"),
                "stale": _section_meta(book, name).get("stale"),
                "error": _section_meta(book, name).get("error"),
            }
            for name in WATCHED_SECTIONS
        },
    }


def totals(accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Roll-up across every account that answered.

    `accounts_missing` is part of the answer, not a footnote: a total that
    quietly omits an unreachable account is a wrong number presented as a right
    one.
    """
    # Keyed on having figures rather than on the agent being up: a row rebuilt
    # from the store has real numbers and belongs in the total. Only an account
    # we know nothing at all about is missing from it.
    live = [row for row in accounts if row.get("funds")]
    return {
        "accounts": len(accounts),
        "accounts_reporting": len(live),
        "accounts_missing": [row["account"] for row in accounts if not row.get("funds")],
        "accounts_from_store": [
            row["account"] for row in accounts if row.get("from_store")
        ],
        "available": round(sum(row["funds"]["available"] for row in live), 2),
        "utilised": round(sum(row["funds"]["utilised"] for row in live), 2),
        "realised_today": round(sum(row["funds"]["realised_today"] for row in live), 2),
        "positions_unrealised": round(sum(row["positions"]["unrealised"] for row in live), 2),
        "holdings_unrealised": round(sum(row["holdings"]["unrealised"] for row in live), 2),
        "holdings_value": round(sum(row["holdings"]["market_value"] for row in live), 2),
        "open_positions": sum(row["positions"]["open"] for row in live),
        "open_orders": sum(row["orders"]["open"] for row in live),
    }
