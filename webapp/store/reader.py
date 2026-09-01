"""The API's side of the store. Read-only by construction.

Its job is to answer the same questions the live agents answer, from what was
last written — so an agent restarting, or a host that has just booted, costs the
dashboard freshness rather than the whole account.

Everything returned carries `as_of`, because a figure from the store is by
definition not current and the screen has to say so.
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional

SNAPSHOT_KINDS = ("positions", "holdings", "funds")


def _loads(text: Optional[str], default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except ValueError:
        return default


class Reader:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # ── accounts ────────────────────────────────────────────────────────────
    def accounts(self) -> List[str]:
        return [r["account"] for r in
                self.conn.execute("SELECT account FROM accounts ORDER BY account")]

    def agent_status(self, account: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT updated_at, live, auth_ok, phase, payload FROM agent_status"
            " WHERE account = ?", (account,)
        ).fetchone()
        if row is None:
            return None
        health = _loads(row["payload"], {})
        health["as_of"] = row["updated_at"]
        health["age_s"] = max(time.time() - row["updated_at"], 0.0)
        # Whatever the stored health said about being live, it was live *then*.
        # The API decides what to call it now, from the age.
        health["from_store"] = True
        return health

    # ── snapshots ───────────────────────────────────────────────────────────
    def latest_snapshot(self, account: str, kind: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT taken_at, payload FROM snapshots WHERE account = ? AND kind = ?"
            " ORDER BY taken_at DESC LIMIT 1", (account, kind)
        ).fetchone()
        if row is None:
            return None
        return {
            "data": _loads(row["payload"], None),
            "as_of": row["taken_at"],
            "age_s": round(max(time.time() - row["taken_at"], 0.0), 2),
        }

    def book(self, account: str) -> Optional[Dict[str, Any]]:
        """The same shape an agent's /book returns, rebuilt from the store.

        Deliberately identical so the aggregation in app/overview.py does not
        need to know which source it is reading — a live book and a stored one
        differ only in their ages, which is exactly what the UI already shows.
        """
        sections: Dict[str, Any] = {}
        found = False

        for kind in SNAPSHOT_KINDS:
            snap = self.latest_snapshot(account, kind)
            if snap is None:
                sections[kind] = self._empty_section(kind)
                continue
            found = True
            sections[kind] = {
                "data": snap["data"],
                "as_of": snap["as_of"],
                "age_s": snap["age_s"],
                # Anything from the store is stale by definition: it is the last
                # thing written, not the current state of the account.
                "stale": True,
                "stale_after_s": 0.0,
                "source": "store",
                "error": None,
            }

        orders = self.orders_today(account)
        if orders:
            found = True
        sections["orders"] = {
            "data": orders,
            "as_of": max((o.pop("_updated_at") for o in orders), default=None),
            "age_s": None,
            "stale": True,
            "stale_after_s": 0.0,
            "source": "store",
            "error": None,
        }
        if sections["orders"]["as_of"]:
            sections["orders"]["age_s"] = round(
                max(time.time() - sections["orders"]["as_of"], 0.0), 2
            )

        if not found:
            return None
        return {"user": account, "sections": sections, "source": "store"}

    @staticmethod
    def _empty_section(kind: str) -> Dict[str, Any]:
        return {
            "data": {} if kind == "funds" else [],
            "as_of": None, "age_s": None, "stale": True,
            "stale_after_s": 0.0, "source": "store",
            "error": "nothing stored for %s yet" % kind,
        }

    # ── orders and fills ────────────────────────────────────────────────────
    def orders_today(self, account: str, day: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = ("SELECT * FROM orders WHERE account = ?"
               + (" AND trading_day = ?" if day else
                  " AND trading_day = (SELECT MAX(trading_day) FROM orders WHERE account = ?)")
               + " ORDER BY order_id")
        rows = self.conn.execute(sql, (account, day or account)).fetchall()
        return [self._order(r) for r in rows]

    @staticmethod
    def _order(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "order_id": row["order_id"],
            "symbol": row["symbol"],
            "side": row["side"],
            "qty": row["qty"],
            "filled_qty": row["filled_qty"],
            "remaining_qty": max(row["qty"] - row["filled_qty"], 0.0),
            "limit_price": row["limit_price"],
            "stop_price": row["stop_price"],
            "traded_price": row["traded_price"],
            "product_type": row["product_type"],
            "kind": row["kind"],
            "status": row["status"],
            "status_code": row["status_code"],
            "is_open": bool(row["is_open"]),
            "source": row["source"],
            "run": row["run"],
            "matched_by": row["matched_by"],
            "placed_at": row["placed_at"],
            "message": row["message"],
            "channel": row["channel"],
            "order_tag": row["order_tag"],
            "_updated_at": row["updated_at"],
        }

    def fills(self, account: str, day: Optional[str] = None,
              limit: int = 500) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM fills WHERE account = ?"
        params: List[Any] = [account]
        if day:
            sql += " AND trading_day = ?"
            params.append(day)
        sql += " ORDER BY recorded_at DESC LIMIT ?"
        params.append(limit)
        return [
            {
                "trade_id": r["trade_id"], "order_id": r["order_id"], "symbol": r["symbol"],
                "side": r["side"], "qty": r["qty"], "price": r["price"], "value": r["value"],
                "product_type": r["product_type"], "kind": r["kind"],
                "traded_at": r["traded_at"], "trading_day": r["trading_day"],
            }
            for r in self.conn.execute(sql, params)
        ]

    def capital_in(self, account: str, upto: Optional[str] = None) -> str:
        """Net money put into an account, as a decimal string.

        Returned as a string rather than a float: it is the denominator of every
        return figure on the page, and SQLite's SUM over REAL would introduce an
        error the rest of the pipeline is careful to avoid.
        """
        sql = "SELECT amount FROM capital WHERE account = ?"
        params: List[Any] = [account]
        if upto:
            sql += " AND on_date <= ?"
            params.append(upto)
        from decimal import Decimal
        total = sum((Decimal(str(row[0])) for row in self.conn.execute(sql, params)),
                    Decimal("0"))
        return str(total)

    def capital_entries(self, account: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT on_date, amount, source, reference, note FROM capital"
            " WHERE account = ? ORDER BY on_date, id", (account,))]

    def counts(self) -> Dict[str, int]:
        """What the store actually holds — for /api/health, so 'the dashboard is
        empty' can be told apart from 'nothing has been written yet'."""
        out = {}
        for table in ("accounts", "orders", "fills", "snapshots", "capital"):
            out[table] = self.conn.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]
        return out
