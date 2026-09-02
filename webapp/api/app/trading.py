"""Placing, changing and cancelling orders.

The only part of this dashboard that can move real money, so its rules are
structural rather than conventional:

* **The account is always explicit.** Never defaulted, never remembered from the
  last action. Six accounts on one screen is the whole point of this dashboard,
  and it is also exactly how an order ends up in the wrong one.
* **The audit row is written before the broker is called**, then updated with the
  outcome. An action that timed out or crashed still leaves a record saying it
  was attempted — a log written only on success is silent about precisely the
  cases worth investigating.
* **The agent decides whether trading is allowed at all.** It refuses unless it
  was started with `--allow-trading`, so a mistake here cannot place an order
  that the host was not deliberately configured to permit.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any, Dict, Optional

LOG = logging.getLogger("api.trading")

PLACE, MODIFY, CANCEL, EXIT = "place", "modify", "cancel", "exit"


def summarise(action: str, payload: Dict[str, Any]) -> str:
    """One line describing what was asked, for the audit log and the UI."""
    if action == PLACE:
        bits = [
            str(payload.get("side") or "?"),
            str(payload.get("qty") or "?"),
            str(payload.get("symbol") or "?"),
            str(payload.get("product_type") or "CNC"),
            str(payload.get("order_type") or "LIMIT"),
        ]
        if payload.get("limit_price"):
            bits.append("@%s" % payload["limit_price"])
        if payload.get("stop_price"):
            bits.append("trigger %s" % payload["stop_price"])
        if payload.get("stop_loss"):
            bits.append("SL %s pts" % payload["stop_loss"])
        if payload.get("take_profit"):
            bits.append("target %s pts" % payload["take_profit"])
        return " ".join(bits)
    if action == MODIFY:
        # A null stop_loss or take_profit cancels that leg — the one change here
        # most worth a log line, and the one a "drop the Nones" filter erased,
        # leaving "order X -> nothing" for an order that lost its stop.
        changes = ", ".join(
            "%s=%s" % (k, "cancelled" if v is None else v)
            for k, v in sorted(payload.items())
            if k != "order_id" and (v is not None or k in ("stop_loss", "take_profit"))
        )
        return "order %s -> %s" % (payload.get("order_id"), changes or "nothing")
    if action == CANCEL:
        return "cancel order %s" % payload.get("order_id")
    if action == EXIT:
        return "exit position %s" % payload.get("position_id")
    return action


def begin(conn: sqlite3.Connection, action: str, account: str,
          payload: Dict[str, Any]) -> Optional[int]:
    """Record the intent. Returns the audit id, or None if it could not be
    written — which never blocks the action itself: refusing to trade because a
    log is unavailable would be the wrong trade-off on a live account."""
    try:
        cursor = conn.execute(
            "INSERT INTO audit (at, action, account, summary, detail, result)"
            " VALUES (?,?,?,?,?,'pending')",
            (time.time(), action, account, summarise(action, payload),
             json.dumps(payload, default=str)),
        )
        conn.commit()
        return cursor.lastrowid
    except Exception as exc:
        LOG.warning("could not write audit row: %s", exc)
        return None


def finish(conn: sqlite3.Connection, audit_id: Optional[int], ok: bool,
           message: str = "") -> None:
    if audit_id is None:
        return
    try:
        conn.execute(
            "UPDATE audit SET result = ?, message = ?, finished_at = ? WHERE id = ?",
            ("ok" if ok else "error", message[:2000], time.time(), audit_id),
        )
        conn.commit()
    except Exception as exc:
        LOG.warning("could not close audit row %s: %s", audit_id, exc)


def recent(conn: sqlite3.Connection, limit: int = 50) -> list:
    return [
        {
            "id": row["id"], "at": row["at"], "action": row["action"],
            "account": row["account"], "summary": row["summary"],
            "result": row["result"], "message": row["message"],
        }
        for row in conn.execute(
            "SELECT id, at, action, account, summary, result, message"
            " FROM audit ORDER BY at DESC LIMIT ?", (limit,))
    ]
