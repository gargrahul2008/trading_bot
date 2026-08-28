"""The agent's side of the store.

Called after each successful poll. Two principles shape it:

**Persisting must never break polling.** A locked database or a full disk is a
reason to lose history, never a reason for the dashboard to go blank or for the
agent to stop reading the broker. Every entry point swallows its own errors and
counts them.

**Write on change, not on poll.** Positions are read every 3 seconds and change
a few times an hour. Storing each read would be 20 rows a minute per account of
almost entirely identical data; storing the changes is the same information.

"Change" has to mean the *position*, not the price. The first version hashed the
whole payload, and since `ltp` and `unrealised` move on every tick it wrote on
every poll after all — measured at 116 MB/day, 28 GB/year, on a host already
short of disk. The digest now ignores the mark-to-market fields, so a row is
written when quantity, average or the set of symbols changes. A periodic refresh
keeps the stored marks from going too stale for the dashboard's fallback view.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import sqlite3
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

LOG = logging.getLogger("store.writer")

# The trading day a row belongs to, in IST — the boundary that matters here is
# the market's, not UTC's.
IST_OFFSET = dt.timedelta(hours=5, minutes=30)

# Fields that move with the market rather than with the account. Excluded from
# the change comparison, but still stored — the dashboard needs the marks, it
# just does not need a copy of them every three seconds.
VOLATILE_FIELDS = {
    "positions": ("ltp", "unrealised", "total_pnl", "raw"),
    "holdings": ("ltp", "market_value", "unrealised", "raw"),
    "funds": (),
}

# Write a snapshot even when nothing structural changed, if the last one is
# older than this. Keeps the marks in the fallback view roughly current without
# storing a row per tick.
REFRESH_SECONDS = 300.0


def trading_day(when: Optional[float] = None) -> str:
    stamp = dt.datetime.utcfromtimestamp(time.time() if when is None else when) + IST_OFFSET
    return stamp.date().isoformat()


def _strip(payload: Any, volatile: Iterable[str]) -> Any:
    """The payload with its mark-to-market fields removed, for comparison only."""
    volatile = set(volatile)
    if isinstance(payload, list):
        return [
            {k: v for k, v in row.items() if k not in volatile} if isinstance(row, dict) else row
            for row in payload
        ]
    if isinstance(payload, dict):
        return {k: v for k, v in payload.items() if k not in volatile}
    return payload


def _digest(payload: Any, volatile: Iterable[str] = ()) -> str:
    return hashlib.sha256(
        json.dumps(_strip(payload, volatile), sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _compact(row: Dict[str, Any]) -> str:
    """The broker's untouched payload, kept so a field we did not map is still
    recoverable later."""
    return json.dumps(row.get("raw") or {}, default=str, separators=(",", ":"))


class Writer:
    def __init__(self, conn: sqlite3.Connection, account: str) -> None:
        self.conn = conn
        self.account = account
        self.errors = 0
        self.writes = 0
        self._digests: Dict[str, str] = {}
        self._written_at: Dict[str, float] = {}
        # The connection is opened with check_same_thread=False so the poller
        # thread can use one made on the main thread; this lock is what makes
        # that safe rather than merely permitted.
        self._lock = threading.Lock()

    # ── plumbing ────────────────────────────────────────────────────────────
    def _safe(self, what: str, fn, *args) -> bool:
        with self._lock:
            try:
                fn(*args)
                self.conn.commit()
                self.writes += 1
                return True
            except Exception as exc:
                self.errors += 1
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                # Deliberately not re-raised: the poller must keep polling.
                # That is right, but it is also why this class counts its
                # errors — a silent failure that never stops anything is one
                # nobody notices. `stats()` is surfaced on /health for exactly
                # this reason.
                LOG.warning("%s: could not persist %s: %s", self.account, what, exc)
                return False

    def seen(self) -> None:
        now = time.time()
        self._safe("account", lambda: self.conn.execute(
            "INSERT INTO accounts (account, first_seen, last_seen) VALUES (?, ?, ?) "
            "ON CONFLICT(account) DO UPDATE SET last_seen = excluded.last_seen",
            (self.account, now, now),
        ))

    # ── orders ──────────────────────────────────────────────────────────────
    def orders(self, rows: Iterable[Dict[str, Any]]) -> None:
        rows = [r for r in rows if r.get("order_id")]
        if not rows:
            return
        now = time.time()
        day = trading_day(now)

        def write():
            self.conn.executemany(
                "INSERT INTO orders (account, order_id, symbol, side, qty, filled_qty,"
                " limit_price, stop_price, traded_price, product_type, kind, status,"
                " status_code, is_open, source, run, matched_by, placed_at, trading_day,"
                " first_seen, updated_at, raw)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(account, order_id) DO UPDATE SET"
                "   filled_qty = excluded.filled_qty,"
                "   traded_price = excluded.traded_price,"
                "   status = excluded.status,"
                "   status_code = excluded.status_code,"
                "   is_open = excluded.is_open,"
                "   limit_price = excluded.limit_price,"
                # Attribution can firm up over time — an order matched by symbol
                # becomes matched by order id once the bot records its claim — so
                # let it improve, but never let a later poll blank a known run.
                "   source = COALESCE(excluded.source, orders.source),"
                "   run = COALESCE(excluded.run, orders.run),"
                "   matched_by = COALESCE(excluded.matched_by, orders.matched_by),"
                "   updated_at = excluded.updated_at,"
                "   raw = excluded.raw"
                " WHERE excluded.updated_at >= orders.updated_at",
                [
                    (
                        self.account, str(r["order_id"]), r.get("symbol", ""),
                        r.get("side", ""), float(r.get("qty") or 0),
                        float(r.get("filled_qty") or 0), float(r.get("limit_price") or 0),
                        float(r.get("stop_price") or 0), float(r.get("traded_price") or 0),
                        r.get("product_type", ""), r.get("kind", ""), r.get("status", ""),
                        r.get("status_code"), 1 if r.get("is_open") else 0,
                        r.get("source"), r.get("run"), r.get("matched_by"),
                        r.get("placed_at"), day, now, now, _compact(r),
                    )
                    for r in rows
                ],
            )

        self._safe("orders", write)

    # ── fills ───────────────────────────────────────────────────────────────
    def fills(self, rows: Iterable[Dict[str, Any]]) -> None:
        rows = [r for r in rows if r.get("trade_id")]
        if not rows:
            return
        now = time.time()
        day = trading_day(now)

        def write():
            # A trade never changes, and the broker returns the same ones all
            # session — so seeing one again is normal, not a conflict.
            self.conn.executemany(
                "INSERT OR IGNORE INTO fills (account, trade_id, order_id, symbol, side,"
                " qty, price, value, product_type, kind, traded_at, trading_day,"
                " recorded_at, raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        self.account, str(r["trade_id"]), str(r.get("order_id") or ""),
                        r.get("symbol", ""), r.get("side", ""), float(r.get("qty") or 0),
                        float(r.get("price") or 0), float(r.get("value") or 0),
                        r.get("product_type", ""), r.get("kind", ""), r.get("traded_at"),
                        day, now, _compact(r),
                    )
                    for r in rows
                ],
            )

        self._safe("fills", write)

    # ── snapshots ───────────────────────────────────────────────────────────
    def snapshot(self, kind: str, payload: Any) -> bool:
        """Store `payload` when the account has changed, or periodically.

        The comparison ignores mark-to-market fields (see VOLATILE_FIELDS), so a
        price tick is not a change — otherwise this writes on every poll, which
        is what it did before and cost 116 MB a day.

        Returns whether a row was written, so "changed" is something the caller
        can act on rather than a hidden detail.
        """
        now = time.time()
        digest = _digest(payload, VOLATILE_FIELDS.get(kind, ()))
        unchanged = self._digests.get(kind) == digest
        fresh = (now - self._written_at.get(kind, 0.0)) < REFRESH_SECONDS
        if unchanged and fresh:
            return False

        def write():
            self.conn.execute(
                "INSERT INTO snapshots (account, kind, taken_at, trading_day, digest, payload)"
                " VALUES (?,?,?,?,?,?)",
                (self.account, kind, now, trading_day(now), digest,
                 json.dumps(payload, default=str, separators=(",", ":"))),
            )

        if self._safe("%s snapshot" % kind, write):
            self._digests[kind] = digest
            self._written_at[kind] = now
            return True
        return False

    # ── agent status ────────────────────────────────────────────────────────
    def status(self, health: Dict[str, Any]) -> None:
        """The agent's last word about itself.

        This is what lets the API say "pratibha, as of four minutes ago" instead
        of "pratibha, unreachable, nothing known" when an agent is restarting.
        """
        now = time.time()

        def write():
            self.conn.execute(
                "INSERT INTO agent_status (account, updated_at, live, auth_ok, phase, payload)"
                " VALUES (?,?,?,?,?,?)"
                " ON CONFLICT(account) DO UPDATE SET"
                "   updated_at = excluded.updated_at, live = excluded.live,"
                "   auth_ok = excluded.auth_ok, phase = excluded.phase,"
                "   payload = excluded.payload",
                (
                    self.account, now,
                    1 if health.get("live") else 0,
                    1 if health.get("auth_ok", True) else 0,
                    (health.get("poller") or {}).get("phase"),
                    json.dumps(health, default=str, separators=(",", ":")),
                ),
            )

        self._safe("status", write)

    def stats(self) -> Dict[str, int]:
        return {"writes": self.writes, "errors": self.errors}
