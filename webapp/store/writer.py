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
    seconds = time.time() if when is None else when
    # Timezone-aware: utcfromtimestamp is deprecated on the host's Python 3.12,
    # and datetime.UTC does not exist on 3.9.
    stamp = dt.datetime.fromtimestamp(seconds, dt.timezone.utc) + IST_OFFSET
    return stamp.date().isoformat()


def day_of(row: Dict[str, Any], default: str) -> str:
    """The day a row belongs to.

    Live polling only ever sees today's book, so `default` (now) is right for it.
    But a row that carries its own day must keep it — otherwise a backfill of
    last week's trades would all be stamped today, and every one of them would
    be classified intraday because its entry and exit share a date.
    """
    day = str(row.get("trading_day") or "").strip()
    return day if day else default


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
    def _transitions(self, rows: List[Dict[str, Any]], now: float,
                     day: str) -> List[tuple]:
        """What changed since the last poll, as rows for order_events.

        Derived from the store rather than from memory, so an agent restart does
        not lose the comparison — and so a transition that happened while no
        agent was running is still noticed on the next poll.
        """
        previous = {
            r["order_id"]: (r["status"], r["filled_qty"])
            for r in self.conn.execute(
                "SELECT order_id, status, filled_qty FROM orders WHERE account = ?",
                (self.account,))
        }

        events = []
        for row in rows:
            oid = str(row["order_id"])
            status = str(row.get("status") or "")
            filled = float(row.get("filled_qty") or 0)
            was = previous.get(oid)

            if was is None:
                kind, from_status = "placed", None
            else:
                from_status, was_filled = was[0], float(was[1] or 0)
                if status == from_status and filled == was_filled:
                    continue
                if status == "FILLED":
                    kind = "filled"
                elif status == "REJECTED":
                    kind = "rejected"
                elif status == "CANCELLED":
                    kind = "cancelled"
                elif filled > was_filled:
                    kind = "partial"
                else:
                    kind = "changed"

            events.append((
                now, self.account, oid, row.get("symbol", ""), row.get("side", ""),
                kind, from_status, status, filled, float(row.get("qty") or 0),
                float(row.get("traded_price") or row.get("limit_price") or 0),
                row.get("source"), row.get("run"), row.get("message"),
                day_of(row, day),
            ))
        return events

    def orders(self, rows: Iterable[Dict[str, Any]]) -> None:
        rows = [r for r in rows if r.get("order_id")]
        if not rows:
            return
        now = time.time()
        day = trading_day(now)

        # Recorded before the upsert overwrites what it is compared against.
        try:
            events = self._transitions(rows, now, day)
        except Exception as exc:
            LOG.warning("%s: could not derive order events: %s", self.account, exc)
            events = []

        def write():
            self.conn.executemany(
                "INSERT INTO orders (account, order_id, symbol, side, qty, filled_qty,"
                " limit_price, stop_price, traded_price, product_type, kind, status,"
                " status_code, is_open, source, run, matched_by, placed_at, message,"
                " channel, order_tag, trading_day, first_seen, updated_at, raw)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
                "   message = COALESCE(excluded.message, orders.message),"
                "   channel = COALESCE(excluded.channel, orders.channel),"
                "   order_tag = COALESCE(excluded.order_tag, orders.order_tag),"
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
                        r.get("placed_at"), r.get("message") or None,
                        r.get("channel") or None, r.get("order_tag") or None,
                        day_of(r, day), now, now, _compact(r),
                    )
                    for r in rows
                ],
            )

        self._safe("orders", write)

        if events:
            self._safe("order events", lambda: self.conn.executemany(
                "INSERT INTO order_events (at, account, order_id, symbol, side, kind,"
                " from_status, to_status, filled_qty, qty, price, source, run, message,"
                " trading_day) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", events))

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
                        day_of(r, day), now, _compact(r),
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

    # ── capital ─────────────────────────────────────────────────────────────
    def capital(self, entries: Iterable[Dict[str, Any]]) -> int:
        """Record money in or out. Idempotent on (account, source, reference).

        Re-importing an overlapping date range is the normal case, not an edge
        one — so a repeated ledger row must not be counted twice. Returns how
        many rows were new.
        """
        entries = [e for e in entries if e.get("reference") and e.get("on_date")]
        if not entries:
            return 0
        now = time.time()
        before = self.conn.execute("SELECT COUNT(*) FROM capital").fetchone()[0]

        def write():
            self.conn.executemany(
                "INSERT OR IGNORE INTO capital"
                " (account, on_date, amount, source, reference, note, recorded_at)"
                " VALUES (?,?,?,?,?,?,?)",
                [
                    (self.account, str(e["on_date"]), float(e.get("amount") or 0),
                     str(e.get("source") or "manual"), str(e["reference"]),
                     e.get("note"), now)
                    for e in entries
                ],
            )

        if not self._safe("capital", write):
            return 0
        return self.conn.execute("SELECT COUNT(*) FROM capital").fetchone()[0] - before

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
