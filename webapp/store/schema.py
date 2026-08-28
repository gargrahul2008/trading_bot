"""Schema and connection handling.

Three agents write concurrently and the API reads while they do, so the
connection settings matter as much as the tables:

* **WAL** — readers never block on a writer, and a dashboard request never waits
  behind a poll.
* **busy_timeout** — three writers will occasionally collide; wait rather than
  raise, because losing a fill to a lock is losing it for good.
* **foreign_keys** — off by default in SQLite, which quietly turns a declared
  reference into a comment.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Optional

SCHEMA_VERSION = 1

DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "dashboard.db"
)

BUSY_TIMEOUT_MS = 5000

DDL = """
-- One row per account the dashboard knows about.
CREATE TABLE IF NOT EXISTS accounts (
    account     TEXT PRIMARY KEY,
    first_seen  REAL NOT NULL,
    last_seen   REAL NOT NULL
);

-- Every order the broker has reported, whatever placed it. Upserted: an order
-- is one row whose status moves, not a row per poll.
CREATE TABLE IF NOT EXISTS orders (
    account       TEXT NOT NULL,
    order_id      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL,
    qty           REAL NOT NULL DEFAULT 0,
    filled_qty    REAL NOT NULL DEFAULT 0,
    limit_price   REAL NOT NULL DEFAULT 0,
    stop_price    REAL NOT NULL DEFAULT 0,
    traded_price  REAL NOT NULL DEFAULT 0,
    product_type  TEXT NOT NULL DEFAULT '',
    kind          TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT '',
    status_code   INTEGER,
    is_open       INTEGER NOT NULL DEFAULT 0,
    source        TEXT,           -- bot | manual | pending
    run           TEXT,           -- e.g. rahul/reliance
    matched_by    TEXT,           -- order_id | symbol
    placed_at     TEXT,
    trading_day   TEXT NOT NULL,
    first_seen    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    raw           TEXT,
    PRIMARY KEY (account, order_id)
);
CREATE INDEX IF NOT EXISTS ix_orders_day ON orders (trading_day, account);
CREATE INDEX IF NOT EXISTS ix_orders_symbol ON orders (account, symbol);

-- Every execution. Insert-once: a trade never changes, and the broker keeps
-- returning it all session.
CREATE TABLE IF NOT EXISTS fills (
    account       TEXT NOT NULL,
    trade_id      TEXT NOT NULL,
    order_id      TEXT NOT NULL DEFAULT '',
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL,
    qty           REAL NOT NULL DEFAULT 0,
    price         REAL NOT NULL DEFAULT 0,
    value         REAL NOT NULL DEFAULT 0,
    product_type  TEXT NOT NULL DEFAULT '',
    kind          TEXT NOT NULL DEFAULT '',
    traded_at     TEXT,
    trading_day   TEXT NOT NULL,
    recorded_at   REAL NOT NULL,
    raw           TEXT,
    PRIMARY KEY (account, trade_id)
);
CREATE INDEX IF NOT EXISTS ix_fills_day ON fills (trading_day, account);
CREATE INDEX IF NOT EXISTS ix_fills_symbol ON fills (account, symbol);

-- Positions, holdings and funds as they were at a moment. Written only when the
-- content changes, so an idle account costs one row a day rather than 20 a
-- minute. `digest` is what makes that comparison cheap.
CREATE TABLE IF NOT EXISTS snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account      TEXT NOT NULL,
    kind         TEXT NOT NULL,     -- positions | holdings | funds
    taken_at     REAL NOT NULL,
    trading_day  TEXT NOT NULL,
    digest       TEXT NOT NULL,
    payload      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_snapshots_latest ON snapshots (account, kind, taken_at DESC);
CREATE INDEX IF NOT EXISTS ix_snapshots_day ON snapshots (trading_day, account, kind);

-- The agent's own last word on each account: what the API falls back to when the
-- agent itself cannot be reached.
CREATE TABLE IF NOT EXISTS agent_status (
    account     TEXT PRIMARY KEY,
    updated_at  REAL NOT NULL,
    live        INTEGER NOT NULL DEFAULT 0,
    auth_ok     INTEGER NOT NULL DEFAULT 1,
    phase       TEXT,
    payload     TEXT NOT NULL
);
"""


def connect(path: Optional[str] = None, *, read_only: bool = False) -> sqlite3.Connection:
    """Open the store, creating its directory if needed.

    `read_only` is a real guarantee, not a convention — the API should not be
    able to write even by accident.
    """
    path = path or os.getenv("DASHBOARD_DB") or DEFAULT_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if read_only and os.path.exists(path):
        conn = sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=BUSY_TIMEOUT_MS / 1000)
    else:
        conn = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000)

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = %d" % BUSY_TIMEOUT_MS)
    conn.execute("PRAGMA foreign_keys = ON")
    if not read_only:
        # WAL survives the connection, but setting it needs a writable handle.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def migrate(conn: sqlite3.Connection) -> int:
    """Bring the schema up to date. Safe to call on every start."""
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            "database is at schema version %d but this code knows %d — "
            "an older process would corrupt it" % (current, SCHEMA_VERSION)
        )
    conn.executescript(DDL)
    if current != SCHEMA_VERSION:
        conn.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
    conn.commit()
    return SCHEMA_VERSION
