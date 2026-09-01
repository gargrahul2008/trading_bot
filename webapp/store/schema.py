"""Schema and connection handling.

Three agents write concurrently and the API reads while they do, so the
connection settings matter as much as the tables:

* **WAL** — readers never block on a writer, and a dashboard request never waits
  behind a poll.
* **busy_timeout** — three writers will occasionally collide; wait rather than
  raise, because losing a fill to a lock is losing it for good.
* **foreign_keys** — off by default in SQLite, which quietly turns a declared
  reference into a comment.
* **check_same_thread=False** — an agent opens its connection on the main thread
  and then writes from the poller's thread. sqlite3 refuses that by default, and
  because the writer swallows its own errors the refusal was invisible: the agent
  ran normally and stored nothing at all. Access is serialised by `Writer`'s lock
  on the write side, and the API opens a fresh connection per request on the read
  side, so the check is not protecting anything here.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Optional

SCHEMA_VERSION = 9

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
    -- The broker's own text on the order. On a reject it is the only thing that
    -- says why, and common/broker/reject_parser.py turns it into a cause.
    message       TEXT,
    channel       TEXT,               -- api | web: how the order was placed
    order_tag     TEXT,               -- the web control that fired it, e.g. 2:Exit
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

-- Money into and out of an account. The broker's balances say what is there
-- now; only this says what was put in, which is what every return figure is
-- measured against.
--
-- `reference` is the ledger row's own id, so re-importing a date range that
-- overlaps one already imported cannot double-count. Manual entries — capital
-- from before the ledger range, or an opening position's cost — carry a
-- reference the operator chooses, for the same reason.
CREATE TABLE IF NOT EXISTS capital (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account      TEXT NOT NULL,
    on_date      TEXT NOT NULL,          -- YYYY-MM-DD
    amount       REAL NOT NULL,          -- positive in, negative out
    source       TEXT NOT NULL,          -- ledger | manual
    reference    TEXT NOT NULL,
    note         TEXT,
    recorded_at  REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_capital ON capital (account, source, reference);
CREATE INDEX IF NOT EXISTS ix_capital_date ON capital (account, on_date);

-- Realised P&L as the broker computes it, per scrip per day.
--
-- The endpoint reports no date, but the window is a free parameter and it is
-- additive over it, so asking one day at a time recovers this. It is the
-- authoritative realised figure — our own matching supplies per-trade detail,
-- never the headline total, so the two can never double-count.
CREATE TABLE IF NOT EXISTS realised_history (
    account     TEXT NOT NULL,
    day         TEXT NOT NULL,
    symbol      TEXT NOT NULL,      -- symbol_name; the exchange fields lie
    realised    REAL NOT NULL DEFAULT 0,
    buy_qty     REAL NOT NULL DEFAULT 0,
    sell_qty    REAL NOT NULL DEFAULT 0,
    buy_rate    REAL NOT NULL DEFAULT 0,
    sell_rate   REAL NOT NULL DEFAULT 0,
    fetched_at  REAL NOT NULL,
    PRIMARY KEY (account, day, symbol)
);
CREATE INDEX IF NOT EXISTS ix_realised_day ON realised_history (account, day);

-- Charges per day. The broker reports these per day and per segment, never per
-- symbol, so this is the control total that per-trade charges are apportioned
-- against rather than guessed at.
CREATE TABLE IF NOT EXISTS charges_daily (
    account              TEXT NOT NULL,
    day                  TEXT NOT NULL,
    total                REAL NOT NULL DEFAULT 0,
    turnover             REAL NOT NULL DEFAULT 0,
    brokerage            REAL NOT NULL DEFAULT 0,
    stt                  REAL NOT NULL DEFAULT 0,
    gst                  REAL NOT NULL DEFAULT 0,
    stamp_duty           REAL NOT NULL DEFAULT 0,
    transaction_charges  REAL NOT NULL DEFAULT 0,
    sebi_toc             REAL NOT NULL DEFAULT 0,
    ipft                 REAL NOT NULL DEFAULT 0,
    fetched_at           REAL NOT NULL,
    PRIMARY KEY (account, day)
);

-- What each account already held before our fill history begins.
--
-- Without this the matcher cannot close a delivery sale: shares bought years
-- ago and sold today have no matching buy in the store, so a FIFO pass opens a
-- short lot instead of closing a long one — and the trade's P&L, which is the
-- sale against the holding's cost, never appears at all.
--
-- Seeded from the earliest holdings snapshot, or entered by hand for positions
-- older than any snapshot. `as_of_day` dates the synthetic opening buy, and
-- every recorded fill from that day onward applies on top of it.
CREATE TABLE IF NOT EXISTS opening_positions (
    account     TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    product_type TEXT NOT NULL DEFAULT 'CNC',
    qty         REAL NOT NULL,
    cost_price  REAL NOT NULL,
    as_of_day   TEXT NOT NULL,
    source      TEXT NOT NULL,          -- holdings_snapshot | manual
    note        TEXT,
    recorded_at REAL NOT NULL,
    PRIMARY KEY (account, symbol, product_type)
);

-- How far each account's history has been fetched, so a re-run picks up where
-- the last left off instead of replaying a hundred days of API calls.
CREATE TABLE IF NOT EXISTS history_progress (
    account     TEXT NOT NULL,
    kind        TEXT NOT NULL,      -- ledger | realised | charges
    from_date   TEXT NOT NULL,
    to_date     TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (account, kind)
);

-- The exchanges' own instrument list, refreshed daily.
--
-- Two things it settles that nothing else can. A symbol typed by hand is either
-- in here or it does not exist — a completion like NSE:RELIA-EQ looks right and
-- is not. And tick size is per instrument, not per account: 20MICRONS trades in
-- paise while 360ONE trades in ten-paise steps, so a price the exchange will
-- not accept is otherwise only discovered as a reject.
CREATE TABLE IF NOT EXISTS symbols (
    symbol      TEXT PRIMARY KEY,       -- NSE:RELIANCE-EQ
    name        TEXT NOT NULL DEFAULT '',
    short_name  TEXT NOT NULL DEFAULT '',
    exchange    TEXT NOT NULL DEFAULT '',
    segment     TEXT NOT NULL DEFAULT '',
    tick_size   REAL NOT NULL DEFAULT 0.05,
    lot_size    REAL NOT NULL DEFAULT 1,
    isin        TEXT,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_symbols_short ON symbols (short_name);

-- Holdings that are not really holdings.
--
-- A scrip that cannot be sold — suspended, delisted, written off — still sits
-- in the broker's book at its cost, and every ratio measured against deployed
-- capital is wrong by that much. Excluding it is a judgement, not a fact, so it
-- is recorded here with its reason rather than edited into the position data.
CREATE TABLE IF NOT EXISTS exclusions (
    account  TEXT NOT NULL,
    symbol   TEXT NOT NULL,
    reason   TEXT NOT NULL DEFAULT '',
    at       REAL NOT NULL,
    PRIMARY KEY (account, symbol)
);

-- What changed, and when.
--
-- Orders are upserted, so a status moving from PENDING to FILLED overwrites the
-- previous value and the change itself is lost. Nothing then tells you a
-- position closed while you were looking elsewhere — it is simply not there any
-- more, which is the difference between a dashboard and a record.
--
-- One row per transition, appended, never updated.
CREATE TABLE IF NOT EXISTS order_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    at           REAL NOT NULL,
    account      TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL DEFAULT '',
    kind         TEXT NOT NULL,          -- placed | filled | partial | cancelled | rejected | changed
    from_status  TEXT,
    to_status    TEXT,
    filled_qty   REAL NOT NULL DEFAULT 0,
    qty          REAL NOT NULL DEFAULT 0,
    price        REAL NOT NULL DEFAULT 0,
    source       TEXT,                   -- bot | manual
    run          TEXT,
    message      TEXT,
    trading_day  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_order_events_at ON order_events (at DESC);
CREATE INDEX IF NOT EXISTS ix_order_events_day ON order_events (trading_day, account);

-- Every order action taken through the dashboard.
--
-- Written BEFORE the broker is called, then updated with the outcome. That
-- ordering is the point: an action that crashed, timed out, or left the process
-- mid-call still leaves a record saying it was attempted. A log written only on
-- success is silent about exactly the cases worth investigating.
CREATE TABLE IF NOT EXISTS audit (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    at        REAL NOT NULL,
    action    TEXT NOT NULL,       -- place | modify | cancel | exit
    account   TEXT NOT NULL,
    summary   TEXT NOT NULL,       -- human-readable, e.g. "BUY 140 NSE:RELIANCE-EQ MTF @1465"
    detail    TEXT,                -- the request, as JSON
    result    TEXT NOT NULL,       -- pending | ok | error
    message   TEXT,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS ix_audit_at ON audit (at DESC);

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
        conn = sqlite3.connect(
            "file:%s?mode=ro" % path, uri=True,
            timeout=BUSY_TIMEOUT_MS / 1000, check_same_thread=False,
        )
    else:
        conn = sqlite3.connect(
            path, timeout=BUSY_TIMEOUT_MS / 1000, check_same_thread=False,
        )

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
