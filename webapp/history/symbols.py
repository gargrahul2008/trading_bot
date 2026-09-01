"""The exchanges' instrument list.

Fyers publishes it as a plain CSV per segment, with no authentication and no
rate limit worth worrying about — so this is the one thing in the dashboard that
does not go through an account's agent.

It settles two questions nothing else can:

* **Does this symbol exist?** A completion like `NSE:RELIA-EQ` looks right and is
  not. With a one-click order pad, that guess would reach the broker.
* **What is its tick size?** It is per instrument, not per account: 20MICRONS
  trades in paise, 360ONE in ten-paise steps. A price off the tick is rejected,
  and without this the only way to find out is to have the order refused.

The file has **no header row**, so the columns are addressed by position. They
are recorded here because a positional read of someone else's CSV is exactly the
thing that breaks silently when the format shifts — `verify_row` is the guard.
"""
from __future__ import annotations

import csv
import io
import logging
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

LOG = logging.getLogger("history.symbols")

BASE_URL = "https://public.fyers.in/sym_details"

# segment file -> (exchange, segment) as we label them.
SEGMENTS = {
    "NSE_CM.csv": ("NSE", "CASH"),
    "BSE_CM.csv": ("BSE", "CASH"),
    "NSE_FO.csv": ("NSE", "FO"),
}

# Column positions in the published CSV, which carries no header.
COL_FYTOKEN = 0
COL_NAME = 1
COL_LOT = 3
COL_TICK = 4
COL_ISIN = 5
COL_SYMBOL = 9
COL_SHORT = 13
MIN_COLUMNS = 14


class SymbolFormatError(RuntimeError):
    pass


def verify_row(row: List[str]) -> None:
    """Fail loudly if the CSV is not shaped the way these positions assume.

    A positional read of someone else's file breaks silently when they add a
    column: every symbol would still import, just with the tick size taken from
    whatever now sits in that slot. Better to import nothing.
    """
    if len(row) < MIN_COLUMNS:
        raise SymbolFormatError(
            "expected at least %d columns, got %d — the published format has changed"
            % (MIN_COLUMNS, len(row))
        )
    symbol = row[COL_SYMBOL]
    if ":" not in symbol:
        raise SymbolFormatError(
            "column %d should hold a symbol like NSE:RELIANCE-EQ, got %r"
            % (COL_SYMBOL, symbol[:40])
        )
    try:
        float(row[COL_TICK])
    except (TypeError, ValueError):
        raise SymbolFormatError(
            "column %d should hold a tick size, got %r" % (COL_TICK, row[COL_TICK][:20])
        )


def parse(text: str, exchange: str, segment: str) -> List[Dict[str, Any]]:
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return []
    verify_row(rows[0])

    out = []
    for row in rows:
        if len(row) < MIN_COLUMNS or ":" not in row[COL_SYMBOL]:
            continue
        try:
            tick = float(row[COL_TICK])
            lot = float(row[COL_LOT] or 1)
        except (TypeError, ValueError):
            continue
        out.append({
            "symbol": row[COL_SYMBOL].strip(),
            "name": row[COL_NAME].strip(),
            "short_name": row[COL_SHORT].strip(),
            "exchange": exchange,
            "segment": segment,
            # A zero tick would let any price through, which is the failure this
            # exists to prevent.
            "tick_size": tick if tick > 0 else 0.05,
            "lot_size": lot if lot > 0 else 1,
            "isin": (row[COL_ISIN] or "").strip() or None,
        })
    return out


def fetch(segment_file: str, get: Optional[Any] = None) -> str:
    if get is None:
        import requests

        get = requests.get
    response = get("%s/%s" % (BASE_URL, segment_file), timeout=60)
    response.raise_for_status()
    return response.text


def store(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    now = time.time()
    conn.executemany(
        "INSERT INTO symbols (symbol, name, short_name, exchange, segment,"
        " tick_size, lot_size, isin, updated_at) VALUES (?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(symbol) DO UPDATE SET name = excluded.name,"
        "  short_name = excluded.short_name, exchange = excluded.exchange,"
        "  segment = excluded.segment, tick_size = excluded.tick_size,"
        "  lot_size = excluded.lot_size, isin = excluded.isin,"
        "  updated_at = excluded.updated_at",
        [(r["symbol"], r["name"], r["short_name"], r["exchange"], r["segment"],
          r["tick_size"], r["lot_size"], r["isin"], now) for r in rows],
    )
    conn.commit()
    return len(rows)


def search(conn: sqlite3.Connection, query: str, limit: int = 12) -> List[Dict[str, Any]]:
    """Symbols matching a fragment, best match first.

    Ranked the way an Indian equity trader means it: an exact short name beats a
    prefix beats a substring; NSE beats BSE; the plain -EQ series beats the
    lettered ones. Without the last two, typing RELI surfaced BSE:RELICAB-B and
    BSE:RELIANCE-A before NSE:RELIANCE-EQ, purely because their symbols are
    shorter.
    """
    text = (query or "").strip().upper()
    if not text:
        return []
    like = "%" + text + "%"
    rows = conn.execute(
        "SELECT symbol, name, short_name, tick_size, lot_size, exchange FROM symbols"
        " WHERE short_name LIKE ? OR symbol LIKE ? OR name LIKE ?"
        " ORDER BY"
        "   CASE WHEN short_name = ? THEN 0"
        "        WHEN short_name LIKE ? THEN 1"
        "        WHEN symbol LIKE ? THEN 2 ELSE 3 END,"
        "   CASE WHEN exchange = 'NSE' THEN 0 ELSE 1 END,"
        "   CASE WHEN symbol LIKE '%-EQ' THEN 0 ELSE 1 END,"
        "   LENGTH(short_name), symbol LIMIT ?",
        (like, like, like, text, text + "%", text + "%", limit),
    )
    return [dict(r) for r in rows]


def lookup(conn: sqlite3.Connection, symbol: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT symbol, name, short_name, tick_size, lot_size, exchange, segment"
        " FROM symbols WHERE symbol = ?", (symbol,)
    ).fetchone()
    return dict(row) if row else None


def counts(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT COUNT(*) AS n, MAX(updated_at) AS updated FROM symbols").fetchone()
    return {"symbols": row["n"], "updated_at": row["updated"]}
