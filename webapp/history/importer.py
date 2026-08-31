"""Loading a broker's history into the store.

Idempotent throughout. Re-running over a range already imported must change
nothing — that is the normal case, not an edge one: the sensible way to keep
history current is to re-fetch the last few days every evening, and a row
counted twice would be wrong in the denominator of every return on the page.

Which figure is authoritative matters here. The **broker's** realised P&L is the
headline; our own FIFO matching supplies per-trade detail. They are never added,
so they cannot double-count, and where both cover a period they can be compared
— a disagreement is a finding, not an inconvenience.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Callable, Dict, List, Optional

LOG = logging.getLogger("history.importer")

# Ledger rows that are money genuinely moving in or out of the account, rather
# than the day's trading being settled against it. Observed types on the live
# accounts: Trading, Non-trading, MTF, Funds added, Funds withdrawn.
CAPITAL_TYPES = ("funds added", "funds withdrawn")


def import_capital(conn: sqlite3.Connection, account: str,
                   ledger: Dict[str, Any]) -> int:
    """Money in and out, from `/ledger-history`.

    Only genuine transfers are capital. A `Trading` row is the day's P&L being
    settled into the balance — counting it as capital would make every rupee
    earned look like a rupee deposited, and the return figure would collapse
    towards zero.
    """
    from webapp.store.writer import Writer

    entries = []
    for row in ledger.get("transactions") or []:
        kind = str(row.get("transaction_type") or "").strip().lower()
        if kind not in CAPITAL_TYPES:
            continue
        amount = float(row.get("credit") or 0) - float(row.get("debit") or 0)
        if amount == 0 or not row.get("date"):
            continue
        entries.append({
            "on_date": row["date"],
            "amount": amount,
            "source": "ledger",
            # The ledger carries no row id, so the reference is built from what
            # identifies a transfer: its day, its size and its description.
            # Two identical transfers on one day are indistinguishable to the
            # API as well, so this loses nothing that was there.
            "reference": "%s|%s|%s" % (row["date"], amount, row.get("description") or ""),
            "note": row.get("description"),
        })
    return Writer(conn, account).capital(entries)


def import_realised(conn: sqlite3.Connection, account: str,
                    rows: List[Dict[str, Any]]) -> int:
    """Realised P&L per scrip per day.

    Keyed on `symbol_name`. The endpoint's own `exch_id`, `exchange_name` and
    `segment_name` contradict it on real rows — BSE:SHISHIND-X comes back as NSE
    — so they are not stored at all rather than stored wrong.
    """
    rows = [r for r in rows if r.get("symbol") and r.get("day")]
    if not rows:
        return 0
    now = time.time()
    conn.executemany(
        "INSERT INTO realised_history"
        " (account, day, symbol, realised, buy_qty, sell_qty, buy_rate, sell_rate, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(account, day, symbol) DO UPDATE SET"
        "   realised = excluded.realised, buy_qty = excluded.buy_qty,"
        "   sell_qty = excluded.sell_qty, buy_rate = excluded.buy_rate,"
        "   sell_rate = excluded.sell_rate, fetched_at = excluded.fetched_at",
        [
            (account, r["day"], r["symbol"], float(r.get("realised") or 0),
             float(r.get("buy_qty") or 0), float(r.get("sell_qty") or 0),
             float(r.get("buy_rate") or 0), float(r.get("sell_rate") or 0), now)
            for r in rows
        ],
    )
    conn.commit()
    return len(rows)


def import_charges(conn: sqlite3.Connection, account: str,
                   charges: Dict[str, Any]) -> int:
    """Charges per day, from the day-wise report."""
    rows = [r for r in (charges.get("rows") or []) if r.get("day")]
    if not rows:
        return 0
    now = time.time()
    conn.executemany(
        "INSERT INTO charges_daily"
        " (account, day, total, turnover, brokerage, stt, gst, stamp_duty,"
        "  transaction_charges, sebi_toc, ipft, fetched_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(account, day) DO UPDATE SET"
        "   total = excluded.total, turnover = excluded.turnover,"
        "   brokerage = excluded.brokerage, stt = excluded.stt, gst = excluded.gst,"
        "   stamp_duty = excluded.stamp_duty,"
        "   transaction_charges = excluded.transaction_charges,"
        "   sebi_toc = excluded.sebi_toc, ipft = excluded.ipft,"
        "   fetched_at = excluded.fetched_at",
        [
            (account, r["day"], float(r.get("total") or 0), float(r.get("turnover") or 0),
             float(r.get("brokerage") or 0), float(r.get("stt") or 0),
             float(r.get("gst") or 0), float(r.get("stamp_duty") or 0),
             float(r.get("transaction_charges") or 0), float(r.get("sebi_toc") or 0),
             float(r.get("ipft") or 0), now)
            for r in rows
        ],
    )
    conn.commit()
    return len(rows)


def record_progress(conn: sqlite3.Connection, account: str, kind: str,
                    from_date: str, to_date: str) -> None:
    """How far this account's history has been fetched.

    So a nightly run asks for the last few days rather than replaying a hundred
    days of API calls against a budget shared with live bots.
    """
    conn.execute(
        "INSERT INTO history_progress (account, kind, from_date, to_date, updated_at)"
        " VALUES (?,?,?,?,?)"
        " ON CONFLICT(account, kind) DO UPDATE SET"
        "   from_date = MIN(history_progress.from_date, excluded.from_date),"
        "   to_date = MAX(history_progress.to_date, excluded.to_date),"
        "   updated_at = excluded.updated_at",
        (account, kind, from_date, to_date, time.time()),
    )
    conn.commit()


def progress(conn: sqlite3.Connection, account: str) -> Dict[str, Dict[str, str]]:
    return {
        row["kind"]: {"from": row["from_date"], "to": row["to_date"]}
        for row in conn.execute(
            "SELECT kind, from_date, to_date FROM history_progress WHERE account = ?",
            (account,),
        )
    }


def realised_total(conn: sqlite3.Connection, account: Optional[str] = None,
                   from_date: Optional[str] = None,
                   to_date: Optional[str] = None) -> Dict[str, str]:
    """Gross realised, charges and net over a period — the broker's own figures.

    Returned as decimal strings: this is money, and it is the headline number on
    the portfolio page.
    """
    from decimal import Decimal

    def total(table: str, column: str) -> Decimal:
        sql = "SELECT %s FROM %s WHERE 1=1" % (column, table)
        params: List[Any] = []
        if account:
            sql += " AND account = ?"
            params.append(account)
        if from_date:
            sql += " AND day >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND day <= ?"
            params.append(to_date)
        return sum((Decimal(str(r[0])) for r in conn.execute(sql, params)), Decimal("0"))

    gross = total("realised_history", "realised")
    charges = total("charges_daily", "total")
    return {"gross": str(gross), "charges": str(charges), "net": str(gross - charges)}
