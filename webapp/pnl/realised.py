"""Realised P&L for the whole financial year, as the broker computes it.

Two sources answer "what did I make", and they are not rivals:

* **This one** — `/realised-pnl-history`, per scrip per day, from 1 April. It is
  complete and exact: it covers shares bought years ago and sold in May, shares
  bought and sold in June, everything. It is the figure the Portfolio page
  leads with.
* **`matcher.py`** — our own FIFO over recorded fills, from the day the agents
  started. It gives per-*trade* detail the broker never provides: which entry
  closed against which exit, held how long, long or short.

They are never added. Where both cover a period they can be compared, and a
disagreement is a finding.

Charges are apportioned here more precisely than at trade level, because this
endpoint reports each scrip's own bought and sold value for the day. A scrip
that was a tenth of the day's turnover took a tenth of the day's charges — from
the broker's own quantities rather than from a reconstruction.
"""
from __future__ import annotations

import sqlite3
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional

D0 = Decimal("0")
PAISE = Decimal("0.01")


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value if value is not None else 0))
    except Exception:
        return D0


def _paise(value: Decimal) -> Decimal:
    return value.quantize(PAISE, rounding=ROUND_HALF_UP)


def scrip_turnover(row: Dict[str, Any]) -> Decimal:
    """What this scrip traded that day, both sides.

    The endpoint gives buy and sell quantities and average rates, so the value
    is the broker's own, not an estimate from our fills.
    """
    return (_dec(row.get("buy_qty")) * _dec(row.get("buy_rate"))
            + _dec(row.get("sell_qty")) * _dec(row.get("sell_rate")))


def by_scrip(conn: sqlite3.Connection, account: Optional[str] = None,
             from_date: Optional[str] = None,
             to_date: Optional[str] = None) -> Dict[str, Any]:
    """Realised P&L per (account, scrip) over a window, net of charges."""
    sql = ("SELECT account, day, symbol, realised, buy_qty, sell_qty, buy_rate, sell_rate"
           " FROM realised_history WHERE 1=1")
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
    rows = [dict(r) for r in conn.execute(sql, params)]

    # Charges are per account per day; turnover is what divides them.
    charges: Dict[tuple, Decimal] = {}
    for row in conn.execute("SELECT account, day, total FROM charges_daily"):
        charges[(row["account"], row["day"])] = _dec(row["total"])

    # The day's turnover as this endpoint sees it — the sum of its own scrips.
    # Using the charges report's turnover instead would mix two definitions and
    # leave a residue that belongs to neither.
    day_turnover: Dict[tuple, Decimal] = {}
    for row in rows:
        key = (row["account"], row["day"])
        day_turnover[key] = day_turnover.get(key, D0) + scrip_turnover(row)

    scrips: Dict[tuple, Dict[str, Any]] = {}
    uncosted = 0
    for row in rows:
        key = (row["account"], row["symbol"])
        entry = scrips.setdefault(key, {
            "account": row["account"], "symbol": row["symbol"],
            "gross": D0, "charges": D0, "days": 0, "costed": True,
        })
        entry["gross"] += _dec(row["realised"])
        entry["days"] += 1

        day_key = (row["account"], row["day"])
        total = charges.get(day_key)
        turnover = day_turnover.get(day_key, D0)
        if total is None or turnover <= 0:
            # No charges recorded for that day, so this scrip's share is
            # unknown. Marked rather than treated as zero.
            entry["costed"] = False
            uncosted += 1
            continue
        entry["charges"] += scrip_turnover(row) / turnover * total

    out = []
    for entry in scrips.values():
        gross = _paise(entry["gross"])
        charge = _paise(entry["charges"])
        out.append({
            "account": entry["account"],
            "symbol": entry["symbol"],
            "gross": str(gross),
            "charges": str(charge) if entry["costed"] else None,
            "net": str(_paise(gross - charge)) if entry["costed"] else None,
            "days": entry["days"],
            "charges_estimated": entry["costed"],
        })
    out.sort(key=lambda r: float(r["net"] or r["gross"]), reverse=True)

    gross_total = sum((_dec(r["gross"]) for r in out), D0)
    charge_total = sum((_dec(r["charges"]) for r in out if r["charges"] is not None), D0)
    return {
        "scrips": out,
        "totals": {
            "scrips": len(out),
            "gross": str(_paise(gross_total)),
            "charges": str(_paise(charge_total)),
            "net": str(_paise(gross_total - charge_total)),
            "scrips_without_charges": sum(1 for r in out if r["charges"] is None),
            "days_without_charges": uncosted,
        },
        "available": bool(out),
    }
