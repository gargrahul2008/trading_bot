"""Putting charges onto individual trades.

The broker reports charges **per day and per segment, never per symbol** — that
was confirmed against the live accounts, and the day-wise and segment-wise
reports reconcile to the paisa. So a per-trade charge cannot be looked up. It has
to be apportioned, and the honest way to do that is by turnover: a trade that was
a tenth of the day's turnover bore about a tenth of the day's charges.

Two rules keep that from becoming quiet fiction:

* **Every apportioned figure is marked estimated.** An exact number and an
  estimated one must never be summed without saying so.
* **Unknown is not zero.** A day with no charges data, or with no turnover to
  divide by, yields `None` — not a confident 0.00 that makes a trade look
  cheaper than it was.

A round trip touches two days, so it takes a slice of each: its entry's share of
the day it opened, plus its exit's share of the day it closed.

Charges are also **per account**. Pooling them across accounts and dividing by
the combined turnover charges one account for another's trading — an account
with no charges recorded came out costed at 364.16 the first time this ran. So
the key is (account, day) throughout.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, Iterable, List, Optional

D0 = Decimal("0")

# Charges are money, and these are apportioned estimates. Dividing a daily total
# by turnover yields 28 significant digits, which is precision the figure does
# not have — paise is the unit and the unit it is rounded to.
PAISE = Decimal("0.01")


def to_paise(value: Decimal) -> Decimal:
    return value.quantize(PAISE, rounding=ROUND_HALF_UP)


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value if value is not None else 0))
    except Exception:
        return D0


class DayCharges:
    """A day's charges and the turnover they were levied on."""

    __slots__ = ("total", "turnover")

    def __init__(self, total: Any, turnover: Any) -> None:
        self.total = _dec(total)
        self.turnover = _dec(turnover)

    @property
    def usable(self) -> bool:
        # Zero turnover cannot apportion anything, and dividing by it would
        # either crash or invent a number.
        return self.turnover > 0


def _key(account: Any, day: Any) -> tuple:
    return (str(account or ""), str(day or ""))


def _share(value: Decimal, day: Optional[DayCharges]) -> Optional[Decimal]:
    if day is None or not day.usable:
        return None
    return value / day.turnover * day.total


def charge_for(match: Any, by_day: Dict[str, DayCharges]) -> Optional[Decimal]:
    """This trade's share of the charges on the days it touched.

    `None` when either day is unknown — a trade cannot be reported net when half
    its cost is missing, and a partial figure presented as complete is worse than
    an honest gap.
    """
    qty = _dec(getattr(match, "qty", None))
    account = getattr(match, "account", None)
    entry = _share(qty * _dec(getattr(match, "entry_price", None)),
                   by_day.get(_key(account, getattr(match, "opened_day", None))))
    exit_ = _share(qty * _dec(getattr(match, "exit_price", None)),
                   by_day.get(_key(account, getattr(match, "closed_day", None))))
    if entry is None or exit_ is None:
        return None
    return to_paise(entry + exit_)


def load_day_charges(conn: Any, account: Optional[str] = None) -> Dict[tuple, DayCharges]:
    """Charges per account per day, keyed `(account, day)`.

    Never pooled across accounts: one account's charges have nothing to do with
    another's turnover, and pooling them silently costed an account that had no
    charges recorded at all.
    """
    sql = "SELECT account, day, total, turnover FROM charges_daily"
    params: List[Any] = []
    if account:
        sql += " WHERE account = ?"
        params.append(account)
    return {_key(row["account"], row["day"]): DayCharges(row["total"], row["turnover"])
            for row in conn.execute(sql, params)}


def net_matches(matches: Iterable[Any], by_day: Dict[str, DayCharges]) -> List[Dict[str, Any]]:
    """Each round trip with its gross, its apportioned charge and its net.

    `charges` and `net` are `None` where the charge could not be established, and
    `charges_estimated` says plainly that this figure was divided out of a daily
    total rather than read off a contract note.
    """
    out = []
    for match in matches:
        row = match.as_dict()
        charge = charge_for(match, by_day)
        gross = _dec(row.get("gross"))
        row["charges"] = None if charge is None else str(charge)
        row["net"] = None if charge is None else str(to_paise(gross - charge))
        row["charges_estimated"] = charge is not None
        out.append(row)
    return out


def summarise_net(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Totals for a set of trades, keeping what is known apart from what is not.

    `trades_without_charges` is part of the answer: a net total that silently
    omits the trades it could not cost is a smaller number pretending to be a
    complete one.
    """
    rows = list(rows)
    gross = sum((_dec(r.get("gross")) for r in rows), D0)
    costed = [r for r in rows if r.get("charges") is not None]
    charges = sum((_dec(r["charges"]) for r in costed), D0)
    return {
        "trades": len(rows),
        "gross": str(to_paise(gross)),
        "charges": str(to_paise(charges)),
        "net": str(to_paise(gross - charges)),
        "trades_costed": len(costed),
        "trades_without_charges": len(rows) - len(costed),
        "charges_estimated": bool(costed),
    }
