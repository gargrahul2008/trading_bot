"""One row per position, not one row per fill.

A hundred-share order fills in whatever pieces the exchange gives it — 1, then
16, then 18 — and FIFO matches each piece against whatever it closes. Both are
correct and neither is readable: the same trade appears as a dozen lines that
have to be added up by eye before they mean anything.

Everything here answers the same question in two directions. What is still open,
as one line per scrip per account with the price actually paid across it. And
what was closed, as one line per scrip per day with the price actually got.

Two groupings are kept apart deliberately rather than netted:

* **Long and short in the same scrip.** Being long 900 in the delivery book and
  short 100 intraday is a hedge, not a position of 800. Netting it would report
  less risk than exists, so mixed directions stay as separate lines.
* **Intraday and positional.** They are taxed differently and cost differently,
  and a day's Reliance trading that was partly each is two facts, not one.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

D0 = Decimal("0")

#: A weighted average divides, so it produces as many digits as Decimal will
#: give. Prices are quoted in paise and percentages read to two places; the rest
#: is noise that makes a table unreadable.
PRICE = Decimal("0.0001")
PERCENT = Decimal("0.01")


def _round(value: Optional[Decimal], to: Decimal) -> Optional[Decimal]:
    return None if value is None else value.quantize(to)


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return D0


def _weighted(pairs: List[tuple]) -> Decimal:
    """Average price, weighted by quantity.

    The plain mean of a 1-share fill and a 99-share fill is not the price paid,
    and on a laddered entry it is not close to it.
    """
    total_qty = sum((qty for qty, _ in pairs), D0)
    if total_qty == 0:
        return D0
    return sum((qty * price for qty, price in pairs), D0) / total_qty


def _pct(pnl: Optional[Decimal], cost: Decimal) -> Optional[Decimal]:
    """Return on what it cost. None, not zero, when there is nothing to divide
    by — a position with no cost basis has no return, and 0% would read as one."""
    if pnl is None or cost == 0:
        return None
    return pnl / cost * Decimal("100")


def closed(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Matched round trips, collapsed to one line per scrip per day.

    Grouped by direction and by intraday/positional as well as by scrip and day,
    so nothing that is genuinely two different things is added together.
    """
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("account"), row.get("symbol"), row.get("closed_day"),
               row.get("kind"), row.get("direction"))
        groups.setdefault(key, []).append(row)

    out = []
    for (account, symbol, day, kind, direction), members in groups.items():
        entries = [(_dec(m.get("qty")), _dec(m.get("entry_price"))) for m in members]
        exits = [(_dec(m.get("qty")), _dec(m.get("exit_price"))) for m in members]
        qty = sum((q for q, _ in entries), D0)
        entry_price = _weighted(entries)
        gross = sum((_dec(m.get("gross")) for m in members), D0)

        # Charges are apportioned per account per day, so every member of a
        # group shares one day's rate: they are known together or not at all.
        # None stays None — an unknown cost is not a zero one.
        unknown = any(m.get("charges") is None for m in members)
        charges = None if unknown else sum((_dec(m.get("charges")) for m in members), D0)
        net = None if charges is None else gross - charges
        cost = qty * entry_price

        out.append({
            "state": "closed",
            "account": account, "symbol": symbol, "direction": direction,
            "trade_kind": kind, "day": day,
            "product_type": members[0].get("product_type"),
            "qty": str(qty),
            "entry_price": str(_round(entry_price, PRICE)),
            "exit_price": str(_round(_weighted(exits), PRICE)),
            "gross": str(gross),
            "charges": None if charges is None else str(charges),
            "net": None if net is None else str(net),
            "pct": _as_str(_round(_pct(net if net is not None else gross, cost), PERCENT)),
            # What it is sorted by. The latest exit in the group, so a scrip
            # traded twice in a day sits where its last trade puts it.
            "at": max(str(m.get("closed_at") or "") for m in members),
            "fills": len(members),
        })
    return out


def open_positions(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Live positions, collapsed to one line per scrip per account.

    The broker keeps settled delivery stock in holdings and everything else in
    positions, so buying more of something already owned shows up as two rows
    for one position. They are the same money and become one line, at the
    average actually paid across both.
    """
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in rows:
        qty = _dec(row.get("net_qty"))
        if qty == 0 or row.get("delivery_sale"):
            continue
        # Direction is part of the key: long in one book and short in another is
        # a hedge, and netting it would report less risk than is actually held.
        groups.setdefault(
            (row.get("account"), row.get("symbol"), "LONG" if qty > 0 else "SHORT"),
            []).append(row)

    out = []
    for (account, symbol, direction), members in groups.items():
        prices = [(abs(_dec(m.get("net_qty"))),
                   _dec(m.get("avg_price")) or _dec(m.get("ltp"))) for m in members]
        qty = sum((q for q, _ in prices), D0)
        entry_price = _weighted(prices)
        unrealised = sum((_dec(m.get("unrealised")) for m in members), D0)
        cost = qty * entry_price

        # The oldest entry across the group: adding to a position does not make
        # it new, and the delivery half is usually the older one.
        days = sorted(str(m.get("opened_day") or "") for m in members if m.get("opened_day"))

        out.append({
            "state": "open",
            "account": account, "symbol": symbol, "direction": direction,
            "trade_kind": None,
            "day": days[0] if days else None,
            # Named where a position spans both books, because "which book" is
            # the difference between selling it today and waiting for settlement.
            "product_type": "+".join(sorted({str(m.get("product_type") or "")
                                             for m in members})),
            "qty": str(qty),
            "entry_price": str(_round(entry_price, PRICE)),
            "exit_price": str(_round(
                _weighted([(abs(_dec(m.get("net_qty"))), _dec(m.get("ltp")))
                           for m in members]), PRICE)),
            "gross": str(unrealised),
            "charges": None,
            "net": None,
            "pct": _as_str(_round(_pct(unrealised, cost), PERCENT)),
            "at": days[0] if days else "",
            "fills": len(members),
        })
    return out


def _as_str(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else str(value)


def recent(open_rows: Iterable[Dict[str, Any]], closed_rows: Iterable[Dict[str, Any]],
           closed_limit: int = 20) -> List[Dict[str, Any]]:
    """Everything open, and the latest closes, newest first.

    Every open position is kept however long the list gets — they are what is
    held, and dropping one to make room for history would hide live money. Only
    the closed tail is capped.
    """
    lines = open_positions(open_rows)
    tail = sorted(closed(closed_rows), key=_order, reverse=True)[:closed_limit]
    return sorted(lines + tail, key=_order, reverse=True)


def _order(line: Dict[str, Any]) -> tuple:
    """Newest first, with a stable tie-break.

    Without the tie-break two trades closed in the same second swap places
    between refreshes, which is how a list becomes unreadable while being read.
    """
    return (str(line.get("at") or ""), str(line.get("day") or ""),
            str(line.get("symbol") or ""), str(line.get("account") or ""))
