"""FIFO matching: fills in, closed round trips out.

The broker reports realised P&L per symbol, not per trade. To answer "what did
this trade make", and to split intraday from positional and long from short, we
have to match exits against entries ourselves.

The model is **signed lots**. A buy is a positive lot, a sell a negative one, and
a fill first consumes any open lots of the opposite sign before opening a lot of
its own. That one rule handles longs and shorts identically — a short is not a
special case, it is a position that happens to be negative — and it handles a
sell that exceeds the position, which closes the long and opens a short in the
same fill.

Books are keyed by **(account, symbol, product_type)**. The same symbol held as
CNC and traded as INTRADAY are different positions to the broker and must not
net against each other.

Everything is `Decimal`, built from strings. These are money figures that get
summed over thousands of fills, and float error is not something to discover in
a tax return.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

D0 = Decimal("0")

BUY = "BUY"
SELL = "SELL"

LONG = "LONG"
SHORT = "SHORT"

INTRADAY = "intraday"
POSITIONAL = "positional"


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return D0


class Lot:
    """An open parcel. `qty` is signed: positive is long, negative is short."""

    __slots__ = ("qty", "price", "trade_id", "order_id", "day", "at")

    def __init__(self, qty: Decimal, price: Decimal, trade_id: str,
                 order_id: str, day: str, at: Optional[str]) -> None:
        self.qty = qty
        self.price = price
        self.trade_id = trade_id
        self.order_id = order_id
        self.day = day
        self.at = at

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "Lot(%s @ %s from %s)" % (self.qty, self.price, self.day)


class Match:
    """One closed round trip: a parcel opened and later closed.

    `gross` is before charges. Charges are apportioned separately, because the
    broker reports them per day and per segment rather than per trade.
    """

    __slots__ = ("account", "symbol", "product_type", "direction", "qty",
                 "entry_price", "exit_price", "entry_trade_id", "exit_trade_id",
                 "entry_order_id", "exit_order_id", "opened_day", "closed_day",
                 "opened_at", "closed_at", "gross")

    def __init__(self, **kw: Any) -> None:
        for slot in self.__slots__:
            setattr(self, slot, kw.get(slot))

    @property
    def kind(self) -> str:
        """Intraday means opened and closed on the same trading day.

        Derived from the days rather than from `product_type`: the product says
        what a position was *allowed* to be, these say what it actually was. A
        CNC buy sold the same afternoon is an intraday trade whatever it was
        booked as.
        """
        return INTRADAY if self.opened_day == self.closed_day else POSITIONAL

    def as_dict(self) -> Dict[str, Any]:
        return {
            "account": self.account,
            "symbol": self.symbol,
            "product_type": self.product_type,
            "direction": self.direction,
            "qty": str(self.qty),
            "entry_price": str(self.entry_price),
            "exit_price": str(self.exit_price),
            "entry_trade_id": self.entry_trade_id,
            "exit_trade_id": self.exit_trade_id,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "opened_day": self.opened_day,
            "closed_day": self.closed_day,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "gross": str(self.gross),
            "kind": self.kind,
        }


def _sort_key(fill: Dict[str, Any]) -> Tuple[str, str, str]:
    """Execution order.

    The tradebook does not come back in any guaranteed order, and matching the
    wrong entry against an exit changes the P&L of both. Day first, then the
    timestamp, then the trade id as a stable tie-break for fills that share a
    timestamp — which they routinely do when one order fills in many pieces.
    """
    return (
        str(fill.get("trading_day") or ""),
        str(fill.get("traded_at") or ""),
        str(fill.get("trade_id") or ""),
    )


def book_key(fill: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(fill.get("account") or ""),
        str(fill.get("symbol") or ""),
        str(fill.get("product_type") or ""),
    )


def match_fills(fills: Iterable[Dict[str, Any]]) -> Tuple[List[Match], Dict[Tuple[str, str, str], List[Lot]]]:
    """Match every fill FIFO within its own book.

    Returns the closed round trips and whatever remains open. Open lots are part
    of the answer, not a leftover: they are the position still carrying risk, and
    their cost basis is what the next exit will be matched against.
    """
    books: Dict[Tuple[str, str, str], List[Lot]] = {}
    matches: List[Match] = []

    for fill in sorted(fills, key=_sort_key):
        qty = abs(_dec(fill.get("qty")))
        if qty <= 0:
            # A zero-quantity fill is noise; letting it through would create
            # empty lots that consume nothing and never close.
            continue

        side = str(fill.get("side") or "").upper()
        if side not in (BUY, SELL):
            continue

        price = _dec(fill.get("price"))
        signed = qty if side == BUY else -qty
        key = book_key(fill)
        lots = books.setdefault(key, [])

        day = str(fill.get("trading_day") or "")
        at = fill.get("traded_at")
        trade_id = str(fill.get("trade_id") or "")
        order_id = str(fill.get("order_id") or "")

        # Consume opposing lots oldest-first, then open a lot with whatever is
        # left. A sell larger than the long position closes it and opens a
        # short in the same pass — no special case needed.
        while lots and signed != 0 and (lots[0].qty > 0) != (signed > 0):
            lot = lots[0]
            taken = min(abs(lot.qty), abs(signed))
            direction = LONG if lot.qty > 0 else SHORT

            # Long: sold above cost is a gain. Short: bought back below the sale
            # price is a gain. One expression covers both, because the lot's
            # sign already carries the direction.
            gross = ((price - lot.price) * taken) if lot.qty > 0 else ((lot.price - price) * taken)

            matches.append(Match(
                account=key[0], symbol=key[1], product_type=key[2],
                direction=direction, qty=taken,
                entry_price=lot.price, exit_price=price,
                entry_trade_id=lot.trade_id, exit_trade_id=trade_id,
                entry_order_id=lot.order_id, exit_order_id=order_id,
                opened_day=lot.day, closed_day=day,
                opened_at=lot.at, closed_at=at,
                gross=gross,
            ))

            if abs(lot.qty) == taken:
                lots.pop(0)
            else:
                lot.qty = lot.qty - taken if lot.qty > 0 else lot.qty + taken
            signed = signed - taken if signed > 0 else signed + taken

        if signed != 0:
            lots.append(Lot(signed, price, trade_id, order_id, day, at))

    return matches, {k: v for k, v in books.items() if v}


def open_position(lots: List[Lot]) -> Dict[str, Decimal]:
    """Net quantity and weighted average cost of what is still open."""
    qty = sum((lot.qty for lot in lots), D0)
    if qty == 0:
        return {"qty": D0, "avg_price": D0, "cost": D0}
    cost = sum((lot.qty * lot.price for lot in lots), D0)
    return {"qty": qty, "avg_price": cost / qty, "cost": cost}


def summarise(matches: Iterable[Match]) -> Dict[str, Any]:
    """Totals a P&L page needs, split the ways that matter."""
    matches = list(matches)
    by_kind: Dict[str, Decimal] = {INTRADAY: D0, POSITIONAL: D0}
    by_direction: Dict[str, Decimal] = {LONG: D0, SHORT: D0}
    by_symbol: Dict[str, Decimal] = {}

    for match in matches:
        by_kind[match.kind] += match.gross
        by_direction[match.direction] += match.gross
        by_symbol[match.symbol] = by_symbol.get(match.symbol, D0) + match.gross

    total = sum((m.gross for m in matches), D0)
    wins = [m for m in matches if m.gross > 0]
    losses = [m for m in matches if m.gross < 0]

    return {
        "trades": len(matches),
        "gross": total,
        "by_kind": by_kind,
        "by_direction": by_direction,
        "by_symbol": by_symbol,
        "wins": len(wins),
        "losses": len(losses),
        "gross_wins": sum((m.gross for m in wins), D0),
        "gross_losses": sum((m.gross for m in losses), D0),
    }
