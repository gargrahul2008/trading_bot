"""Risk limits, checked before an order leaves this machine.

Enforced here rather than in the browser. The UI is a convenience; the API is
the only way an order reaches a broker, so it is the only place a limit is
worth putting. A refusal that can be skipped by opening the network tab is not
a limit, it is a suggestion — and the confirm dialog it replaces was removed
precisely because it was in the way rather than in the path.

Every rule states its number in the refusal. "Blocked by risk limits" tells the
holder nothing; "order value 6,20,000 exceeds the 5,00,000 limit for rahul"
tells them what to change.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

D0 = Decimal("0")

#: Every limit, with the unit it is measured in. A limit of 0 means "no limit":
#: an unset rule must not be indistinguishable from one set to refuse
#: everything, which is what a plain absence of a row would give.
LIMITS: Dict[str, str] = {
    "max_order_value": "rupees, one order's notional",
    "max_symbol_exposure": "rupees, one scrip in one account, including this order",
    "max_daily_loss": "rupees, today's realised loss, beyond which no new order",
    "max_orders_per_minute": "orders through this dashboard, per account",
}

#: Deliberately generous. These bound a mistake — a fat-fingered quantity, a
#: script in a loop — not a strategy. Numbers that get in the way of ordinary
#: trades get raised until they no longer bound anything.
DEFAULTS: Dict[str, Decimal] = {
    "max_order_value": Decimal("500000"),
    "max_symbol_exposure": Decimal("0"),
    "max_daily_loss": Decimal("0"),
    "max_orders_per_minute": Decimal("10"),
}


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return D0


def _money(value: Decimal) -> str:
    """Indian grouping, because that is how the figure will be checked against
    the broker's screen."""
    whole = int(value.copy_abs())
    text = str(whole)
    if len(text) > 3:
        head, tail = text[:-3], text[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        text = ",".join(parts + [tail])
    return ("-" if value < 0 else "") + text


class Breach(Exception):
    """A refused order, with the rule and both numbers."""

    def __init__(self, rule: str, reason: str, limit: Decimal, actual: Decimal):
        super().__init__(reason)
        self.rule = rule
        self.reason = reason
        self.limit = limit
        self.actual = actual

    def as_dict(self) -> Dict[str, Any]:
        return {"rule": self.rule, "reason": self.reason,
                "limit": str(self.limit), "actual": str(self.actual)}


def resolve(rows: Iterable[Dict[str, Any]], account: str) -> Dict[str, Decimal]:
    """The limits in force for one account.

    An account's own row wins over the '*' default, which wins over the built-in.
    Three levels because the accounts differ by an order of magnitude: a cap that
    is right for piyush's 25 lakh is a cage on pratibha's 58.
    """
    limits = dict(DEFAULTS)
    for scope in ("*", account):
        for row in rows:
            if row.get("account") == scope and row.get("name") in LIMITS:
                limits[row["name"]] = _dec(row.get("value"))
    return limits


def order_value(qty: Any, price: Any) -> Decimal:
    return abs(_dec(qty)) * abs(_dec(price))


def exposure_in(book: Optional[Dict[str, Any]], symbol: str) -> Decimal:
    """What one scrip is already worth in one account, at cost.

    Positions and holdings together — the broker keeps them in separate books,
    but a limit on how much of one name to hold does not care which book it is
    in. Absolute, so adding to a short counts as adding.
    """
    sections = (book or {}).get("sections") or {}

    def rows(kind: str) -> List[Dict[str, Any]]:
        data = (sections.get(kind) or {}).get("data")
        return [r for r in (data or []) if isinstance(r, dict)]

    total = D0
    for position in rows("positions"):
        if position.get("symbol") == symbol and not position.get("delivery_sale"):
            total += abs(_dec(position.get("net_qty")) * _dec(position.get("avg_price")))
    for holding in rows("holdings"):
        if holding.get("symbol") == symbol and holding.get("is_open"):
            total += abs(_dec(holding.get("invested")))
    return total


def realised_today(book: Optional[Dict[str, Any]]) -> Decimal:
    """The broker's own figure for the day, not our matched trades.

    A daily loss limit has to act on the number the broker would act on, and it
    has to be available the instant it moves — which the matched figure, built
    from the tradebook, is not.
    """
    funds = ((book or {}).get("sections") or {}).get("funds") or {}
    return _dec((funds.get("data") or {}).get("realised_pnl"))


def check(
    *,
    account: str,
    symbol: str,
    qty: Any,
    price: Any,
    limits: Dict[str, Decimal],
    book: Optional[Dict[str, Any]] = None,
    recent_orders: int = 0,
    reducing: bool = False,
) -> None:
    """Raise `Breach` if this order must not be placed.

    `reducing` exempts an order that closes rather than opens. A daily loss
    limit that stops someone cutting a losing position is not a risk control —
    it is the trap the control was meant to prevent.
    """
    value = order_value(qty, price)

    cap = limits.get("max_order_value", D0)
    if cap > 0 and value > cap:
        raise Breach(
            "max_order_value",
            "order value %s exceeds the %s per-order limit for %s"
            % (_money(value), _money(cap), account),
            cap, value)

    rate = limits.get("max_orders_per_minute", D0)
    if rate > 0 and _dec(recent_orders) >= rate:
        raise Breach(
            "max_orders_per_minute",
            "%d orders already placed in %s in the last minute, at the limit of %s"
            % (recent_orders, account, _money(rate)),
            rate, _dec(recent_orders))

    if reducing:
        return

    cap = limits.get("max_symbol_exposure", D0)
    if cap > 0:
        after = exposure_in(book, symbol) + value
        if after > cap:
            raise Breach(
                "max_symbol_exposure",
                "%s in %s would reach %s, over the %s limit for one scrip"
                % (symbol, account, _money(after), _money(cap)),
                cap, after)

    cap = limits.get("max_daily_loss", D0)
    if cap > 0:
        loss = -realised_today(book)
        if loss >= cap:
            raise Breach(
                "max_daily_loss",
                "%s is down %s today, at or past the %s daily loss limit — "
                "closing orders are still allowed"
                % (account, _money(loss), _money(cap)),
                cap, loss)


def describe(limits: Dict[str, Decimal]) -> List[Tuple[str, str, str]]:
    """(rule, value, unit) for display. A zero reads as 'off', not as zero."""
    return [(name, "off" if limits.get(name, D0) <= 0 else str(limits[name]), unit)
            for name, unit in LIMITS.items()]
