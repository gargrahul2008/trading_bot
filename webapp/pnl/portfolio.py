"""The portfolio view: one account, and all of them together.

Pure functions over data already fetched, so what each figure means is readable
in one place — see the definitions table in `docs/dashboard_plan.md`.

Two things this is careful about, both of which are easy to get subtly wrong:

**Cost is not market value.** "Deployed" is what was paid. Showing market value
under that heading makes a losing book look smaller and a winning one look
larger than the capital actually committed.

**A short deploys no capital.** It uses margin. Selling 2,150 CROMPTON futures
ties up no cash, so counting the notional as "deployed" would overstate what is
committed. Shorts are reported as exposure, separately.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Set

D0 = Decimal("0")


def _dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value if value is not None else 0))
    except Exception:
        return D0


def _sum(rows: Iterable[Dict[str, Any]], key: str) -> Decimal:
    return sum((_dec(row.get(key)) for row in rows), D0)


def _basis(position: Dict[str, Any]) -> Decimal:
    """What a position cost, falling back to its mark.

    A missing average price would otherwise report an open short as having zero
    exposure and an open long as having nothing deployed — understating risk,
    which is the wrong direction to be wrong in. Valuing it at the mark is not
    the cost basis, but it is the right order of magnitude.
    """
    cost = _dec(position.get("avg_price"))
    return cost if cost else _dec(position.get("ltp"))


def account_portfolio(
    account: str,
    funds: Optional[Dict[str, Any]],
    positions: Optional[List[Dict[str, Any]]],
    holdings: Optional[List[Dict[str, Any]]],
    capital_in: Any = 0,
    realised: Any = 0,
    realised_is_partial: bool = False,
    excluded: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """One account's standing.

    `excluded` names scrips the holder has judged unsellable. They are kept out
    of deployed, market value, unrealised and the return, and reported on their
    own — because a ratio measured against capital that includes a written-off
    holding is wrong by that much, and quietly so.

    `realised` is passed in rather than computed here: for the period the store
    covers it comes from matched round trips, and for earlier months from the
    broker's own history. `realised_is_partial` records that the two have been
    stitched together, so the page can say so instead of implying one number.
    """
    funds = funds or {}
    positions = positions or []
    holdings = holdings or []

    # A delivery sale is stock already sold, awaiting settlement — no open risk.
    # Its `unrealised` from the broker is the mark-to-market of a short that does
    # not exist, so including it would put a number in the unrealised column that
    # is neither unrealised nor the right size.
    excluded = excluded or set()
    open_positions = [p for p in positions
                      if _dec(p.get("net_qty")) != 0 and not p.get("delivery_sale")]

    # Split before anything is summed, so no total can accidentally include
    # both. The excluded side is measured the same way, just reported apart.
    dead_positions = [p for p in open_positions if p.get("symbol") in excluded]
    dead_holdings = [h for h in holdings
                     if h.get("is_open") and h.get("symbol") in excluded]
    open_positions = [p for p in open_positions if p.get("symbol") not in excluded]
    longs = [p for p in open_positions if _dec(p.get("net_qty")) > 0]
    shorts = [p for p in open_positions if _dec(p.get("net_qty")) < 0]
    held = [h for h in holdings
            if h.get("is_open") and h.get("symbol") not in excluded]

    # What was paid for what is owned. Holdings carry their cost price; a long
    # position carries its average.
    position_cost = sum((_dec(p.get("net_qty")) * _basis(p) for p in longs), D0)
    holdings_cost = _sum(held, "invested")
    deployed = position_cost + holdings_cost

    position_value = sum(
        (_dec(p.get("net_qty")) * _dec(p.get("ltp")) for p in longs), D0
    )
    holdings_value = _sum(held, "market_value")
    market_value = position_value + holdings_value

    # Notional sold short. Not deployed capital — margin, and reported apart.
    short_exposure = abs(sum((_dec(p.get("net_qty")) * _basis(p) for p in shorts), D0))

    # Taken from the broker's own per-row figures rather than recomputed, so the
    # dashboard agrees with what the broker's app shows.
    unrealised = _sum(open_positions, "unrealised") + _sum(held, "unrealised")

    free = _dec(funds.get("available"))
    capital_in = _dec(capital_in)
    realised = _dec(realised)

    return {
        "account": account,
        "capital_in": capital_in,
        "free": free,
        "utilised": _dec(funds.get("utilised")),
        "deployed": deployed,
        "market_value": market_value,
        "short_exposure": short_exposure,
        "unrealised": unrealised,
        "realised": realised,
        "realised_is_partial": realised_is_partial,
        # Everything the account is worth: cash plus what its open book would
        # fetch. Shorts are already reflected in unrealised, not here.
        "net_worth": free + market_value,
        "pnl": realised + unrealised,
        "counts": {
            "positions": len(open_positions),
            "long": len(longs),
            "short": len(shorts),
            "holdings": len(held),
        },
        # Return on the money actually put in. None rather than zero when no
        # capital is recorded — a return of "0%" reads as a fact, and this is
        # the absence of one.
        "return_pct": (
            (realised + unrealised) / capital_in * Decimal("100")
            if capital_in > 0 else None
        ),
        # Deployed exceeding capital means one of two things, and the page has
        # to say which is possible rather than show a return measured against a
        # base that is too small. Either the account is leveraged — MTF is 3x —
        # or its capital base is incomplete, because the ledger records opening
        # *cash* and not the securities already owned at the start of the year.
        "deployed_exceeds_capital": bool(capital_in > 0 and deployed > capital_in),
        "excluded": _excluded_block(dead_positions, dead_holdings),
    }


def _excluded_block(positions: List[Dict[str, Any]],
                    holdings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """What was set aside, and what it is nominally worth.

    Reported so the exclusion stays visible: money written off is still money,
    and a page that simply dropped these rows would be a different kind of lie
    from the one it is trying to fix.
    """
    longs = [p for p in positions if _dec(p.get("net_qty")) > 0]
    cost = (sum((_dec(p.get("net_qty")) * _basis(p) for p in longs), D0)
            + _sum(holdings, "invested"))
    value = (sum((_dec(p.get("net_qty")) * _dec(p.get("ltp")) for p in longs), D0)
             + _sum(holdings, "market_value"))
    return {
        "count": len(positions) + len(holdings),
        "symbols": sorted({r.get("symbol", "") for r in positions + holdings}),
        "cost": cost,
        "market_value": value,
        "unrealised": _sum(positions, "unrealised") + _sum(holdings, "unrealised"),
    }


def consolidate(accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """All accounts as one book — the reason this dashboard exists.

    Anyone can read one account in the broker's own app; nobody can read six at
    once.
    """
    totals: Dict[str, Any] = {
        key: sum((a[key] for a in accounts), D0)
        for key in ("capital_in", "free", "utilised", "deployed", "market_value",
                    "short_exposure", "unrealised", "realised", "net_worth", "pnl")
    }
    totals["accounts"] = len(accounts)
    totals["counts"] = {
        key: sum(a["counts"][key] for a in accounts)
        for key in ("positions", "long", "short", "holdings")
    }
    # If any account's realised had to be stitched from two sources, the total
    # inherits that — a caveat that applies to a part applies to the sum.
    totals["realised_is_partial"] = any(a["realised_is_partial"] for a in accounts)
    blocks = [a.get("excluded") or {} for a in accounts]
    totals["excluded"] = {
        "count": sum(int(b.get("count") or 0) for b in blocks),
        "symbols": sorted({s for b in blocks for s in (b.get("symbols") or [])}),
        "cost": sum((b.get("cost") or D0 for b in blocks), D0),
        "market_value": sum((b.get("market_value") or D0 for b in blocks), D0),
        "unrealised": sum((b.get("unrealised") or D0 for b in blocks), D0),
    }
    totals["return_pct"] = (
        totals["pnl"] / totals["capital_in"] * Decimal("100")
        if totals["capital_in"] > 0 else None
    )
    return totals


def as_json(value: Any) -> Any:
    """Decimals to strings, not floats — these are money and the browser should
    receive exactly what was computed."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: as_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [as_json(v) for v in value]
    return value
