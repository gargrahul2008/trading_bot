"""Risk limits.

Every test here states the mistake the rule exists to catch. A limit whose
purpose cannot be named is a limit that will be raised the first time it fires.
"""
from decimal import Decimal

import pytest

from webapp.pnl.rms import DEFAULTS, Breach, check, exposure_in, resolve


def book(positions=(), holdings=(), realised=0.0):
    def section(data):
        return {"data": data, "as_of": 1.0, "age_s": 1.0, "stale": False,
                "stale_after_s": 10.0, "source": "rest", "error": None}
    return {"sections": {"positions": section(list(positions)),
                         "holdings": section(list(holdings)),
                         "funds": section({"realised_pnl": realised})}}


def limits(**over):
    out = dict(DEFAULTS)
    out.update({k: Decimal(str(v)) for k, v in over.items()})
    return out


def place(**kw):
    args = {"account": "rahul", "symbol": "NSE:RELIANCE-EQ", "qty": 100,
            "price": 1300, "limits": DEFAULTS}
    args.update(kw)
    return check(**args)


# ── order value ─────────────────────────────────────────────────────────────

def test_a_quantity_with_an_extra_zero_is_refused():
    """140 shares of Reliance is ₹1.8 lakh; 1400 is ₹18 lakh. The typo is one
    keystroke and the loss is the difference."""
    with pytest.raises(Breach) as caught:
        place(qty=1400, price=1300, limits=limits(max_order_value=500000))

    assert caught.value.rule == "max_order_value"
    assert "18,20,000" in caught.value.reason, "the refusal must name the figure"
    assert "5,00,000" in caught.value.reason, "and what it was measured against"


def test_an_ordinary_order_passes():
    place(qty=140, price=1300, limits=limits(max_order_value=500000))


def test_a_limit_of_zero_is_off_not_a_refusal_of_everything():
    """An unset rule and a rule set to refuse everything must not look the
    same — otherwise the first order after a fresh install is blocked."""
    place(qty=100000, price=1300, limits=limits(max_order_value=0))


# ── per-scrip exposure ──────────────────────────────────────────────────────

def test_adding_to_a_scrip_already_at_the_cap_is_refused():
    """Concentration builds one ordinary-looking order at a time. Each one
    passes the per-order limit; together they are the whole account."""
    held = book(holdings=[{"symbol": "NSE:RELIANCE-EQ", "invested": 900000.0,
                           "is_open": True}])
    with pytest.raises(Breach) as caught:
        place(qty=100, price=1300, book=held,
              limits=limits(max_symbol_exposure=1000000))

    assert caught.value.rule == "max_symbol_exposure"
    assert "10,30,000" in caught.value.reason


def test_exposure_counts_positions_and_holdings_together():
    """The broker keeps settled stock in a separate book. A limit on how much
    of one name to hold does not care which book it is in."""
    both = book(
        positions=[{"symbol": "NSE:RELIANCE-EQ", "net_qty": 100, "avg_price": 1300}],
        holdings=[{"symbol": "NSE:RELIANCE-EQ", "invested": 200000.0, "is_open": True}])

    assert exposure_in(both, "NSE:RELIANCE-EQ") == Decimal("330000")


def test_a_delivery_sale_is_not_exposure():
    """Stock already sold and awaiting settlement carries no risk, and counting
    it would block a fresh buy of a name the account no longer holds."""
    sold = book(positions=[{"symbol": "NSE:RELIANCE-EQ", "net_qty": -1000,
                            "avg_price": 1300, "delivery_sale": True}])

    assert exposure_in(sold, "NSE:RELIANCE-EQ") == Decimal("0")


def test_a_short_counts_towards_exposure():
    """Being short 2,150 futures is a position to be bought back. Adding to it
    is adding risk, whatever the sign."""
    short = book(positions=[{"symbol": "NSE:CROMPTON26SEPFUT", "net_qty": -2150,
                             "avg_price": 242.65}])

    assert exposure_in(short, "NSE:CROMPTON26SEPFUT") == Decimal("521697.50")


# ── daily loss ──────────────────────────────────────────────────────────────

def test_no_new_position_once_the_day_is_lost():
    """The rule exists for the hour after a bad morning, which is when it is
    least likely to be obeyed voluntarily."""
    down = book(realised=-52000.0)
    with pytest.raises(Breach) as caught:
        place(book=down, limits=limits(max_daily_loss=50000))

    assert caught.value.rule == "max_daily_loss"
    assert "52,000" in caught.value.reason


def test_closing_is_still_allowed_when_the_day_is_lost():
    """A limit that stops someone cutting a losing position is not a risk
    control; it is the trap the control was meant to prevent."""
    down = book(
        positions=[{"symbol": "NSE:RELIANCE-EQ", "net_qty": 100, "avg_price": 1300}],
        realised=-52000.0)
    place(qty=100, book=down, limits=limits(max_daily_loss=50000), reducing=True)


def test_a_profitable_day_is_not_a_loss():
    place(book=book(realised=52000.0), limits=limits(max_daily_loss=50000))


# ── rate ────────────────────────────────────────────────────────────────────

def test_a_loop_placing_orders_is_stopped():
    """The failure this catches is a retry that does not know it succeeded."""
    with pytest.raises(Breach) as caught:
        place(recent_orders=10, limits=limits(max_orders_per_minute=10))

    assert caught.value.rule == "max_orders_per_minute"


def test_the_rate_limit_applies_to_closing_orders_too():
    """Unlike the loss limit: a runaway loop cancelling and re-placing an exit
    is still a runaway loop."""
    with pytest.raises(Breach):
        place(recent_orders=10, limits=limits(max_orders_per_minute=10),
              reducing=True)


# ── resolution ──────────────────────────────────────────────────────────────

def test_an_accounts_own_limit_beats_the_default():
    """A cap that is right for piyush's 25 lakh is a cage on pratibha's 58."""
    rows = [{"account": "*", "name": "max_order_value", "value": "200000"},
            {"account": "pratibha", "name": "max_order_value", "value": "1500000"}]

    assert resolve(rows, "pratibha")["max_order_value"] == Decimal("1500000")
    assert resolve(rows, "piyush")["max_order_value"] == Decimal("200000")


def test_an_unconfigured_account_gets_the_built_in():
    assert resolve([], "rahul")["max_order_value"] == DEFAULTS["max_order_value"]


def test_an_unknown_rule_name_is_ignored():
    """Rows outlive the code that reads them. A renamed rule must not become a
    limit of whatever the stale row happened to say."""
    rows = [{"account": "rahul", "name": "max_trades_per_moon", "value": "1"}]

    assert resolve(rows, "rahul") == DEFAULTS
