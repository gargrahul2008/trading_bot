"""Validating an order before it can reach the market.

This is the last thing between a mistyped box and a real trade, so each case
states the mistake it prevents rather than the rule it enforces.
"""
from decimal import Decimal

import pytest

from webapp.agent.gateway import build_order_request

BASE = {"symbol": "NSE:RELIANCE-EQ", "side": "BUY", "qty": 10}


def build(**kw):
    return build_order_request(dict(BASE, **kw))


def refuses(match, **kw):
    with pytest.raises(ValueError, match=match):
        build(**kw)


# ── the four order types ─────────────────────────────────────────────────────
def test_a_market_order_carries_no_prices():
    order = build(order_type="MARKET")
    assert order.order_type == "MARKET"
    assert order.limit_price == Decimal("0") and order.stop_price == Decimal("0")


def test_a_limit_order_needs_its_limit():
    """Without one it would fill at market — an order nobody asked for."""
    assert build(order_type="LIMIT", limit_price=1465.5).limit_price == Decimal("1465.5")
    refuses("limit_price is required", order_type="LIMIT")


def test_a_stop_market_order_needs_its_trigger():
    """Without one it would fire immediately."""
    assert build(order_type="SL_M", stop_price=1400).stop_price == Decimal("1400")
    refuses("stop_price is required", order_type="SL_M")


def test_a_stop_limit_order_needs_both():
    order = build(order_type="SL", stop_price=1400, limit_price=1399)
    assert (order.stop_price, order.limit_price) == (Decimal("1400"), Decimal("1399"))
    refuses("stop_price is required", order_type="SL", limit_price=1399)
    refuses("limit_price is required", order_type="SL", stop_price=1400)


def test_a_market_order_with_a_limit_price_is_refused():
    """At best the price is ignored; at worst it is honoured. Either way it is
    not the order that was typed."""
    refuses("takes no limit_price", order_type="MARKET", limit_price=1465)


def test_sl_m_is_accepted_however_it_is_written():
    assert build(order_type="sl-m", stop_price=1400).order_type == "SL_M"


# ── bracket and cover legs ───────────────────────────────────────────────────
def test_a_bracket_order_needs_both_legs():
    order = build(order_type="LIMIT", limit_price=1465, product_type="BO",
                  stop_loss=10, take_profit=25)
    assert order.stop_loss == Decimal("10") and order.take_profit == Decimal("25")
    refuses("needs both stop_loss and take_profit", order_type="LIMIT",
            limit_price=1465, product_type="BO", stop_loss=10)


def test_a_cover_order_needs_its_stop():
    assert build(order_type="LIMIT", limit_price=1465, product_type="CO",
                 stop_loss=10).stop_loss == Decimal("10")
    refuses("CO order needs stop_loss", order_type="LIMIT", limit_price=1465,
            product_type="CO")


def test_legs_on_an_ordinary_order_are_refused_rather_than_dropped():
    """The broker ignores them on a CNC order. Someone who typed a stop-loss
    would believe they had one."""
    refuses("BO and CO orders only", order_type="LIMIT", limit_price=1465, stop_loss=5)


def test_a_leg_given_as_a_price_rather_than_points_is_caught():
    """Fyers reads these as POINTS from the entry. Typing 1465 meaning 'stop at
    1465' would place a stop 1,465 points away — which is no stop at all."""
    refuses("POINTS away from the entry", order_type="LIMIT", limit_price=1465,
            product_type="BO", stop_loss=1465, take_profit=25)


# ── the basics ───────────────────────────────────────────────────────────────
def test_quantity_must_be_a_positive_whole_number():
    refuses("positive whole number", order_type="MARKET", qty=0)
    refuses("positive whole number", order_type="MARKET", qty=-5)
    refuses("positive whole number", order_type="MARKET", qty=1.5)


def test_the_side_must_be_one_of_two_words():
    refuses("side must be BUY or SELL", side="SIDEWAYS", order_type="MARKET")


def test_an_unknown_product_is_refused():
    refuses("product_type must be one of", order_type="MARKET", product_type="SWING")


def test_an_unknown_order_type_is_refused():
    refuses("order_type must be one of", order_type="BRACKET", limit_price=1)


def test_validity_is_checked():
    assert build(order_type="MARKET", validity="ioc").validity == "IOC"
    refuses("validity must be DAY or IOC", order_type="MARKET", validity="GTC")


def test_a_symbol_is_required():
    with pytest.raises(ValueError, match="symbol is required"):
        build_order_request({"side": "BUY", "qty": 1, "order_type": "MARKET"})
