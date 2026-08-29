"""FIFO matching.

The broker reports realised P&L per symbol, not per trade — so every figure a
P&L page shows about a *trade* comes from this file. Each test states the money
outcome explicitly rather than asserting a shape.
"""
from decimal import Decimal

import pytest

from webapp.pnl.matcher import (
    INTRADAY, LONG, POSITIONAL, SHORT, match_fills, open_position, summarise,
)


def fill(trade_id, side, qty, price, day="2026-08-28", symbol="NSE:X-EQ",
         product="CNC", account="rahul", at=None):
    return {"account": account, "symbol": symbol, "product_type": product,
            "trade_id": trade_id, "order_id": "o" + trade_id, "side": side,
            "qty": qty, "price": price, "trading_day": day,
            "traded_at": at if at is not None else trade_id}


def test_a_long_round_trip():
    matches, still_open = match_fills([fill("1", "BUY", 100, 10), fill("2", "SELL", 100, 12)])
    assert len(matches) == 1
    assert matches[0].gross == Decimal("200")
    assert matches[0].direction == LONG
    assert matches[0].kind == INTRADAY
    assert still_open == {}


def test_a_short_round_trip():
    """Sold at 12, bought back at 10: a 200 gain, not a loss. Getting the sign
    wrong here would invert every short trade on the page."""
    matches, _ = match_fills([fill("1", "SELL", 100, 12), fill("2", "BUY", 100, 10)])
    assert matches[0].gross == Decimal("200")
    assert matches[0].direction == SHORT


def test_a_short_that_loses():
    matches, _ = match_fills([fill("1", "SELL", 100, 10), fill("2", "BUY", 100, 12)])
    assert matches[0].gross == Decimal("-200")
    assert matches[0].direction == SHORT


def test_a_partial_exit_leaves_the_rest_open():
    matches, still_open = match_fills([fill("1", "BUY", 100, 10), fill("2", "SELL", 40, 12)])
    assert matches[0].qty == Decimal("40")
    assert matches[0].gross == Decimal("80")

    lots = list(still_open.values())[0]
    assert open_position(lots)["qty"] == Decimal("60")
    assert open_position(lots)["avg_price"] == Decimal("10")


def test_one_exit_closes_several_entries_oldest_first():
    """FIFO, not average cost: the first parcel bought is the first sold, and
    each match carries its own entry price."""
    matches, still_open = match_fills([
        fill("1", "BUY", 50, 10),
        fill("2", "BUY", 50, 20),
        fill("3", "SELL", 80, 30),
    ])
    assert [(m.qty, m.entry_price, m.gross) for m in matches] == [
        (Decimal("50"), Decimal("10"), Decimal("1000")),
        (Decimal("30"), Decimal("20"), Decimal("300")),
    ]
    assert open_position(list(still_open.values())[0])["qty"] == Decimal("20")


def test_selling_more_than_held_flips_the_position():
    """Closes the long and opens a short in the same fill — the case a
    long-only model gets silently wrong."""
    matches, still_open = match_fills([fill("1", "BUY", 100, 10), fill("2", "SELL", 150, 12)])
    assert len(matches) == 1
    assert matches[0].qty == Decimal("100")
    assert matches[0].gross == Decimal("200")

    lots = list(still_open.values())[0]
    assert open_position(lots)["qty"] == Decimal("-50"), "now short 50"
    assert open_position(lots)["avg_price"] == Decimal("12")


def test_buying_back_more_than_shorted_flips_the_other_way():
    matches, still_open = match_fills([fill("1", "SELL", 100, 12), fill("2", "BUY", 150, 10)])
    assert matches[0].gross == Decimal("200")
    assert open_position(list(still_open.values())[0])["qty"] == Decimal("50")


def test_held_overnight_is_positional():
    matches, _ = match_fills([
        fill("1", "BUY", 100, 10, day="2026-08-27"),
        fill("2", "SELL", 100, 12, day="2026-08-28"),
    ])
    assert matches[0].kind == POSITIONAL
    assert matches[0].opened_day == "2026-08-27"
    assert matches[0].closed_day == "2026-08-28"


def test_a_cnc_buy_sold_the_same_day_is_intraday():
    """Classified by what happened, not by what the product allowed. A CNC
    position closed the same afternoon was an intraday trade."""
    matches, _ = match_fills([
        fill("1", "BUY", 10, 100, product="CNC"),
        fill("2", "SELL", 10, 105, product="CNC"),
    ])
    assert matches[0].kind == INTRADAY
    assert matches[0].product_type == "CNC"


def test_the_same_symbol_on_two_products_is_two_books():
    """CNC and INTRADAY are separate positions to the broker. Netting them
    would invent a round trip that never happened."""
    matches, still_open = match_fills([
        fill("1", "BUY", 100, 10, product="CNC"),
        fill("2", "SELL", 100, 12, product="INTRADAY"),
    ])
    assert matches == [], "no round trip: these are different positions"
    assert len(still_open) == 2


def test_two_accounts_never_net_against_each_other():
    matches, still_open = match_fills([
        fill("1", "BUY", 100, 10, account="rahul"),
        fill("2", "SELL", 100, 12, account="pratibha"),
    ])
    assert matches == []
    assert len(still_open) == 2


def test_fills_are_matched_in_execution_order_not_input_order():
    """The tradebook comes back in no guaranteed order, and matching the wrong
    entry against an exit changes the P&L of both."""
    ordered, _ = match_fills([
        fill("1", "BUY", 50, 10, at="09:20"),
        fill("2", "BUY", 50, 20, at="09:30"),
        fill("3", "SELL", 50, 30, at="09:40"),
    ])
    shuffled, _ = match_fills([
        fill("3", "SELL", 50, 30, at="09:40"),
        fill("2", "BUY", 50, 20, at="09:30"),
        fill("1", "BUY", 50, 10, at="09:20"),
    ])
    assert [m.gross for m in ordered] == [m.gross for m in shuffled] == [Decimal("1000")]


def test_a_day_boundary_beats_a_timestamp():
    """Yesterday's 15:20 fill precedes today's 09:20 one, whatever the clock
    strings sort to."""
    matches, _ = match_fills([
        fill("2", "SELL", 10, 12, day="2026-08-28", at="09:20"),
        fill("1", "BUY", 10, 10, day="2026-08-27", at="15:20"),
    ])
    assert matches[0].direction == LONG
    assert matches[0].gross == Decimal("20")


def test_an_order_filled_in_pieces_matches_as_one_position():
    """Fyers reports one order as many trades sharing a timestamp — pratibha's
    SHRINGARMS sale came back as 27 of them."""
    pieces = [fill(str(i), "SELL", 10, 100 + i, at="10:00") for i in range(1, 6)]
    matches, still_open = match_fills(
        [fill("0", "BUY", 50, 90, at="09:00")] + pieces
    )
    assert sum(m.qty for m in matches) == Decimal("50")
    assert still_open == {}
    assert sum(m.gross for m in matches) == Decimal("650")


def test_zero_and_unknown_fills_are_ignored():
    matches, still_open = match_fills([
        fill("1", "BUY", 0, 10),
        fill("2", "HOLD", 100, 10),
        fill("3", "BUY", 100, 10),
        fill("4", "SELL", 100, 11),
    ])
    assert len(matches) == 1
    assert matches[0].gross == Decimal("100")
    assert still_open == {}


def test_fractional_prices_do_not_drift():
    """Decimal, not float: these are summed over thousands of fills. The float
    answer here is 0.30000000000000004."""
    matches, _ = match_fills([
        fill("1", "BUY", 1, "10.1"),
        fill("2", "SELL", 1, "10.4"),
    ])
    assert matches[0].gross == Decimal("0.3")


def test_summarise_splits_the_ways_a_page_needs():
    matches, _ = match_fills([
        fill("1", "BUY", 10, 100, day="2026-08-27"),
        fill("2", "SELL", 10, 110, day="2026-08-28"),        # positional long, +100
        fill("3", "SELL", 10, 100, day="2026-08-28", symbol="NSE:Y-EQ"),
        fill("4", "BUY", 10, 95, day="2026-08-28", symbol="NSE:Y-EQ"),   # intraday short, +50
        fill("5", "BUY", 10, 50, day="2026-08-28", symbol="NSE:Z-EQ"),
        fill("6", "SELL", 10, 46, day="2026-08-28", symbol="NSE:Z-EQ"),  # intraday long, -40
    ])
    s = summarise(matches)
    assert s["trades"] == 3
    assert s["gross"] == Decimal("110")
    assert s["by_kind"][POSITIONAL] == Decimal("100")
    assert s["by_kind"][INTRADAY] == Decimal("10")
    assert s["by_direction"][LONG] == Decimal("60")
    assert s["by_direction"][SHORT] == Decimal("50")
    assert s["by_symbol"]["NSE:Z-EQ"] == Decimal("-40")
    assert (s["wins"], s["losses"]) == (2, 1)


def test_nothing_in_nothing_out():
    assert match_fills([]) == ([], {})
    assert summarise([])["gross"] == Decimal("0")
