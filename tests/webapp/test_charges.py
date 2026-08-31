"""Apportioning charges onto individual trades.

The broker reports charges per day and per segment, never per symbol — confirmed
against the live accounts, where day-wise and segment-wise reconcile to the
paisa. So a per-trade charge is divided out, not looked up, and the honesty of
that division is what these tests are about.
"""
from decimal import Decimal

import pytest

from webapp.pnl.charges import DayCharges, charge_for, net_matches, summarise_net
from webapp.pnl.matcher import match_fills


def fill(trade_id, side, qty, price, day, symbol="NSE:X-EQ", account="rahul"):
    return {"account": account, "symbol": symbol, "product_type": "CNC",
            "trade_id": trade_id, "order_id": "o" + trade_id, "side": side,
            "qty": qty, "price": price, "trading_day": day, "traded_at": trade_id}


def one_match(open_day="2026-08-28", close_day="2026-08-28"):
    matches, _ = match_fills([
        fill("1", "BUY", 100, 1000, open_day),
        fill("2", "SELL", 100, 1020, close_day),
    ])
    return matches[0]


def test_a_trade_takes_its_share_of_the_days_turnover():
    """A trade that was a tenth of the day's turnover bore about a tenth of the
    day's charges."""
    day = {("rahul", "2026-08-28"): DayCharges(total=1000, turnover=1000000)}
    match = one_match()
    # 100,000 bought + 102,000 sold against 1,000,000 turnover, at 1,000 charges.
    assert charge_for(match, day) == Decimal("202.00")


def test_a_round_trip_takes_a_slice_of_each_day_it_touched():
    by_day = {
        ("rahul", "2026-08-27"): DayCharges(total=200, turnover=400000),   # entry day
        ("rahul", "2026-08-28"): DayCharges(total=400, turnover=400000),   # exit day
    }
    match = one_match("2026-08-27", "2026-08-28")
    # 100,000/400,000 × 200  +  102,000/400,000 × 400
    assert charge_for(match, by_day) == Decimal("152.00")   # 50 + 102


def test_an_unknown_day_gives_no_charge_rather_than_zero():
    """A confident 0.00 makes a trade look cheaper than it was. A trade cannot
    be reported net when half its cost is missing."""
    match = one_match("2026-08-27", "2026-08-28")
    assert charge_for(match, {("rahul", "2026-08-27"): DayCharges(200, 400000)}) is None
    assert charge_for(match, {}) is None


def test_zero_turnover_cannot_apportion():
    """Dividing by it would either crash or invent a number."""
    match = one_match()
    assert charge_for(match, {("rahul", "2026-08-28"): DayCharges(total=500, turnover=0)}) is None


def test_net_rows_carry_the_estimate_flag():
    """An exact number and an estimated one must never be summed without saying
    so."""
    matches, _ = match_fills([fill("1", "BUY", 100, 1000, "2026-08-28"),
                              fill("2", "SELL", 100, 1020, "2026-08-28")])
    rows = net_matches(matches, {("rahul", "2026-08-28"): DayCharges(1000, 1000000)})

    assert rows[0]["gross"] == "2000"
    assert rows[0]["charges"] == "202.00"
    assert rows[0]["net"] == "1798.00"
    assert rows[0]["charges_estimated"] is True


def test_a_trade_whose_charges_are_unknown_says_so_rather_than_guessing():
    matches, _ = match_fills([fill("1", "BUY", 100, 1000, "2026-08-28"),
                              fill("2", "SELL", 100, 1020, "2026-08-28")])
    rows = net_matches(matches, {})

    assert rows[0]["gross"] == "2000"
    assert rows[0]["charges"] is None
    assert rows[0]["net"] is None
    assert rows[0]["charges_estimated"] is False


def test_the_summary_counts_what_it_could_not_cost():
    """A net total that silently omits the trades it could not cost is a smaller
    number pretending to be a complete one."""
    matches, _ = match_fills([
        fill("1", "BUY", 100, 1000, "2026-08-28"),
        fill("2", "SELL", 100, 1020, "2026-08-28"),
        fill("3", "BUY", 50, 500, "2026-07-01", symbol="NSE:Y-EQ"),
        fill("4", "SELL", 50, 520, "2026-07-01", symbol="NSE:Y-EQ"),
    ])
    rows = net_matches(matches, {("rahul", "2026-08-28"): DayCharges(1000, 1000000)})
    summary = summarise_net(rows)

    assert summary["trades"] == 2
    assert summary["gross"] == "3000.00"       # 2000 + 1000
    assert summary["charges"] == "202.00"      # only the day we know
    assert summary["net"] == "2798.00"
    assert summary["trades_costed"] == 1
    assert summary["trades_without_charges"] == 1


def test_summarising_nothing_is_not_an_error():
    summary = summarise_net([])
    assert summary["trades"] == 0
    assert summary["net"] == "0.00"
    assert summary["charges_estimated"] is False


def test_a_short_is_costed_the_same_way():
    """Turnover is turnover — a sale opening a position and a sale closing one
    attract the same charges."""
    matches, _ = match_fills([fill("1", "SELL", 100, 1020, "2026-08-28"),
                              fill("2", "BUY", 100, 1000, "2026-08-28")])
    rows = net_matches(matches, {("rahul", "2026-08-28"): DayCharges(1000, 1000000)})
    assert rows[0]["direction"] == "SHORT"
    assert rows[0]["charges"] == "202.00"
    assert rows[0]["net"] == "1798.00"


def test_one_accounts_charges_are_never_used_for_another():
    """Pooling charges across accounts and dividing by the combined turnover
    costs one account for another's trading. An account with none recorded came
    out costed at 364.16 the first time this ran."""
    matches, _ = match_fills([
        fill("1", "BUY", 500, 419.90, "2026-08-28", symbol="NSE:BHEL-EQ", account="piyush"),
        fill("2", "SELL", 500, 415.50, "2026-08-28", symbol="NSE:BHEL-EQ", account="piyush"),
    ])
    # Charges exist for rahul on that day, and none for piyush.
    rows = net_matches(matches, {("rahul", "2026-08-28"): DayCharges(1000, 1000000)})

    assert rows[0]["account"] == "piyush"
    assert rows[0]["charges"] is None, "piyush has no charges of his own"
    assert rows[0]["net"] is None
