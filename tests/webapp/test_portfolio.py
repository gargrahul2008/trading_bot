"""The portfolio figures.

Every number here is one Rahul reads to decide something, so each test states
what the figure must be and why, rather than asserting a shape.
"""
from decimal import Decimal

import pytest

from webapp.pnl.portfolio import account_portfolio, consolidate

FUNDS = {"available": 445036.54, "utilised": 599888.18}


def long_position(qty=1808, avg=1308.80, ltp=1288.20, unreal=-37244.80):
    return {"net_qty": qty, "avg_price": avg, "ltp": ltp, "unrealised": unreal}


def short_position(qty=-2150, avg=242.65, ltp=240.30, unreal=5052.50):
    return {"net_qty": qty, "avg_price": avg, "ltp": ltp, "unrealised": unreal}


def holding(invested=206028.0, value=212301.0, unreal=6273.0, is_open=True):
    return {"is_open": is_open, "invested": invested,
            "market_value": value, "unrealised": unreal}


def test_deployed_is_cost_not_market_value():
    """Showing market value under 'deployed' makes a losing book look smaller
    than the capital actually committed to it."""
    p = account_portfolio("rahul", FUNDS, [long_position()], [holding()])
    assert p["deployed"] == Decimal("1808") * Decimal("1308.80") + Decimal("206028.0")
    assert p["market_value"] == Decimal("1808") * Decimal("1288.20") + Decimal("212301.0")
    assert p["deployed"] > p["market_value"], "this book is down; deployed must exceed value"


def test_a_short_is_exposure_not_deployed_capital():
    """Selling 2,150 futures ties up margin, not cash. Counting the notional as
    deployed would overstate what is committed by five lakh."""
    p = account_portfolio("rahul", FUNDS, [short_position()], [])
    assert p["deployed"] == Decimal("0")
    assert p["short_exposure"] == Decimal("2150") * Decimal("242.65")
    assert p["counts"]["short"] == 1


def test_a_closed_position_counts_for_nothing():
    flat = {"net_qty": 0, "avg_price": 0, "ltp": 3633.0, "unrealised": 0, "realised": -9275.0}
    p = account_portfolio("rahul", FUNDS, [flat], [])
    assert p["counts"]["positions"] == 0
    assert p["deployed"] == Decimal("0")
    assert p["unrealised"] == Decimal("0")


def test_a_sold_out_holding_is_excluded():
    """Fyers keeps returning the row with qty 0 and the old cost price."""
    p = account_portfolio("rahul", FUNDS, [], [holding(), holding(is_open=False)])
    assert p["counts"]["holdings"] == 1
    assert p["deployed"] == Decimal("206028.0")


def test_unrealised_comes_from_the_broker_not_a_recomputation():
    """So the dashboard agrees with what the broker's own app shows, rather than
    disagreeing by a rounding rule."""
    p = account_portfolio("rahul", FUNDS, [long_position(), short_position()], [holding()])
    assert p["unrealised"] == Decimal("-37244.80") + Decimal("5052.50") + Decimal("6273.0")


def test_return_is_measured_against_capital_put_in():
    p = account_portfolio("rahul", FUNDS, [], [], capital_in=1000000, realised=50000)
    p2 = account_portfolio("rahul", FUNDS, [holding()], [], capital_in=1000000, realised=50000)
    assert p["return_pct"] == Decimal("5")
    assert p2["pnl"] == Decimal("50000")


def test_no_capital_recorded_gives_no_return_rather_than_zero():
    """A return of 0% reads as a fact. This is the absence of one, and the page
    must be able to tell the difference."""
    p = account_portfolio("piyush", FUNDS, [], [], capital_in=0, realised=1000)
    assert p["return_pct"] is None
    assert p["pnl"] == Decimal("1000")


def test_net_worth_is_cash_plus_what_the_book_would_fetch():
    p = account_portfolio("rahul", FUNDS, [long_position()], [holding()])
    assert p["net_worth"] == Decimal("445036.54") + p["market_value"]


def test_a_missing_account_contributes_nothing_rather_than_failing():
    """An agent that is down must not take the consolidated view with it."""
    p = account_portfolio("pratibha", None, None, None)
    assert p["free"] == Decimal("0")
    assert p["deployed"] == Decimal("0")
    assert p["counts"]["positions"] == 0


def test_consolidation_sums_every_account():
    accounts = [
        account_portfolio("rahul", FUNDS, [long_position()], [holding()],
                          capital_in=2000000, realised=51014.60),
        account_portfolio("pratibha", {"available": 300021.34, "utilised": -116645.80},
                          [short_position()], [], capital_in=1000000, realised=-3750),
    ]
    total = consolidate(accounts)

    assert total["accounts"] == 2
    assert total["capital_in"] == Decimal("3000000")
    assert total["free"] == Decimal("445036.54") + Decimal("300021.34")
    assert total["realised"] == Decimal("51014.60") + Decimal("-3750")
    assert total["counts"]["long"] == 1
    assert total["counts"]["short"] == 1
    assert total["return_pct"] == total["pnl"] / Decimal("3000000") * Decimal("100")


def test_a_partial_realised_figure_is_flagged_all_the_way_up():
    """Realised before 28 Aug comes from the broker's history and after it from
    matched trades. A caveat that applies to a part applies to the sum."""
    accounts = [
        account_portfolio("rahul", FUNDS, [], [], realised=100, realised_is_partial=True),
        account_portfolio("piyush", FUNDS, [], [], realised=50),
    ]
    assert consolidate(accounts)["realised_is_partial"] is True


def test_consolidating_nothing_does_not_divide_by_zero():
    total = consolidate([])
    assert total["accounts"] == 0
    assert total["return_pct"] is None
    assert total["pnl"] == Decimal("0")


def test_a_position_without_an_average_price_is_not_reported_as_zero_exposure():
    """A missing avg_price would report an open short as having no exposure and
    an open long as nothing deployed — understating risk, which is the wrong
    direction to be wrong in. The mark is not the cost basis, but it is the
    right order of magnitude."""
    no_avg_short = {"net_qty": -50, "ltp": 3633.0, "unrealised": -4.0}
    p = account_portfolio("pratibha", FUNDS, [no_avg_short], [])
    assert p["counts"]["short"] == 1
    assert p["short_exposure"] == Decimal("50") * Decimal("3633.0")

    no_avg_long = {"net_qty": 100, "ltp": 250.0, "unrealised": 0}
    q = account_portfolio("rahul", FUNDS, [no_avg_long], [])
    assert q["deployed"] == Decimal("100") * Decimal("250.0")


def test_the_cost_basis_is_preferred_when_it_is_present():
    with_avg = {"net_qty": -50, "avg_price": 3706.0, "ltp": 3633.0, "unrealised": 0}
    p = account_portfolio("pratibha", FUNDS, [with_avg], [])
    assert p["short_exposure"] == Decimal("50") * Decimal("3706.0")


def test_a_delivery_sale_is_not_counted_as_unrealised():
    """The shares are sold; there is no open risk. And the broker's figure on
    such a row is the mark-to-market of a short that does not exist —
    SHRINGARMS showed +3,130 unrealised where the sale was a −6,660 realised
    loss."""
    sale = {"net_qty": -1000, "avg_price": 223.91, "ltp": 220.78,
            "unrealised": 3130.0, "delivery_sale": True}
    p = account_portfolio("pratibha", FUNDS, [sale, long_position()], [])

    assert p["unrealised"] == Decimal("-37244.80"), "only the real position"
    assert p["counts"]["positions"] == 1
    assert p["short_exposure"] == Decimal("0"), "it is not a short"


def test_deployed_exceeding_capital_is_flagged():
    """The ledger records opening cash, never the securities already owned at
    the start of the year. An account that began with 35 lakh of stock shows a
    base 35 lakh too small, and every return measured against it is too large.
    Leverage produces the same symptom legitimately, so the page names both."""
    p = account_portfolio("pratibha", FUNDS, [], [holding(invested=5000000)],
                          capital_in=1707978.49, realised=0)
    assert p["deployed"] > p["capital_in"]
    assert p["deployed_exceeds_capital"] is True


def test_a_normal_account_is_not_flagged():
    p = account_portfolio("rahul", FUNDS, [], [holding(invested=206028)],
                          capital_in=680990.27, realised=0)
    assert p["deployed_exceeds_capital"] is False


def test_no_capital_recorded_is_not_treated_as_over_deployed():
    """That case has its own banner; flagging it twice says nothing new."""
    p = account_portfolio("piyush", FUNDS, [], [holding()], capital_in=0)
    assert p["deployed_exceeds_capital"] is False


# ── setting a scrip aside ───────────────────────────────────────────────────
#
# A suspended or written-off holding still sits in the broker's book at cost.
# Left in, it drags every ratio measured against deployed capital — and does so
# invisibly, which is worse than being wrong loudly.


def test_an_excluded_holding_leaves_deployed_and_the_return():
    """VIKASECO cost ₹1.38 lakh and cannot be sold. Counting it as deployed
    makes the return on the money that is actually working look smaller."""
    live = holding(invested=206028.0, value=212301.0, unreal=6273.0)
    dead = dict(holding(invested=137662.0, value=0.0, unreal=-137662.0),
                symbol="NSE:VIKASECO-EQ")

    with_it = account_portfolio("rahul", FUNDS, [], [live, dead], capital_in=500000)
    without = account_portfolio("rahul", FUNDS, [], [live, dead], capital_in=500000,
                                excluded={"NSE:VIKASECO-EQ"})

    assert with_it["deployed"] == Decimal("343690.0")
    assert without["deployed"] == Decimal("206028.0")
    assert without["unrealised"] == Decimal("6273.0")
    assert without["return_pct"] > with_it["return_pct"], (
        "excluding a write-off must not flatter the return, but it must stop "
        "the write-off from dragging the working book's")


def test_what_was_set_aside_is_still_reported():
    """Money written off is still money. A page that simply dropped the row
    would be a different kind of lie from the one exclusion fixes."""
    dead = dict(holding(invested=137662.0, value=1400.0, unreal=-136262.0),
                symbol="NSE:VIKASECO-EQ")
    p = account_portfolio("rahul", FUNDS, [], [dead], excluded={"NSE:VIKASECO-EQ"})

    assert p["excluded"]["count"] == 1
    assert p["excluded"]["symbols"] == ["NSE:VIKASECO-EQ"]
    assert p["excluded"]["cost"] == Decimal("137662.0")
    assert p["excluded"]["unrealised"] == Decimal("-136262.0")
    assert p["deployed"] == Decimal("0"), "it must be gone from the working figure"


def test_excluding_a_position_not_only_a_holding():
    """The same scrip can sit in either book; the judgement is about the scrip."""
    stuck = dict(long_position(qty=100, avg=50.0, ltp=1.0, unreal=-4900.0),
                 symbol="NSE:STUCK-EQ")
    p = account_portfolio("rahul", FUNDS, [stuck], [], excluded={"NSE:STUCK-EQ"})

    assert p["deployed"] == Decimal("0")
    assert p["unrealised"] == Decimal("0")
    assert p["counts"]["positions"] == 0
    assert p["excluded"]["cost"] == Decimal("5000.00")


def test_exclusions_consolidate_across_accounts():
    """Two accounts each stuck with the same scrip is one line, one total."""
    dead = dict(holding(invested=100000.0, value=0.0, unreal=-100000.0),
                symbol="NSE:VIKASECO-EQ")
    accounts = [
        account_portfolio(name, FUNDS, [], [dead], excluded={"NSE:VIKASECO-EQ"})
        for name in ("rahul", "pratibha")
    ]
    totals = consolidate(accounts)

    assert totals["excluded"]["count"] == 2
    assert totals["excluded"]["symbols"] == ["NSE:VIKASECO-EQ"], "one name, not two"
    assert totals["excluded"]["cost"] == Decimal("200000.0")
    assert totals["deployed"] == Decimal("0")
