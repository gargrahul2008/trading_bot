"""Opening positions — what an account held before our fills begin.

The case this exists for: pratibha sold 1,000 SHRINGARMS out of holdings on
2026-08-28. The buy was long before our store began, so FIFO had no long lot to
close, opened a short instead, and the trade's real P&L never appeared. What the
screen showed instead was the broker's `unrealized_profit` on that phantom
short: +3,130, where the sale was a −6,660 realised loss.
"""
import pytest

from webapp.pnl.opening import (
    as_fills, earliest_fill_day, record_manual, seed_from_holdings,
)
from webapp.pnl.service import report
from webapp.store.schema import connect, migrate
from webapp.store.writer import Writer


@pytest.fixture
def db(tmp_path):
    conn = connect(str(tmp_path / "o.db"))
    migrate(conn)
    return conn


def held(conn, holdings):
    Writer(conn, "pratibha").snapshot("holdings", holdings)


def sold(conn, symbol="NSE:SHRINGARMS-EQ", qty=1000, price=223.91, day="2026-08-28"):
    Writer(conn, "pratibha").fills([
        {"trade_id": "s1", "order_id": "o1", "symbol": symbol, "side": "SELL",
         "qty": qty, "price": price, "product_type": "CNC",
         "trading_day": day, "traded_at": "10:31"}])


HOLDING = [{"symbol": "NSE:SHRINGARMS-EQ", "qty": 1000, "cost_price": 230.57,
            "ltp": 231.0, "is_open": True}]


def test_without_an_opening_position_a_sale_opens_a_phantom_short(db):
    held(db, HOLDING)
    sold(db)
    result = report(db, "pratibha")

    assert result["trades"] == []
    assert result["open_positions"][0]["direction"] == "SHORT"


def test_with_one_the_sale_closes_against_what_it_cost(db):
    """−6,660: sold at 223.91 against a holding cost of 230.57."""
    held(db, HOLDING)
    sold(db)
    assert seed_from_holdings(db, "pratibha") == 1

    result = report(db, "pratibha")
    trade = result["trades"][0]
    assert trade["direction"] == "LONG"
    assert float(trade["entry_price"]) == pytest.approx(230.57)
    assert float(trade["exit_price"]) == pytest.approx(223.91)
    assert float(trade["gross"]) == pytest.approx(-6660.0)
    assert result["open_positions"] == [], "no phantom short left behind"


def test_the_opening_buy_is_dated_before_the_first_fill(db):
    """A fill recorded on the first day must apply on top of the opening
    position, not be counted twice."""
    held(db, HOLDING)
    sold(db, day="2026-08-28")
    seed_from_holdings(db, "pratibha")

    opening = as_fills(db, "pratibha")[0]
    assert opening["trading_day"] == "2026-08-27"
    assert opening["side"] == "BUY"
    assert earliest_fill_day(db, "pratibha") == "2026-08-28"


def test_an_opening_trade_is_traceable_to_an_assumed_cost(db):
    """Its entry is the broker's average, not a recorded fill, and the trade id
    says so."""
    held(db, HOLDING)
    seed_from_holdings(db, "pratibha")
    assert as_fills(db, "pratibha")[0]["trade_id"].startswith("opening:")


def test_a_partial_sale_leaves_the_rest_open_at_cost(db):
    held(db, HOLDING)
    sold(db, qty=400)
    seed_from_holdings(db, "pratibha")

    result = report(db, "pratibha")
    assert float(result["trades"][0]["qty"]) == 400
    assert float(result["trades"][0]["gross"]) == pytest.approx(400 * (223.91 - 230.57))
    remaining = result["open_positions"][0]
    assert float(remaining["qty"]) == 600
    assert float(remaining["avg_price"]) == pytest.approx(230.57)


def test_a_manual_entry_outranks_a_reseed(db):
    """Someone who typed a cost basis in knew something the snapshot did not."""
    held(db, HOLDING)
    record_manual(db, "pratibha", "NSE:SHRINGARMS-EQ", qty=1000, cost_price=180.0,
                  as_of_day="2026-04-01", note="bought 2024")
    seed_from_holdings(db, "pratibha")

    assert float(as_fills(db, "pratibha")[0]["price"]) == pytest.approx(180.0)


def test_reseeding_does_not_duplicate(db):
    held(db, HOLDING)
    seed_from_holdings(db, "pratibha")
    seed_from_holdings(db, "pratibha")
    assert len(as_fills(db, "pratibha")) == 1


def test_a_sold_out_holding_is_not_an_opening_position(db):
    held(db, [{"symbol": "NSE:GONE-EQ", "qty": 0, "cost_price": 100.0, "is_open": False}])
    assert seed_from_holdings(db, "pratibha") == 0
    assert as_fills(db, "pratibha") == []


def test_the_earliest_snapshot_is_used_not_the_latest(db):
    """A later snapshot already reflects sales we have recorded; using it would
    hand the matcher the same shares twice."""
    writer = Writer(db, "pratibha")
    writer.snapshot("holdings", HOLDING)
    writer.snapshot("holdings", [{"symbol": "NSE:SHRINGARMS-EQ", "qty": 600,
                                  "cost_price": 230.57, "ltp": 221.0, "is_open": True}])
    seed_from_holdings(db, "pratibha")
    assert float(as_fills(db, "pratibha")[0]["qty"]) == 1000
