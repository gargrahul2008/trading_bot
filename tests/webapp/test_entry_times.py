"""When a position was entered.

The broker's book has no such field — a position is simply there. Without one,
any list claiming to show "recent trades" has nothing stable to order by, and
the pad's fell back to sorting by unrealised P&L. That moves on every tick, so
the rows reshuffled themselves while they were being read.
"""
import pytest

from webapp.pnl.service import open_lots_by_position
from webapp.store.schema import connect, migrate
from webapp.store.writer import Writer


@pytest.fixture
def db(tmp_path):
    conn = connect(str(tmp_path / "e.db"))
    migrate(conn)
    return conn


def fill(trade_id, side, qty, price, day, symbol="NSE:RELIANCE-EQ",
         product="CNC", at=None):
    return {"trade_id": trade_id, "order_id": "o" + trade_id, "symbol": symbol,
            "side": side, "qty": qty, "price": price, "product_type": product,
            "trading_day": day, "traded_at": at or (day + " 10:00:00")}


def test_a_position_is_dated_from_when_it_was_opened(db):
    Writer(db, "rahul").fills([fill("1", "BUY", 100, 1300, "2026-08-20")])

    when = open_lots_by_position(db)
    assert when[("rahul", "NSE:RELIANCE-EQ")]["opened_day"] == "2026-08-20"


def test_adding_to_a_position_does_not_make_it_look_new(db):
    """The oldest still-open parcel, not the latest fill. Otherwise topping up
    a month-old position would push it to the top of a 'recent' list."""
    Writer(db, "rahul").fills([
        fill("1", "BUY", 100, 1300, "2026-08-20"),
        fill("2", "BUY", 50, 1320, "2026-08-28"),
    ])

    assert open_lots_by_position(db)[("rahul", "NSE:RELIANCE-EQ")]["opened_day"] \
        == "2026-08-20"


def test_selling_the_oldest_parcel_moves_the_date_forward(db):
    """FIFO closed the August lot, so what is still open began in September and
    the position is correctly younger than it was."""
    Writer(db, "rahul").fills([
        fill("1", "BUY", 100, 1300, "2026-08-20"),
        fill("2", "BUY", 50, 1320, "2026-09-01"),
        fill("3", "SELL", 100, 1350, "2026-09-02"),
    ])

    assert open_lots_by_position(db)[("rahul", "NSE:RELIANCE-EQ")]["opened_day"] \
        == "2026-09-01"


def test_a_closed_position_is_not_listed(db):
    Writer(db, "rahul").fills([
        fill("1", "BUY", 100, 1300, "2026-08-20"),
        fill("2", "SELL", 100, 1350, "2026-08-21"),
    ])

    assert open_lots_by_position(db) == {}


def test_one_scrip_held_in_two_books_is_dated_from_the_first_buy(db):
    """The broker keeps delivery stock and everything else apart; the trader
    does not, and the caller joins this onto both."""
    Writer(db, "rahul").fills([
        fill("1", "BUY", 100, 1300, "2026-08-20", product="CNC"),
        fill("2", "BUY", 40, 1310, "2026-08-25", product="MTF"),
    ])

    when = open_lots_by_position(db)
    assert len(when) == 1
    assert when[("rahul", "NSE:RELIANCE-EQ")]["opened_day"] == "2026-08-20"


def test_accounts_are_kept_apart(db):
    """Two people holding the same scrip entered it on their own days."""
    Writer(db, "rahul").fills([fill("1", "BUY", 100, 1300, "2026-08-20")])
    Writer(db, "pratibha").fills([fill("2", "BUY", 60, 1290, "2026-08-11")])

    when = open_lots_by_position(db)
    assert when[("rahul", "NSE:RELIANCE-EQ")]["opened_day"] == "2026-08-20"
    assert when[("pratibha", "NSE:RELIANCE-EQ")]["opened_day"] == "2026-08-11"


def test_a_short_has_an_entry_date_too(db):
    Writer(db, "rahul").fills([
        fill("1", "SELL", 100, 1300, "2026-08-20", symbol="NSE:X26SEPFUT")])

    assert open_lots_by_position(db)[("rahul", "NSE:X26SEPFUT")]["opened_day"] \
        == "2026-08-20"


def test_one_account_can_be_asked_for_alone(db):
    Writer(db, "rahul").fills([fill("1", "BUY", 100, 1300, "2026-08-20")])
    Writer(db, "pratibha").fills([fill("2", "BUY", 60, 1290, "2026-08-11")])

    assert set(open_lots_by_position(db, "rahul")) == {("rahul", "NSE:RELIANCE-EQ")}
