"""Realised P&L per scrip for the year, from the broker's own history.

This is the complete source — it covers shares bought years before our fills
begin and sold in May, and everything bought and sold within the year. Our FIFO
matching supplies per-trade detail the broker never gives, and the two are never
added together.
"""
import pytest

from webapp.history.importer import import_charges, import_realised
from webapp.pnl.realised import by_scrip, scrip_turnover
from webapp.store.schema import connect, migrate


@pytest.fixture
def db(tmp_path):
    conn = connect(str(tmp_path / "r.db"))
    migrate(conn)
    return conn


def realised(day, symbol, pnl, buy_qty=0, sell_qty=0, buy_rate=0, sell_rate=0):
    return {"day": day, "symbol": symbol, "realised": pnl, "buy_qty": buy_qty,
            "sell_qty": sell_qty, "buy_rate": buy_rate, "sell_rate": sell_rate}


def test_a_scrip_sold_from_a_holding_bought_years_ago_is_included(db):
    """Our matching cannot see this trade — the buy predates every fill we have.
    The broker's history reports it regardless, which is why this is the
    complete source."""
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:VIKASECO-EQ", -2000.0, sell_qty=66184, sell_rate=2.08)])
    import_charges(db, "rahul", {"rows": [{"day": "2026-05-12", "total": 500.0}]})

    result = by_scrip(db, "rahul")
    scrip = result["scrips"][0]
    assert scrip["symbol"] == "NSE:VIKASECO-EQ"
    assert float(scrip["gross"]) == pytest.approx(-2000.0)
    assert float(scrip["net"]) == pytest.approx(-2500.0)


def test_a_scrip_traded_on_several_days_is_one_row(db):
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:CGCL-EQ", 5134.18, 1140, 1140, 180.74, 185.25),
        realised("2026-06-01", "NSE:CGCL-EQ", 1000.00, 100, 100, 190.0, 200.0),
    ])
    import_charges(db, "rahul", {"rows": [{"day": "2026-05-12", "total": 900.0},
                                          {"day": "2026-06-01", "total": 50.0}]})
    result = by_scrip(db, "rahul")
    assert len(result["scrips"]) == 1
    assert result["scrips"][0]["days"] == 2
    assert float(result["scrips"][0]["gross"]) == pytest.approx(6134.18)


def test_charges_are_split_by_the_brokers_own_scrip_turnover(db):
    """This endpoint reports each scrip's bought and sold value, so the split
    uses the broker's quantities rather than a reconstruction from our fills."""
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:A-EQ", 100.0, 100, 100, 10.0, 10.0),   # 2,000
        realised("2026-05-12", "NSE:B-EQ", 200.0, 100, 100, 40.0, 40.0),   # 8,000
    ])
    import_charges(db, "rahul", {"rows": [{"day": "2026-05-12", "total": 1000.0}]})

    scrips = {s["symbol"]: s for s in by_scrip(db, "rahul")["scrips"]}
    assert float(scrips["NSE:A-EQ"]["charges"]) == pytest.approx(200.0)   # 20%
    assert float(scrips["NSE:B-EQ"]["charges"]) == pytest.approx(800.0)   # 80%


def test_the_apportioned_charges_sum_to_the_day_total(db):
    """No residue: every paisa the broker charged lands on some scrip."""
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:A-EQ", 100.0, 100, 100, 10.0, 11.0),
        realised("2026-05-12", "NSE:B-EQ", 200.0, 33, 33, 7.0, 9.0),
        realised("2026-05-12", "NSE:C-EQ", -50.0, 7, 7, 3.0, 2.0),
    ])
    import_charges(db, "rahul", {"rows": [{"day": "2026-05-12", "total": 777.77}]})
    totals = by_scrip(db, "rahul")["totals"]
    assert float(totals["charges"]) == pytest.approx(777.77, abs=0.02)


def test_a_day_without_charges_leaves_the_scrip_uncosted(db):
    """Not costed at zero — that would claim the trading was free."""
    import_realised(db, "rahul", [realised("2026-05-12", "NSE:A-EQ", 100.0, 10, 10, 5, 6)])
    result = by_scrip(db, "rahul")

    assert result["scrips"][0]["charges"] is None
    assert result["scrips"][0]["net"] is None
    assert result["totals"]["scrips_without_charges"] == 1


def test_accounts_are_kept_apart(db):
    import_realised(db, "rahul", [realised("2026-05-12", "NSE:A-EQ", 100.0, 10, 10, 5, 6)])
    import_realised(db, "pratibha", [realised("2026-05-12", "NSE:A-EQ", -300.0, 10, 10, 6, 5)])

    both = by_scrip(db)
    assert len(both["scrips"]) == 2, "same symbol, two accounts, two rows"
    assert float(by_scrip(db, "rahul")["totals"]["gross"]) == pytest.approx(100.0)


def test_the_window_can_be_narrowed(db):
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:A-EQ", 5000.0, 10, 10, 5, 6),
        realised("2026-08-28", "NSE:B-EQ", 1000.0, 10, 10, 5, 6),
    ])
    august = by_scrip(db, "rahul", from_date="2026-08-01")
    assert [s["symbol"] for s in august["scrips"]] == ["NSE:B-EQ"]


def test_biggest_first(db):
    import_realised(db, "rahul", [
        realised("2026-05-12", "NSE:LOSS-EQ", -900.0, 10, 10, 5, 6),
        realised("2026-05-12", "NSE:GAIN-EQ", 400.0, 10, 10, 5, 6),
    ])
    import_charges(db, "rahul", {"rows": [{"day": "2026-05-12", "total": 10.0}]})
    assert [s["symbol"] for s in by_scrip(db, "rahul")["scrips"]] == ["NSE:GAIN-EQ", "NSE:LOSS-EQ"]


def test_nothing_imported_is_not_an_error(db):
    result = by_scrip(db, "rahul")
    assert result["scrips"] == [] and result["available"] is False


def test_turnover_counts_both_sides(db):
    assert scrip_turnover({"buy_qty": 10, "buy_rate": 100,
                           "sell_qty": 10, "sell_rate": 110}) == 2100
