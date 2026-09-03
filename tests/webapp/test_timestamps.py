"""Broker timestamps, made comparable.

The bug this exists for: Fyers writes "31-Aug-2026 10:15:23", and as text that
sorts *after* "03-Sep-2026". The pad's Recent list ordered on it and showed one
block of closed trades above the open positions and another below them.
"""
from webapp.pnl.consolidate import recent
from webapp.timestamps import to_iso


def test_the_brokers_own_form_is_parsed():
    assert to_iso("31-Aug-2026 10:15:23") == "2026-08-31 10:15:23"
    assert to_iso("03-Sep-2026 09:45:12") == "2026-09-03 09:45:12"


def test_august_now_sorts_before_september():
    """The whole point. As raw text these compare the other way round."""
    august, september = "31-Aug-2026 10:15:23", "03-Sep-2026 09:45:12"

    assert august > september, "which is what made this necessary"
    assert to_iso(august) < to_iso(september)


def test_iso_survives_unchanged():
    """A store written by a later version costs nothing to read."""
    assert to_iso("2026-09-01 10:00:00") == "2026-09-01 10:00:00"


def test_a_day_first_numeric_date_is_read_day_first():
    assert to_iso("01-09-2026 11:00:00") == "2026-09-01 11:00:00"


def test_an_epoch_is_accepted_as_a_number_or_a_string():
    """It survives JSON and then SQLite's TEXT columns as a string."""
    assert to_iso(1756636631) == to_iso("1756636631") != ""


def test_a_price_is_not_mistaken_for_an_epoch():
    """140.0 is a quantity, not 1970. A floor keeps small numbers out."""
    assert to_iso(140.0) == ""
    assert to_iso(0) == ""


def test_nothing_unreadable_raises():
    """A trade with an odd stamp still belongs in the list, at the bottom."""
    for value in (None, "", "rubbish", "  "):
        assert to_iso(value) == ""


def match(day, at, symbol):
    return {"account": "rahul", "symbol": symbol, "closed_day": day,
            "kind": "positional", "direction": "LONG", "qty": 10,
            "entry_price": 100, "exit_price": 110, "gross": 100, "charges": "1",
            "product_type": "CNC", "closed_at": at}


def position(symbol, opened):
    return {"account": "rahul", "symbol": symbol, "net_qty": 10, "avg_price": 100,
            "ltp": 110, "unrealised": 100, "product_type": "CNC",
            "opened_day": opened}


def test_the_recent_list_is_in_date_order_across_a_month_boundary():
    """The reported symptom: closed trades from 31 August above open positions
    from 2 September, and closed trades from 3 September below them."""
    lines = recent(
        [position("NSE:OPEN-EQ", "2026-09-02")],
        [match("2026-08-31", "31-Aug-2026 10:15:23", "NSE:AUG-EQ"),
         match("2026-09-03", "03-Sep-2026 09:45:12", "NSE:SEP-EQ")],
    )

    assert [line["symbol"] for line in lines] == [
        "NSE:SEP-EQ", "NSE:OPEN-EQ", "NSE:AUG-EQ"]


def test_a_row_with_no_readable_time_still_sorts_by_its_day():
    """Leading with the timestamp dropped every such row to the bottom whatever
    its date — which is how the list ended up in two blocks."""
    lines = recent([], [match("2026-09-03", "rubbish", "NSE:NEW-EQ"),
                        match("2026-08-01", "01-Aug-2026 10:00:00", "NSE:OLD-EQ")])

    assert [line["symbol"] for line in lines] == ["NSE:NEW-EQ", "NSE:OLD-EQ"]


def test_a_time_without_a_date_still_orders_among_its_own():
    """to_iso cannot turn "09:20" into a full stamp and returns "". The sort key
    keeps the raw string after the parsed one so those rows still order against
    each other instead of collapsing to a tie and falling through to trade id —
    which silently reversed a short round trip into a long one."""
    from webapp.pnl.matcher import _sort_key

    early = {"trading_day": "2026-09-02", "traded_at": "09:20", "trade_id": "s"}
    late = {"trading_day": "2026-09-02", "traded_at": "14:20", "trade_id": "b"}

    assert _sort_key(early) < _sort_key(late), "despite 's' sorting after 'b'"
