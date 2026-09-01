"""The exchanges' instrument list.

Two things nothing else can settle: whether a symbol exists, and what its tick
size is. Both matter more since the order pad places on one click.
"""
import pytest

from webapp.history.symbols import (
    MIN_COLUMNS, SymbolFormatError, counts, lookup, parse, search, store, verify_row,
)
from webapp.store.schema import connect, migrate

# Real rows from the published NSE file, unaltered.
NSE_CSV = (
    "101000000016921,20 MICRONS LTD,0,1,0.01,INE144J01027,0915-1530|1815-1915:,"
    "2026-08-31,,NSE:20MICRONS-EQ,10,10,16921,20MICRONS,16921,-1.0,XX,"
    "101000000016921,None,1,2.0\n"
    "101000000013061,360 ONE WAM LIMITED,0,1,0.1,INE466L01038,0915-1530|1815-1915:,"
    "2026-08-31,,NSE:360ONE-EQ,10,10,13061,360ONE,13061,-1.0,XX,"
    "101000000013061,None,1,3.2\n"
    "101000000002885,RELIANCE INDUSTRIES LTD,0,1,0.1,INE002A01018,0915-1530|1815-1915:,"
    "2026-08-31,,NSE:RELIANCE-EQ,10,10,2885,RELIANCE,2885,-1.0,XX,"
    "101000000002885,None,1,3.2\n"
)


@pytest.fixture
def db(tmp_path):
    conn = connect(str(tmp_path / "s.db"))
    migrate(conn)
    store(conn, parse(NSE_CSV, "NSE", "CASH"))
    return conn


def test_tick_size_is_per_instrument_not_per_exchange():
    """20MICRONS trades in paise and RELIANCE in ten-paise steps, both on NSE.
    A single hardcoded tick is wrong for one of them, and an off-tick price is
    rejected by the exchange."""
    rows = {r["symbol"]: r for r in parse(NSE_CSV, "NSE", "CASH")}
    assert rows["NSE:20MICRONS-EQ"]["tick_size"] == 0.01
    assert rows["NSE:RELIANCE-EQ"]["tick_size"] == 0.1


def test_the_columns_are_read_from_the_right_positions():
    row = parse(NSE_CSV, "NSE", "CASH")[2]
    assert row["symbol"] == "NSE:RELIANCE-EQ"
    assert row["short_name"] == "RELIANCE"
    assert row["name"] == "RELIANCE INDUSTRIES LTD"
    assert row["isin"] == "INE002A01018"
    assert row["lot_size"] == 1


def test_a_changed_format_fails_loudly_rather_than_importing_nonsense():
    """A positional read of someone else's CSV breaks silently when they add a
    column: every symbol still imports, with the tick size taken from whatever
    now sits in that slot. Importing nothing is better."""
    with pytest.raises(SymbolFormatError, match="at least"):
        verify_row(["only", "three", "columns"])

    shifted = ["x"] * MIN_COLUMNS
    with pytest.raises(SymbolFormatError, match="symbol like"):
        verify_row(shifted)


def test_a_zero_tick_falls_back_rather_than_letting_any_price_through():
    csv_row = NSE_CSV.replace(",0.01,", ",0,", 1)
    rows = {r["symbol"]: r for r in parse(csv_row, "NSE", "CASH")}
    assert rows["NSE:20MICRONS-EQ"]["tick_size"] == 0.05


def test_search_puts_the_one_you_meant_first(db):
    """Ranked the way a trader means it: NSE before BSE, the plain -EQ series
    before the lettered ones. Without that, typing RELI surfaced BSE:RELICAB-B
    ahead of NSE:RELIANCE-EQ purely because its symbol is shorter."""
    store(db, parse(
        "1,RELIANCE INDUSTRIES LTD,0,1,0.05,INE002A01018,x,2026-08-31,,BSE:RELIANCE-A,"
        "12,12,1,RELIANCE,1,-1.0,XX,1,None,1,1.0\n", "BSE", "CASH"))

    assert search(db, "reliance")[0]["symbol"] == "NSE:RELIANCE-EQ"


def test_an_exact_short_name_beats_a_longer_match(db):
    assert search(db, "360ONE")[0]["symbol"] == "NSE:360ONE-EQ"


def test_nothing_typed_suggests_nothing(db):
    """A dropdown that opens with arbitrary suggestions is noise — nobody wants
    the first eight symbols alphabetically."""
    assert search(db, "") == []
    assert search(db, "   ") == []


def test_lookup_says_no_for_a_symbol_that_does_not_exist(db):
    """This is how the order pad answers 'is this real?' — a completion like
    NSE:RELIA-EQ looks right and is not."""
    assert lookup(db, "NSE:RELIANCE-EQ")["tick_size"] == 0.1
    assert lookup(db, "NSE:RELIA-EQ") is None


def test_reimporting_updates_rather_than_duplicates(db):
    before = counts(db)["symbols"]
    store(db, parse(NSE_CSV, "NSE", "CASH"))
    assert counts(db)["symbols"] == before


def test_a_tick_change_is_picked_up_on_the_next_refresh(db):
    store(db, parse(NSE_CSV.replace(",0.1,INE002A01018", ",0.05,INE002A01018"), "NSE", "CASH"))
    assert lookup(db, "NSE:RELIANCE-EQ")["tick_size"] == 0.05
