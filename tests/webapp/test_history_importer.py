"""Loading broker history into the store.

Figures here come from the live accounts on 2026-08-31 (docs/host_state.md §12).
"""
import pytest

from webapp.history.importer import (
    import_capital, import_charges, import_realised, progress,
    realised_total, record_progress,
)
from webapp.store.reader import Reader
from webapp.store.schema import connect, migrate


@pytest.fixture
def db(tmp_path):
    conn = connect(str(tmp_path / "h.db"))
    migrate(conn)
    return conn


LEDGER = {"transactions": [
    {"date": "2026-04-05", "credit": 1509420, "debit": 0,
     "transaction_type": "Funds added", "description": "NEFT"},
    {"date": "2026-06-01", "credit": 0, "debit": 1033830.81,
     "transaction_type": "Funds withdrawn", "description": "payout"},
    # The day's trading settled into the balance — not capital.
    {"date": "2026-08-28", "credit": 134414.27, "debit": 0,
     "transaction_type": "Trading",
     "description": "Executed trades for the day in equity cash segment"},
    {"date": "2026-08-20", "credit": 0, "debit": 5000,
     "transaction_type": "MTF", "description": "MTF interest"},
]}


def test_only_genuine_transfers_count_as_capital(db):
    """A Trading row is the day's P&L settling into the balance. Counting it as
    capital would make every rupee earned look like a rupee deposited, and drive
    the return figure towards zero."""
    assert import_capital(db, "rahul", LEDGER) == 2

    reader = Reader(db)
    assert reader.capital_in("rahul") == "475589.19"   # 1,509,420 − 1,033,830.81
    assert [e["note"] for e in reader.capital_entries("rahul")] == ["NEFT", "payout"]


def test_reimporting_an_overlapping_range_changes_nothing(db):
    """The sensible way to stay current is to re-fetch the last few days each
    evening, so this is the normal path, not an edge case."""
    assert import_capital(db, "rahul", LEDGER) == 2
    assert import_capital(db, "rahul", LEDGER) == 0
    assert Reader(db).capital_in("rahul") == "475589.19"


def test_two_accounts_keep_separate_capital(db):
    import_capital(db, "rahul", LEDGER)
    import_capital(db, "piyush", {"transactions": [
        {"date": "2026-04-02", "credit": 2500000, "debit": 0,
         "transaction_type": "Funds added", "description": "opening"}]})
    reader = Reader(db)
    assert reader.capital_in("rahul") == "475589.19"
    assert reader.capital_in("piyush") == "2500000.0"


def test_realised_is_stored_per_scrip_per_day_and_upserted(db):
    rows = [{"day": "2026-08-28", "symbol": "NSE:CGCL-EQ", "realised": 5134.176,
             "buy_qty": 1140, "sell_qty": 1140, "buy_rate": 180.7421, "sell_rate": 185.2458}]
    assert import_realised(db, "rahul", rows) == 1

    # A re-fetch of the same day corrects rather than duplicates.
    import_realised(db, "rahul", [dict(rows[0], realised=5200.0)])
    stored = db.execute("SELECT COUNT(*), SUM(realised) FROM realised_history").fetchone()
    assert stored[0] == 1 and stored[1] == 5200.0


def test_the_lying_exchange_fields_are_not_stored(db):
    """BSE:SHISHIND-X comes back with exch_id 10 / NSE, which is wrong. Storing
    it wrong is worse than not storing it."""
    import_realised(db, "pratibha", [{"day": "2026-08-12", "symbol": "BSE:SHISHIND-X",
                                      "realised": 100.0}])
    columns = {row[1] for row in db.execute("PRAGMA table_info(realised_history)")}
    assert "exch_id" not in columns and "segment_name" not in columns
    assert db.execute("SELECT symbol FROM realised_history").fetchone()[0] == "BSE:SHISHIND-X"


def test_charges_are_stored_per_day_and_upserted(db):
    row = {"day": "2026-08-28", "total": 195.37, "turnover": 142657.0002,
           "brokerage": 30, "stt": 135, "gst": 7.99, "stamp_duty": 8,
           "transaction_charges": 13.83, "sebi_toc": 0.55, "ipft": 0}
    assert import_charges(db, "rahul", {"rows": [row]}) == 1
    import_charges(db, "rahul", {"rows": [row]})
    assert db.execute("SELECT COUNT(*) FROM charges_daily").fetchone()[0] == 1


def test_realised_total_nets_charges_off_gross(db):
    """This is the headline realised figure, and it is the broker's own — our
    matching supplies per-trade detail and is never added to it."""
    import_realised(db, "rahul", [
        {"day": "2026-08-27", "symbol": "NSE:A-EQ", "realised": 1000},
        {"day": "2026-08-28", "symbol": "NSE:B-EQ", "realised": -300},
    ])
    import_charges(db, "rahul", {"rows": [
        {"day": "2026-08-27", "total": 50}, {"day": "2026-08-28", "total": 20}]})

    # Strings, not floats — this is money and the browser gets exactly what was
    # computed. The trailing .0 is SQLite's REAL storage showing through.
    assert realised_total(db, "rahul") == {"gross": "700.0", "charges": "70.0", "net": "630.0"}


def test_realised_total_can_be_asked_for_a_window(db):
    import_realised(db, "rahul", [
        {"day": "2026-07-31", "symbol": "NSE:A-EQ", "realised": 5000},
        {"day": "2026-08-28", "symbol": "NSE:B-EQ", "realised": 1000},
    ])
    august = realised_total(db, "rahul", from_date="2026-08-01")
    assert august["gross"] == "1000.0"


def test_progress_widens_rather_than_replaces(db):
    """A later run that fetched only the last week must not shrink the recorded
    range and make the earlier import look undone."""
    record_progress(db, "rahul", "realised", "2026-04-01", "2026-08-29")
    record_progress(db, "rahul", "realised", "2026-08-25", "2026-08-31")

    assert progress(db, "rahul")["realised"] == {"from": "2026-04-01", "to": "2026-08-31"}


def test_nothing_to_import_is_not_an_error(db):
    assert import_capital(db, "rahul", {"transactions": []}) == 0
    assert import_realised(db, "rahul", []) == 0
    assert import_charges(db, "rahul", {"rows": []}) == 0
    assert realised_total(db, "rahul")["net"] == "0"


def test_epoch_conversion_is_timezone_aware():
    """The host runs Python 3.12, where utcfromtimestamp is deprecated and will
    eventually be removed; local runs 3.9, where datetime.UTC does not exist.
    timezone.utc is the one that works on both."""
    import warnings

    from webapp.history.client import epoch_ms_to_date
    from webapp.store.writer import trading_day

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert epoch_ms_to_date(1787875200000) == "2026-08-28"
        assert len(trading_day()) == 10


PRATIBHA_LEDGER = {"transactions": [
    {"date": "2026-04-01", "credit": 2204655.11, "debit": 0,
     "transaction_type": "Opening Balance", "description": "Equity"},
    {"date": "2026-04-17", "credit": 0, "debit": 1096676.62,
     "transaction_type": "Funds withdrawn", "description": "Funds sent"},
    {"date": "2026-04-29", "credit": 1000000, "debit": 0,
     "transaction_type": "Funds added", "description": "Funds added"},
    {"date": "2026-06-09", "credit": 0, "debit": 400000,
     "transaction_type": "Funds withdrawn", "description": "to bank"},
]}


def test_transfers_alone_can_be_negative_which_is_a_useless_base(db):
    """Pratibha's real ledger: her transfers net to minus 496,676.62 over the
    year. Divide a P&L by that and the return figure is nonsense."""
    import_capital(db, "pratibha", PRATIBHA_LEDGER)
    assert Reader(db).capital_in("pratibha") == "-496676.62"


def test_the_opening_balance_makes_the_base_the_money_actually_at_work(db):
    """2,204,655.11 was in her account on 1 April. That is what the year's P&L
    was earned on."""
    assert import_capital(db, "pratibha", PRATIBHA_LEDGER, opening_for="2026-04-01") == 4
    assert Reader(db).capital_in("pratibha") == "1707978.49"


def test_reimporting_the_same_backfill_adds_no_opening_balance(db):
    import_capital(db, "pratibha", PRATIBHA_LEDGER, opening_for="2026-04-01")
    assert import_capital(db, "pratibha", PRATIBHA_LEDGER, opening_for="2026-04-01") == 0
    assert Reader(db).capital_in("pratibha") == "1707978.49"


def test_a_later_window_must_not_contribute_its_opening_balance(db):
    """The opening balance of a seven-day window already contains the year's
    profits. Counting it would record earnings as money paid in — the same
    mistake as counting a Trading row, in a form that is harder to see."""
    import_capital(db, "pratibha", PRATIBHA_LEDGER, opening_for="2026-04-01")
    weekly = {"transactions": [
        {"date": "2026-08-24", "credit": 58660.02, "debit": 0,
         "transaction_type": "Opening Balance", "description": "Equity"},
    ]}
    assert import_capital(db, "pratibha", weekly) == 0
    assert Reader(db).capital_in("pratibha") == "1707978.49"


def test_both_segment_rows_of_an_opening_balance_are_kept(db):
    """Fyers returns the opening balance split across segments — rahul's came
    back as two rows summing to 205,401.08."""
    ledger = {"transactions": [
        {"date": "2026-04-01", "credit": 200000.00, "debit": 0,
         "transaction_type": "Opening Balance", "description": "Equity"},
        {"date": "2026-04-01", "credit": 5401.08, "debit": 0,
         "transaction_type": "Opening Balance", "description": "Derivatives"},
    ]}
    assert import_capital(db, "rahul", ledger, opening_for="2026-04-01") == 2
    assert Reader(db).capital_in("rahul") == "205401.08"
