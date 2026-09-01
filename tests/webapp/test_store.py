"""The store, exercised the way the agent actually uses it.

The regression this file exists for: the agent opens its connection on the main
thread and writes from the poller's thread. sqlite3 refuses that by default, and
because the writer swallows its own errors to keep polling alive, the refusal
was completely invisible — the agent ran normally and stored nothing.
"""
import sqlite3
import threading

import pytest

from webapp.store.reader import Reader
from webapp.store.schema import SCHEMA_VERSION, connect, migrate
from webapp.store.writer import Writer


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "store.db")
    conn = connect(path)
    migrate(conn)
    return path, conn


def test_a_connection_made_on_one_thread_is_writable_from_another(db):
    path, conn = db
    writer = Writer(conn, "rahul")

    def poll():
        writer.seen()
        writer.snapshot("funds", {"available": 1.0})
        writer.orders([{"order_id": "1", "symbol": "NSE:X-EQ", "side": "BUY"}])

    thread = threading.Thread(target=poll)
    thread.start()
    thread.join()

    assert writer.errors == 0, "the poller thread could not write"
    reader = Reader(connect(path, read_only=True))
    assert reader.accounts() == ["rahul"]
    assert reader.counts()["snapshots"] == 1
    reader.conn.close()


def test_concurrent_writers_do_not_lose_rows(db):
    """Three agents write to one file. WAL plus busy_timeout should mean they
    wait for each other rather than dropping a fill."""
    path, _ = db
    writers = [Writer(connect(path), name) for name in ("rahul", "pratibha", "piyush")]

    def hammer(writer, tag):
        for i in range(25):
            writer.fills([{"trade_id": "%s-%d" % (tag, i), "symbol": "NSE:X-EQ",
                           "side": "BUY", "qty": 1, "price": 100.0}])

    threads = [threading.Thread(target=hammer, args=(w, w.account)) for w in writers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(w.errors for w in writers) == 0
    reader = Reader(connect(path, read_only=True))
    assert reader.counts()["fills"] == 75
    reader.conn.close()


def test_a_write_failure_never_raises_into_the_poller(db):
    """Losing history is acceptable; stopping the agent is not."""
    path, conn = db
    writer = Writer(conn, "rahul")
    conn.close()  # the worst case: the handle is gone

    writer.seen()
    writer.orders([{"order_id": "1", "symbol": "NSE:X-EQ", "side": "BUY"}])
    writer.snapshot("funds", {"available": 1.0})
    writer.status({"live": True})

    assert writer.errors == 4, "every failure counted"
    assert writer.writes == 0


def test_the_api_cannot_write_through_a_read_only_connection(db):
    path, _ = db
    reader_conn = connect(path, read_only=True)
    with pytest.raises(sqlite3.OperationalError):
        reader_conn.execute("INSERT INTO accounts VALUES ('x', 0, 0)")
    reader_conn.close()


def test_migrate_refuses_a_newer_schema(db):
    """An older process must not silently write into a schema it does not
    understand."""
    path, conn = db
    conn.execute("PRAGMA user_version = %d" % (SCHEMA_VERSION + 1))
    conn.commit()
    with pytest.raises(RuntimeError, match="schema version"):
        migrate(conn)


def test_snapshots_are_written_only_when_the_payload_changes(db):
    _, conn = db
    writer = Writer(conn, "rahul")
    payload = [{"symbol": "NSE:X-EQ", "net_qty": 10}]

    assert writer.snapshot("positions", payload) is True
    assert writer.snapshot("positions", list(payload)) is False, "same content, new list"
    assert writer.snapshot("positions", [{"symbol": "NSE:X-EQ", "net_qty": 11}]) is True

    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2


def test_attribution_improves_but_never_regresses(db):
    """A later poll that has not yet matched an order must not blank the run a
    previous poll established."""
    path, conn = db
    writer = Writer(conn, "rahul")
    writer.orders([{"order_id": "1", "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
                    "source": "bot", "run": "rahul/reliance", "matched_by": "symbol"}])
    writer.orders([{"order_id": "1", "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
                    "filled_qty": 140, "status": "FILLED"}])

    row = conn.execute("SELECT run, source, filled_qty, status FROM orders").fetchone()
    assert row["run"] == "rahul/reliance"
    assert row["source"] == "bot"
    assert row["filled_qty"] == 140, "the fill still landed"
    assert row["status"] == "FILLED"


def test_a_fill_seen_twice_is_stored_once(db):
    """The broker returns the same trades all session."""
    _, conn = db
    writer = Writer(conn, "rahul")
    fill = [{"trade_id": "t1", "symbol": "NSE:X-EQ", "side": "BUY", "qty": 1, "price": 1.0}]
    writer.fills(fill)
    writer.fills(fill)
    assert conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1


def test_the_default_db_argument_does_not_disable_the_store():
    """`--db none` turns persistence off. The default must not.

    argparse's default for --db is Python's None, and `str(None).lower()` is the
    string "none" — so the original check disabled the store whenever the flag
    was omitted, which is every time. The agents ran for a day writing nothing
    and reporting themselves healthy.
    """
    from webapp.agent.main import persistence_disabled

    assert persistence_disabled(None) is False, "the default must keep the store on"
    assert persistence_disabled("") is False
    assert persistence_disabled("/var/lib/dashboard.db") is False

    assert persistence_disabled("none") is True
    assert persistence_disabled("NONE") is True
    assert persistence_disabled(" none ") is True


def test_a_price_tick_is_not_a_change(db):
    """The regression: the digest covered ltp and unrealised, which move on
    every tick, so 'write on change' wrote on every poll — measured at 116 MB a
    day on a host already short of disk."""
    _, conn = db
    writer = Writer(conn, "rahul")

    def positions(ltp, qty=1808):
        return [{"symbol": "NSE:RELIANCE-EQ", "net_qty": qty, "avg_price": 1308.8,
                 "ltp": ltp, "unrealised": (ltp - 1308.8) * qty, "raw": {"ltp": ltp}}]

    assert writer.snapshot("positions", positions(1288.2)) is True
    assert writer.snapshot("positions", positions(1288.3)) is False
    assert writer.snapshot("positions", positions(1305.0)) is False

    # A real change to the position still writes immediately.
    assert writer.snapshot("positions", positions(1305.0, qty=1948)) is True
    assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2


def test_the_stored_snapshot_still_contains_the_marks(db):
    """Excluded from the comparison, not from the payload — the dashboard's
    fallback view needs the marks."""
    path, conn = db
    writer = Writer(conn, "rahul")
    writer.snapshot("positions", [{"symbol": "NSE:X-EQ", "net_qty": 1, "ltp": 99.5,
                                   "unrealised": 12.5}])

    stored = Reader(connect(path, read_only=True)).latest_snapshot("rahul", "positions")
    assert stored["data"][0]["ltp"] == 99.5
    assert stored["data"][0]["unrealised"] == 12.5


def test_marks_are_refreshed_periodically_even_when_nothing_changes(db):
    """Otherwise a position held all day would keep its opening mark, and the
    fallback view would show a price hours old as if it were the last one."""
    from webapp.store.writer import REFRESH_SECONDS

    _, conn = db
    writer = Writer(conn, "rahul")
    payload = [{"symbol": "NSE:X-EQ", "net_qty": 1, "ltp": 100.0}]

    assert writer.snapshot("positions", payload) is True
    assert writer.snapshot("positions", payload) is False

    writer._written_at["positions"] -= REFRESH_SECONDS + 1
    assert writer.snapshot("positions", payload) is True


def test_holdings_ignore_their_own_volatile_fields(db):
    _, conn = db
    writer = Writer(conn, "rahul")

    def holding(ltp):
        return [{"symbol": "NSE:HFCL-EQ", "qty": 900, "cost_price": 228.92, "ltp": ltp,
                 "market_value": 900 * ltp, "unrealised": 900 * (ltp - 228.92)}]

    assert writer.snapshot("holdings", holding(235.89)) is True
    assert writer.snapshot("holdings", holding(236.40)) is False
    assert writer.snapshot("holdings", [{"symbol": "NSE:HFCL-EQ", "qty": 800,
                                         "cost_price": 228.92, "ltp": 236.40,
                                         "market_value": 0, "unrealised": 0}]) is True


def test_a_fill_keeps_its_own_trading_day(db):
    """Live polling only sees today's book, so stamping 'now' is right for it —
    but a row that carries its own day must keep it. Otherwise a backfill of
    last week's trades lands stamped today, and every one is then classified
    intraday because its entry and exit share a date."""
    path, conn = db
    writer = Writer(conn, "rahul")
    writer.fills([{"trade_id": "old", "symbol": "NSE:X-EQ", "side": "BUY",
                   "qty": 1, "price": 10.0, "trading_day": "2026-08-27"}])
    writer.fills([{"trade_id": "new", "symbol": "NSE:X-EQ", "side": "SELL",
                   "qty": 1, "price": 12.0}])

    days = dict(conn.execute("SELECT trade_id, trading_day FROM fills"))
    assert days["old"] == "2026-08-27"
    assert days["new"] != "2026-08-27", "no day given, so today"


def test_an_order_keeps_its_own_trading_day(db):
    _, conn = db
    Writer(conn, "rahul").orders([{"order_id": "1", "symbol": "NSE:X-EQ",
                                   "side": "BUY", "trading_day": "2026-08-27"}])
    assert conn.execute("SELECT trading_day FROM orders").fetchone()[0] == "2026-08-27"


def test_capital_is_idempotent_on_its_reference(db):
    """Re-importing an overlapping date range is the normal case, not an edge
    one: the same ledger row must not be counted twice, or every return figure
    on the page is wrong."""
    path, conn = db
    writer = Writer(conn, "rahul")
    row = {"on_date": "2026-04-05", "amount": 500000, "source": "ledger",
           "reference": "LED-1", "note": "opening transfer"}

    assert writer.capital([row]) == 1
    assert writer.capital([row]) == 0, "same reference, already recorded"
    assert writer.capital([dict(row, reference="LED-2", amount=250000)]) == 1

    reader = Reader(connect(path, read_only=True))
    assert reader.capital_in("rahul") == "750000.0"
    reader.conn.close()


def test_a_withdrawal_reduces_capital_in(db):
    path, conn = db
    writer = Writer(conn, "rahul")
    writer.capital([{"on_date": "2026-04-05", "amount": 500000,
                     "source": "ledger", "reference": "IN"},
                    {"on_date": "2026-06-01", "amount": -125000,
                     "source": "ledger", "reference": "OUT"}])
    reader = Reader(connect(path, read_only=True))
    assert reader.capital_in("rahul") == "375000.0"
    reader.conn.close()


def test_capital_can_be_asked_as_at_a_date(db):
    """So a financial-year view measures against the capital that was in at the
    time, not against money added since."""
    path, conn = db
    Writer(conn, "rahul").capital([
        {"on_date": "2026-04-05", "amount": 500000, "source": "ledger", "reference": "A"},
        {"on_date": "2026-09-01", "amount": 300000, "source": "ledger", "reference": "B"},
    ])
    reader = Reader(connect(path, read_only=True))
    assert reader.capital_in("rahul", upto="2026-08-31") == "500000.0"
    assert reader.capital_in("rahul") == "800000.0"
    reader.conn.close()


# ── order events ────────────────────────────────────────────────────────────
#
# Orders are upserted, so a status moving from PENDING to FILLED overwrites the
# previous value and the change itself is lost. Nothing then tells you a
# position closed while you were looking elsewhere.


def order(status, filled=0.0, **kw):
    row = {"order_id": "1", "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
           "qty": 140, "filled_qty": filled, "status": status}
    row.update(kw)
    return row


def events(conn):
    return [dict(r) for r in conn.execute(
        "SELECT kind, order_id, from_status, to_status, filled_qty"
        " FROM order_events ORDER BY id")]


def test_a_status_change_is_recorded_before_it_is_overwritten(db):
    _, conn = db
    writer = Writer(conn, "rahul")

    writer.orders([order("PENDING")])
    writer.orders([order("FILLED", 140)])

    assert [e["kind"] for e in events(conn)] == ["placed", "filled"]
    assert events(conn)[1]["from_status"] == "PENDING"


def test_an_unchanged_order_records_nothing(db):
    """The poller re-reads every order every few seconds. Logging each read
    would bury the changes in thousands of rows that say nothing happened."""
    _, conn = db
    writer = Writer(conn, "rahul")

    for _ in range(5):
        writer.orders([order("PENDING")])

    assert len(events(conn)) == 1, "one placement, not five"


def test_a_partial_fill_is_its_own_event(db):
    """Quantity moving without the status moving is still something happening,
    and on a large order it is the thing worth watching."""
    _, conn = db
    writer = Writer(conn, "rahul")

    writer.orders([order("PENDING")])
    writer.orders([order("PENDING", 70)])
    writer.orders([order("PENDING", 110)])

    kinds = [e["kind"] for e in events(conn)]
    assert kinds == ["placed", "partial", "partial"]
    assert events(conn)[-1]["filled_qty"] == 110.0


def test_a_reject_keeps_its_cause(db):
    _, conn = db
    Writer(conn, "rahul").orders([
        order("REJECTED", message="RED:Margin shortfall for this order")])

    row = conn.execute("SELECT kind, message FROM order_events").fetchone()
    assert row["kind"] == "placed"
    assert "Margin" in row["message"]


def test_transitions_survive_an_agent_restart(db):
    """The comparison comes from the store, not from memory — so a change that
    happened while no agent was running is still noticed on the next poll."""
    _, conn = db
    Writer(conn, "rahul").orders([order("PENDING")])

    # A fresh writer, as after a restart, with no memory of the previous poll.
    Writer(conn, "rahul").orders([order("CANCELLED")])

    assert [e["kind"] for e in events(conn)] == ["placed", "cancelled"]


def test_one_account_does_not_see_another_accounts_order_ids(db):
    """Fyers order ids are not unique across accounts. Comparing them globally
    would report pratibha's order as a change to rahul's."""
    _, conn = db
    Writer(conn, "rahul").orders([order("PENDING")])
    Writer(conn, "pratibha").orders([order("FILLED", 140)])

    kinds = [(r["account"], r["kind"]) for r in conn.execute(
        "SELECT account, kind FROM order_events ORDER BY id")]
    assert kinds == [("rahul", "placed"), ("pratibha", "placed")]
