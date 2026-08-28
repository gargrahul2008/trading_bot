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
