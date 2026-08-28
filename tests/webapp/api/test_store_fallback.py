"""When an agent is down, the dashboard should say what it last knew — not
nothing.

Before the store existed, a restarting agent meant that account had no row at
all. Every agent restart used to take 90 seconds, so this was not a rare state.
"""
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.reader import Reader          # noqa: E402
from webapp.store.schema import connect, migrate  # noqa: E402
from webapp.store.writer import Writer          # noqa: E402

from tests.webapp.api.conftest import RAHUL_BOOK, health, serve_json, wire  # noqa: E402


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A store holding what pratibha's agent wrote before it died."""
    path = str(tmp_path / "dashboard.db")
    conn = connect(path)
    migrate(conn)

    writer = Writer(conn, "pratibha")
    writer.seen()
    writer.snapshot("funds", {"available": 300021.34, "utilised": -116645.8,
                              "total": 408588.04, "realised_pnl": -3750.0})
    writer.snapshot("positions", [
        {"symbol": "NSE:CROMPTON26SEPFUT", "net_qty": -2150, "unrealised": 5052.5,
         "realised": 0.0, "is_derivative": True},
    ])
    writer.snapshot("holdings", [])
    writer.orders([{"order_id": "9", "symbol": "NSE:CROMPTON26SEPFUT", "side": "SELL",
                    "qty": 2150, "filled_qty": 2150, "status": "FILLED",
                    "is_open": False, "source": "manual"}])
    writer.status(health(live=True, phase="live"))
    conn.close()

    monkeypatch.setenv("DASHBOARD_DB", path)
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "REPO", REPO)
    return path


def test_the_store_answers_when_the_agent_does_not(store, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    rahul = serve_json({"/book": RAHUL_BOOK, "/health": health()})
    # pratibha's agent is gone — nothing is listening on her port.
    ports = {"rahul": rahul.server_address[1], "pratibha": 9}
    wire(monkeypatch, ports)
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})
    try:
        payload = client.get("/api/overview").json()
    finally:
        rahul.shutdown()
        rahul.server_close()

    rows = {row["account"]: row for row in payload["accounts"]}
    pratibha = rows["pratibha"]

    # The row is populated from what the agent last wrote.
    assert pratibha["from_store"] is True
    assert pratibha["funds"]["available"] == pytest.approx(300021.34)
    assert pratibha["positions"]["open"] == 1
    assert pratibha["orders"]["total"] == 1

    # And it says plainly that this is not live: every section stale, with the
    # agent's own error kept alongside.
    assert all(section["stale"] for section in pratibha["sections"].values())
    assert pratibha["agent_error"]

    # The live account is unaffected and is NOT marked as coming from the store.
    assert rows["rahul"]["from_store"] is False
    assert rows["rahul"]["funds"]["available"] == pytest.approx(309748.42)


def test_totals_include_the_stored_account_rather_than_dropping_it(store, monkeypatch):
    """A total that silently omits an account is a wrong number presented as a
    right one — the store is what lets it be included honestly."""
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    rahul = serve_json({"/book": RAHUL_BOOK, "/health": health()})
    wire(monkeypatch, {"rahul": rahul.server_address[1], "pratibha": 9})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})
    try:
        totals = client.get("/api/overview").json()["totals"]
    finally:
        rahul.shutdown()
        rahul.server_close()

    assert totals["accounts_reporting"] == 2
    assert totals["accounts_missing"] == []
    assert totals["available"] == pytest.approx(309748.42 + 300021.34)


def test_an_account_with_nothing_stored_is_still_reported_missing(monkeypatch, tmp_path):
    """The fallback must not invent data for an account the store has never
    seen — that row still has to read as unreachable."""
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    path = str(tmp_path / "empty.db")
    migrate(connect(path))
    monkeypatch.setenv("DASHBOARD_DB", path)

    wire(monkeypatch, {"pratibha": 9})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})
    payload = client.get("/api/overview").json()

    row = payload["accounts"][0]
    assert row["reachable"] is False
    assert row["funds"] is None
    assert payload["totals"]["accounts_missing"] == ["pratibha"]


def test_stored_figures_carry_their_age(store):
    """Everything from the store is stale by definition, and the screen has to
    be able to say how stale."""
    reader = Reader(connect(store, read_only=True))
    book = reader.book("pratibha")
    funds = book["sections"]["funds"]

    assert funds["source"] == "store"
    assert funds["stale"] is True
    assert funds["as_of"] <= time.time()
    assert funds["age_s"] >= 0
    reader.conn.close()
