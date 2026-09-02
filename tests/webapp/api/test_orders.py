"""The Orders page: everything placed, including what was rejected and why."""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.schema import connect, migrate  # noqa: E402
from webapp.store.writer import Writer  # noqa: E402

from tests.webapp.api.conftest import health, serve_json, wire  # noqa: E402


def order(order_id, symbol, side="BUY", status="PENDING", is_open=True, **kw):
    row = {"order_id": order_id, "symbol": symbol, "side": side, "qty": 140,
           "filled_qty": 0, "limit_price": 1290.5, "product_type": "MTF",
           "status": status, "is_open": is_open, "kind": "positional"}
    row.update(kw)
    return row


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = str(tmp_path / "ord.db")
    conn = connect(path)
    migrate(conn)
    Writer(conn, "rahul").orders([
        order("OLD-1", "NSE:SBIN-EQ", status="REJECTED", is_open=False,
              message="RED:Margin shortfall for this order",
              trading_day="2026-08-27", source="manual", channel="web"),
        order("OLD-2", "NSE:RELIANCE-EQ", status="FILLED", is_open=False,
              filled_qty=140, trading_day="2026-08-27",
              source="bot", run="rahul/reliance", matched_by="order_id"),
    ])
    conn.close()
    monkeypatch.setenv("DASHBOARD_DB", path)
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "REPO", REPO)
    return path


@pytest.fixture
def client(store, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    agent = serve_json({
        "/orders": {"data": [
            order("LIVE-1", "NSE:RELIANCE-EQ", source="bot", run="rahul/reliance",
                  matched_by="order_id", channel="api"),
            order("LIVE-2", "NSE:OFSS-EQ", status="REJECTED", is_open=False,
                  message="You are not allowed to trade in this market (16387)",
                  source="manual", channel="web", order_tag="2:Charts"),
        ], "age_s": 2.0, "stale": False},
        "/health": health(),
    })
    wire(monkeypatch, {"rahul": agent.server_address[1]})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    test_client = TestClient(app)
    test_client.post("/api/auth/login", json={"password": "pw"})
    yield test_client
    agent.shutdown()
    agent.server_close()


def test_live_and_stored_orders_are_both_shown(client):
    ids = {o["order_id"] for o in client.get("/api/orders").json()["orders"]}
    assert ids == {"LIVE-1", "LIVE-2", "OLD-1", "OLD-2"}


def test_a_reject_carries_its_cause_not_just_the_broker_text(client):
    """Named with the same taxonomy the live bots act on, so a reject reads the
    same way here as in the bot's own log."""
    rows = {o["order_id"]: o for o in client.get("/api/orders").json()["orders"]}

    assert rows["OLD-1"]["kind"] == "MARGIN_SHORTFALL"
    assert rows["LIVE-2"]["kind"] == "SESSION_CLOSED"
    assert rows["OLD-1"]["message"].startswith("RED:Margin shortfall")


def test_a_filled_order_has_no_reject_cause(client):
    rows = {o["order_id"]: o for o in client.get("/api/orders").json()["orders"]}
    assert rows["OLD-2"]["kind"] is None


def test_attribution_survives_to_the_page(client):
    rows = {o["order_id"]: o for o in client.get("/api/orders").json()["orders"]}
    assert rows["LIVE-1"]["source"] == "bot"
    assert rows["LIVE-1"]["run"] == "rahul/reliance"
    assert rows["LIVE-2"]["source"] == "manual"
    assert rows["LIVE-2"]["order_tag"] == "2:Charts"


def test_the_live_copy_wins_over_the_stored_one(client, store):
    """The store is only as current as the last poll, so a status that has moved
    since must not be shown as it was."""
    conn = connect(store)
    Writer(conn, "rahul").orders([
        order("LIVE-1", "NSE:RELIANCE-EQ", status="CANCELLED", is_open=False,
              trading_day="2026-08-28")])
    conn.close()

    rows = {o["order_id"]: o for o in client.get("/api/orders").json()["orders"]}
    assert rows["LIVE-1"]["status"] == "PENDING", "the agent's copy"
    assert rows["LIVE-1"]["live"] is True


def test_newest_first(client):
    days = [o["trading_day"] for o in client.get("/api/orders").json()["orders"]]
    assert days == sorted(days, reverse=True)


def test_it_needs_a_session(client):
    from fastapi.testclient import TestClient

    from app.main import app

    assert TestClient(app).get("/api/orders").status_code == 401


def test_a_live_order_carries_todays_date(client):
    """The agent normalises an order without a trading day — it only ever sees
    today's book. That left the Day column blank on every live row, so "today"
    had to be inferred from an empty cell."""
    import datetime as dt

    live = [o for o in client.get("/api/orders").json()["orders"] if o["live"]]

    assert live, "this fixture must have live orders for the test to mean anything"
    today = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).date()
    assert all(o["trading_day"] == today.isoformat() for o in live)
