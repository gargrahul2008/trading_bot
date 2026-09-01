"""The Activity stream and the set-aside list.

Both exist for the same complaint: things change without anyone seeing. An
order's status is overwritten in place, and a holding nobody can sell quietly
distorts every ratio measured against deployed capital.
"""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.schema import connect, migrate  # noqa: E402
from webapp.store.writer import Writer  # noqa: E402

from tests.webapp.api.conftest import book, health, serve_json, wire  # noqa: E402


def order(oid, status, filled=0.0, **kw):
    row = {"order_id": oid, "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
           "qty": 100, "filled_qty": filled, "status": status,
           "trading_day": "2026-08-28"}
    row.update(kw)
    return row


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = str(tmp_path / "a.db")
    conn = connect(path)
    migrate(conn)

    writer = Writer(conn, "rahul")
    writer.orders([order("1", "PENDING")])
    writer.orders([order("1", "FILLED", 100, traded_price=1300.0)])
    writer.orders([order("2", "REJECTED", message="RED:Margin shortfall for this order",
                         source="bot", run="rahul/reliance")])
    # A round trip, so the stream carries a close as well as the order events.
    writer.fills([
        {"trade_id": "t1", "order_id": "1", "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
         "qty": 100, "price": 1300, "product_type": "MTF", "trading_day": "2026-08-28"},
        {"trade_id": "t2", "order_id": "3", "symbol": "NSE:RELIANCE-EQ", "side": "SELL",
         "qty": 100, "price": 1320, "product_type": "MTF", "trading_day": "2026-08-28"},
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
        "/book": book(holdings=[
            {"symbol": "NSE:HFCL-EQ", "qty": 900, "invested": 206028.0,
             "market_value": 212301.0, "unrealised": 6273.0, "is_open": True},
            {"symbol": "NSE:VIKASECO-EQ", "qty": 66184, "invested": 137662.7,
             "market_value": 0.0, "unrealised": -137662.7, "is_open": True},
        ]),
        "/health": health(),
    })
    wire(monkeypatch, {"rahul": agent.server_address[1]})

    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    monkeypatch.setattr(
        auth_mod, "_setting",
        lambda name: "test-secret" if name == "SESSION_SECRET" else "")

    test_client = TestClient(app)
    assert test_client.post("/api/auth/login", json={"password": "pw"}).status_code == 200
    yield test_client
    agent.shutdown()
    agent.server_close()


# ── the stream ──────────────────────────────────────────────────────────────

def test_a_fill_is_still_there_after_the_order_moved_on(client):
    """The order row now says FILLED and nothing else. Without the event log
    there is no record that it was ever working, or when it filled."""
    events = client.get("/api/activity").json()["events"]
    kinds = [(e["order_id"], e["event"]) for e in events]

    assert ("1", "placed") in kinds
    assert ("1", "filled") in kinds


def test_a_close_carries_what_the_broker_does_not_report(client):
    """A close is not an event the broker sends — it is the absence of a
    position. Derived here, it can say what the entry was and what was made."""
    closed = [e for e in client.get("/api/activity").json()["events"]
              if e["event"] == "closed"]

    assert len(closed) == 1
    assert closed[0]["symbol"] == "NSE:RELIANCE-EQ"
    assert float(closed[0]["entry_price"]) == 1300.0
    assert float(closed[0]["price"]) == 1320.0
    assert float(closed[0]["pnl"]) == 2000.0
    assert closed[0]["net_pnl"] is None, (
        "no charge data for the day, and an unknown cost is not a zero one")


def test_a_reject_names_its_cause(client):
    """The one event worth acting on immediately."""
    rejected = [e for e in client.get("/api/activity").json()["events"]
                if e["event"] == "rejected" or e.get("to_status") == "REJECTED"]

    assert rejected, "a rejected order must appear in the stream"
    assert rejected[0]["kind"] == "MARGIN_SHORTFALL"
    assert rejected[0]["run"] == "rahul/reliance"


def test_the_stream_is_newest_first(client):
    """It is read from the top, once, to answer 'what changed?'"""
    events = client.get("/api/activity").json()["events"]
    times = [e["at"] for e in events if isinstance(e["at"], (int, float))]

    assert times == sorted(times, reverse=True)


def test_activity_requires_a_session(client):
    client.post("/api/auth/logout")
    assert client.get("/api/activity").status_code == 401


# ── setting a scrip aside ───────────────────────────────────────────────────

def test_setting_a_scrip_aside_takes_it_out_of_deployed(client):
    """66,184 VIKASECO shares at ₹2.08 cannot be sold. Until they are excluded
    they count as ₹1.38 lakh of working capital that is not working."""
    before = client.get("/api/portfolio").json()["totals"]

    client.post("/api/exclusions", json={"account": "rahul",
                                         "symbol": "NSE:VIKASECO-EQ",
                                         "reason": "delisted"})
    after = client.get("/api/portfolio").json()["totals"]

    assert float(before["deployed"]) - float(after["deployed"]) == pytest.approx(137662.7)
    assert after["excluded"]["count"] == 1
    assert after["excluded"]["symbols"] == ["NSE:VIKASECO-EQ"]
    assert float(after["excluded"]["cost"]) == pytest.approx(137662.7)


def test_an_exclusion_is_reversible(client):
    client.post("/api/exclusions", json={"account": "rahul",
                                         "symbol": "NSE:VIKASECO-EQ", "reason": "x"})
    client.delete("/api/exclusions?account=rahul&symbol=NSE:VIKASECO-EQ")

    totals = client.get("/api/portfolio").json()["totals"]
    assert totals["excluded"]["count"] == 0
    assert float(totals["deployed"]) == pytest.approx(343690.7)


def test_an_exclusion_applies_to_one_account_only(client):
    """The same scrip can be stuck in one account and freely tradable in
    another; the judgement belongs to the account that holds it."""
    client.post("/api/exclusions", json={"account": "pratibha",
                                         "symbol": "NSE:VIKASECO-EQ", "reason": "x"})

    totals = client.get("/api/portfolio").json()["totals"]
    assert float(totals["deployed"]) == pytest.approx(343690.7), (
        "rahul's holding must be untouched by pratibha's exclusion")


def test_the_reason_is_kept(client):
    """Why a scrip was set aside matters more than that it was — it is the
    thing you will not remember in six months."""
    client.post("/api/exclusions", json={"account": "rahul",
                                         "symbol": "NSE:VIKASECO-EQ",
                                         "reason": "suspended since May"})

    stored = client.get("/api/exclusions").json()["exclusions"]
    assert stored["rahul"]["NSE:VIKASECO-EQ"]["reason"] == "suspended since May"
