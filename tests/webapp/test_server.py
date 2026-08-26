"""The HTTP surface, driven over a real socket — auth, routing, and the two
gates that stop this process placing an order it was not meant to."""
import json
import threading
import urllib.error
import urllib.request

import pytest

from webapp.agent.book import Book
from webapp.agent.budget import Budget
from webapp.agent.poller import Poller
from webapp.agent.server import Agent, serve

from tests.webapp.fakes import FakeClock, FakeGateway, FixedSession

TOKEN = "test-token"
LIVE = {"positions": 3.0, "orders": 3.0, "funds": 30.0, "holdings": 60.0}


class TradingGateway(FakeGateway):
    def __init__(self):
        super().__init__()
        self.placed = []
        self.cancelled = []

    def place_order(self, request):
        self.placed.append(request)
        return "ORDER-1"

    def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        return {"s": "ok", "id": order_id}

    def modify_order(self, order_id, **fields):
        return {"s": "ok", "id": order_id, "fields": fields}

    def exit_position(self, position_id):
        return {"s": "ok", "id": position_id}


@pytest.fixture
def agent_server(request):
    allow_trading = getattr(request, "param", False)
    gateway = TradingGateway()
    book = Book("rahul")
    poller = Poller(
        gateway, book,
        budget=Budget(per_min=600.0, burst=50, clock=FakeClock()),
        session=FixedSession(LIVE),
        clock=FakeClock(),
    )
    poller.tick()  # one round so the book has data
    agent = Agent(
        user="rahul", book=book, poller=poller, gateway=gateway, allow_trading=allow_trading
    )
    httpd = serve(agent, host="127.0.0.1", port=0, token=TOKEN)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:%d" % httpd.server_address[1], gateway
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def call(base, path, *, method="GET", token=TOKEN, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    if token is not None:
        req.add_header("Authorization", "Bearer " + token)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def test_reads_require_the_token(agent_server):
    base, _ = agent_server
    assert call(base, "/health", token=None)[0] == 401
    assert call(base, "/health", token="wrong")[0] == 401
    assert call(base, "/health")[0] == 200


def test_health_reports_liveness_and_the_budget(agent_server):
    base, _ = agent_server
    status, payload = call(base, "/health")
    assert status == 200
    assert payload["user"] == "rahul"
    assert payload["live"] is True
    assert payload["allow_trading"] is False
    assert payload["poller"]["budget"]["per_min"] == 600.0
    assert "positions" in payload["sections"]


def test_book_carries_data_with_its_age(agent_server):
    base, _ = agent_server
    status, payload = call(base, "/book")
    assert status == 200
    positions = payload["sections"]["positions"]
    assert positions["data"][0]["symbol"] == "NSE:RELIANCE-EQ"
    assert positions["stale"] is False
    assert positions["as_of"] is not None, "no figure is served without its timestamp"


def test_sections_are_individually_addressable(agent_server):
    base, _ = agent_server
    assert call(base, "/positions")[1]["data"][0]["net_qty"] == 70.0
    assert call(base, "/funds")[1]["data"]["available"] == 585876.0
    assert call(base, "/nonsense")[0] == 404


def test_a_read_only_agent_refuses_to_trade(agent_server):
    base, gateway = agent_server
    body = {"symbol": "NSE:RELIANCE-EQ", "side": "BUY", "qty": 1, "order_type": "MARKET"}

    status, payload = call(base, "/orders", method="POST", body=body)
    assert status == 403
    assert "read-only" in payload["error"]

    assert call(base, "/orders/123", method="DELETE")[0] == 403
    assert call(base, "/positions/abc/exit", method="POST")[0] == 403
    assert gateway.placed == [] and gateway.cancelled == []


@pytest.mark.parametrize("agent_server", [True], indirect=True)
def test_trading_agent_places_modifies_and_cancels(agent_server):
    base, gateway = agent_server

    status, payload = call(
        base, "/orders", method="POST",
        body={"symbol": "NSE:RELIANCE-EQ", "side": "BUY", "qty": 70,
              "order_type": "LIMIT", "limit_price": 1460.5, "product_type": "MTF"},
    )
    assert status == 200 and payload["order_id"] == "ORDER-1"
    placed = gateway.placed[0]
    assert (placed.symbol, placed.side, int(placed.qty), placed.product_type) == (
        "NSE:RELIANCE-EQ", "BUY", 70, "MTF",
    )

    assert call(base, "/orders/ORDER-1", method="PATCH", body={"qty": 35})[0] == 200
    assert call(base, "/orders/ORDER-1", method="DELETE")[0] == 200
    assert gateway.cancelled == ["ORDER-1"]


@pytest.mark.parametrize("agent_server", [True], indirect=True)
def test_a_bad_order_is_rejected_before_it_reaches_the_broker(agent_server):
    base, gateway = agent_server
    for body, expected in [
        ({"side": "BUY", "qty": 1}, "symbol is required"),
        ({"symbol": "NSE:X-EQ", "side": "SIDEWAYS", "qty": 1}, "side must be"),
        ({"symbol": "NSE:X-EQ", "side": "BUY", "qty": -5}, "positive whole number"),
        ({"symbol": "NSE:X-EQ", "side": "BUY", "qty": 1, "order_type": "LIMIT"},
         "limit_price is required"),
    ]:
        status, payload = call(base, "/orders", method="POST", body=body)
        assert status == 400, body
        assert expected in payload["error"]
    assert gateway.placed == [], "nothing invalid may reach the broker"


@pytest.mark.parametrize("agent_server", [True], indirect=True)
def test_an_order_action_expedites_the_next_order_refresh(agent_server):
    base, _ = agent_server
    call(base, "/orders/ORDER-1", method="DELETE")
    status, payload = call(base, "/health")
    assert payload["poller"]["next_due_in_s"]["orders"] == 0.0


@pytest.mark.parametrize("agent_server", [True], indirect=True)
def test_a_modify_with_nothing_to_change_is_rejected(agent_server):
    base, _ = agent_server
    status, payload = call(base, "/orders/ORDER-1", method="PATCH", body={})
    assert status == 400 and "nothing to modify" in payload["error"]


def test_a_malformed_body_does_not_crash_the_agent(agent_server):
    base, _ = agent_server
    req = urllib.request.Request(base + "/orders", data=b"{not json", method="POST")
    req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        urllib.request.urlopen(req, timeout=5)
        assert False, "should have failed"
    except urllib.error.HTTPError as exc:
        assert exc.code == 400
    assert call(base, "/health")[0] == 200, "agent still serving"


def test_serve_refuses_without_a_token():
    with pytest.raises(ValueError, match="token is required"):
        serve(Agent(user="x", book=Book("x"), poller=None, gateway=None), port=0, token="")
