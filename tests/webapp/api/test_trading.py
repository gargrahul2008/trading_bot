"""Placing, changing and cancelling orders — the only part that moves money.

Each test states the mistake it prevents.
"""
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.schema import connect, migrate  # noqa: E402

from tests.webapp.api.conftest import TOKEN, wire  # noqa: E402


class RecordingAgent:
    """Stands in for one account's agent, recording what reached it."""

    def __init__(self, allow_trading=True, delay=0.0, ltp=None):
        # Only writes. The risk check reads /book and /quote first, and a test
        # asserting "nothing reached the broker" means no order was placed —
        # not that nothing was asked.
        self.calls = []
        self.reads = []
        self.allow_trading = allow_trading
        # What /quote answers, in the shape the real agent uses.
        self.ltp = ltp
        # Seconds the agent spends waiting on the broker before answering. The
        # point of the delay is that the order still goes through.
        self.delay = delay

    def start(self):
        import json as _json
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        agent = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _respond(self, code, payload):
                body = _json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _handle(self, method):
                if self.headers.get("Authorization") != "Bearer " + TOKEN:
                    return self._respond(401, {"error": "unauthorised"})
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                body = _json.loads(raw) if raw else {}
                agent.calls.append({"method": method, "path": self.path, "body": body})
                if agent.delay:
                    import time as _time
                    _time.sleep(agent.delay)
                if not agent.allow_trading:
                    return self._respond(403, {"error": "this agent is read-only"})
                self._respond(200, {"order_id": "NEW-1"})

            def do_GET(self):
                if self.headers.get("Authorization") != "Bearer " + TOKEN:
                    return self._respond(401, {"error": "unauthorised"})
                agent.reads.append(self.path)
                if self.path.startswith("/quote") and agent.ltp is not None:
                    symbol = self.path.split("symbols=", 1)[1]
                    return self._respond(200, {"quotes": {symbol: agent.ltp}})
                self._respond(404, {"error": "not found"})

            def do_POST(self):
                self._handle("POST")

            def do_PATCH(self):
                self._handle("PATCH")

            def do_DELETE(self):
                self._handle("DELETE")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self.httpd.server_address[1]

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def setup(tmp_path, monkeypatch):
    path = str(tmp_path / "t.db")
    migrate(connect(path))
    monkeypatch.setenv("DASHBOARD_DB", path)

    from app import main as main_mod
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "REPO", REPO)
    monkeypatch.setattr(main_mod, "REPO", REPO)

    agent = RecordingAgent()
    port = agent.start()
    wire(monkeypatch, {"rahul": port, "pratibha": port + 1})

    from fastapi.testclient import TestClient
    from app import auth as auth_mod
    from app.main import app

    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})

    yield client, agent, path
    agent.stop()


ORDER = {"account": "rahul", "symbol": "NSE:RELIANCE-EQ", "side": "BUY", "qty": 140,
         "product_type": "MTF", "order_type": "LIMIT", "limit_price": 1465.5}


def audit_rows(path):
    conn = connect(path, read_only=True)
    rows = [dict(r) for r in conn.execute("SELECT * FROM audit ORDER BY id")]
    conn.close()
    return rows


def test_an_order_reaches_the_named_account(setup):
    client, agent, _ = setup
    response = client.post("/api/orders", json=ORDER)

    assert response.status_code == 200
    assert response.json()["account"] == "rahul"
    assert agent.calls[0]["path"] == "/orders"
    assert agent.calls[0]["body"]["symbol"] == "NSE:RELIANCE-EQ"
    assert "account" not in agent.calls[0]["body"], "the agent already is the account"


def test_an_unknown_account_is_refused_before_anything_happens(setup):
    """Six accounts on one screen is also how an order reaches the wrong one."""
    client, agent, path = setup
    response = client.post("/api/orders", json=dict(ORDER, account="nobody"))

    assert response.status_code == 404
    assert agent.calls == []
    assert audit_rows(path) == [], "nothing attempted, nothing logged"


def test_the_attempt_is_recorded_before_the_broker_is_called(setup):
    """An action that timed out or crashed still has to leave a record. A log
    written only on success is silent about the cases worth investigating."""
    client, _, path = setup
    client.post("/api/orders", json=ORDER)

    row = audit_rows(path)[0]
    assert row["action"] == "place"
    assert row["account"] == "rahul"
    assert row["result"] == "ok"
    assert "BUY 140 NSE:RELIANCE-EQ MTF LIMIT @1465.5" == row["summary"]
    assert json.loads(row["detail"])["limit_price"] == 1465.5


def test_a_refusal_is_recorded_as_an_error_not_lost(setup, monkeypatch):
    client, agent, path = setup
    agent.allow_trading = False

    response = client.post("/api/orders", json=ORDER)
    assert response.status_code == 502

    row = audit_rows(path)[0]
    assert row["result"] == "error"
    assert "read-only" in row["message"]


def test_a_read_only_agent_stops_the_order(setup):
    """The agent refuses unless started with --allow-trading, so the dashboard
    cannot reach a broker the host did not deliberately enable."""
    client, agent, _ = setup
    agent.allow_trading = False
    assert client.post("/api/orders", json=ORDER).status_code == 502


def test_cancel_and_modify_name_their_account_too(setup):
    client, agent, path = setup
    assert client.request("DELETE", "/api/orders/OID-1?account=rahul").status_code == 200
    assert client.patch("/api/orders/OID-1",
                        json={"account": "rahul", "qty": 70}).status_code == 200

    paths = [c["path"] for c in agent.calls]
    assert paths == ["/orders/OID-1", "/orders/OID-1"]
    actions = [r["action"] for r in audit_rows(path)]
    assert actions == ["cancel", "modify"]


def test_exiting_a_position_is_audited(setup):
    client, agent, path = setup
    response = client.post("/api/positions/NSE:RELIANCE-EQ-MTF/exit",
                           json={"account": "rahul"})
    assert response.status_code == 200
    row = audit_rows(path)[0]
    assert row["action"] == "exit"
    assert "NSE:RELIANCE-EQ-MTF" in row["summary"]


def test_the_audit_endpoint_shows_what_was_done(setup):
    client, _, _ = setup
    client.post("/api/orders", json=ORDER)
    client.request("DELETE", "/api/orders/OID-1?account=rahul")

    entries = client.get("/api/audit").json()["entries"]
    assert [e["action"] for e in entries] == ["cancel", "place"], "newest first"
    assert all(e["result"] == "ok" for e in entries)


def test_every_trading_route_needs_a_session(setup):
    from fastapi.testclient import TestClient

    from app.main import app

    anonymous = TestClient(app)
    assert anonymous.post("/api/orders", json=ORDER).status_code == 401
    assert anonymous.request("DELETE", "/api/orders/1?account=rahul").status_code == 401
    assert anonymous.post("/api/positions/x/exit", json={"account": "rahul"}).status_code == 401
    assert anonymous.get("/api/audit").status_code == 401


def test_a_malformed_order_never_reaches_the_agent(setup):
    """Pydantic stops it at the edge; the agent's own validation is the second
    line, not the first."""
    client, agent, _ = setup
    assert client.post("/api/orders", json={"account": "rahul"}).status_code == 422
    assert agent.calls == []


def test_the_audit_table_is_created_if_the_agents_have_not_yet(tmp_path, monkeypatch):
    """The agents normally create and migrate the store. An API started before
    any agent has written found no audit table — and because audit failures are
    swallowed so they cannot block a trade, that silence was invisible. The
    first order placed through the pad reached the broker and was never logged.
    """
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app import main as main_mod
    from app import store as store_mod

    empty = str(tmp_path / "never-migrated.db")
    monkeypatch.setenv("DASHBOARD_DB", empty)
    monkeypatch.setattr(store_mod, "REPO", REPO)
    monkeypatch.setattr(main_mod, "REPO", REPO)
    monkeypatch.setattr(main_mod, "_audit_ready", False)

    agent = RecordingAgent()
    wire(monkeypatch, {"rahul": agent.start()})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    from app.main import app
    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})
    try:
        assert client.post("/api/orders", json=ORDER).status_code == 200
        assert len(audit_rows(empty)) == 1, "the order was logged"
        assert client.get("/api/audit").json()["entries"][0]["action"] == "place"
    finally:
        agent.stop()


def test_an_unreadable_audit_log_does_not_break_the_page(setup, monkeypatch):
    """It shares a page with live account figures; a broken log should not take
    those down with it."""
    client, _, path = setup
    from app import trading as trading_mod
    monkeypatch.setattr(trading_mod, "recent",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("corrupt")))

    response = client.get("/api/audit")
    assert response.status_code == 200
    assert response.json()["available"] is False


def test_a_quote_needs_a_real_account(setup):
    client, _, _ = setup
    assert client.get("/api/quote?account=nobody&symbols=NSE:X-EQ").status_code == 404


def test_symbols_are_searched_in_the_exchanges_list(setup):
    """Suggestions come from the instrument master, so a symbol is either real
    or absent — which is what a one-click pad needs before it will send."""
    client, _, path = setup
    conn = connect(path)
    from webapp.history.symbols import parse, store as store_symbols
    store_symbols(conn, parse(
        "1,RELIANCE INDUSTRIES LTD,0,1,0.1,INE002A01018,x,2026-08-31,,NSE:RELIANCE-EQ,"
        "10,10,2885,RELIANCE,2885,-1.0,XX,1,None,1,3.2\n", "NSE", "CASH"))
    conn.close()

    matches = client.get("/api/symbols?q=reli").json()["matches"]
    assert matches[0]["symbol"] == "NSE:RELIANCE-EQ"
    assert matches[0]["tick_size"] == 0.1


def test_nothing_typed_returns_nothing(setup):
    assert client_matches(setup, "") == []


def client_matches(setup, query):
    client, _, _ = setup
    return client.get("/api/symbols?q=%s" % query).json()["matches"]


def test_an_unlisted_symbol_is_a_404(setup):
    """This is how the pad answers 'is this real?'. NSE:RELIA-EQ looks right
    and does not exist."""
    client, _, _ = setup
    assert client.get("/api/symbols/NSE:RELIA-EQ").status_code == 404


# ── risk limits ─────────────────────────────────────────────────────────────
#
# Enforced here rather than in the browser. The API is the only path to a
# broker, so it is the only place a limit is worth putting: a rule the network
# tab can skip is a suggestion.


def set_limit(client, name, value, account="*"):
    response = client.post("/api/limits",
                           json={"account": account, "name": name, "value": str(value)})
    assert response.status_code == 200, response.text


def test_an_order_over_the_value_limit_never_reaches_the_broker(setup):
    """1,400 shares instead of 140 is one keystroke and ₹20 lakh."""
    client, agent, _ = setup
    set_limit(client, "max_order_value", 500000)

    response = client.post("/api/orders", json=dict(ORDER, qty=1400))

    assert response.status_code == 403
    assert "20,51,700" in response.json()["detail"], "say what the figure was"
    assert agent.calls == [], "nothing may reach the broker"


def test_a_refusal_is_audited(setup):
    """An order that never reached the broker is still something someone tried
    to do, and the pattern of what the limits stop is why they are kept."""
    client, _, path = setup
    set_limit(client, "max_order_value", 500000)
    client.post("/api/orders", json=dict(ORDER, qty=1400))

    rows = audit_rows(path)
    assert len(rows) == 1
    assert rows[0]["result"] == "refused"
    assert "max_order_value" not in rows[0]["message"], "the reason, not the rule name"
    assert "20,51,700" in rows[0]["message"]


def test_a_refused_order_does_not_count_towards_the_rate_limit(setup):
    """Otherwise one fat-fingered order locks the account out for a minute."""
    client, agent, _ = setup
    set_limit(client, "max_order_value", 500000)
    set_limit(client, "max_orders_per_minute", 2)

    for _ in range(5):
        client.post("/api/orders", json=dict(ORDER, qty=1400))
    accepted = client.post("/api/orders", json=ORDER)

    assert accepted.status_code == 200
    assert len(agent.calls) == 1


def test_the_rate_limit_stops_a_loop(setup):
    client, agent, _ = setup
    set_limit(client, "max_orders_per_minute", 3)

    codes = [client.post("/api/orders", json=ORDER).status_code for _ in range(5)]

    assert codes == [200, 200, 200, 403, 403]
    assert len(agent.calls) == 3


def test_a_per_account_limit_beats_the_default(setup):
    """A cap that is right for one account is a cage on another."""
    client, agent, _ = setup
    set_limit(client, "max_order_value", 100000)
    set_limit(client, "max_order_value", 900000, account="rahul")

    assert client.post("/api/orders", json=ORDER).status_code == 200
    assert client.post("/api/orders", json=dict(ORDER, account="pratibha")
                       ).status_code == 403


def test_a_market_order_with_no_obtainable_price_is_refused(setup):
    """Every limit here is measured in the order's value. A market order whose
    price cannot be established has no value to measure, and passing it through
    unchecked would make the limits optional exactly when they matter."""
    client, agent, _ = setup

    response = client.post("/api/orders", json=dict(
        ORDER, order_type="MARKET", limit_price=0))

    assert response.status_code == 422
    assert "no price available" in response.json()["detail"]
    assert agent.calls == []


def test_limits_are_readable(setup):
    client, _, _ = setup
    set_limit(client, "max_order_value", 750000, account="rahul")

    payload = client.get("/api/limits").json()

    assert payload["limits"]["rahul"]["max_order_value"] == "750000"
    assert payload["limits"]["pratibha"]["max_order_value"] == "500000", "the built-in"
    assert "max_daily_loss" in payload["rules"]


def test_a_nonsense_limit_is_rejected(setup):
    client, _, _ = setup

    assert client.post("/api/limits", json={"name": "max_moons", "value": "1"}
                       ).status_code == 400
    assert client.post("/api/limits", json={"name": "max_order_value", "value": "lots"}
                       ).status_code == 400
    assert client.post("/api/limits", json={"name": "max_order_value", "value": "-1"}
                       ).status_code == 400


def test_setting_limits_requires_a_session(setup):
    client, _, _ = setup
    client.post("/api/auth/logout")

    assert client.post("/api/limits", json={"name": "max_order_value", "value": "1"}
                       ).status_code == 401
    assert client.get("/api/limits").status_code == 401


# ── a slow broker ───────────────────────────────────────────────────────────
#
# The regression this exists for: every agent call shared one 2.5s timeout,
# chosen for the concurrent read fan-out that polls all accounts at once. A read
# is answered from the agent's memory, so slowness there means it is wedged. A
# trade is the agent waiting on Fyers, where slowness is ordinary — and giving
# up early never stopped an order, it only stopped us learning what became of
# it. A live order was reported to the user as "timed out", which reads as
# "it did not go" and invites the retry that opens the position twice.


@pytest.fixture
def slow_setup(tmp_path, monkeypatch):
    path = str(tmp_path / "slow.db")
    migrate(connect(path))
    monkeypatch.setenv("DASHBOARD_DB", path)

    from app import main as main_mod
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "REPO", REPO)
    monkeypatch.setattr(main_mod, "REPO", REPO)

    agent = RecordingAgent(delay=0.6)
    port = agent.start()
    wire(monkeypatch, {"rahul": port})

    from fastapi.testclient import TestClient
    from app import auth as auth_mod
    from app.main import app

    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})

    yield client, agent, path
    agent.stop()


def test_a_slow_broker_does_not_fail_the_order(slow_setup, monkeypatch):
    """An agent that takes longer than a read timeout is doing its job."""
    from app import config

    settings = config.get_settings()
    monkeypatch.setattr(settings, "agent_timeout", 0.2)      # would have failed
    monkeypatch.setattr(settings, "agent_trade_timeout", 5.0)

    client, agent, _ = slow_setup
    response = client.post("/api/orders", json=ORDER)

    assert response.status_code == 200
    assert len(agent.calls) == 1


def test_a_timeout_is_reported_as_unknown_not_failed(slow_setup, monkeypatch):
    """The order reached the agent. Calling that a failure invites a retry, and
    a retry after a timeout is how one intention becomes two positions."""
    from app import config

    settings = config.get_settings()
    monkeypatch.setattr(settings, "agent_trade_timeout", 0.2)

    client, agent, _ = slow_setup
    response = client.post("/api/orders", json=ORDER)

    assert response.status_code == 504, "not 502 — nothing refused this order"
    detail = response.json()["detail"]
    assert "may have reached the broker" in detail
    assert "check Orders" in detail
    assert agent.calls, "the order did reach the agent, which is the whole problem"


def test_a_timed_out_order_is_audited_as_unknown(slow_setup, monkeypatch):
    """'error' and 'unknown' must not look alike in the log: one says the order
    is not there, the other says nobody knows whether it is."""
    from app import config

    monkeypatch.setattr(config.get_settings(), "agent_trade_timeout", 0.2)

    client, _, path = slow_setup
    client.post("/api/orders", json=ORDER)

    rows = audit_rows(path)
    assert len(rows) == 1
    assert rows[0]["result"] == "unknown"
    assert "timed out" in rows[0]["message"]


def test_a_timed_out_order_still_counts_towards_the_rate_limit(slow_setup, monkeypatch):
    """It may be live. Not counting it would let a retry loop past the limit at
    exactly the moment the limit matters most."""
    from app import config

    monkeypatch.setattr(config.get_settings(), "agent_trade_timeout", 0.2)

    client, _, _ = slow_setup
    client.post("/api/limits", json={"account": "*", "name": "max_orders_per_minute",
                                     "value": "2"})
    codes = [client.post("/api/orders", json=ORDER).status_code for _ in range(3)]

    assert codes == [504, 504, 403]


# ── pricing a market order ──────────────────────────────────────────────────

@pytest.fixture
def quoting(tmp_path, monkeypatch):
    """A host whose agent answers /quote, as the real one does."""
    path = str(tmp_path / "q.db")
    migrate(connect(path))
    monkeypatch.setenv("DASHBOARD_DB", path)

    from app import main as main_mod
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "REPO", REPO)
    monkeypatch.setattr(main_mod, "REPO", REPO)

    agent = RecordingAgent(ltp=3125.5)
    port = agent.start()
    wire(monkeypatch, {"rahul": port})

    from fastapi.testclient import TestClient
    from app import auth as auth_mod
    from app.main import app

    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})

    yield client, agent, path
    agent.stop()


MARKET = {"account": "rahul", "symbol": "NSE:TCS-EQ", "side": "BUY", "qty": 1,
          "product_type": "CNC", "order_type": "MARKET"}


def test_a_market_order_is_valued_from_the_agents_quote(quoting):
    """The agent normalises a quote to {"quotes": {symbol: price}}. Reading the
    broker's own {"d": [{"v": {"lp": ...}}]} shape matched nothing, so every
    market order on a symbol the account did not already hold was refused for
    having no price."""
    client, agent, _ = quoting

    response = client.post("/api/orders", json=MARKET)

    assert response.status_code == 200, response.text
    assert any(path.startswith("/quote") for path in agent.reads)
    assert [call["path"] for call in agent.calls] == ["/orders"]


def test_the_quoted_price_is_what_the_limit_is_measured_against(quoting):
    """One share of TCS at 3,125 passes a 5,000 cap; forty do not."""
    client, _, _ = quoting
    client.post("/api/limits", json={"account": "*", "name": "max_order_value",
                                     "value": "5000"})

    assert client.post("/api/orders", json=MARKET).status_code == 200
    over = client.post("/api/orders", json=dict(MARKET, qty=40))
    assert over.status_code == 403
    assert "1,25,020" in over.json()["detail"]
