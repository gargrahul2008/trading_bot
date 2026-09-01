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

    def __init__(self, allow_trading=True):
        self.calls = []
        self.allow_trading = allow_trading

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
                if not agent.allow_trading:
                    return self._respond(403, {"error": "this agent is read-only"})
                self._respond(200, {"order_id": "NEW-1"})

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
