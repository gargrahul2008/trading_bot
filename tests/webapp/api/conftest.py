import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
for path in (str(REPO), str(REPO / "webapp" / "api")):
    if path not in sys.path:
        sys.path.insert(0, path)

TOKEN = "test-agent-token"


def serve_json(routes, port=0, delay=0.0):
    """A stand-in agent: `routes` maps path -> payload."""
    import time

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            if delay:
                time.sleep(delay)
            if self.headers.get("Authorization") != "Bearer " + TOKEN:
                body, code = json.dumps({"error": "unauthorised"}).encode(), 401
            elif self.path in routes:
                body, code = json.dumps(routes[self.path]).encode(), 200
            else:
                body, code = json.dumps({"error": "not found"}).encode(), 404
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    httpd.daemon_threads = True
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def book(positions=(), holdings=(), orders=(), funds=None, stale=False):
    def section(data, age=1.0):
        return {"data": data, "as_of": 1.0, "age_s": age, "stale": stale,
                "stale_after_s": 10.0, "source": "rest", "error": None,
                "ok_count": 1, "fail_count": 0}

    return {"user": "x", "started_at": 0.0, "sections": {
        "positions": section(list(positions)),
        "holdings": section(list(holdings)),
        "orders": section(list(orders)),
        "trades": section([]),
        "funds": section(funds or {"available": 0.0, "utilised": 0.0,
                                   "total": 0.0, "realised_pnl": 0.0}),
    }}


def health(live=True, phase="live", allow_trading=False):
    return {"user": "x", "live": live, "uptime_s": 10.0, "sections": {},
            "allow_trading": allow_trading,
            "poller": {"phase": phase, "ticks": 5, "intervals": {}, "budget": {}}}


RAHUL_BOOK = book(
    positions=[{"symbol": "NSE:RELIANCE-EQ", "net_qty": 1668, "unrealised": -20516.4,
                "realised": 0.0, "opened_today": False, "carried": True}],
    holdings=[{"symbol": "NSE:HFCL-EQ", "qty": 900, "invested": 206028.0,
               "market_value": 212301.0, "unrealised": 6273.0, "is_open": True},
              {"symbol": "NSE:SOLD-EQ", "qty": 0, "invested": 0.0,
               "market_value": 0.0, "unrealised": 0.0, "is_open": False}],
    orders=[{"order_id": "1", "is_open": False, "status": "CANCELLED",
             "source": "bot", "run": "rahul/reliance"},
            {"order_id": "2", "is_open": True, "status": "PENDING", "source": "manual"}],
    funds={"available": 309748.42, "utilised": 1275475.43,
           "total": 400000.0, "realised_pnl": 1751.0})

PRATIBHA_BOOK = book(
    positions=[
        {"symbol": "NSE:CROMPTON26SEPFUT", "net_qty": -2150, "unrealised": 5052.5,
         "realised": 0.0, "is_derivative": True, "opened_today": True},
        {"symbol": "NSE:TATAELXSI26SEPFUT", "net_qty": 0, "unrealised": 0.0,
         "realised": -9275.0, "is_derivative": True, "carried": True},
        {"symbol": "NSE:SHRINGARMS-EQ", "net_qty": -1000, "unrealised": 3134.5,
         "realised": 0.0, "delivery_sale": True},
    ],
    funds={"available": 300021.34, "utilised": -116645.8,
           "total": 408588.04, "realised_pnl": -3750.0})


def wire(monkeypatch, ports):
    from app import agents as agents_mod
    from app import config
    from app import main as main_mod

    for module in (config, agents_mod):
        monkeypatch.setattr(module, "agent_ports", lambda: ports)
        monkeypatch.setattr(module, "agent_token", lambda: TOKEN)
    monkeypatch.setattr(main_mod, "agent_ports", lambda: ports)
    monkeypatch.setattr(main_mod, "known_accounts", lambda: sorted(ports))


@pytest.fixture
def agents(monkeypatch):
    rahul = serve_json({"/book": RAHUL_BOOK, "/health": health()})
    pratibha = serve_json({"/book": PRATIBHA_BOOK, "/health": health(phase="closed")})
    ports = {"rahul": rahul.server_address[1], "pratibha": pratibha.server_address[1]}
    wire(monkeypatch, ports)
    yield ports
    for server in (rahul, pratibha):
        server.shutdown()
        server.server_close()


@pytest.fixture
def client(agents, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    monkeypatch.setattr(
        auth_mod, "_setting",
        lambda name: "test-secret" if name == "SESSION_SECRET" else "",
    )

    test_client = TestClient(app)
    assert test_client.post("/api/auth/login", json={"password": "pw"}).status_code == 200
    return test_client
