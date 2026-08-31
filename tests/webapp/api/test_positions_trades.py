"""The Positions and Trades endpoints."""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.history.importer import import_charges  # noqa: E402
from webapp.store.schema import connect, migrate  # noqa: E402
from webapp.store.writer import Writer  # noqa: E402

from tests.webapp.api.conftest import health, serve_json, wire  # noqa: E402


def position(symbol, qty, avg, ltp, unreal, **kw):
    row = {"symbol": symbol, "net_qty": qty, "avg_price": avg, "ltp": ltp,
           "unrealised": unreal, "realised": 0.0, "product_type": "CNC",
           "kind": "positional", "direction": "LONG" if qty > 0 else "SHORT"}
    row.update(kw)
    return row


def book(positions):
    def section(data):
        return {"data": data, "as_of": 1.0, "age_s": 2.0, "stale": False,
                "stale_after_s": 10.0, "source": "rest", "error": None,
                "ok_count": 1, "fail_count": 0}
    return {"user": "x", "sections": {"positions": section(positions),
                                      "holdings": section([]), "orders": section([]),
                                      "trades": section([]), "funds": section({})}}


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = str(tmp_path / "t.db")
    conn = connect(path)
    migrate(conn)
    writer = Writer(conn, "rahul")
    # A round trip carried overnight, and one opened and closed the same day.
    writer.fills([
        {"trade_id": "1", "order_id": "a", "symbol": "NSE:RELIANCE-EQ", "side": "BUY",
         "qty": 100, "price": 1300, "product_type": "MTF", "trading_day": "2026-08-27"},
        {"trade_id": "2", "order_id": "b", "symbol": "NSE:RELIANCE-EQ", "side": "SELL",
         "qty": 100, "price": 1320, "product_type": "MTF", "trading_day": "2026-08-28"},
        {"trade_id": "3", "order_id": "c", "symbol": "NSE:ANTHEM-EQ", "side": "BUY",
         "qty": 50, "price": 905, "product_type": "CNC", "trading_day": "2026-08-28"},
        {"trade_id": "4", "order_id": "d", "symbol": "NSE:ANTHEM-EQ", "side": "SELL",
         "qty": 50, "price": 900, "product_type": "CNC", "trading_day": "2026-08-28"},
    ])
    import_charges(conn, "rahul", {"rows": [
        {"day": "2026-08-27", "total": 130, "turnover": 130000},
        {"day": "2026-08-28", "total": 195.37, "turnover": 142657},
    ]})
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
        "/book": book([
            position("NSE:RELIANCE-EQ", 1808, 1308.80, 1288.20, -37244.80),
            position("NSE:CROMPTON26SEPFUT", -2150, 242.65, 240.30, 5052.50,
                     is_derivative=True),
            position("NSE:SHRINGARMS-EQ", -1000, 223.91, 220.78, 3134.50,
                     delivery_sale=True),
            # Closed today — belongs on Trades, not among what is at risk.
            position("NSE:TATAELXSI26SEPFUT", 0, 0, 3633.0, 0.0),
        ]),
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


# ── positions ────────────────────────────────────────────────────────────────
def test_a_flat_position_is_not_shown_as_open(client):
    """It is a trade that closed today, and belongs on the Trades page. Listing
    it among what is at risk overstates the open book."""
    symbols = [p["symbol"] for p in client.get("/api/positions").json()["positions"]]
    assert "NSE:TATAELXSI26SEPFUT" not in symbols
    assert len(symbols) == 3


def test_positions_are_ordered_by_how_far_they_have_moved(client):
    """The row worth acting on is the one furthest from where you wanted it,
    in either direction."""
    rows = client.get("/api/positions").json()["positions"]
    moves = [abs(float(r["unrealised"])) for r in rows]
    assert moves == sorted(moves, reverse=True)
    assert rows[0]["symbol"] == "NSE:RELIANCE-EQ"


def test_every_position_carries_its_account_and_freshness(client):
    """Flattened across accounts, so each row has to say whose it is."""
    row = client.get("/api/positions").json()["positions"][0]
    assert row["account"] == "rahul"
    assert row["stale"] is False
    assert row["from_store"] is False
    assert row["age_s"] == 2.0


def test_the_delivery_sale_distinction_survives_to_the_api(client):
    rows = {p["symbol"]: p for p in client.get("/api/positions").json()["positions"]}
    assert rows["NSE:SHRINGARMS-EQ"]["delivery_sale"] is True
    assert rows["NSE:CROMPTON26SEPFUT"].get("delivery_sale") in (None, False)


# ── trades ───────────────────────────────────────────────────────────────────
def test_trades_are_matched_round_trips_with_their_own_pnl(client):
    payload = client.get("/api/trades").json()
    trades = {t["symbol"]: t for t in payload["trades"]}

    assert float(trades["NSE:RELIANCE-EQ"]["gross"]) == pytest.approx(2000)
    assert float(trades["NSE:ANTHEM-EQ"]["gross"]) == pytest.approx(-250)
    assert trades["NSE:RELIANCE-EQ"]["kind"] == "positional"
    assert trades["NSE:ANTHEM-EQ"]["kind"] == "intraday"


def test_each_trade_is_costed_and_marked_estimated(client):
    """Charges come per day, never per symbol, so a per-trade figure is divided
    out — and says so."""
    trade = {t["symbol"]: t for t in client.get("/api/trades").json()["trades"]}
    reliance = trade["NSE:RELIANCE-EQ"]

    assert reliance["charges"] is not None
    assert reliance["charges_estimated"] is True
    assert float(reliance["net"]) == pytest.approx(
        float(reliance["gross"]) - float(reliance["charges"]))


def test_the_totals_say_how_many_trades_could_not_be_costed(client):
    totals = client.get("/api/trades").json()["totals"]
    assert totals["trades"] == 2
    assert totals["trades_without_charges"] == 0
    assert float(totals["net"]) == pytest.approx(
        float(totals["gross"]) - float(totals["charges"]))


def test_newest_trades_come_first(client):
    days = [t["closed_day"] for t in client.get("/api/trades").json()["trades"]]
    assert days == sorted(days, reverse=True)


def test_filtering_by_day_narrows_what_is_shown_not_what_is_matched(client):
    """A position opened last week and closed today is a today trade, and its
    entry is only findable by replaying the fills before it."""
    payload = client.get("/api/trades?day=2026-08-28").json()
    assert {t["symbol"] for t in payload["trades"]} == {"NSE:RELIANCE-EQ", "NSE:ANTHEM-EQ"}
    assert float(payload["trades"][0]["gross"]) != 0


def test_both_endpoints_require_a_session(client):
    from fastapi.testclient import TestClient

    from app.main import app

    anonymous = TestClient(app)
    assert anonymous.get("/api/positions").status_code == 401
    assert anonymous.get("/api/trades").status_code == 401


def test_an_account_holding_nothing_still_gets_a_column(client, monkeypatch):
    """Letting the page infer its columns from the rows makes an account with
    nothing open vanish — indistinguishable from one that could not be read.
    The server knows who exists."""
    from app import main as main_mod
    monkeypatch.setattr(main_mod, "known_accounts", lambda: ["rahul", "pratibha", "piyush"])

    payload = client.get("/api/positions").json()
    assert payload["accounts"] == ["rahul", "pratibha", "piyush"]
    # Only rahul actually holds anything in this fixture.
    assert {p["account"] for p in payload["positions"]} == {"rahul"}


def test_trades_also_name_every_account(client, monkeypatch):
    from app import main as main_mod
    monkeypatch.setattr(main_mod, "known_accounts", lambda: ["rahul", "pratibha", "piyush"])
    assert client.get("/api/trades").json()["accounts"] == ["rahul", "pratibha", "piyush"]


def test_filtering_trades_to_one_account_narrows_the_columns_too(client):
    payload = client.get("/api/trades?account=rahul").json()
    assert payload["accounts"] == ["rahul"]
