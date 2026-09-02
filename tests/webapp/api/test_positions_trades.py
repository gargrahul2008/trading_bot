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
    # The delivery sale is excluded too, for the same reason.
    assert symbols == ["NSE:RELIANCE-EQ", "NSE:CROMPTON26SEPFUT"]


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


def test_stock_sold_from_holdings_is_reported_apart_from_open_risk(client):
    """The shares are gone and the trade is realised — and the broker's
    unrealized_profit on such a row is the mark-to-market of a short that does
    not exist. SHRINGARMS showed +3,130 where the sale was a −6,660 loss."""
    payload = client.get("/api/positions").json()

    assert "NSE:SHRINGARMS-EQ" not in {p["symbol"] for p in payload["positions"]}
    assert payload["sold_today"] == [
        {"account": "rahul", "symbol": "NSE:SHRINGARMS-EQ", "qty": 1000.0}
    ]


def test_a_real_short_is_still_shown_as_open(client):
    """Only the delivery case is excluded; a short future is genuine risk."""
    rows = {p["symbol"]: p for p in client.get("/api/positions").json()["positions"]}
    assert rows["NSE:CROMPTON26SEPFUT"]["net_qty"] == -2150


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


def holding(symbol, qty, cost, ltp, is_open=True):
    return {"symbol": symbol, "qty": qty, "cost_price": cost, "ltp": ltp,
            "invested": qty * cost, "market_value": qty * ltp,
            "unrealised": qty * (ltp - cost), "is_open": is_open,
            "holding_type": "HLD"}


@pytest.fixture
def client_with_holdings(store, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    def section(data):
        return {"data": data, "as_of": 1.0, "age_s": 30.0, "stale": False,
                "stale_after_s": 180.0, "source": "rest", "error": None,
                "ok_count": 1, "fail_count": 0}

    agent = serve_json({
        "/book": {"user": "pratibha", "sections": {
            "positions": section([position("NSE:CROMPTON26SEPFUT", -2150, 242.65,
                                           240.30, 5052.50)]),
            "holdings": section([
                holding("NSE:INDOTHAI-EQ", 6682, 264.24, 51.16),
                holding("BSE:ARL-B", 7225, 60.14, 42.87),
                holding("NSE:SOLD-EQ", 0, 230.57, 220.78, is_open=False),
            ]),
            "orders": section([]), "trades": section([]), "funds": section({}),
        }},
        "/health": health(),
    })
    wire(monkeypatch, {"pratibha": agent.server_address[1]})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    test_client = TestClient(app)
    test_client.post("/api/auth/login", json={"password": "pw"})
    yield test_client
    agent.shutdown()
    agent.server_close()


def test_holdings_appear_alongside_positions(client_with_holdings):
    """The broker keeps settled delivery stock in holdings and everything else
    in positions. That split is bookkeeping, not the trader's view — showing
    only one left an account with three rows out of fifteen."""
    rows = client_with_holdings.get("/api/positions").json()["positions"]
    symbols = {r["symbol"] for r in rows}

    assert "NSE:CROMPTON26SEPFUT" in symbols, "the position"
    assert "NSE:INDOTHAI-EQ" in symbols and "BSE:ARL-B" in symbols, "the holdings"
    assert len(rows) == 3


def test_a_sold_out_holding_is_not_shown_as_owned(client_with_holdings):
    """It comes back with qty 0 and its old cost price, but it is gone."""
    symbols = {r["symbol"] for r in
               client_with_holdings.get("/api/positions").json()["positions"]}
    assert "NSE:SOLD-EQ" not in symbols


def test_a_holding_carries_its_cost_and_reads_as_long(client_with_holdings):
    rows = {r["symbol"]: r for r in
            client_with_holdings.get("/api/positions").json()["positions"]}
    indothai = rows["NSE:INDOTHAI-EQ"]

    assert indothai["book"] == "holding"
    assert indothai["direction"] == "LONG"
    assert indothai["net_qty"] == 6682
    assert indothai["avg_price"] == pytest.approx(264.24)
    assert indothai["unrealised"] == pytest.approx(6682 * (51.16 - 264.24))
    assert indothai["carried"] is True


def test_positions_and_holdings_are_told_apart(client_with_holdings):
    rows = {r["symbol"]: r for r in
            client_with_holdings.get("/api/positions").json()["positions"]}
    assert rows["NSE:CROMPTON26SEPFUT"]["book"] == "position"
    assert rows["NSE:INDOTHAI-EQ"]["book"] == "holding"


# ── the pad's Recent list ───────────────────────────────────────────────────

def test_recent_returns_one_line_per_scrip(client):
    """Not one per fill, and not one per broker book."""
    lines = client.get("/api/recent").json()["lines"]

    symbols = [line["symbol"] for line in lines if line["state"] == "open"]
    assert len(symbols) == len(set(symbols)), "a scrip must appear once while open"


def test_recent_needs_a_session(client):
    client.post("/api/auth/logout")
    assert client.get("/api/recent").status_code == 401
