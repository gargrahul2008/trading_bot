"""The Overview screen: every account in one place, and what happens when one
of them cannot be reached."""
import pytest

from tests.webapp.api.conftest import TOKEN, health, serve_json, wire


def test_login_is_required(agents, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    monkeypatch.setattr(auth_mod, "password_hash", lambda: auth_mod.hash_password("pw"))
    anonymous = TestClient(app)
    assert anonymous.get("/api/overview").status_code == 401
    assert anonymous.get("/api/accounts").status_code == 401


def test_a_wrong_password_is_refused(agents, monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app

    stored = auth_mod.hash_password("right")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)
    guest = TestClient(app)
    assert guest.post("/api/auth/login", json={"password": "wrong"}).status_code == 401


def test_overview_returns_every_account(client):
    payload = client.get("/api/overview").json()
    assert [row["account"] for row in payload["accounts"]] == ["pratibha", "rahul"]
    assert payload["totals"]["accounts_reporting"] == 2


def test_totals_add_up_across_accounts(client):
    totals = client.get("/api/overview").json()["totals"]
    assert totals["available"] == pytest.approx(309748.42 + 300021.34)
    assert totals["realised_today"] == pytest.approx(1751.0 - 3750.0)
    # rahul's one open position plus pratibha's two (the flat one is not open).
    assert totals["open_positions"] == 3
    assert totals["open_orders"] == 1


def test_a_delivery_sale_is_not_counted_as_a_short(client):
    """pratibha's SHRINGARMS is stock sold out of holdings. Counting it as a
    short would show open risk that has to be bought back and does not exist."""
    rows = {row["account"]: row for row in client.get("/api/overview").json()["accounts"]}
    positions = rows["pratibha"]["positions"]
    assert positions["short"] == 1, "only the CROMPTON future is a real short"
    assert positions["delivery_sales"] == 1
    assert positions["open"] == 2


def test_a_flat_position_contributes_realised_not_unrealised(client):
    """The TATAELXSI round trip closed today: nothing unrealised left, but its
    realised P&L is the whole point of still showing the row."""
    rows = {row["account"]: row for row in client.get("/api/overview").json()["accounts"]}
    positions = rows["pratibha"]["positions"]
    assert positions["realised"] == pytest.approx(-9275.0)
    assert positions["unrealised"] == pytest.approx(5052.5 + 3134.5)


def test_the_two_realised_figures_are_kept_apart(client):
    """A position's realised covers the life of the trade; the account's covers
    today. For pratibha they are -9,275 and -3,750, and both are right."""
    rows = {row["account"]: row for row in client.get("/api/overview").json()["accounts"]}
    assert rows["pratibha"]["positions"]["realised"] == pytest.approx(-9275.0)
    assert rows["pratibha"]["funds"]["realised_today"] == pytest.approx(-3750.0)


def test_sold_out_holdings_are_excluded_from_the_value(client):
    rows = {row["account"]: row for row in client.get("/api/overview").json()["accounts"]}
    holdings = rows["rahul"]["holdings"]
    assert holdings["count"] == 1, "the qty-0 row is not a holding any more"
    assert holdings["sold_today"] == 1
    assert holdings["market_value"] == pytest.approx(212301.0)


def test_orders_are_broken_down_by_who_placed_them(client):
    rows = {row["account"]: row for row in client.get("/api/overview").json()["accounts"]}
    assert rows["rahul"]["orders"]["by_source"] == {"bot": 1, "manual": 1}


def test_one_dead_agent_does_not_take_down_the_page(monkeypatch):
    """The rule this API is built around."""
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app.main import app
    from tests.webapp.api.conftest import RAHUL_BOOK

    rahul = serve_json({"/book": RAHUL_BOOK, "/health": health()})
    # pratibha's port has nothing listening on it.
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
    assert rows["rahul"]["reachable"] is True
    assert rows["rahul"]["funds"]["available"] == pytest.approx(309748.42)

    # The broken one is still a row, named and flagged — not silently dropped,
    # which would make a dead agent look like a closed account.
    assert rows["pratibha"]["reachable"] is False
    assert rows["pratibha"]["error"]

    # And the totals say so rather than quietly under-reporting.
    assert payload["totals"]["accounts_missing"] == ["pratibha"]
    assert payload["totals"]["accounts_reporting"] == 1


def test_a_slow_agent_times_out_rather_than_hanging_the_page(monkeypatch):
    from fastapi.testclient import TestClient

    from app import auth as auth_mod
    from app import config
    from app.main import app
    from tests.webapp.api.conftest import RAHUL_BOOK

    slow = serve_json({"/book": RAHUL_BOOK, "/health": health()}, delay=3.0)
    wire(monkeypatch, {"rahul": slow.server_address[1]})
    monkeypatch.setattr(config.get_settings(), "agent_timeout", 0.3)
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    client = TestClient(app)
    client.post("/api/auth/login", json={"password": "pw"})
    import time
    started = time.time()
    try:
        payload = client.get("/api/overview").json()
    finally:
        slow.shutdown()
        slow.server_close()

    assert time.time() - started < 2.5, "a wedged agent must not hold the page"
    assert payload["accounts"][0]["reachable"] is False


def test_health_is_reachable_without_signing_in(client):
    """It is unauthenticated so a monitor can poll it, which means it must
    describe the wiring and never the money."""
    payload = client.get("/api/health").json()
    assert "accounts" in payload and "problems" in payload

    # Account names are fine — they are in the repo. Balances, positions and
    # P&L are not, in any nesting.
    for forbidden in ("funds", "positions", "holdings", "totals",
                      "realised", "unrealised", "utilised"):
        assert forbidden not in str(payload), "health leaked %r" % forbidden

    # It does report whether the store is readable, which is wiring, not money.
    assert "store" in payload
