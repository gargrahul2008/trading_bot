"""The portfolio endpoint: capital, deployment and P&L across every account."""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.history.importer import import_capital, import_charges, import_realised  # noqa: E402
from webapp.store.schema import connect, migrate  # noqa: E402

from tests.webapp.api.conftest import RAHUL_BOOK, health, serve_json, wire  # noqa: E402


@pytest.fixture
def store(tmp_path, monkeypatch):
    """Rahul's real figures: 205,401.08 opening, +1,509,420 in, −1,033,830.81
    out, and a year's realised P&L of 168,296.239 gross less 36,509.41 charges."""
    path = str(tmp_path / "p.db")
    conn = connect(path)
    migrate(conn)

    import_capital(conn, "rahul", {"transactions": [
        {"date": "2026-04-01", "credit": 205401.08, "debit": 0,
         "transaction_type": "Opening Balance", "description": "Opening Balance"},
        {"date": "2026-04-20", "credit": 1509420, "debit": 0,
         "transaction_type": "Funds added", "description": "added"},
        {"date": "2026-08-07", "credit": 0, "debit": 1033830.81,
         "transaction_type": "Funds withdrawn", "description": "withdrawn"},
    ]}, opening_for="2026-04-01")
    import_realised(conn, "rahul", [
        {"day": "2026-05-12", "symbol": "NSE:CGCL-EQ", "realised": 168296.239}])
    import_charges(conn, "rahul", {"rows": [{"day": "2026-05-12", "total": 36509.41}]})
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

    rahul = serve_json({"/book": RAHUL_BOOK, "/health": health()})
    wire(monkeypatch, {"rahul": rahul.server_address[1]})
    stored = auth_mod.hash_password("pw")
    monkeypatch.setattr(auth_mod, "password_hash", lambda: stored)
    monkeypatch.setattr(auth_mod, "cookie_secure", lambda: False)

    test_client = TestClient(app)
    test_client.post("/api/auth/login", json={"password": "pw"})
    yield test_client
    rahul.shutdown()
    rahul.server_close()


def test_capital_is_the_opening_balance_plus_net_transfers(client):
    """205,401.08 + 1,509,420 − 1,033,830.81. Without the opening balance the
    base is wrong, and for one real account it is negative."""
    row = client.get("/api/portfolio").json()["accounts"][0]
    assert float(row["capital_in"]) == pytest.approx(680990.27)


def test_realised_is_the_brokers_figure_net_of_charges(client):
    row = client.get("/api/portfolio").json()["accounts"][0]
    assert float(row["realised"]) == pytest.approx(131786.829)
    assert row["realised_detail"]["gross"].startswith("168296.239")
    assert float(row["realised_detail"]["charges"]) == pytest.approx(36509.41)
    assert row["realised_detail"]["available"] is True


def test_return_is_measured_against_the_capital_put_in(client):
    row = client.get("/api/portfolio").json()["accounts"][0]
    expected = (float(row["realised"]) + float(row["unrealised"])) / 680990.27 * 100
    assert float(row["return_pct"]) == pytest.approx(expected)


def test_deployed_is_cost_and_market_value_is_separate(client):
    row = client.get("/api/portfolio").json()["accounts"][0]
    # RAHUL_BOOK holds 1,668 RELIANCE and a 900-share HFCL holding.
    assert float(row["deployed"]) > 0
    assert float(row["market_value"]) > 0
    assert row["deployed"] != row["market_value"]


def test_every_figure_is_a_string_not_a_float(client):
    """These are money. Sending them as JSON floats would round them in the
    browser after being computed exactly."""
    row = client.get("/api/portfolio").json()["accounts"][0]
    for key in ("capital_in", "free", "deployed", "market_value", "unrealised",
                "realised", "pnl", "net_worth"):
        assert isinstance(row[key], str), "%s came back as %r" % (key, type(row[key]))


def test_an_account_with_no_capital_imported_has_no_return(client, monkeypatch):
    """0% reads as a fact. An unimported base is the absence of one, and a
    return computed against it would be wildly wrong and look precise."""
    from app import store as store_mod
    monkeypatch.setattr(store_mod, "store_capital", lambda account: "0")
    from app import main as main_mod
    monkeypatch.setattr(main_mod, "store_capital", lambda account: "0")

    row = client.get("/api/portfolio").json()["accounts"][0]
    assert row["return_pct"] is None


def test_the_consolidated_total_is_present_and_sums_the_accounts(client):
    payload = client.get("/api/portfolio").json()
    totals = payload["totals"]
    assert totals["accounts"] == 1
    assert float(totals["capital_in"]) == pytest.approx(680990.27)
    assert float(totals["realised"]) == pytest.approx(131786.829)


def test_it_reports_which_financial_year_it_measured(client):
    assert client.get("/api/portfolio").json()["fy_start"] == "2026-04-01"


def test_login_is_required(client):
    from fastapi.testclient import TestClient

    from app.main import app

    assert TestClient(app).get("/api/portfolio").status_code == 401
