"""The three history endpoints.

Every fixture here is shaped from what the live accounts actually returned on
2026-08-31 (docs/host_state.md §12), including the traps that report found.
"""
import pytest

from webapp.history.client import (
    DAY_WISE, MAX_PAGE_SIZE, HistoryClient, HistoryError, epoch_ms_to_date,
)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeTransport:
    """Records every call, and replays queued payloads in order."""

    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def __call__(self, url, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "params": dict(params or {})})
        return FakeResponse(self.payloads.pop(0) if self.payloads else
                            {"s": "ok", "data": [], "summary_data": {}})


def ok(data, summary=None):
    return {"code": 200, "message": "", "s": "ok",
            "data": data, "summary_data": summary or {}}


def client(payloads, **kw):
    transport = FakeTransport(payloads)
    return HistoryClient("APP-100", "tok", transport=transport,
                         pause=0, sleep=lambda s: None, **kw), transport


LEDGER_ROW = {
    "credit_amount": 134414.27, "date": 1787875200000, "debit_amount": 0,
    "description": "Executed trades for the day in equity cash segment",
    "running_balance": 568402.2499999998, "transaction_type": "Trading",
}


def test_the_ledger_is_paged_until_a_short_page():
    """A plain call returns exactly 100 rows and looks complete. On the live
    account that was one month of the five requested."""
    full = [dict(LEDGER_ROW) for _ in range(MAX_PAGE_SIZE)]
    api, transport = client([
        ok(full, {"funds_added": 1509420}),
        ok(full),
        ok([dict(LEDGER_ROW) for _ in range(32)]),
    ])
    result = api.ledger("2026-04-01", "2026-08-29")

    assert len(result["transactions"]) == 232
    assert [c["params"]["page_no"] for c in transport.calls] == [1, 2, 3]
    assert result["summary"]["funds_added"] == 1509420


def test_a_full_final_page_is_not_mistaken_for_the_end():
    """Stopping on a full page that happens to be last would silently drop
    whatever came after it — which is the truncation bug, inverted."""
    full = [dict(LEDGER_ROW) for _ in range(MAX_PAGE_SIZE)]
    api, transport = client([ok(full), ok([])])
    assert len(api.ledger("2026-04-01", "2026-08-29")["transactions"]) == 100
    assert len(transport.calls) == 2, "it must ask again after a full page"


def test_ledger_dates_are_read_as_day_stamps():
    api, _ = client([ok([dict(LEDGER_ROW)])])
    transaction = api.ledger("2026-08-28", "2026-08-28")["transactions"][0]
    assert transaction["date"] == "2026-08-28"
    assert transaction["credit"] == 134414.27
    assert transaction["transaction_type"] == "Trading"


def test_realised_keys_on_the_symbol_not_the_exchange_fields():
    """BSE:SHISHIND-X comes back with exch_id 10 / exchange_name NSE, which is
    wrong — the local store has 50 fills of it on BSE. Keying on those fields
    would file it under the wrong exchange."""
    api, _ = client([ok([{
        "symbol_name": "BSE:SHISHIND-X", "realized_pnl": 5134.176,
        "buy_qty": 1140, "sell_qty": 1140, "buy_rate": 180.7421, "sell_rate": 185.2458,
        "exch_id": 10, "exchange_name": "NSE", "segment_name": "NSE_CASH",
    }], {"gross_pnl": 168296.239, "charges": 36509.41, "net_pnl": 131786.829})])

    result = api.realised("2026-04-01", "2026-08-29")
    scrip = result["scrips"][0]
    assert scrip["symbol"] == "BSE:SHISHIND-X"
    assert "exchange_name" not in scrip and "segment_name" not in scrip
    assert result["summary"]["net_pnl"] == 131786.829


def test_a_row_without_a_symbol_is_dropped():
    api, _ = client([ok([{"realized_pnl": 100}, {"symbol_name": "NSE:X-EQ", "realized_pnl": 5}])])
    assert [s["symbol"] for s in api.realised("2026-04-01", "2026-04-30")["scrips"]] == ["NSE:X-EQ"]


def test_realised_by_day_asks_one_day_at_a_time():
    """The endpoint carries no date, but the window is a free parameter and it
    is additive over it — so a one-day window is that day's figure."""
    api, transport = client([
        ok([{"symbol_name": "NSE:A-EQ", "realized_pnl": 100}]),
        ok([]),
        ok([{"symbol_name": "NSE:B-EQ", "realized_pnl": -40}]),
    ])
    rows = api.realised_by_day("2026-08-26", "2026-08-28")

    assert [(r["day"], r["symbol"], r["realised"]) for r in rows] == [
        ("2026-08-26", "NSE:A-EQ", 100),
        ("2026-08-28", "NSE:B-EQ", -40),
    ]
    windows = [(c["params"]["from_date"], c["params"]["to_date"]) for c in transport.calls]
    assert windows == [("2026-08-26", "2026-08-26"), ("2026-08-27", "2026-08-27"),
                       ("2026-08-28", "2026-08-28")]


def test_charges_come_back_day_wise_with_the_control_total():
    api, _ = client([ok([{
        "brokerage": 30, "gst": 7.99, "ipft": 0, "sebi_toc": 0.55, "stamp_duty": 8,
        "stt": 135, "total": 195.37, "trade_date": 1787875200000,
        "transaction_charges": 13.83, "turnover": 142657.0002,
    }], {"total": 36509.41})])

    result = api.charges("2026-04-01", "2026-08-29", DAY_WISE)
    row = result["rows"][0]
    assert row["day"] == "2026-08-28"
    assert row["total"] == 195.37
    assert row["turnover"] == 142657.0002
    assert result["summary"]["total"] == 36509.41


def test_a_rate_limit_waits_and_retries_rather_than_failing():
    """These share a budget with the live bots, so waiting is always right —
    the probe's own run ended on a -429."""
    waits = []
    transport = FakeTransport([
        {"code": -429, "message": "Request limit reached", "s": "error"},
        ok([dict(LEDGER_ROW)]),
    ])
    api = HistoryClient("APP-100", "tok", transport=transport, pause=0,
                        sleep=waits.append)

    assert len(api.ledger("2026-08-28", "2026-08-28")["transactions"]) == 1
    assert api.rate_limited == 1
    assert waits and waits[0] > 0, "it waited before retrying"


def test_an_error_that_is_not_a_rate_limit_is_raised():
    api, _ = client([{"code": -50, "message": "Invalid input", "s": "error"}])
    with pytest.raises(HistoryError, match="Invalid input"):
        api.ledger("2026-04-01", "2026-08-29")


def test_it_never_asks_for_more_than_the_api_allows():
    """page_size 101 and 500 both come back -50 Invalid input."""
    api, transport = client([ok([])])
    api.charges("2026-04-01", "2026-08-29")
    assert transport.calls[0]["params"]["page_size"] == MAX_PAGE_SIZE


def test_the_auth_header_is_the_documented_shape():
    api, transport = client([ok([])])
    api.realised("2026-04-01", "2026-08-29")
    assert transport.calls[0]["headers"]["Authorization"] == "APP-100:tok"
    assert transport.calls[0]["headers"]["version"] == "3"


def test_a_bad_epoch_does_not_crash_the_import():
    assert epoch_ms_to_date(None) is None
    assert epoch_ms_to_date("nonsense") is None
