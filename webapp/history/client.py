"""The three history endpoints, called directly.

Shapes, traps and reconciliations were established against the live accounts on
2026-08-31 — `docs/host_state.md` §12. The three things that report found, which
this file exists to get right:

* **The ledger paginates and silently truncates.** A plain call returns exactly
  100 rows and looks complete; rahul's first page covered one month of the five
  requested. Every paged endpoint here reads until a short page.
* **`exch_id`, `exchange_name` and `segment_name` are wrong on real rows.**
  `BSE:SHISHIND-X` comes back as NSE. Key on `symbol_name`; never join on the
  exchange fields or bucket by segment.
* **Realised P&L has no date field but the window is a free parameter**, and the
  endpoint is additive over it — two half-windows summed to the full window
  exactly. So per-day realised is recoverable by asking one day at a time.

Every call must leave by its account's whitelisted IP, so this runs under that
account's `account.env`; `requests` takes the proxy from the environment exactly
as the SDK does.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

LOG = logging.getLogger("history.client")

BASE_URL = "https://api-t1.fyers.in/api/v3"

LEDGER = "/ledger-history"
REALISED = "/realised-pnl-history"
CHARGES = "/charges-history"

# The API rejects anything above 100 with -50 Invalid input.
MAX_PAGE_SIZE = 100

# A full year is ~250 calls per account for the per-day realised loop. The
# probe's own run ended on a -429, so this paces itself rather than finding out.
DEFAULT_PAUSE = 0.35
RATE_LIMIT_BACKOFF = 20.0
MAX_RETRIES = 4

DAY_WISE = "1"
SEGMENT_WISE = "2"


class HistoryError(RuntimeError):
    def __init__(self, message: str, payload: Any = None) -> None:
        super().__init__(message)
        self.payload = payload


def epoch_ms_to_date(value: Any) -> Optional[str]:
    """Ledger `date` and charges `trade_date` are epoch milliseconds at UTC
    midnight — a day stamp, with no intraday time in them."""
    try:
        seconds = int(value) / 1000.0
    except (TypeError, ValueError):
        return None
    return dt.datetime.utcfromtimestamp(seconds).date().isoformat()


def days_between(from_date: str, to_date: str) -> Iterator[str]:
    start = dt.date.fromisoformat(from_date)
    end = dt.date.fromisoformat(to_date)
    while start <= end:
        yield start.isoformat()
        start += dt.timedelta(days=1)


class HistoryClient:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        *,
        transport: Optional[Callable[..., Any]] = None,
        pause: float = DEFAULT_PAUSE,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._auth = "%s:%s" % (client_id, access_token)
        self._pause = pause
        self._sleep = sleep
        self.calls = 0
        self.rate_limited = 0
        if transport is None:
            import requests

            transport = requests.get
        self._get = transport

    # ── plumbing ────────────────────────────────────────────────────────────
    def _call(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"Authorization": self._auth, "version": "3"}
        for attempt in range(1, MAX_RETRIES + 1):
            self.calls += 1
            response = self._get(BASE_URL + path, headers=headers, params=params, timeout=30)
            payload = response.json()
            if not isinstance(payload, dict):
                raise HistoryError("unexpected response from %s" % path, payload)

            if payload.get("s") == "ok":
                if self._pause:
                    self._sleep(self._pause)
                return payload

            code = payload.get("code")
            if code in (-429, 429):
                self.rate_limited += 1
                # These are read-only batch calls sharing a budget with live
                # bots. Waiting is always the right answer.
                wait = RATE_LIMIT_BACKOFF * attempt
                LOG.warning("%s rate limited, waiting %.0fs (attempt %d)", path, wait, attempt)
                self._sleep(wait)
                continue
            raise HistoryError(
                "%s failed: %s" % (path, payload.get("message") or payload), payload
            )
        raise HistoryError("%s still rate limited after %d attempts" % (path, MAX_RETRIES))

    def _paged(self, path: str, params: Dict[str, Any]) -> Tuple[List[Dict], Dict]:
        """Read until a short page.

        Not "until an empty page": the ledger returns exactly `page_size` rows
        when more remain, and a full final page followed by an empty one is
        indistinguishable from truncation if you stop early.
        """
        rows: List[Dict[str, Any]] = []
        summary: Dict[str, Any] = {}
        page = 1
        while True:
            payload = self._call(path, dict(params, page_no=page, page_size=MAX_PAGE_SIZE))
            summary = payload.get("summary_data") or summary
            batch = payload.get("data") or []
            rows.extend(row for row in batch if isinstance(row, dict))
            if len(batch) < MAX_PAGE_SIZE:
                return rows, summary
            page += 1

    # ── endpoints ───────────────────────────────────────────────────────────
    def ledger(self, from_date: str, to_date: str) -> Dict[str, Any]:
        """Money in and out, per transaction, plus the window's summary.

        `summary_data` carries funds_added / funds_withdrawn / opening_balance /
        closing_balance for the whole window and is correct after one call —
        the paging is only needed for the individual transactions.
        """
        rows, summary = self._paged(LEDGER, {"from_date": from_date, "to_date": to_date})
        return {
            "summary": summary,
            "transactions": [
                {
                    "date": epoch_ms_to_date(row.get("date")),
                    "credit": row.get("credit_amount") or 0,
                    "debit": row.get("debit_amount") or 0,
                    "description": row.get("description") or "",
                    "transaction_type": row.get("transaction_type") or "",
                    "running_balance": row.get("running_balance"),
                }
                for row in rows
            ],
        }

    def realised(self, from_date: str, to_date: str) -> Dict[str, Any]:
        """Realised P&L per scrip for the window. No date field — see `by_day`."""
        payload = self._call(REALISED, {"from_date": from_date, "to_date": to_date,
                                        "page_no": 1, "page_size": MAX_PAGE_SIZE})
        rows = payload.get("data") or []
        return {
            "summary": payload.get("summary_data") or {},
            "scrips": [
                {
                    # symbol_name is the only trustworthy identifier on this
                    # endpoint; the exchange fields contradict it on real rows.
                    "symbol": str(row.get("symbol_name") or ""),
                    "realised": row.get("realized_pnl") or 0,
                    "buy_qty": row.get("buy_qty") or 0,
                    "sell_qty": row.get("sell_qty") or 0,
                    "buy_rate": row.get("buy_rate") or 0,
                    "sell_rate": row.get("sell_rate") or 0,
                }
                for row in rows if isinstance(row, dict) and row.get("symbol_name")
            ],
        }

    def realised_by_day(self, from_date: str, to_date: str,
                        on_day: Optional[Callable[[str, Dict], None]] = None
                        ) -> List[Dict[str, Any]]:
        """Realised per scrip per day, by asking one day at a time.

        The endpoint reports no date, but two half-windows summed to the full
        window exactly, so a one-day window gives that day's figure. Expensive —
        one call per calendar day — so callers pass a narrow range, and days with
        no trading come back empty and cost one cheap call.
        """
        out: List[Dict[str, Any]] = []
        for day in days_between(from_date, to_date):
            result = self.realised(day, day)
            if on_day:
                on_day(day, result)
            for scrip in result["scrips"]:
                out.append(dict(scrip, day=day))
        return out

    def charges(self, from_date: str, to_date: str,
                report_type: str = DAY_WISE) -> Dict[str, Any]:
        """Charges, day-wise (report_type 1) or segment-wise (2).

        Both reconcile to the paisa against each other and against realised
        P&L's own `charges` figure, so either can be used as a control total for
        apportioning charges onto individual trades.
        """
        rows, summary = self._paged(CHARGES, {
            "from_date": from_date, "to_date": to_date,
            "segment_type": "0", "exchange_type": "0", "report_type": report_type,
        })
        return {
            "summary": summary,
            "rows": [
                {
                    "day": epoch_ms_to_date(row.get("trade_date")),
                    "segment": row.get("segment"),
                    "turnover": row.get("turnover") or 0,
                    "total": row.get("total") or 0,
                    "brokerage": row.get("brokerage") or 0,
                    "stt": row.get("stt") or 0,
                    "gst": row.get("gst") or 0,
                    "stamp_duty": row.get("stamp_duty") or 0,
                    "transaction_charges": row.get("transaction_charges") or 0,
                    "sebi_toc": row.get("sebi_toc") or 0,
                    "ipft": row.get("ipft") or 0,
                }
                for row in rows if isinstance(row, dict)
            ],
        }
