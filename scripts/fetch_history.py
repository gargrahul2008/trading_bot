#!/usr/bin/env python3
"""Fetch one account's history into the dashboard store.

MUST run under the account's environment so its calls leave by that account's
whitelisted IP:

    env $(grep -v '^#' accounts/rahul/account.env | xargs) \
        env/bin/python scripts/fetch_history.py --account rahul

    # first run, back to the start of the financial year
    ... scripts/fetch_history.py --account rahul --from 2026-04-01 --daily-realised

Read-only against the broker: three GET endpoints, no order, modify or cancel.

Two costs worth knowing before running it:

* `--daily-realised` asks the realised endpoint once per calendar day, because
  it carries no date field and only a one-day window yields a one-day figure.
  April to today is ~150 calls per account. Without the flag, one call gives the
  same total attributed to a single day.
* These share a rate budget with the live bots. The client paces itself and
  backs off on -429, but the sensible time to backfill a year is outside market
  hours.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.auth_json import get_fyers_creds_from_json  # noqa: E402
from webapp.history.client import HistoryClient  # noqa: E402
from webapp.history.importer import (  # noqa: E402
    import_capital, import_charges, import_realised, realised_total, record_progress,
)
from webapp.pnl.opening import seed_from_holdings  # noqa: E402
from webapp.store import connect, migrate  # noqa: E402

LOG = logging.getLogger("fetch_history")

# The Indian financial year, and the point this dashboard measures from.
FY_START = "2026-04-01"


#: The market's day, not UTC's. Timezone-aware rather than utcnow(), which is
#: deprecated on the host's Python 3.12; datetime.UTC would not work on 3.9.
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def today() -> str:
    return dt.datetime.now(IST).date().isoformat()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Import a Fyers account's history")
    parser.add_argument("--account", required=True, help="accounts/<name>")
    parser.add_argument("--user-key", default=None,
                        help="defaults to FYERS_USER_KEY from account.env")
    parser.add_argument("--from", dest="from_date", default=None,
                        help="default: the last 7 days; use %s for a full backfill" % FY_START)
    parser.add_argument("--to", dest="to_date", default=None, help="default: today")
    parser.add_argument("--daily-realised", action="store_true",
                        help="one call per day, for per-day realised P&L (slow)")
    parser.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    parser.add_argument("--db", default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch and report, write nothing")
    parser.add_argument("--no-seed-openings", action="store_true",
                        help="skip recording what the account already held")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    user_key = args.user_key or os.getenv("FYERS_USER_KEY") or ""
    if not user_key:
        raise SystemExit("no user key: run under the account's account.env, or pass --user-key")

    to_date = args.to_date or today()
    from_date = args.from_date or (
        (dt.date.fromisoformat(to_date) - dt.timedelta(days=7)).isoformat()
    )

    proxy = os.getenv("HTTPS_PROXY") or ""
    print("account : %s (%s)" % (args.account, user_key))
    print("egress  : %s" % (proxy or "host IP — no proxy set"))
    print("window  : %s → %s" % (from_date, to_date))
    print()

    client_id, token = get_fyers_creds_from_json(args.auth_file, user_key=user_key)
    api = HistoryClient(client_id, token)

    conn = connect(args.db)
    migrate(conn)

    # ── capital ─────────────────────────────────────────────────────────────
    ledger = api.ledger(from_date, to_date)
    summary = ledger.get("summary") or {}
    print("ledger    : %d transaction(s)" % len(ledger["transactions"]))
    print("            added %s, withdrawn %s, closing %s"
          % (summary.get("funds_added"), summary.get("funds_withdrawn"),
             summary.get("closing_balance")))

    # Which rows are transfers is the one thing here we have to classify, and
    # getting it wrong is silent — the first run stored zero capital against a
    # summary reporting 15 lakh added. So show the breakdown every time.
    by_type: dict = {}
    for row in ledger["transactions"]:
        kind = str(row.get("transaction_type") or "?")
        entry = by_type.setdefault(kind, {"n": 0, "credit": 0.0, "debit": 0.0})
        entry["n"] += 1
        entry["credit"] += float(row.get("credit") or 0)
        entry["debit"] += float(row.get("debit") or 0)
    for kind, e in sorted(by_type.items()):
        print("            %-16s %4d rows  credit %14.2f  debit %14.2f"
              % (kind, e["n"], e["credit"], e["debit"]))

    # ── realised ────────────────────────────────────────────────────────────
    if args.daily_realised:
        seen = []
        rows = api.realised_by_day(
            from_date, to_date,
            on_day=lambda day, r: seen.append(day) or (
                print("            %s: %d scrip(s)" % (day, len(r["scrips"])))
                if r["scrips"] else None),
        )
        print("realised  : %d scrip-day row(s) over %d day(s)" % (len(rows), len(seen)))
    else:
        window = api.realised(from_date, to_date)
        # Without the per-day loop the endpoint gives one figure for the whole
        # window. Attributing it to the last day would be a lie the store cannot
        # later distinguish from a real one-day figure, so say so and skip.
        print("realised  : %d scrip(s) for the whole window — no per-day detail"
              % len(window["scrips"]))
        print("            gross %s, charges %s, net %s"
              % (window["summary"].get("gross_pnl"), window["summary"].get("charges"),
                 window["summary"].get("net_pnl")))
        print("            pass --daily-realised to store it per day")
        rows = []

    # ── charges ─────────────────────────────────────────────────────────────
    charges = api.charges(from_date, to_date)
    print("charges   : %d day(s), total %s"
          % (len(charges["rows"]), (charges.get("summary") or {}).get("total")))

    if args.dry_run:
        print("\nDry run — nothing written. %d API call(s)." % api.calls)
        return 0

    # The opening balance is only capital when the window starts where returns
    # are measured from. For any later start it already contains this year's
    # profits, and counting it would inflate the base.
    added = import_capital(
        conn, args.account, ledger,
        opening_for=from_date if from_date == FY_START else None,
    )
    if from_date != FY_START:
        print("            (opening balance not counted — window does not start at %s)"
              % FY_START)
    realised_rows = import_realised(conn, args.account, rows) if rows else 0
    charge_rows = import_charges(conn, args.account, charges)

    # What the account already held before our fills begin. Without it a
    # delivery sale has no buy to match against, so its P&L never appears and
    # the broker's mark-to-market of a phantom short shows up instead.
    if not args.no_seed_openings:
        seeded = seed_from_holdings(conn, args.account)
        if seeded:
            print("openings  : %d holding(s) recorded as opening positions" % seeded)

    record_progress(conn, args.account, "ledger", from_date, to_date)
    record_progress(conn, args.account, "charges", from_date, to_date)
    if rows:
        record_progress(conn, args.account, "realised", from_date, to_date)

    print()
    print("stored    : %d new capital entr(ies), %d realised row(s), %d charge day(s)"
          % (added, realised_rows, charge_rows))

    # Reconcile what we classified as capital against the broker's own totals.
    # Deriving transfers from row types is the fragile step, and a silent zero
    # is exactly how it fails — so check it against a number we did not compute.
    expected = float(summary.get("funds_added") or 0) - float(summary.get("funds_withdrawn") or 0)
    if from_date == FY_START:
        # Compare like with like: when the opening balance is imported it is
        # part of the stored capital, so it has to be part of the expectation
        # too. Leaving it out made a correct import look broken.
        expected += float(summary.get("opening_balance") or 0)
    if expected:
        from webapp.store.reader import Reader
        stored_total = float(Reader(conn).capital_in(args.account,
                                                     upto=to_date) or 0)
        if abs(stored_total - expected) > 1:
            print()
            print("  WARNING: capital does not reconcile.")
            print("    broker says %+.2f over this window (opening %s + added %s − withdrawn %s)"
                  % (expected, summary.get("opening_balance") if from_date == FY_START else 0,
                     summary.get("funds_added"), summary.get("funds_withdrawn")))
            print("    stored capital to %s is %+.2f" % (to_date, stored_total))
            print("    The transaction_type breakdown above says which rows carry")
            print("    the transfers; webapp/history/importer.py::CAPITAL_TYPES lists")
            print("    the ones counted. They do not agree.")
    totals = realised_total(conn, args.account)
    print("to date   : gross %s − charges %s = net %s"
          % (totals["gross"], totals["charges"], totals["net"]))
    print("api calls : %d (%d rate-limited)" % (api.calls, api.rate_limited))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
