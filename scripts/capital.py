#!/usr/bin/env python3
"""See and correct each account's capital base.

    env/bin/python scripts/capital.py                      show it
    env/bin/python scripts/capital.py --set pratibha 3500000 \
        --on 2026-04-01 --note "securities held at FY start, at cost"

**Why a manual entry is needed.** `/ledger-history` gives the opening *cash*
balance and every transfer since — but not the securities the account already
owned on 1 April. That money went in during earlier years and no endpoint in
this financial year records it.

The visible symptom is deployed capital exceeding capital in: pratibha shows
17,07,978 in against 50,07,446 deployed, because roughly 35 lakh of stock she
already held is missing from the base. Every return figure measured against it
is wrong by that much.

(Leverage is a separate, legitimate reason for the same symptom — rahul's
RELIANCE ladder is 3x MTF, so his deployed exceeds his cash by design.)

Enter the cost of what each account held on 1 April and the base is right. The
figure is idempotent on its reference, so re-running replaces rather than adds.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.reader import Reader  # noqa: E402
from webapp.store.schema import connect  # noqa: E402
from webapp.store.writer import Writer  # noqa: E402

OPENING_SECURITIES = "opening-securities"


def show(conn) -> int:
    reader = Reader(conn)
    accounts = [r[0] for r in conn.execute(
        "SELECT DISTINCT account FROM capital ORDER BY account")]
    if not accounts:
        print("No capital recorded. Run scripts/fetch_history.py --from 2026-04-01.")
        return 1

    for account in accounts:
        print("%s — capital in %s" % (account, reader.capital_in(account)))
        for entry in reader.capital_entries(account):
            print("   %-12s %16.2f  %-8s %s"
                  % (entry["on_date"], entry["amount"], entry["source"],
                     (entry["note"] or "")[:52]))
        has_securities = any(
            str(e["reference"]).startswith(OPENING_SECURITIES)
            for e in reader.capital_entries(account))
        if not has_securities:
            print("   %-12s %16s  %-8s %s"
                  % ("", "—", "missing", "securities held at the FY start"))
        print()

    print("The ledger gives opening CASH and transfers. Securities already owned on")
    print("1 April are not in it — enter them with --set to make returns measurable.")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Capital base per account")
    parser.add_argument("--db", default=None)
    parser.add_argument("--set", nargs=2, metavar=("ACCOUNT", "AMOUNT"),
                        help="cost of securities held at the start of the year")
    parser.add_argument("--on", default="2026-04-01")
    parser.add_argument("--note", default="securities held at the FY start, at cost")
    args = parser.parse_args(argv)

    conn = connect(args.db, read_only=not args.set)

    if args.set:
        account, amount = args.set
        # A fixed reference, so correcting the figure replaces it rather than
        # adding a second one — this is one number, not a series of transfers.
        conn.execute("DELETE FROM capital WHERE account = ? AND reference LIKE ?",
                     (account, OPENING_SECURITIES + "%"))
        conn.commit()
        Writer(conn, account).capital([{
            "on_date": args.on, "amount": float(amount), "source": "manual",
            "reference": "%s|%s" % (OPENING_SECURITIES, args.on), "note": args.note,
        }])
        print("Recorded %s for %s as at %s.\n" % (amount, account, args.on))

    return show(conn)


if __name__ == "__main__":
    raise SystemExit(main())
