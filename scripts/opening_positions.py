#!/usr/bin/env python3
"""What each account already held, before our fill history begins.

Seeded automatically from the earliest holdings snapshot — the broker gives each
holding's buy average, which is exactly the cost basis the matcher needs. This
command is for seeing what was recorded, and for correcting it.

    env/bin/python scripts/opening_positions.py                    show them
    env/bin/python scripts/opening_positions.py --seed rahul       (re)seed from holdings
    env/bin/python scripts/opening_positions.py --set rahul NSE:HFCL-EQ 900 228.92 \
        --as-of 2026-04-01 --note "bought 2024"

A manual entry outranks a re-seed: whoever types a cost basis in knows something
the snapshot does not. Read-only unless --seed or --set is given.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.pnl.opening import record_manual, seed_from_holdings  # noqa: E402
from webapp.store.schema import connect  # noqa: E402


def show(conn) -> int:
    rows = conn.execute(
        "SELECT account, symbol, product_type, qty, cost_price, as_of_day, source, note"
        " FROM opening_positions ORDER BY account, symbol"
    ).fetchall()
    if not rows:
        print("Nothing recorded yet.")
        print("Run scripts/fetch_history.py, or --seed <account> here, to take them")
        print("from the earliest holdings snapshot.")
        return 1

    print("%-10s %-22s %-6s %12s %12s %14s  %-18s %s"
          % ("account", "symbol", "prod", "qty", "cost", "value", "as of / source", "note"))
    print("-" * 118)
    account = None
    for row in rows:
        if row["account"] != account:
            account = row["account"]
        print("%-10s %-22s %-6s %12.2f %12.2f %14.2f  %-10s %-7s %s"
              % (row["account"], row["symbol"], row["product_type"], row["qty"],
                 row["cost_price"], row["qty"] * row["cost_price"],
                 row["as_of_day"], row["source"], row["note"] or ""))

    total = sum(r["qty"] * r["cost_price"] for r in rows)
    print()
    print("%d position(s), %.2f at cost" % (len(rows), total))
    print()
    print("These are the cost bases a sale is matched against. A holding's cost is the")
    print("broker's average across every buy, which is what it uses for delivery too.")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Opening positions")
    parser.add_argument("--db", default=None)
    parser.add_argument("--seed", metavar="ACCOUNT",
                        help="(re)seed from that account's earliest holdings snapshot")
    parser.add_argument("--set", nargs=4, metavar=("ACCOUNT", "SYMBOL", "QTY", "COST"),
                        help="record one by hand")
    parser.add_argument("--as-of", default="2026-04-01",
                        help="the day the manual position is dated (default the FY start)")
    parser.add_argument("--product", default="CNC")
    parser.add_argument("--note", default=None)
    args = parser.parse_args(argv)

    writing = bool(args.seed or args.set)
    conn = connect(args.db, read_only=not writing)

    if args.seed:
        count = seed_from_holdings(conn, args.seed)
        print("Seeded %d opening position(s) for %s from its earliest holdings snapshot."
              % (count, args.seed))
        if not count:
            print("No holdings snapshot stored yet — the agents record one on their first poll.")
        print()

    if args.set:
        account, symbol, qty, cost = args.set
        record_manual(conn, account, symbol, float(qty), float(cost),
                      as_of_day=args.as_of, product_type=args.product, note=args.note)
        print("Recorded %s %s: %s @ %s as of %s"
              % (account, symbol, qty, cost, args.as_of))
        print()

    return show(conn)


if __name__ == "__main__":
    raise SystemExit(main())
