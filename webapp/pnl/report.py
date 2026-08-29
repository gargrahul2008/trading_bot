#!/usr/bin/env python3
"""Per-trade P&L from the command line.

    env/bin/python -m webapp.pnl.report                 every account, all time
    env/bin/python -m webapp.pnl.report --account rahul
    env/bin/python -m webapp.pnl.report --day 2026-08-28

Read-only. Matching always replays every fill; --day narrows which closed trades
are shown, not which fills are considered.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.pnl.service import days_available, report  # noqa: E402
from webapp.store.schema import connect  # noqa: E402


def signed(value) -> str:
    """P&L always carries its sign — the same rule the web UI follows, because
    red and green alone are not enough to read a number by."""
    amount = float(value)
    return "%s%.2f" % ("+" if amount > 0 else "", amount)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Per-trade P&L from matched fills")
    parser.add_argument("--account", default=None)
    parser.add_argument("--day", default=None, help="filter by the day a trade CLOSED")
    parser.add_argument("--db", default=None)
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args(argv)

    conn = connect(args.db, read_only=True)
    days = days_available(conn, args.account)
    if not days:
        print("No fills stored yet. The agents record them as they poll —")
        print("check with: env/bin/python -m webapp.store.status")
        return 1

    data = report(conn, args.account, args.day)
    t = data["totals"]

    print("account : %s" % (args.account or "all"))
    print("days    : %s%s" % (", ".join(days[:6]), " …" if len(days) > 6 else ""))
    print("fills   : %d considered" % data["fills_considered"])
    print()

    if not data["trades"]:
        print("No closed round trips%s." % (" on %s" % args.day if args.day else ""))
    else:
        print("%-11s %-22s %-6s %-11s %10s %10s %12s"
              % ("closed", "symbol", "side", "kind", "qty", "entry", "gross"))
        print("-" * 88)
        for trade in sorted(data["trades"], key=lambda t: t["closed_at"] or "")[-args.limit:]:
            print("%-11s %-22s %-6s %-11s %10s %10s %12s"
                  % (trade["closed_day"], trade["symbol"], trade["direction"],
                     trade["kind"], trade["qty"], trade["entry_price"],
                     signed(float(trade["gross"]))))
        if len(data["trades"]) > args.limit:
            print("... %d more" % (len(data["trades"]) - args.limit))

    print()
    print("trades  : %d  (%d won, %d lost)" % (t["trades"], t["wins"], t["losses"]))
    print("gross   : %s   intraday %s · positional %s"
          % (signed(float(t["gross"])),
             signed(float(t["intraday"])), signed(float(t["positional"]))))
    print("          long %s · short %s"
          % (signed(float(t["long"])), signed(float(t["short"]))))
    print()
    print("Gross of charges — the broker reports those per day and segment,")
    print("not per trade, so they are apportioned separately.")

    if data["open_positions"]:
        print()
        print("still open:")
        for p in data["open_positions"]:
            print("  %-10s %-22s %-6s %10s @ %-10s since %s"
                  % (p["account"], p["symbol"], p["direction"], p["qty"],
                     p["avg_price"], p["opened_day"]))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
