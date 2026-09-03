#!/usr/bin/env python3
"""Why one scrip's P&L is what it is.

Prints every fill the store holds for an account and symbol, in the order the
matcher reads them, then what FIFO made of them and what is left open. When a
line on the dashboard looks wrong, this is the whole derivation behind it.

    env/bin/python scripts/explain_symbol.py rahul NSE:RELIANCE-EQ

A short round trip in a scrip you only ever bought, or an entry price equal to
its exit, means the fills are not what they should be — a missing opening
position, or the same execution recorded twice. Both show up here plainly.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.pnl.matcher import book_key, match_fills, open_position  # noqa: E402
from webapp.pnl.service import _fills  # noqa: E402
from webapp.store.schema import connect  # noqa: E402
from webapp.timestamps import to_iso  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("account")
    parser.add_argument("symbol")
    parser.add_argument("--db", default=None)
    args = parser.parse_args()

    conn = connect(args.db, read_only=True)
    fills = [f for f in _fills(conn, args.account)
             if f.get("symbol") == args.symbol]
    if not fills:
        print("No fills for %s in %s." % (args.symbol, args.account))
        print("If the dashboard shows a position, it predates the store's history —")
        print("record what it cost with scripts/opening_positions.py --set")
        return

    fills.sort(key=lambda f: (str(f.get("trading_day") or ""),
                              to_iso(f.get("traded_at")),
                              str(f.get("trade_id") or "")))

    print("FILLS  (%d, in the order the matcher reads them)" % len(fills))
    print("%-12s %-19s %-5s %10s %12s %-10s %s"
          % ("day", "at", "side", "qty", "price", "product", "trade id"))
    running = 0.0
    for fill in fills:
        signed = float(fill["qty"]) * (1 if fill["side"] == "BUY" else -1)
        running += signed
        print("%-12s %-19s %-5s %10s %12s %-10s %s"
              % (fill.get("trading_day"), to_iso(fill.get("traded_at")) or "—",
                 fill.get("side"), fill.get("qty"), fill.get("price"),
                 fill.get("product_type"), fill.get("trade_id")))
    print("net quantity from these fills: %s" % running)
    print("Compare that with what the dashboard shows as open. If the broker holds")
    print("more than this, the fills do not go back far enough and the position was")
    print("bought before the store existed — record its cost with")
    print("scripts/opening_positions.py --set, or FIFO will open a short on the next")
    print("sale and report the broker's mark of a position that does not exist.")

    # The same duplicate detection the eye does, done properly: two executions
    # of the same side, quantity and price in one day are almost never real.
    seen = Counter((f.get("trading_day"), f.get("side"), f.get("qty"), f.get("price"))
                   for f in fills)
    repeats = [k for k, n in seen.items() if n > 1]
    if repeats:
        print("\nSUSPECTED DUPLICATES — same day, side, quantity and price:")
        for day, side, qty, price in repeats:
            print("  %s %s %s @ %s  ×%d" % (day, side, qty, price,
                                            seen[(day, side, qty, price)]))
        print("  One execution recorded twice matches against itself: FIFO opens")
        print("  a lot and closes it at the same price, which is where a zero-P&L")
        print("  round trip in the wrong direction comes from.")

    matches, books = match_fills(fills)
    print("\nCLOSED  (%d round trips)" % len(matches))
    for m in matches:
        row = m.as_dict()
        print("  %-11s %-6s %8s  in %10s (%s)  out %10s (%s)  %12s  %s"
              % (row["closed_day"], row["direction"], row["qty"],
                 row["entry_price"], row["opened_day"],
                 row["exit_price"], row["closed_day"], row["gross"], row["kind"]))

    print("\nSTILL OPEN")
    found = False
    for key, lots in books.items():
        if key[0] != args.account or key[1] != args.symbol:
            continue
        position = open_position(lots)
        if position["qty"] == 0:
            continue
        found = True
        print("  %-10s %8s @ %s   oldest parcel %s"
              % (key[2], position["qty"], position["avg_price"],
                 min(lot.day for lot in lots)))
    if not found:
        print("  nothing")


if __name__ == "__main__":
    main()
