#!/usr/bin/env python3
"""Refresh the exchanges' instrument list.

    env/bin/python scripts/fetch_symbols.py

Public CSVs from Fyers — no authentication, no account, no proxy. This is the
one fetch in the dashboard that does not go through an agent, so it costs no
part of the accounts' rate budget and can run whenever.

Run daily: instruments are added, renamed and moved between series constantly,
and the order pad treats "not in this list" as "does not exist".
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.history.symbols import (  # noqa: E402
    SEGMENTS, SymbolFormatError, counts, fetch, parse, store,
)
from webapp.store import connect, migrate  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Refresh the instrument list")
    parser.add_argument("--db", default=None)
    parser.add_argument("--segments", default="NSE_CM.csv,BSE_CM.csv",
                        help="comma-separated files; add NSE_FO.csv for derivatives")
    args = parser.parse_args(argv)

    conn = connect(args.db)
    migrate(conn)

    total = 0
    failed = []
    for name in [s.strip() for s in args.segments.split(",") if s.strip()]:
        if name not in SEGMENTS:
            print("skipping %s — not a known segment file" % name)
            continue
        exchange, segment = SEGMENTS[name]
        try:
            rows = parse(fetch(name), exchange, segment)
        except SymbolFormatError as exc:
            # Importing a misread file is worse than importing nothing: every
            # symbol would still land, with the tick size read from whatever
            # column now sits in that position.
            print("%-12s FORMAT CHANGED: %s" % (name, exc))
            failed.append(name)
            continue
        except Exception as exc:
            print("%-12s FAILED: %s" % (name, exc))
            failed.append(name)
            continue

        stored = store(conn, rows)
        total += stored
        print("%-12s %6d instruments" % (name, stored))

    summary = counts(conn)
    print()
    print("store now holds %d instruments" % summary["symbols"])
    if failed:
        print("failed: %s" % ", ".join(failed))
    conn.close()
    return 1 if failed and not total else 0


if __name__ == "__main__":
    raise SystemExit(main())
