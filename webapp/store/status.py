#!/usr/bin/env python3
"""What is actually in the store.

    env/bin/python -m webapp.store.status

Read-only. Answers the two questions you have when you first point the agents
at a database: is anything being written, and is it still being written now.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.schema import DEFAULT_PATH, connect  # noqa: E402


def age(seconds):
    if seconds is None:
        return "never"
    if seconds < 90:
        return "%ds ago" % round(seconds)
    if seconds < 5400:
        return "%dm ago" % round(seconds / 60)
    return "%.1fh ago" % (seconds / 3600)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Summarise the dashboard store")
    parser.add_argument("--db", default=None)
    args = parser.parse_args(argv)

    path = args.db or os.getenv("DASHBOARD_DB") or DEFAULT_PATH
    if not os.path.exists(path):
        print("No store at %s — no agent has written yet." % path)
        print("Agents create it on their first poll; check they are running and")
        print("were not started with --db none.")
        return 1

    size_mb = os.path.getsize(path) / (1024 * 1024)
    print("store  %s  (%.1f MB)" % (path, size_mb))
    conn = connect(path, read_only=True)
    now = time.time()

    totals = {}
    for table in ("accounts", "orders", "fills", "snapshots"):
        try:
            totals[table] = conn.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]
        except Exception:
            print("  schema not initialised yet — the agents have not migrated it.")
            return 1
    print("rows   accounts=%(accounts)d orders=%(orders)d fills=%(fills)d "
          "snapshots=%(snapshots)d" % totals)

    accounts = [r["account"] for r in
                conn.execute("SELECT account FROM accounts ORDER BY account")]
    if not accounts:
        print("\nNo account has registered. An agent writes its first row within ~15s"
              " of starting.")
        return 1

    print()
    header = "%-10s %-12s %-9s %-9s %-9s %-9s %s" % (
        "account", "last heard", "positions", "holdings", "funds", "orders", "fills")
    print(header)
    print("-" * len(header))

    for account in accounts:
        status = conn.execute(
            "SELECT updated_at, live, auth_ok FROM agent_status WHERE account = ?",
            (account,)).fetchone()
        heard = age(now - status["updated_at"]) if status else "never"

        cells = []
        for kind in ("positions", "holdings", "funds"):
            row = conn.execute(
                "SELECT taken_at FROM snapshots WHERE account = ? AND kind = ?"
                " ORDER BY taken_at DESC LIMIT 1", (account, kind)).fetchone()
            cells.append(age(now - row["taken_at"]) if row else "—")

        orders = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE account = ?", (account,)).fetchone()[0]
        fills = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE account = ?", (account,)).fetchone()[0]

        flag = ""
        if status and not status["auth_ok"]:
            flag = "  ← token rejected"
        elif status and (now - status["updated_at"]) > 300:
            # Status is written every ~15s, so five minutes of silence means the
            # agent is not running, not that it is quiet.
            flag = "  ← agent silent"

        print("%-10s %-12s %-9s %-9s %-9s %-9d %d%s"
              % (account, heard, cells[0], cells[1], cells[2], orders, fills, flag))

    days = conn.execute(
        "SELECT trading_day, COUNT(*) FROM orders GROUP BY trading_day"
        " ORDER BY trading_day DESC LIMIT 5").fetchall()
    if days:
        print("\norders by trading day: " +
              ", ".join("%s=%d" % (d[0], d[1]) for d in days))

    print("\nIf this looks empty while the agents are running, ask them directly —"
          "\n  curl -s -H \"Authorization: Bearer $AGENT_TOKEN\" localhost:9102/health"
          "\n  | python3 -c \"import json,sys; print(json.load(sys.stdin)['store'])\""
          "\nA rising 'errors' there means writes are being attempted and failing.")

    latest = conn.execute("SELECT MAX(taken_at) FROM snapshots").fetchone()[0]
    if latest:
        # The single most useful line: is it filling right now, or did it stop.
        print("last write: %s" % age(now - latest))
        if now - latest > 300:
            print("  Nothing has been written for over five minutes — check the agents.")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
