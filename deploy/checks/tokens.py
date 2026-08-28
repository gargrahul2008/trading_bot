"""Is every Fyers token fresh today?

Prints LEVEL|message lines for deploy/preflight.sh to fold into its counters.

Tokens last a day. Anything not refreshed today means the bots and agents are
authenticating — or are about to — with a dead credential, which is what left
three agents wedged for 34 hours before this check existed.
"""
from __future__ import annotations

import datetime as dt
import json
import sys


def main(repo: str) -> int:
    try:
        users = json.load(open(repo + "/fyers_auth.json"))["users"]
    except Exception as exc:
        print("FAIL|cannot read fyers_auth.json: %s" % exc)
        return 0

    now = dt.datetime.now(dt.timezone.utc)
    for key, rec in sorted(users.items()):
        label = rec.get("label") or key
        # A user with auto_refresh off is deliberately dormant, not broken.
        if not rec.get("auto_refresh", True):
            print("SKIP|%s: auto_refresh off, ignored" % label)
            continue

        stamp = rec.get("token_updated_at")
        if not stamp:
            print("FAIL|%s: no token_updated_at — never refreshed" % label)
            continue
        try:
            when = dt.datetime.fromisoformat(stamp)
        except ValueError:
            print("WARN|%s: unparseable token_updated_at %r" % (label, stamp))
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=dt.timezone.utc)

        age_h = (now - when).total_seconds() / 3600.0
        if when.date() == now.date():
            print("PASS|%s: token refreshed %.1fh ago" % (label, age_h))
        else:
            print("FAIL|%s: token is %.1fh old (last %s) — run deploy/cron/refresh_tokens.sh"
                  % (label, age_h, when.date()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
