#!/usr/bin/env python3
"""Bring accounts/ into line with fyers_auth.json.

Add a user to `fyers_auth.json` — credentials, plus `account` and `proxy` — and
this creates what the rest of the system needs: the account directory, its
`account.env`, and its agent port. `deploy/gen_systemd_units.py` then emits its
unit, and `deploy/cron/refresh_tokens.sh` picks it up on its own because that
reads the same register.

    deploy/onboard.py            show what would change
    deploy/onboard.py --apply    make the changes

Never touches an account that already has an `account.env`. That file names a
live account's whitelisted IP, and rewriting it from a stale field in the auth
file would silently redirect a real account's orders.
"""
from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from accounts import (  # noqa: E402
    ACCOUNTS_DIR, PORTS_FILE, Account, duplicate_direct, load, ports,
)

BASE_PORT = int(os.environ.get("AGENT_BASE_PORT", "9101"))

ENV_TEMPLATE = """# Written by deploy/onboard.py from fyers_auth.json — edit here, not there,
# once the account is live: this file is what systemd loads.
#
# Gitignored on purpose: it names the account's whitelisted IP.

# Account identity (the accounts/<name> folder).
ACCOUNT_ID={account}

# The user_key in fyers_auth.json for this account.
FYERS_USER_KEY={user_key}
"""

PROXY_TEMPLATE = """
# This account's dedicated static proxy = its whitelisted static IP. All Fyers
# REST and auth traffic exits through here, and Fyers rejects it from anywhere
# else.
HTTPS_PROXY={proxy}
HTTP_PROXY={proxy}
"""

DIRECT_NOTE = """
# No proxy: this account's whitelisted IP is the host's own address. Only one
# account can be set up this way.
"""


def assign_port(existing: Dict[str, int], name: str) -> int:
    if name in existing:
        return existing[name]
    taken = set(existing.values())
    port = BASE_PORT
    while port in taken:
        port += 1
    existing[name] = port
    return port


def plan(accounts: List[Account]) -> List[Account]:
    return [a for a in accounts if a.usable and not a.configured]


def describe(accounts: List[Account]) -> None:
    for a in accounts:
        if a.status() == "incomplete":
            print("  skip   %-10s (%s) — missing %s"
                  % (a.name or "?", a.user_key, ", ".join(a.missing)))
        elif a.status() == "dormant":
            print("  skip   %-10s (%s) — auto_refresh off" % (a.name, a.user_key))
        elif a.configured:
            print("  ok     %-10s (%s) — already set up, untouched"
                  % (a.name, a.user_key))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Set up accounts named in fyers_auth.json")
    parser.add_argument("--apply", action="store_true", help="make the changes")
    parser.add_argument("--auth-file", default=None)
    args = parser.parse_args(argv)

    accounts = load(args.auth_file)
    describe(accounts)
    todo = plan(accounts)

    if not todo:
        print("\nNothing to onboard — every usable account already has an account.env.")
        return 0

    # A new account with no proxy would go out on the host IP, which is already
    # whitelisted for a different app. Fyers would reject its orders, and the
    # failure would look like a credentials problem.
    direct = duplicate_direct(accounts)
    blocked = []
    for a in todo:
        if not a.declared_proxy and len(direct) > 1:
            blocked.append(a)

    print()
    for a in todo:
        if a in blocked:
            print("  BLOCK  %-10s (%s) — no proxy, and %s already goes out directly."
                  % (a.name, a.user_key, ", ".join(n for n in direct if n != a.name)))
            print("         Add \"proxy\": \"http://<its-ip>:3128\" to its fyers_auth.json record.")
        else:
            print("  NEW    %-10s (%s) — create accounts/%s/account.env, egress %s"
                  % (a.name, a.user_key, a.name, a.expected_ip or "host IP"))

    todo = [a for a in todo if a not in blocked]
    if not todo:
        print("\nNothing can be created until the above is resolved.")
        return 1

    if not args.apply:
        print("\nDry run. Re-run with --apply to create these.")
        return 0

    assigned = ports()
    for a in todo:
        os.makedirs(a.dir, exist_ok=True)
        body = ENV_TEMPLATE.format(account=a.name, user_key=a.user_key)
        body += (PROXY_TEMPLATE.format(proxy=a.declared_proxy)
                 if a.declared_proxy else DIRECT_NOTE)
        with open(a.env_path, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.chmod(a.env_path, stat.S_IRUSR | stat.S_IWUSR)
        port = assign_port(assigned, a.name)
        print("  created accounts/%s/account.env (mode 600), agent port %d" % (a.name, port))

    with open(PORTS_FILE, "w", encoding="utf-8") as fh:
        json.dump(dict(sorted(assigned.items())), fh, indent=2)
        fh.write("\n")

    print("\nNext, in this order — nothing above has started anything yet:")
    print("  1. deploy/preflight.sh          confirm each account leaves by its own IP")
    print("  2. python3 deploy/gen_systemd_units.py")
    print("  3. cp deploy/systemd/generated/agent-*.service /etc/systemd/system/")
    print("  4. systemctl daemon-reload && systemctl enable --now agent-<name>")
    print("  5. commit deploy/agent_ports.json so the port is recorded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
