#!/usr/bin/env python3
"""Who the accounts are, resolved once, from one place.

`fyers_auth.json` is where credentials are added by hand, so it is the register:
add a user there and everything else — the account directory, its egress proxy,
its agent port, its unit, its daily token refresh — follows from it.

Two fields beyond what Fyers needs make that possible, both optional:

    "account": "piyush"                      the accounts/<name> directory
    "proxy":   "http://15.252.102.31:3128"   its whitelisted egress; omit for the
                                             host IP

`account` defaults to the label, lower-cased. `proxy` is only read when the
account has no `account.env` yet — once that file exists it is the truth, since
it is what systemd actually loads.

Used by deploy/onboard.py, deploy/cron/refresh_tokens.sh and deploy/preflight.sh
so none of them can disagree about who exists.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTH_FILE = os.path.join(REPO, "fyers_auth.json")
ACCOUNTS_DIR = os.path.join(REPO, "accounts")
PORTS_FILE = os.path.join(REPO, "deploy", "agent_ports.json")

# Everything a login needs. A record missing any of these is a placeholder,
# not an account — user4 has sat in the file unused for months.
REQUIRED = ("client_id", "secret_key", "totp_key", "pin", "redirect_uri")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "", str(text or "").strip().lower())


def read_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


class Account:
    def __init__(self, user_key: str, record: Dict[str, Any]) -> None:
        self.user_key = user_key
        self.label = str(record.get("label") or user_key)
        self.name = _slug(record.get("account") or self.label)
        self.auto_refresh = bool(record.get("auto_refresh", True))
        self.fy_id = str(record.get("fy_id") or "")
        self.missing = [f for f in REQUIRED if not str(record.get(f) or "").strip()]
        self.declared_proxy = str(record.get("proxy") or "").strip()

        self.dir = os.path.join(ACCOUNTS_DIR, self.name) if self.name else ""
        self.env_path = os.path.join(self.dir, "account.env") if self.dir else ""
        self.env = read_env_file(self.env_path) if self.env_path else {}

    @property
    def configured(self) -> bool:
        """Has an account.env — systemd can actually load it."""
        return bool(self.env.get("FYERS_USER_KEY"))

    @property
    def usable(self) -> bool:
        """Enough credentials to log in, and meant to be used."""
        return self.auto_refresh and not self.missing and bool(self.name)

    @property
    def proxy(self) -> str:
        """The live account.env wins: it is what systemd loads."""
        return self.env.get("HTTPS_PROXY", self.declared_proxy)

    @property
    def expected_ip(self) -> Optional[str]:
        """The address this account's traffic must appear to come from.

        Derived from its own proxy rather than a list kept somewhere else, so
        there is nothing to fall out of step. None means it goes out directly.
        """
        if not self.proxy:
            return None
        return re.sub(r"^https?://", "", self.proxy).split(":")[0] or None

    def status(self) -> str:
        if not self.name:
            return "unnamed"
        if self.missing:
            return "incomplete"
        if not self.auto_refresh:
            return "dormant"
        return "configured" if self.configured else "new"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "user_key": self.user_key, "account": self.name, "label": self.label,
            "status": self.status(), "proxy": self.proxy or "",
            "expected_ip": self.expected_ip or "", "missing": self.missing,
        }


def load(auth_file: Optional[str] = None) -> List[Account]:
    path = auth_file or AUTH_FILE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            users = (json.load(fh) or {}).get("users") or {}
    except (OSError, ValueError) as exc:
        raise SystemExit("cannot read %s: %s" % (path, exc))
    return [Account(key, rec) for key, rec in sorted(users.items())
            if isinstance(rec, dict)]


def duplicate_direct(accounts: List[Account]) -> List[str]:
    """Accounts with no proxy. Only one can be right: the host has a single IP,
    and it is whitelisted for exactly one Fyers app."""
    return [a.name for a in accounts if a.usable and not a.proxy]


def ports(path: Optional[str] = None) -> Dict[str, int]:
    try:
        with open(path or PORTS_FILE, "r", encoding="utf-8") as fh:
            return {str(k): int(v) for k, v in json.load(fh).items()}
    except (OSError, ValueError):
        return {}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Resolve accounts from fyers_auth.json")
    parser.add_argument("--auth-file", default=None)
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--refreshable", action="store_true",
                        help="print '<account> <user_key>' per line for the token cron")
    args = parser.parse_args(argv)

    accounts = load(args.auth_file)

    if args.refreshable:
        # Only accounts that can log in AND have an account.env to log in
        # through — without one there is no proxy, and the login would leave
        # from the wrong IP.
        for a in accounts:
            if a.usable and a.configured:
                print("%s %s" % (a.name, a.user_key))
        return 0

    if args.json:
        print(json.dumps([a.as_dict() for a in accounts], indent=2))
        return 0

    assigned = ports()
    print("%-10s %-9s %-12s %-16s %s" % ("account", "user_key", "status", "egress", "port"))
    print("-" * 62)
    for a in accounts:
        print("%-10s %-9s %-12s %-16s %s"
              % (a.name or "?", a.user_key, a.status(),
                 a.expected_ip or "host IP", assigned.get(a.name, "-")))
        if a.missing:
            print("           missing: %s" % ", ".join(a.missing))

    clashes = duplicate_direct(accounts)
    if len(clashes) > 1:
        print("\nWARNING: %s all go out directly, but the host has one IP and Fyers"
              " whitelists it for one app. All but one need a proxy."
              % ", ".join(clashes))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
