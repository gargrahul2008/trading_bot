#!/usr/bin/env python3
"""Set the dashboard password.

    webapp/api/.venv/bin/python webapp/api/set_password.py

Prompts, so the password never appears in a command line — and therefore never
in shell history, in `ps` output, or in a terminal someone scrolls back through.

Writes webapp/dashboard.env, generating a session secret the first time and
preserving anything already in the file.
"""
from __future__ import annotations

import getpass
import os
import secrets
import stat
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO / "webapp" / "api") not in sys.path:
    sys.path.insert(0, str(REPO / "webapp" / "api"))

# Only the hashing module, so this script needs bcrypt and nothing else.
from app.passwords import MAX_PASSWORD_BYTES, hash_password  # noqa: E402

ENV_PATH = REPO / "webapp" / "dashboard.env"
MIN_LENGTH = 8


def read_existing() -> dict:
    values = {}
    try:
        for line in ENV_PATH.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


def prompt() -> str:
    while True:
        password = getpass.getpass("New dashboard password: ")
        if len(password) < MIN_LENGTH:
            print("  Too short — at least %d characters." % MIN_LENGTH)
            continue
        if len(password.encode("utf-8")) > MAX_PASSWORD_BYTES:
            # bcrypt truncates past 72 bytes, so two different passwords would
            # both work. Refuse rather than silently accept a weaker one.
            print("  Too long — at most %d bytes." % MAX_PASSWORD_BYTES)
            continue
        if password != getpass.getpass("Confirm: "):
            print("  They do not match. Try again.")
            continue
        return password


def main() -> int:
    if not sys.stdin.isatty():
        print("Refusing to read a password from a pipe — run this in a terminal.")
        return 2

    values = read_existing()
    print("Writing %s" % ENV_PATH)
    values["DASHBOARD_PASSWORD_HASH"] = hash_password(prompt())
    if not values.get("SESSION_SECRET") or values["SESSION_SECRET"] == "replace-me":
        values["SESSION_SECRET"] = secrets.token_urlsafe(32)
        print("  Generated a new SESSION_SECRET (this signs out any open session).")
    values.setdefault("COOKIE_SECURE", "false")

    body = "".join("%s=%s\n" % (key, value) for key, value in sorted(values.items()))
    ENV_PATH.write_text(
        "# Written by webapp/api/set_password.py — do not commit.\n" + body
    )
    os.chmod(ENV_PATH, stat.S_IRUSR | stat.S_IWUSR)

    print("  Done. COOKIE_SECURE=%s" % values["COOKIE_SECURE"])
    if values["COOKIE_SECURE"].lower() in ("false", "0", "no"):
        print("  Keep this behind an SSH tunnel until there is TLS in front of it:")
        print("    the session cookie is not marked Secure, so it would travel in clear.")
    print("  Restart the API for the change to take effect.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
