"""Password hashing, with no web-framework imports.

Separate from auth.py so the command-line password setter can use it without
pulling FastAPI into a script whose only job is to hash a string.
"""
from __future__ import annotations

import bcrypt

# bcrypt silently truncates at 72 bytes, so a longer password would have its
# tail ignored — two different passwords could then both work. Reject instead.
MAX_PASSWORD_BYTES = 72


def hash_password(raw: str) -> str:
    encoded = raw.encode("utf-8")
    if len(encoded) > MAX_PASSWORD_BYTES:
        raise ValueError("password must be at most %d bytes" % MAX_PASSWORD_BYTES)
    return bcrypt.hashpw(encoded, bcrypt.gensalt()).decode("ascii")


def check_password(raw: str, stored: str) -> bool:
    encoded = raw.encode("utf-8")
    if len(encoded) > MAX_PASSWORD_BYTES:
        return False
    try:
        return bcrypt.checkpw(encoded, stored.encode("utf-8"))
    except (ValueError, TypeError):
        # A malformed hash must read as "wrong password", never as an exception
        # that leaks how the credential is stored.
        return False
