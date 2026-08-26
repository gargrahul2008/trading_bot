"""One password, one signed session cookie.

Ported from the journal dashboard, minus the client/master split — this app has
one operator. The password is a bcrypt hash supplied as DASHBOARD_PASSWORD_HASH
or in a file, never in the repo.

The session is httpOnly and carries its own issue time, so it expires on
idleness rather than on total length: leaving a tab open all day does not keep
a live trading dashboard unlocked forever.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import Depends, HTTPException, Request, Response, status
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.config import REPO, _read_env_file
from app.passwords import MAX_PASSWORD_BYTES, check_password, hash_password  # noqa: F401

COOKIE_NAME = "trading_dashboard"
IDLE_TIMEOUT_SECONDS = 60 * 60 * 4

DASHBOARD_ENV = REPO / "webapp" / "dashboard.env"

def _setting(name: str) -> str:
    """Environment first, then webapp/dashboard.env.

    A bcrypt hash is full of "$", which every layer that carries an environment
    variable treats as a variable reference and silently blanks — leaving a
    truncated hash that can never match any password. File contents pass
    through none of that.
    """
    return os.getenv(name) or _read_env_file(DASHBOARD_ENV).get(name, "")


def password_hash() -> str:
    return _setting("DASHBOARD_PASSWORD_HASH")


def _serializer() -> URLSafeTimedSerializer:
    secret = _setting("SESSION_SECRET") or "dev-secret-change-me"
    return URLSafeTimedSerializer(secret, salt="trading-dashboard-session")


def cookie_secure() -> bool:
    """Secure by default. Only false for local http, where the browser would
    otherwise drop the cookie."""
    return _setting("COOKIE_SECURE").lower() not in ("false", "0", "no")


def verify_password(raw: str) -> bool:
    stored = password_hash()
    if not stored:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "No password configured. Set DASHBOARD_PASSWORD_HASH.",
        )
    return check_password(raw, stored)


def issue_session(response: Response) -> None:
    response.set_cookie(
        COOKIE_NAME,
        _serializer().dumps({"sub": "operator"}),
        httponly=True,
        samesite="lax",
        secure=cookie_secure(),
    )


def clear_session(response: Response) -> None:
    response.delete_cookie(COOKIE_NAME)


def require_session(request: Request) -> str:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    try:
        _serializer().loads(token, max_age=IDLE_TIMEOUT_SECONDS)
    except SignatureExpired:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "Session timed out. Please sign in again."
        ) from None
    except BadSignature:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid session") from None
    return "operator"


Session = Depends(require_session)
