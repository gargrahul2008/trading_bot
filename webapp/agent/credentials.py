"""Keeping the agent's broker credentials current.

Fyers access tokens expire daily. `scripts/fyers_auto_auth.py` refreshes
fyers_auth.json each morning, so a long-lived process must re-read the file —
the token it started with is worthless within a day.

The bots get this for free by crashing on the auth error and letting systemd
restart them. The agent deliberately survives broker errors so one bad poll does
not blank the dashboard, which means it has to handle this itself: it reloads
when the file changes, and immediately when the broker says the token is bad.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Callable, Optional, Tuple

LOG = logging.getLogger("agent.credentials")

# Fyers returns -16 "Could not authenticate the user" for an expired or invalid
# token. -15 and -17 are the neighbouring auth failures.
AUTH_ERROR_CODES = (-15, -16, -17)


def is_auth_error(exc: BaseException) -> bool:
    resp = getattr(exc, "resp", None)
    if isinstance(resp, dict):
        try:
            if int(resp.get("code")) in AUTH_ERROR_CODES:
                return True
        except (TypeError, ValueError):
            pass
    text = str(exc).lower()
    return "could not authenticate" in text or "'code': -16" in text


class CredentialSource:
    """Builds a broker client, and rebuilds it when the token file changes.

    `build` is injected so this can be tested without the Fyers SDK.
    """

    def __init__(
        self,
        auth_file: str,
        user_key: str,
        build: Callable[[str, str], Any],
        read_creds: Optional[Callable[[str, str], Tuple[str, str]]] = None,
    ) -> None:
        self.auth_file = auth_file
        self.user_key = user_key
        self._build = build
        self._read_creds = read_creds or self._default_read
        self._lock = threading.Lock()
        self._client: Optional[Any] = None
        self._mtime: Optional[float] = None
        self._token_tail = ""
        self.reloads = 0
        self.last_error: Optional[str] = None

    @staticmethod
    def _default_read(auth_file: str, user_key: str) -> Tuple[str, str]:
        from common.broker.auth_json import get_fyers_creds_from_json

        return get_fyers_creds_from_json(auth_file, user_key=user_key)

    def _file_mtime(self) -> Optional[float]:
        try:
            return os.path.getmtime(self.auth_file)
        except OSError:
            return None

    def _load(self) -> Any:
        client_id, access_token = self._read_creds(self.auth_file, self.user_key)
        self._client = self._build(client_id, access_token)
        self._mtime = self._file_mtime()
        # Enough to see in a log that the token actually changed, without ever
        # writing a usable credential to disk.
        self._token_tail = access_token[-6:] if access_token else ""
        self.reloads += 1
        self.last_error = None
        return self._client

    def client(self) -> Any:
        """The current client, rebuilt if the auth file has been rewritten."""
        with self._lock:
            if self._client is None:
                return self._load()
            mtime = self._file_mtime()
            if mtime is not None and mtime != self._mtime:
                LOG.info(
                    "%s: auth file changed — reloading token (…%s -> new)",
                    self.user_key, self._token_tail,
                )
                return self._load()
            return self._client

    def invalidate(self) -> Any:
        """Force a reload after the broker rejected the token.

        The daily refresh may have landed a moment ago, in which case this
        recovers immediately; if the file still holds the dead token, the error
        surfaces and `/health` says the token needs refreshing.
        """
        with self._lock:
            try:
                return self._load()
            except Exception as exc:
                self.last_error = str(exc)
                raise

    def status(self) -> dict:
        return {
            "auth_file": self.auth_file,
            "user_key": self.user_key,
            "loaded": self._client is not None,
            "reloads": self.reloads,
            "token_tail": self._token_tail,
            "file_mtime": self._mtime,
            "last_error": self.last_error,
        }
