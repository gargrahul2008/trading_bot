"""Talking to the account agents.

The API holds no broker credentials and never calls Fyers. It reads each
account's agent over loopback and merges the answers.

The rule that governs this file: **one unreachable account must not cost you the
others.** Calls fan out concurrently with a short timeout, and a failure is
returned as data — an `error` on that account — rather than raised. A dashboard
that shows five accounts and says the sixth is unreachable is useful; one that
shows an error page because a single agent is restarting is not.
"""
from __future__ import annotations

import json
import logging
import socket
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from app.config import agent_ports, agent_token, get_settings

LOG = logging.getLogger("api.agents")


class AgentResult:
    """One account's answer, or why there isn't one."""

    def __init__(
        self,
        account: str,
        data: Optional[Any] = None,
        error: Optional[str] = None,
        reachable: bool = True,
        timed_out: bool = False,
    ) -> None:
        self.account = account
        self.data = data
        self.error = error
        self.reachable = reachable
        # We gave up waiting. Distinct from every other failure, because the
        # thing asked for may have happened anyway — on a trade, that is the
        # difference between "it did not go" and "nobody knows".
        self.timed_out = timed_out

    @property
    def ok(self) -> bool:
        return self.error is None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "account": self.account,
            "data": self.data,
            "error": self.error,
            "reachable": self.reachable,
            "timed_out": self.timed_out,
        }


def _is_timeout(exc: Any) -> bool:
    """Whether this failure was us giving up, rather than a refusal.

    On Python 3.9 socket.timeout and TimeoutError are distinct classes; from
    3.10 the former is an alias of the latter. Both are checked so the
    distinction survives whichever the host runs.
    """
    return isinstance(exc, (socket.timeout, TimeoutError))


class AgentClient:
    def __init__(self, host: Optional[str] = None, timeout: Optional[float] = None) -> None:
        settings = get_settings()
        self.host = host or settings.agent_host
        self.timeout = timeout if timeout is not None else settings.agent_timeout

    def url(self, account: str, path: str) -> Optional[str]:
        port = agent_ports().get(account)
        if port is None:
            return None
        return "http://%s:%d%s" % (self.host, port, path)

    def call(
        self,
        account: str,
        path: str,
        *,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> AgentResult:
        url = self.url(account, path)
        if url is None:
            return AgentResult(account, error="no agent port for %s" % account, reachable=False)

        token = agent_token()
        if not token:
            return AgentResult(account, error="no AGENT_TOKEN configured", reachable=False)

        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Authorization", "Bearer " + token)
        if data is not None:
            request.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(
                    request, timeout=self.timeout if timeout is None else timeout) as response:
                raw = response.read().decode("utf-8")
            return AgentResult(account, data=json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            try:
                detail = json.loads(detail).get("error", detail)
            except ValueError:
                pass
            # The agent answered, so it is up — it just refused this call.
            return AgentResult(account, error="agent %s: %s" % (exc.code, detail))
        except urllib.error.URLError as exc:
            if _is_timeout(exc.reason):
                return AgentResult(account, error="timed out after %.1fs"
                                   % (self.timeout if timeout is None else timeout),
                                   reachable=True, timed_out=True)
            return AgentResult(
                account, error="agent unreachable: %s" % exc.reason, reachable=False
            )
        except Exception as exc:  # a malformed reply must not 500 the page
            if _is_timeout(exc):
                # Reached the agent and gave up waiting. Not the same as
                # unreachable: whatever was asked for may well have happened.
                return AgentResult(account, error="timed out after %.1fs"
                                   % (self.timeout if timeout is None else timeout),
                                   reachable=True, timed_out=True)
            LOG.warning("%s: unexpected agent failure: %s", account, exc)
            return AgentResult(account, error=str(exc), reachable=False)

    def fan_out(self, path: str, accounts: Optional[List[str]] = None) -> List[AgentResult]:
        """Ask every account at once. Order follows `accounts` so the dashboard
        renders in a stable order regardless of who replies first."""
        accounts = accounts if accounts is not None else sorted(agent_ports())
        if not accounts:
            return []
        with ThreadPoolExecutor(max_workers=max(len(accounts), 1)) as pool:
            return list(pool.map(lambda account: self.call(account, path), accounts))
