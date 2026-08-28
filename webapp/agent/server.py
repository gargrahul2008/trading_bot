"""The agent's HTTP face: localhost only, token-gated, stdlib only.

Deliberately not FastAPI. This process runs in the same virtualenv as the live
bots, and adding a web framework there means adding dependencies to the
environment that places real orders. The surface is a dozen routes; the standard
library covers it.

Two safety properties are structural rather than conventional:

* it binds 127.0.0.1, so the agent is reachable only from the host, and
* trading routes exist only when the process was started with --allow-trading.

Anything on the host could otherwise place an order in a real account, so the
bearer token is required even on loopback.
"""
from __future__ import annotations

import hmac
import json
import logging
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Tuple

from webapp.agent.gateway import build_order_request

LOG = logging.getLogger("agent.server")

MAX_BODY_BYTES = 64 * 1024

_ORDER_ID = re.compile(r"^/orders/([A-Za-z0-9_.:-]{1,64})$")
_POSITION_EXIT = re.compile(r"^/positions/([A-Za-z0-9_.:%|-]{1,128})/exit$")


class AgentError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class Agent:
    """Everything the HTTP layer is allowed to do, with no HTTP in it."""

    def __init__(
        self,
        *,
        user: str,
        book: Any,
        poller: Any,
        gateway: Any,
        allow_trading: bool = False,
    ) -> None:
        self.user = user
        self.book = book
        self.poller = poller
        self.gateway = gateway
        self.allow_trading = allow_trading

    # ── reads ───────────────────────────────────────────────────────────────
    def health(self) -> Dict[str, Any]:
        health = self.book.health()
        health["poller"] = self.poller.status()
        health["allow_trading"] = self.allow_trading
        credentials = getattr(self.gateway, "credentials", None)
        if credentials is not None:
            health["credentials"] = credentials.status()

        # Persistence failures are swallowed so the poller survives them, which
        # means nothing else would ever show them. A rising error count here is
        # the only signal that history is being lost.
        writer = getattr(self.poller, "writer", None)
        health["store"] = writer.stats() if writer is not None else {"enabled": False}
        # An expired token makes every section fail identically. Saying so once,
        # at the top, is the difference between "the broker is unreachable" and
        # "run the auth unit" — they need different responses.
        health["auth_ok"] = not any(
            section.get("error") and "authenticate" in section["error"].lower()
            for section in health.get("sections", {}).values()
        )
        return health

    def snapshot(self) -> Dict[str, Any]:
        return self.book.snapshot()

    def section(self, name: str) -> Dict[str, Any]:
        try:
            return self.book.get(name)
        except KeyError:
            raise AgentError(404, "no such section: %s" % name)

    # ── writes ──────────────────────────────────────────────────────────────
    def _require_trading(self) -> None:
        if not self.allow_trading:
            raise AgentError(
                403,
                "this agent is read-only; restart it with --allow-trading to place orders",
            )

    def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._require_trading()
        try:
            request = build_order_request(payload)
        except ValueError as exc:
            raise AgentError(400, str(exc))

        LOG.warning(
            "%s: PLACE %s %s x%s %s @%s",
            self.user, request.side, request.symbol, request.qty,
            request.product_type, request.limit_price,
        )
        order_id = self.gateway.place_order(request)
        # The order book is now out of date by definition; refresh it on the
        # next tick rather than making the caller wait a full interval to see
        # what they just placed.
        self.poller.expedite("orders")
        return {"order_id": order_id}

    def modify_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._require_trading()
        fields: Dict[str, Any] = {}
        if payload.get("qty") is not None:
            fields["qty"] = int(payload["qty"])
        if payload.get("limit_price") is not None:
            fields["limit_price"] = payload["limit_price"]
        if payload.get("stop_price") is not None:
            fields["stop_price"] = payload["stop_price"]
        if payload.get("order_type") is not None:
            fields["order_type"] = payload["order_type"]
        if not fields:
            raise AgentError(400, "nothing to modify")

        LOG.warning("%s: MODIFY %s %s", self.user, order_id, fields)
        result = self.gateway.modify_order(order_id, **fields)
        self.poller.expedite("orders")
        return {"result": result}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        self._require_trading()
        LOG.warning("%s: CANCEL %s", self.user, order_id)
        result = self.gateway.cancel_order(order_id)
        self.poller.expedite("orders")
        return {"result": result}

    def exit_position(self, position_id: str) -> Dict[str, Any]:
        self._require_trading()
        LOG.warning("%s: EXIT %s", self.user, position_id)
        result = self.gateway.exit_position(position_id)
        self.poller.expedite("positions")
        self.poller.expedite("orders")
        return {"result": result}

    # ── routing ─────────────────────────────────────────────────────────────
    def dispatch(self, method: str, path: str, body: Optional[Dict[str, Any]]) -> Tuple[int, Any]:
        body = body or {}

        if method == "GET":
            if path == "/health":
                return 200, self.health()
            if path == "/book":
                return 200, self.snapshot()
            if path.startswith("/") and path[1:] in self.book.STALE_AFTER:
                return 200, self.section(path[1:])
            raise AgentError(404, "not found")

        if method == "POST" and path == "/orders":
            return 200, self.place_order(body)

        if method == "PATCH":
            match = _ORDER_ID.match(path)
            if match:
                return 200, self.modify_order(match.group(1), body)

        if method == "DELETE":
            match = _ORDER_ID.match(path)
            if match:
                return 200, self.cancel_order(match.group(1))

        if method == "POST":
            match = _POSITION_EXIT.match(path)
            if match:
                return 200, self.exit_position(match.group(1))

        raise AgentError(404, "not found")


def make_handler(agent: Agent, token: str) -> Callable[..., BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "FyersAgent/1.0"
        protocol_version = "HTTP/1.1"

        # ── plumbing ────────────────────────────────────────────────────────
        def log_message(self, fmt: str, *args: Any) -> None:
            LOG.debug("%s - %s", self.address_string(), fmt % args)

        def _send(self, status: int, payload: Any) -> None:
            data = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authorised(self) -> bool:
            header = self.headers.get("Authorization") or ""
            prefix = "Bearer "
            if not header.startswith(prefix):
                return False
            return hmac.compare_digest(header[len(prefix):], token)

        def _body(self) -> Optional[Dict[str, Any]]:
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                raise AgentError(400, "bad Content-Length")
            if length <= 0:
                return {}
            if length > MAX_BODY_BYTES:
                raise AgentError(413, "body too large")
            raw = self.rfile.read(length)
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise AgentError(400, "body must be JSON")
            if not isinstance(parsed, dict):
                raise AgentError(400, "body must be a JSON object")
            return parsed

        def _handle(self, method: str) -> None:
            if not self._authorised():
                self._send(401, {"error": "unauthorised"})
                return
            try:
                body = self._body() if method in ("POST", "PATCH", "PUT") else None
                status, payload = agent.dispatch(method, self.path.split("?")[0], body)
                self._send(status, payload)
            except AgentError as exc:
                self._send(exc.status, {"error": exc.message})
            except Exception as exc:  # a broker failure must not kill the agent
                LOG.exception("%s: %s %s failed", agent.user, method, self.path)
                self._send(502, {"error": str(exc)})

        def do_GET(self) -> None:      # noqa: N802 - stdlib naming
            self._handle("GET")

        def do_POST(self) -> None:     # noqa: N802
            self._handle("POST")

        def do_PATCH(self) -> None:    # noqa: N802
            self._handle("PATCH")

        def do_DELETE(self) -> None:   # noqa: N802
            self._handle("DELETE")

    return Handler


def serve(agent: Agent, *, host: str = "127.0.0.1", port: int = 9101, token: str) -> ThreadingHTTPServer:
    if not token:
        raise ValueError("an agent token is required: anything on this host could place orders")
    httpd = ThreadingHTTPServer((host, port), make_handler(agent, token))
    httpd.daemon_threads = True
    return httpd
