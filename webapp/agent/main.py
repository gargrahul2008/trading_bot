"""Start one account's agent.

Must run under that account's environment so its Fyers calls egress through the
account's whitelisted static IP:

    env $(grep -v '^#' accounts/pratibha/account.env | xargs) \\
        python -m webapp.agent.main --user pratibha --port 9102

The proxy is picked up from HTTPS_PROXY by `requests`, exactly as the bots do —
see docs/multi_account_architecture.md §2. Nothing here sets it, and nothing
here may serve two accounts: a second account in this process would share the
proxy environment and could leave through the wrong IP.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.fyers_client import FyersClient
from webapp.agent.attribution import Attribution
from webapp.agent.credentials import CredentialSource
from webapp.agent.book import Book
from webapp.agent.budget import DEFAULT_PER_MIN, Budget
from webapp.agent.gateway import FyersGateway
from webapp.agent.poller import Poller
from webapp.agent.server import Agent, serve
from webapp.agent.session import Session

LOG = logging.getLogger("agent")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Per-account Fyers dashboard agent")
    parser.add_argument("--user", required=True, help="account directory name, e.g. pratibha")
    parser.add_argument(
        "--user-key",
        default=None,
        help="key in fyers_auth.json; defaults to FYERS_USER_KEY from account.env",
    )
    parser.add_argument("--port", type=int, required=True, help="loopback port to listen on")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (loopback by default)")
    parser.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    parser.add_argument("--accounts-dir", default=str(REPO / "accounts"))
    parser.add_argument(
        "--per-min",
        type=float,
        default=DEFAULT_PER_MIN,
        help="requests per minute this agent may spend, leaving the rest to the bots",
    )
    parser.add_argument(
        "--allow-trading",
        action="store_true",
        help="expose place/modify/cancel/exit. Read-only without it.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_agent(args: argparse.Namespace) -> Agent:
    user_key = args.user_key or os.getenv("FYERS_USER_KEY") or ""
    if not user_key:
        raise SystemExit(
            "no user key: pass --user-key or run under the account's account.env "
            "(which sets FYERS_USER_KEY)"
        )

    proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or ""
    LOG.info(
        "%s: user_key=%s egress=%s",
        args.user, user_key, proxy or "host IP (no proxy set)",
    )

    # Read through a CredentialSource rather than once at startup: the token
    # expires daily and fyers_auto_auth rewrites the file each morning.
    credentials = CredentialSource(
        auth_file=args.auth_file,
        user_key=user_key,
        build=lambda client_id, token: FyersClient(client_id=client_id, access_token=token),
    )
    gateway = FyersGateway(credentials)

    book = Book(args.user)
    poller = Poller(
        gateway,
        book,
        budget=Budget(per_min=args.per_min),
        session=Session(),
        attribution=Attribution(os.path.join(args.accounts_dir, args.user)),
    )
    return Agent(
        user=args.user,
        book=book,
        poller=poller,
        gateway=gateway,
        allow_trading=args.allow_trading,
    )


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # common.utils.logger.setup_logger attaches its own handler to the "fyers"
    # logger, and propagation then carries every record up to the root handler
    # basicConfig just added — so each line was written twice, in two formats,
    # filling the journal ring buffer at double rate. The bots rely on that
    # handler (they configure no root logger), so silence the duplicate here
    # rather than changing shared code.
    logging.getLogger("fyers").propagate = False

    token = os.getenv("AGENT_TOKEN") or ""
    if not token:
        raise SystemExit(
            "AGENT_TOKEN is not set. The agent can place real orders, so it "
            "refuses to listen without one even on loopback."
        )

    agent = build_agent(args)
    httpd = serve(agent, host=args.host, port=args.port, token=token)

    agent.poller.start()
    LOG.info(
        "%s: listening on %s:%s (trading %s)",
        args.user, args.host, args.port, "ENABLED" if args.allow_trading else "disabled",
    )

    def shutdown(signum, _frame):
        # httpd.shutdown() blocks until serve_forever() returns, and a signal
        # handler runs on the main thread — the same thread sitting inside
        # serve_forever(). Calling it here deadlocks, systemd waits out its
        # 90s TimeoutStopSec and SIGKILLs, and every restart is recorded as a
        # failure. Hand it to another thread and return immediately.
        LOG.info("%s: signal %s — shutting down", args.user, signum)
        agent.poller.stop()
        threading.Thread(target=httpd.shutdown, name="agent-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        httpd.serve_forever()
    finally:
        agent.poller.stop()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
