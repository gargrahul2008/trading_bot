"""One read-only round-trip against the real broker, for the control host.

Fetches each section once — five requests, no loop, no server, nothing written —
and prints what came back. Run it after installing an agent to confirm the token
is valid and the calls are leaving through the right IP, before pointing the
dashboard at it.

    env $(grep -v '^#' accounts/pratibha/account.env | xargs) \\
        .venv/bin/python -m webapp.agent.smoke --user pratibha

It places no orders and cannot: it never constructs an order request.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient
from webapp.agent.attribution import Attribution
from webapp.agent.gateway import FyersGateway
from webapp.agent.session import Session


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Read-only agent smoke test")
    parser.add_argument("--user", required=True)
    parser.add_argument("--user-key", default=None)
    parser.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    parser.add_argument("--accounts-dir", default=str(REPO / "accounts"))
    args = parser.parse_args(argv)

    user_key = args.user_key or os.getenv("FYERS_USER_KEY") or ""
    if not user_key:
        print("no user key: pass --user-key or run under the account's account.env")
        return 2

    proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or ""
    session = Session()
    print("account   : %s (user_key=%s)" % (args.user, user_key))
    print("egress    : %s" % (proxy or "host IP — no proxy set"))
    print("session   : %s  intervals=%s" % (session.phase(), session.intervals()))
    print()

    client_id, access_token = get_fyers_creds_from_json(args.auth_file, user_key=user_key)
    gateway = FyersGateway(FyersClient(client_id=client_id, access_token=access_token))

    attribution = Attribution(os.path.join(args.accounts_dir, args.user))
    attribution.refresh(force=True)
    print("bot runs claiming orders: %s" % (attribution.runs() or "none"))
    print()

    failures = 0
    for name, fetch in (
        ("funds", gateway.funds),
        ("positions", gateway.positions),
        ("holdings", gateway.holdings),
        ("orders", gateway.orders),
        ("trades", gateway.trades),
    ):
        try:
            result = fetch()
        except Exception as exc:
            failures += 1
            print("%-10s FAILED  %s" % (name, exc))
            continue

        if name == "funds":
            print("%-10s available=%.2f utilised=%.2f realised=%.2f"
                  % (name, result["available"], result["utilised"], result["realised_pnl"]))
            continue

        print("%-10s %d row(s)" % (name, len(result)))
        for row in result[:5]:
            if name == "positions":
                print("    %-22s %-9s %8.0f @ %-10.2f ltp %-10.2f unreal %10.2f  [%s]"
                      % (row["symbol"], row["direction"], row["net_qty"], row["avg_price"],
                         row["ltp"], row["unrealised"], row["kind"]))
            elif name == "holdings":
                print("    %-22s %8.0f @ %-10.2f ltp %-10.2f unreal %10.2f"
                      % (row["symbol"], row["qty"], row["cost_price"], row["ltp"],
                         row["unrealised"]))
            elif name == "orders":
                label = attribution.label(row["order_id"], None)
                print("    %-12s %-22s %-4s %6.0f/%-6.0f %-9s %-10s %s"
                      % (row["order_id"], row["symbol"], row["side"], row["filled_qty"],
                         row["qty"], row["status"], row["product_type"],
                         label["run"] or label["source"]))
            else:
                print("    %-12s %-22s %-4s %6.0f @ %.2f  [%s]"
                      % (row["order_id"], row["symbol"], row["side"], row["qty"],
                         row["price"], row["kind"]))
        if len(result) > 5:
            print("    ... %d more" % (len(result) - 5))

    print()
    print("FAILED (%d section(s))" % failures if failures else "OK — all five sections read")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
