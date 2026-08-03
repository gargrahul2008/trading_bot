#!/usr/bin/env python3
"""
Broker portfolio snapshot for ONE account — holdings + open positions + funds, with
mark-to-market UNREALIZED P&L. This is the "what I hold and what it's worth" view (Layer 1),
independent of the bot. Writes accounts/<user>/reports/portfolio.json for the dashboard.

Uses the broker's own ltp / pl / unrealized_profit fields — no extra quote calls (429-friendly).
Read-only. MUST run under the account env so calls egress the whitelisted IP:

    env $(grep -v '^#' accounts/rahul/account.env | xargs) \
        python scripts/fetch_broker_portfolio.py --account rahul --user-key user1
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient


def _f(x: Any) -> float:
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--account", required=True)
    ap.add_argument("--user-key", required=True)
    ap.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    args = ap.parse_args()

    cid, tok = get_fyers_creds_from_json(args.auth_file, user_key=args.user_key)
    broker = FyersClient(client_id=cid, access_token=tok)
    errors: dict = {}

    holdings = []
    try:
        for h in broker.holdings():
            raw = h.raw or {}
            qty = _f(raw.get("remainingQuantity") or raw.get("quantity") or h.remaining_qty)
            cost = _f(raw.get("costPrice") or h.cost_price)
            ltp = _f(raw.get("ltp"))
            mv = _f(raw.get("marketVal")) or (ltp * qty)
            un = _f(raw.get("pl")) if raw.get("pl") is not None else (ltp - cost) * qty
            holdings.append({"symbol": h.symbol, "qty": qty, "avg_cost": round(cost, 2),
                             "ltp": round(ltp, 2), "market_value": round(mv, 2),
                             "unrealized": round(un, 2), "type": raw.get("holdingType") or h.holding_type})
    except Exception as e:
        errors["holdings"] = str(e)

    positions = []
    try:
        for p in broker.positions():
            raw = p.raw or {}
            netqty = _f(raw.get("netQty") or p.net_qty)
            if netqty == 0:
                continue  # closed intraday leg
            avg = _f(raw.get("netAvg") or raw.get("avgPrice") or p.avg_price)
            ltp = _f(raw.get("ltp"))
            un = (_f(raw.get("unrealized_profit")) if raw.get("unrealized_profit") is not None
                  else _f(raw.get("pl")) if raw.get("pl") is not None else (ltp - avg) * netqty)
            positions.append({"symbol": p.symbol, "net_qty": netqty, "avg": round(avg, 2),
                              "ltp": round(ltp, 2), "market_value": round(ltp * netqty, 2),
                              "unrealized": round(un, 2),
                              "realized_day": round(_f(raw.get("realized_profit")), 2),
                              "product": raw.get("productType") or ""})
    except Exception as e:
        errors["positions"] = str(e)

    funds = {}
    try:
        funds = {k: round(float(v), 2) for k, v in broker.funds_detail().items()}
    except Exception as e:
        errors["funds"] = str(e)

    h_val = round(sum(x["market_value"] for x in holdings), 2)
    h_un = round(sum(x["unrealized"] for x in holdings), 2)
    p_un = round(sum(x["unrealized"] for x in positions), 2)

    out = {
        "account": args.account,
        "user_key": args.user_key,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "funds": funds,
        "holdings": holdings,
        "positions": positions,
        "totals": {
            "holdings_value": h_val,
            "holdings_unrealized": h_un,
            "positions_unrealized": p_un,
            "unrealized_total": round(h_un + p_un, 2),
        },
        "errors": errors,
    }
    reports = REPO / "accounts" / args.account / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "portfolio.json").write_text(json.dumps(out, indent=2) + "\n")
    print(f"[fetch_broker_portfolio] {args.account}: {len(holdings)} holdings, "
          f"{len(positions)} positions, unrealized_total={out['totals']['unrealized_total']}, "
          f"available={funds.get('available')}{'  errors:' + str(list(errors)) if errors else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
