#!/usr/bin/env python3
"""
IP-bound broker-P&L fetch for ONE account. Pulls broker-truth data (tradebook, realized
profit, charges) from Fyers and caches it to accounts/<account>/reports/broker_pnl.json for
the read-only dashboard to aggregate.

MUST run under the account's environment (account.env) so its Fyers calls egress through the
account's whitelisted IP:

    env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
        python scripts/fetch_broker_pnl.py --account pratibha --user-key user2

Typically installed as a per-account systemd timer (e.g. after market close). The dashboard
never calls Fyers itself — it only reads the JSON this writes.

Responses are stored RAW (nothing lost) plus a best-effort normalized `by_symbol` /
`totals` layer; calibrate the normalization to the real response fields once seen on the host.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient


def _today() -> str:
    return dt.date.today().isoformat()


def _num(x: Any) -> float:
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return 0.0


def _find(d: Dict[str, Any], *keys: str) -> Any:
    """Return the first present key (case-insensitive) from a dict."""
    if not isinstance(d, dict):
        return None
    low = {str(k).lower(): v for k, v in d.items()}
    for k in keys:
        if k.lower() in low:
            return low[k.lower()]
    return None


def _rows(resp: Any) -> list:
    """Pull the list of rows out of a Fyers response envelope, tolerating field names."""
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []
    for k in ("tradeBook", "trades", "data", "realized_pnl", "orders", "result", "d"):
        v = resp.get(k)
        if isinstance(v, list):
            return v
    return []


def normalize(tradebook: dict, realised: dict, charges: dict) -> Dict[str, Any]:
    """Best-effort per-symbol realized + account-level charges. Defensive: unknown shapes
    degrade to zeros rather than crashing, and raw responses are always retained."""
    by_symbol: Dict[str, Dict[str, float]] = {}

    # Broker realized P&L per symbol.
    for r in _rows(realised):
        sym = str(_find(r, "symbol", "tradingSymbol", "sym") or "").strip()
        if not sym:
            continue
        pnl = _num(_find(r, "realized_pnl", "realizedPnl", "pl", "profit", "realized"))
        by_symbol.setdefault(sym, {})["broker_realized"] = pnl

    # Turnover per symbol from tradebook (for apportioning account-level charges).
    turnover: Dict[str, float] = {}
    for t in _rows(tradebook):
        sym = str(_find(t, "symbol", "tradingSymbol") or "").strip()
        if not sym:
            continue
        val = _num(_find(t, "tradeValue", "orderValue", "value"))
        if val <= 0:
            val = _num(_find(t, "tradedPrice", "price")) * _num(_find(t, "tradedQty", "qty", "quantity"))
        turnover[sym] = turnover.get(sym, 0.0) + val

    # Total charges (charges_history is usually account/segment level, not per-symbol).
    total_charges = 0.0
    for c in _rows(charges):
        total_charges += _num(
            _find(c, "total_charges", "totalCharges", "charges", "amount", "value")
        )

    # Apportion charges by turnover so each strategy/symbol gets a net figure.
    tot_turn = sum(turnover.values()) or 0.0
    for sym in set(by_symbol.keys()) | set(turnover.keys()):
        share = (turnover.get(sym, 0.0) / tot_turn) if tot_turn > 0 else 0.0
        chg = total_charges * share
        row = by_symbol.setdefault(sym, {})
        row["turnover"] = turnover.get(sym, 0.0)
        row["charges_apportioned"] = chg
        row["net_realized"] = row.get("broker_realized", 0.0) - chg

    return {
        "by_symbol": by_symbol,
        "totals": {
            "broker_realized": sum(v.get("broker_realized", 0.0) for v in by_symbol.values()),
            "total_charges": total_charges,
            "net_realized": sum(v.get("net_realized", 0.0) for v in by_symbol.values()),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--account", required=True, help="Account folder name, e.g. pratibha")
    ap.add_argument("--user-key", required=True, help="user_key in fyers_auth.json")
    ap.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    ap.add_argument("--from-date", default=_today())
    ap.add_argument("--to-date", default=_today())
    ap.add_argument("--segment-type", default="0", help="0=all,1=equity (see SDK)")
    ap.add_argument("--exchange-type", default="0", help="0=all,1=NSE,2=BSE (see SDK)")
    args = ap.parse_args()

    client_id, access_token = get_fyers_creds_from_json(args.auth_file, user_key=args.user_key)
    broker = FyersClient(client_id=client_id, access_token=access_token)

    date_range = {
        "from_date": args.from_date,
        "to_date": args.to_date,
        "segment_type": args.segment_type,
        "exchange_type": args.exchange_type,
    }

    out: Dict[str, Any] = {
        "account": args.account,
        "user_key": args.user_key,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "raw": {},
        "errors": {},
    }
    # Each call isolated so one failure doesn't lose the others.
    for name, fn in (
        ("tradebook", lambda: broker.tradebook()),
        ("realised_profit", lambda: broker.realised_profit_history(dict(date_range))),
        ("charges", lambda: broker.charges_history(dict(date_range))),
    ):
        try:
            out["raw"][name] = fn()
        except Exception as e:  # keep partial results
            out["errors"][name] = str(e)
            out["raw"][name] = {}

    out["normalized"] = normalize(
        out["raw"].get("tradebook", {}),
        out["raw"].get("realised_profit", {}),
        out["raw"].get("charges", {}),
    )

    out_dir = REPO / "accounts" / args.account / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "broker_pnl.json"
    out_file.write_text(json.dumps(out, indent=2) + "\n")
    print(f"[fetch_broker_pnl] wrote {out_file.relative_to(REPO)}"
          f"  (errors: {list(out['errors']) or 'none'})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
