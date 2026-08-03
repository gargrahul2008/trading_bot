#!/usr/bin/env python3
"""
Broker P&L reconciliation for ONE account — computes BOTH a broker-truth realized P&L and the
bot's own realized, side by side, so the dashboard surfaces any discrepancy.

Why not just fetch it: the Fyers API has NO realized-P&L or charges endpoint (only tradebook /
positions / holdings / funds). So we:
  1. fetch the day's tradebook (broker truth of executions),
  2. accumulate it into a persistent per-account store (dedup by tradeNumber) — the API is
     today-only, so we build history forward from first run,
  3. seed the broker-side ledger from the bot's current lots ONCE (first run), then replay the
     accumulated trades through the bot's exact LIFO + sell-first("borrowed") accounting →
     broker_realized. Seeded + same method, so it EQUALS the bot until a real discrepancy
     (a missed/extra/mispriced trade) makes them diverge,
  4. estimate charges from the trades (intraday vs delivery by same-day qty matching — see
     common/reporting/charges.py),
  5. read the bot's own realized from state.json and write both + the discrepancy.

MUST run under the account's env (account.env) so Fyers calls egress the whitelisted IP:
    env $(grep -v '^#' accounts/rahul/account.env | xargs) \
        python scripts/fetch_broker_pnl.py --account rahul --user-key user1
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient
from common.reporting.charges import compute_charges

D0 = Decimal("0")


def _D(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _today() -> str:
    return dt.date.today().isoformat()


def _parse_dtm(s: str) -> str:
    """'31-Jul-2026 12:54:17' -> sortable ISO. Unknown -> ''."""
    try:
        return dt.datetime.strptime(str(s).strip(), "%d-%b-%Y %H:%M:%S").isoformat()
    except Exception:
        return ""


def _norm_trade(r: Dict[str, Any]) -> Dict[str, Any]:
    dtm = str(r.get("orderDateTime") or "")
    ts_iso = _parse_dtm(dtm)
    date = ts_iso.split("T")[0] if ts_iso else ""
    return {
        "trade_id": str(r.get("tradeNumber") or ""),
        "order_id": str(r.get("orderNumber") or ""),
        "symbol": str(r.get("symbol") or ""),
        "side": "BUY" if r.get("side") in (1, "1") else "SELL",
        "qty": float(r.get("tradedQty") or 0),
        "price": float(r.get("tradePrice") or 0),
        "value": float(r.get("tradeValue") or 0),
        "date": date,
        "ts_iso": ts_iso,
        "exchange": str(r.get("exchange") or ""),
    }


def _tb_rows(resp: Any) -> List[dict]:
    if isinstance(resp, dict):
        for k in ("tradeBook", "trades", "data"):
            v = resp.get(k)
            if isinstance(v, list):
                return v
    return resp if isinstance(resp, list) else []


def accumulate(store: Path, new_trades: List[dict]) -> tuple[List[dict], int]:
    """Append new trades to the persistent store, dedup by trade_id, keep chronological order."""
    seen: Dict[str, dict] = {}
    if store.exists():
        for line in store.read_text().splitlines():
            line = line.strip()
            if line:
                t = json.loads(line)
                seen[t["trade_id"]] = t
    added = 0
    for t in new_trades:
        if t["trade_id"] and t["trade_id"] not in seen:
            seen[t["trade_id"]] = t
            added += 1
    trades = sorted(seen.values(), key=lambda x: (x.get("ts_iso") or "", x.get("trade_id")))
    store.write_text("\n".join(json.dumps(t) for t in trades) + ("\n" if trades else ""))
    return trades, added


def collect_bot_states(account: str) -> Dict[str, Dict[str, Any]]:
    """Per-symbol lots/borrowed/realized from every run's state.json under the account."""
    out: Dict[str, Dict[str, Any]] = {}
    for sf in sorted((REPO / "accounts" / account).glob("*/state/state.json")):
        try:
            d = json.loads(sf.read_text())
        except Exception:
            continue
        for sym, ss in (d.get("symbol_states") or {}).items():
            out[sym] = {
                "lots": ss.get("lots") or [],
                "borrowed_qty": ss.get("borrowed_qty") or 0,
                "borrowed_avg_sell": ss.get("borrowed_avg_sell") or 0,
                "realized_pnl": ss.get("realized_pnl") or 0,
                "run": sf.parent.parent.name,
            }
    return out


def load_or_create_seed(seed_path: Path, bot_states: Dict[str, Dict[str, Any]],
                        seed_date: str) -> Dict[str, Any]:
    """First run: snapshot the bot's lots/borrowed/realized as the broker-ledger starting basis.
    Trades on/before seed_date are already reflected in that realized, so the replay skips them."""
    if seed_path.exists():
        return json.loads(seed_path.read_text())
    seed = {"seed_date": seed_date, "by_symbol": {}}
    for sym, s in bot_states.items():
        seed["by_symbol"][sym] = {
            "lots": [{"qty": float(_D(l.get("qty"))), "price": float(_D(l.get("price") or l.get("avg_price")))}
                     for l in s["lots"] if isinstance(l, dict)],
            "borrowed_qty": float(_D(s["borrowed_qty"])),
            "borrowed_avg_sell": float(_D(s["borrowed_avg_sell"])),
            "realized_start": float(_D(s["realized_pnl"])),
        }
    seed_path.write_text(json.dumps(seed, indent=2) + "\n")
    return seed


def replay_realized(seed: Dict[str, Any], trades: List[dict]) -> Dict[str, Decimal]:
    """Replay accumulated broker trades through the bot's exact accounting (LIFO lots + sell-first
    'borrowed' buffer, mirroring GenericRunner._apply_fill) → absolute realized per symbol.
    Skips trades on/before seed_date (already inside the seeded realized)."""
    seed_date = seed.get("seed_date") or ""
    led: Dict[str, Dict[str, Any]] = {}
    for sym, s in seed.get("by_symbol", {}).items():
        led[sym] = {
            "lots": [[_D(l["qty"]), _D(l["price"])] for l in s["lots"]],
            "borrowed_qty": _D(s["borrowed_qty"]),
            "borrowed_avg_sell": _D(s["borrowed_avg_sell"]),
            "realized": _D(s["realized_start"]),
        }
    for t in trades:
        if (t.get("date") or "") <= seed_date:
            continue
        sym = t["symbol"]
        L = led.setdefault(sym, {"lots": [], "borrowed_qty": D0, "borrowed_avg_sell": D0, "realized": D0})
        qty, price = _D(t["qty"]), _D(t["price"])
        if qty <= 0:
            continue
        if t["side"] == "SELL":
            traded_qty = sum((lq for lq, _ in L["lots"]), D0)
            sell_from_traded = min(qty, traded_qty)
            remaining = sell_from_traded
            while remaining > 0 and L["lots"]:               # LIFO consume from the end
                lq, lp = L["lots"][-1]
                take = remaining if remaining < lq else lq
                L["realized"] += take * (price - lp)
                lq -= take; remaining -= take
                if lq <= 0:
                    L["lots"].pop()
                else:
                    L["lots"][-1] = [lq, lp]
            sell_from_borrow = qty - sell_from_traded
            if sell_from_borrow > 0:
                ob = L["borrowed_qty"]; nb = ob + sell_from_borrow
                if nb > 0:
                    L["borrowed_avg_sell"] = ((L["borrowed_avg_sell"] * ob) + price * sell_from_borrow) / nb
                L["borrowed_qty"] = nb
        else:                                                 # BUY
            cover = min(qty, L["borrowed_qty"])
            if cover > 0:
                L["realized"] += cover * (L["borrowed_avg_sell"] - price)
                L["borrowed_qty"] -= cover
            remaining = qty - cover
            if remaining > 0:
                L["lots"].append([remaining, price])
    return {sym: L["realized"] for sym, L in led.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--account", required=True)
    ap.add_argument("--user-key", required=True)
    ap.add_argument("--auth-file", default=str(REPO / "fyers_auth.json"))
    args = ap.parse_args()

    reports = REPO / "accounts" / args.account / "reports"
    reports.mkdir(parents=True, exist_ok=True)

    cid, tok = get_fyers_creds_from_json(args.auth_file, user_key=args.user_key)
    broker = FyersClient(client_id=cid, access_token=tok)

    errors: Dict[str, str] = {}
    try:
        raw_tb = broker.tradebook()
    except Exception as e:
        raw_tb = {}
        errors["tradebook"] = str(e)

    new_trades = [_norm_trade(r) for r in _tb_rows(raw_tb)]
    all_trades, added = accumulate(reports / "trades_all.jsonl", new_trades)

    bot_states = collect_bot_states(args.account)
    seed = load_or_create_seed(reports / "pnl_seed.json", bot_states, _today())
    broker_realized = replay_realized(seed, all_trades)
    charges = compute_charges(all_trades)

    # Per-symbol comparison.
    syms = set(broker_realized) | set(bot_states) | set(charges["by_symbol"])
    by_symbol: Dict[str, Any] = {}
    for sym in sorted(syms):
        br = float(broker_realized.get(sym, D0))
        bot = float(_D(bot_states.get(sym, {}).get("realized_pnl", 0)))
        chg = float(charges["by_symbol"].get(sym, {}).get("total_charges", 0.0))
        by_symbol[sym] = {
            "broker_realized": round(br, 2),
            "bot_realized": round(bot, 2),
            "discrepancy": round(br - bot, 2),
            "charges": round(chg, 2),
            "broker_net": round(br - chg, 2),
        }

    out = {
        "account": args.account,
        "user_key": args.user_key,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "seed_date": seed.get("seed_date"),
        "trades_new": added,
        "trades_total": len(all_trades),
        "by_symbol": by_symbol,
        "totals": {
            "broker_realized": round(sum(v["broker_realized"] for v in by_symbol.values()), 2),
            "bot_realized": round(sum(v["bot_realized"] for v in by_symbol.values()), 2),
            "discrepancy": round(sum(v["discrepancy"] for v in by_symbol.values()), 2),
            "charges": round(charges["totals"].get("total_charges", 0.0), 2),
            "broker_net": round(sum(v["broker_net"] for v in by_symbol.values()), 2),
        },
        "charges_detail": charges,
        "errors": errors,
        "raw": {"tradebook": raw_tb},
    }
    (reports / "broker_pnl.json").write_text(json.dumps(out, indent=2) + "\n")
    print(f"[fetch_broker_pnl] {args.account}: +{added} trades (total {len(all_trades)}), "
          f"broker_realized={out['totals']['broker_realized']} bot_realized={out['totals']['bot_realized']} "
          f"discrepancy={out['totals']['discrepancy']} charges={out['totals']['charges']}"
          f"{'  errors:' + str(list(errors)) if errors else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
