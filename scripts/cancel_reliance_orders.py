#!/usr/bin/env python
"""
cancel_reliance_orders.py — emergency: cancel all OPEN Reliance orders.

Used to "freeze and hold" the Reliance MTF grid position (e.g. ahead of an
expected gap-up). The bot MUST be stopped first, otherwise it re-places
cancelled orders within ~5s. The wrapper reliance_hold_stop.sh does that.

Usage:
    python scripts/cancel_reliance_orders.py            # DRY RUN: list only, cancels nothing
    python scripts/cancel_reliance_orders.py --execute  # actually cancel
"""
import sys, os, argparse
sys.path.insert(0, "/root/trading_bot")
os.chdir("/root/trading_bot")

from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient

AUTH_FILE = "fyers_auth.json"
USER_KEY  = "user1"
SYMBOL    = "NSE:RELIANCE-EQ"

# Fyers integer statuses: 1=Cancelled, 2=Filled, 4=Transit, 5=Rejected, 6=Pending
# Cancellable = pending/transit; string fallbacks included for safety.
PENDING_INT = {6, 4}
PENDING_STR = {"PENDING", "OPEN", "TRANSIT", "TRIGGER PENDING"}


def is_open(o):
    st = o.get("status")
    if isinstance(st, int):
        return st in PENDING_INT
    return str(st or "").upper() in PENDING_STR


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="actually cancel (default: dry run)")
    args = ap.parse_args()

    client_id, access_token = get_fyers_creds_from_json(AUTH_FILE, user_key=USER_KEY)
    broker = FyersClient(client_id, access_token)

    ob = broker.orderbook()
    orders = ob.get("orderBook") or ob.get("data") or ob.get("orders") or []
    if isinstance(orders, dict):
        orders = list(orders.values())

    rel = [o for o in orders if isinstance(o, dict)
           and str(o.get("symbol") or o.get("tradingSymbol") or "") == SYMBOL]
    open_orders = [o for o in rel if is_open(o)]

    print(f"Reliance orders in book: {len(rel)} total, {len(open_orders)} open/cancellable")
    for o in open_orders:
        oid  = str(o.get("id") or o.get("order_id") or "")
        side = o.get("side")
        side_s = "BUY" if side in (1, "1", "BUY") else "SELL" if side in (-1, "-1", "SELL") else str(side)
        qty  = o.get("qty") or o.get("quantity")
        px   = o.get("limitPrice") or o.get("price") or o.get("limit_price")
        print(f"  [{side_s}] id={oid} qty={qty} px={px} status={o.get('status')}")

    if not args.execute:
        print("\nDRY RUN — nothing cancelled. Re-run with --execute to cancel.")
        return

    ok, fail = 0, 0
    for o in open_orders:
        oid = str(o.get("id") or o.get("order_id") or "")
        try:
            broker.cancel_order(oid, symbol=SYMBOL)
            print(f"  CANCELLED id={oid}")
            ok += 1
        except Exception as ex:
            print(f"  FAILED   id={oid}: {ex}")
            fail += 1

    # Re-verify
    ob2 = broker.orderbook()
    orders2 = ob2.get("orderBook") or ob2.get("data") or ob2.get("orders") or []
    if isinstance(orders2, dict):
        orders2 = list(orders2.values())
    still_open = [o for o in orders2 if isinstance(o, dict)
                  and str(o.get("symbol") or o.get("tradingSymbol") or "") == SYMBOL and is_open(o)]
    print(f"\nDone: cancelled={ok} failed={fail}. Reliance open orders remaining: {len(still_open)}")
    if still_open:
        print("  WARNING: some orders still open — check Fyers manually.")
        sys.exit(1)


if __name__ == "__main__":
    main()
