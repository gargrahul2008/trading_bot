#!/usr/bin/env python3
"""
mexc_audit.py — one-shot reconciliation + PnL audit for the MEXC buckets.

Run this ANY TIME to confirm the whole account is consistent — no need to
eyeball state files. It checks, against the LIVE MEXC account:

  1. ETH reconciliation : Σ bucket.traded_qty + HODL  ==  live account ETH
  2. USDC reconciliation: Σ bucket.cash               ==  live account USDC
  3. PnL-baseline invariant (per bucket):
        portfolio_start_value  ==  cash + lot_cost - realized_pnl
     This is the invariant that guarantees the reported Net (= PV - start_value)
     equals realized + unrealized. It is CONSERVED by grid trades, so once true
     it stays true. If it breaks, a manual state edit didn't update the baseline.
  4. Lot integrity: Σ lot_qty == traded_qty (LIFO lots drive traded_qty).
  5. Account PnL total = Σ (realized + unrealized) over buckets + HODL.

Exit code 0 = all green, 1 = at least one FAIL.

    env/bin/python scripts/mexc_audit.py

See docs/mexc_pnl_model.md for the full model.
"""
import sys, os, json, glob
sys.path.insert(0, "/root/trading_bot")
from common.broker.mexc_spot_client import MexcSpotClient

# ── Off-bot HODL (17.709 ETH held outside all buckets). Set its real cost basis. ──
HODL_ETH        = 17.709
HODL_COST_PRICE = None   # <-- SET the avg USD cost of the HODL ETH (None = skip HODL PnL)

BUCKETS = ["bucket1", "bucket2", "bucket3"]
STATE_DIR = "/root/trading_bot/strategies/pct_ladder/state"
SECRETS   = "/root/trading_bot/strategies/pct_ladder/secrets/mexc_spot.json"
TOL_ETH   = 0.02      # ETH reconciliation tolerance
TOL_USD   = 5.0       # USDC / baseline tolerance ($)


def _state(b):
    f = sorted(glob.glob(f"{STATE_DIR}/{b}/state_*_v1.json"))
    return json.load(open(f[-1])) if f else None


def main():
    s = json.load(open(SECRETS))
    cl = MexcSpotClient(api_key=s.get("api_key") or s.get("apiKey"),
                        api_secret=s.get("api_secret") or s.get("apiSecret"))
    bal = cl.balances()
    px  = float(cl.get_ltps(["ETHUSDC"])["ETHUSDC"])
    acct_eth  = float(bal["ETH"]["free"])  + float(bal["ETH"]["locked"])
    acct_usdc = float(bal["USDC"]["free"]) + float(bal["USDC"]["locked"])

    fails = []
    print(f"=== MEXC AUDIT @ ETH ${px:,.2f} ===\n")
    sum_qty = sum_cash = sum_realized = sum_unreal = sum_start = 0.0

    hdr = f"{'bucket':9s} {'ETH':>10s} {'cash':>11s} {'realized':>10s} {'unreal':>10s} {'start_val':>11s} {'invariant':>18s}"
    print(hdr)
    for b in BUCKETS:
        d = _state(b)
        if not d:
            print(f"{b:9s}  (no state file)"); fails.append(f"{b}: no state"); continue
        ss = d["symbol_states"]["ETHUSDC"]; ex = d.get("extras", {})
        qty = float(ss["traded_qty"]); cash = float(d["cash"]); realized = float(ss.get("realized_pnl") or 0)
        lots = ss.get("lots", []); lot_qty = sum(float(l["qty"]) for l in lots)
        lot_cost = sum(float(l["qty"]) * float(l["price"]) for l in lots)
        start = float(ex.get("portfolio_start_value") or 0)
        unreal = qty * px - lot_cost
        want_start = cash + lot_cost - realized
        inv_ok = abs(start - want_start) < TOL_USD
        lot_ok = abs(lot_qty - qty) < 1e-4
        tag = "OK" if (inv_ok and lot_ok) else ("BASELINE OFF" if not inv_ok else "LOT MISMATCH")
        if not inv_ok: fails.append(f"{b}: start_value {start:.0f} != cash+lotcost-realized {want_start:.0f} (Δ{start-want_start:+.0f})")
        if not lot_ok: fails.append(f"{b}: lot_qty {lot_qty:.4f} != traded_qty {qty:.4f}")
        print(f"{b:9s} {qty:>10.4f} {cash:>11,.0f} {realized:>10,.1f} {unreal:>10,.1f} {start:>11,.0f} {tag:>18s}")
        sum_qty += qty; sum_cash += cash; sum_realized += realized; sum_unreal += unreal; sum_start += start

    # HODL
    hodl_unreal = None
    if HODL_COST_PRICE:
        hodl_unreal = HODL_ETH * (px - HODL_COST_PRICE)
        print(f"{'HODL':9s} {HODL_ETH:>10.4f} {0:>11,.0f} {0:>10,.1f} {hodl_unreal:>10,.1f} {HODL_ETH*HODL_COST_PRICE:>11,.0f} {'held':>18s}")
    else:
        print(f"{'HODL':9s} {HODL_ETH:>10.4f}  (cost basis not set — HODL PnL excluded)")

    # ── Reconciliation checks ──
    print("\n--- Reconciliation vs live account ---")
    tot_eth = sum_qty + HODL_ETH
    eth_ok = abs(tot_eth - acct_eth) < TOL_ETH
    usd_ok = abs(sum_cash - acct_usdc) < TOL_USD
    if not eth_ok: fails.append(f"ETH: tracked {tot_eth:.4f} != account {acct_eth:.4f}")
    if not usd_ok: fails.append(f"USDC: tracked {sum_cash:.2f} != account {acct_usdc:.2f}")
    print(f"ETH : tracked {tot_eth:.4f}  vs account {acct_eth:.4f}   [{'PASS' if eth_ok else 'FAIL'}]")
    print(f"USDC: tracked ${sum_cash:,.2f} vs account ${acct_usdc:,.2f}  [{'PASS' if usd_ok else 'FAIL'}]")

    # ── PnL totals ──
    print("\n--- PnL (realized + unrealized, cost-basis) ---")
    bucket_pnl = sum_realized + sum_unreal
    total_pnl = bucket_pnl + (hodl_unreal or 0.0)
    acct_value = acct_eth * px + acct_usdc
    print(f"Buckets: realized ${sum_realized:,.1f} + unrealized ${sum_unreal:,.1f} = ${bucket_pnl:,.1f}")
    if hodl_unreal is not None:
        print(f"HODL unrealized: ${hodl_unreal:,.1f}")
    print(f"TOTAL account PnL: ${total_pnl:,.1f}")
    print(f"Account value: ${acct_value:,.2f}  (invested baseline: ${acct_value - total_pnl:,.2f})")

    print("\n" + ("✅ ALL CHECKS PASS — no gaps." if not fails else "❌ ISSUES FOUND:"))
    for f in fails:
        print("   -", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
