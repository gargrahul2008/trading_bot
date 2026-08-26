#!/usr/bin/env python3
"""
mexc_portfolio_report.py — transparent per-bucket portfolio message for Telegram.

Every send shows, for EACH bucket, exactly how much ETH and cash it holds, the
off-bot HODL, and then RECONCILES the sum against the LIVE MEXC balance so the
recipient can see our books match the exchange. No derived baselines — the ETH
and cash shown are the actual tracked holdings, checked against the exchange at
send time.

    env/bin/python scripts/mexc_portfolio_report.py            # dry-run (print only)
    env/bin/python scripts/mexc_portfolio_report.py --send     # send to Telegram

See docs/mexc_pnl_model.md for the accounting model.
"""
import sys, os, json, glob, argparse, urllib.request, urllib.parse
sys.path.insert(0, "/root/trading_bot")
from common.broker.mexc_spot_client import MexcSpotClient

STATE_DIR = "/root/trading_bot/strategies/pct_ladder/state"
SECRETS   = "/root/trading_bot/strategies/pct_ladder/secrets/mexc_spot.json"
TG_SECRET = "/root/trading_bot/strategies/pct_ladder/secrets/telegram.json"

# Off-bot HODL held outside all buckets (see docs/mexc_pnl_model.md).
HODL_ETH  = 17.709

# bucket -> human label
LABEL = {"bucket1": "3% grid", "bucket2": "10% wide", "bucket3": "2% tight"}
BUCKETS = ["bucket1", "bucket2", "bucket3"]

TOL_ETH = 0.02   # ETH reconciliation tolerance
TOL_USD = 5.0    # USDC reconciliation tolerance ($)


def _state(b):
    f = sorted(glob.glob(f"{STATE_DIR}/{b}/state_*_v1.json"))
    return json.load(open(f[-1])) if f else None


def build_message() -> str:
    s = json.load(open(SECRETS))
    cl = MexcSpotClient(api_key=s.get("api_key") or s.get("apiKey"),
                        api_secret=s.get("api_secret") or s.get("apiSecret"))
    bal = cl.balances()
    px  = float(cl.get_ltps(["ETHUSDC"])["ETHUSDC"])
    acct_eth  = float(bal["ETH"]["free"])  + float(bal["ETH"]["locked"])
    acct_usdc = float(bal["USDC"]["free"]) + float(bal["USDC"]["locked"])

    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    ts  = now.strftime("%d %b %H:%M UTC")

    rows = []          # (label, eth, cash)
    sum_eth = sum_cash = 0.0
    for b in BUCKETS:
        d = _state(b)
        if not d:
            continue
        ss = d["symbol_states"]["ETHUSDC"]
        eth = float(ss["traded_qty"]); cash = float(d["cash"])
        rows.append((f"{b[-1]} ({LABEL[b]})", eth, cash))
        sum_eth += eth; sum_cash += cash
    rows.append(("HODL (off-bot)", HODL_ETH, 0.0))
    sum_eth += HODL_ETH

    # Reconciliation vs live exchange
    d_eth = sum_eth - acct_eth
    d_usd = sum_cash - acct_usdc
    eth_ok = abs(d_eth) < TOL_ETH
    usd_ok = abs(d_usd) < TOL_USD
    pv = acct_eth * px + acct_usdc

    L = []
    L.append(f"*MEXC Portfolio* — {ts}   @ ${px:,.2f}")
    L.append("")
    L.append("*Per bucket (ETH / cash):*")
    L.append("```")
    L.append(f"{'Bucket':16s}{'ETH':>10s}{'Cash':>12s}")
    for label, eth, cash in rows:
        L.append(f"{label:16s}{eth:>10.4f}{cash:>12,.0f}")
    L.append(f"{'-'*38}")
    L.append(f"{'Total (books)':16s}{sum_eth:>10.4f}{sum_cash:>12,.0f}")
    L.append(f"{'Live on MEXC':16s}{acct_eth:>10.4f}{acct_usdc:>12,.0f}")
    L.append("```")
    # Reconciliation verdict
    if eth_ok and usd_ok:
        L.append("Reconciled with exchange: ✅ ETH  ✅ USDC")
    else:
        eth_tag = "✅" if eth_ok else f"⚠️ Δ{d_eth:+.4f}"
        usd_tag = "✅" if usd_ok else f"⚠️ Δ${d_usd:+,.2f}"
        L.append(f"Reconciled with exchange: {eth_tag} ETH  {usd_tag} USDC")
    L.append(f"*Portfolio value: ${pv:,.0f}*")
    return "\n".join(L)


def send(text: str):
    s = json.load(open(TG_SECRET))
    token = s["bot_token"]; chats = s["chat_id"]
    if isinstance(chats, str): chats = [chats]
    for c in chats:
        data = urllib.parse.urlencode({"chat_id": c, "text": text[:4000],
                                       "parse_mode": "Markdown"}).encode()
        try:
            urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, method="POST"), timeout=15)
        except Exception as e:
            print("telegram failed:", e, file=sys.stderr)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true", help="send to Telegram (default: print only)")
    args = ap.parse_args()
    msg = build_message()
    print(msg)
    if args.send:
        send(msg)
        print("\n[sent to Telegram]")
