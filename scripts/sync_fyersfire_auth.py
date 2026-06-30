#!/usr/bin/env python3
"""
sync_fyersfire_auth.py — push this repo's fresh Fyers token into FyersFire's config.

FyersFire (the C# fast-order app at /root/trade/FyersFire) authenticates with a
`clientId:accessToken` string in fyers.json that expires daily. Instead of pasting
it by hand, this reads the repo's fyers_auth.json (user1 on this VPS) and writes the
current credential into fyers.json — sets the global `auth` and blanks each order's
per-order `auth` so every order uses the one fresh token.

Run it right after the daily Fyers auth/refresh.

Usage:
    python scripts/sync_fyersfire_auth.py                 # sync user1 -> FyersFire
    python scripts/sync_fyersfire_auth.py --dry-run       # show what it would write (masked)
    python scripts/sync_fyersfire_auth.py --user user1 --target /root/trade/FyersFire/fyers.json
"""
import sys, os, json, base64, argparse, time
sys.path.insert(0, "/root/trading_bot")
from common.broker.auth_json import get_fyers_creds_from_json

AUTH_FILE = "/root/trading_bot/fyers_auth.json"
TARGET    = "/root/trade/FyersFire/fyers.json"


def jwt_exp(token: str):
    """Return (exp_epoch, seconds_left) from a Fyers JWT access token, or (None, None)."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)            # pad base64
        data = json.loads(base64.urlsafe_b64decode(payload))
        exp = int(data.get("exp"))
        return exp, exp - int(time.time())
    except Exception:
        return None, None


def mask(auth: str) -> str:
    app, _, tok = auth.partition(":")
    return f"{app}:...{tok[-6:]}" if tok else app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default="user1", help="user key in fyers_auth.json")
    ap.add_argument("--auth-file", default=AUTH_FILE)
    ap.add_argument("--target", default=TARGET, help="FyersFire fyers.json path")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    client_id, access_token = get_fyers_creds_from_json(args.auth_file, user_key=args.user)
    if not client_id or not access_token:
        sys.exit(f"ERROR: {args.user} has empty client_id/access_token in {args.auth_file}")
    auth = f"{client_id}:{access_token}"

    # safety: warn (don't fail) if the token is already expired / near expiry
    exp, left = jwt_exp(access_token)
    if left is not None:
        if left <= 0:
            print(f"WARNING: {args.user} token EXPIRED {-left//60} min ago — refresh auth first!")
        else:
            print(f"Token valid for ~{left//3600}h {left%3600//60}m (user={args.user}, {mask(auth)})")

    if not os.path.exists(args.target):
        sys.exit(f"ERROR: FyersFire config not found: {args.target}")
    cfg = json.load(open(args.target))

    n_orders = len(cfg.get("orders") or [])
    if args.dry_run:
        print(f"[DRY RUN] would set global auth -> {mask(auth)}")
        print(f"[DRY RUN] would blank per-order auth on {n_orders} order(s) (they fall back to global)")
        return

    # backup, then write
    bak = args.target + ".bak"
    json.dump(cfg, open(bak, "w"), indent=2)
    cfg["auth"] = auth
    for o in cfg.get("orders") or []:
        if "auth" in o:
            o["auth"] = ""        # use the global (user1) auth
    json.dump(cfg, open(args.target, "w"), indent=2)
    print(f"OK: synced {args.user} -> {args.target}  (global auth set, {n_orders} order auth(s) blanked)")
    print(f"   backup: {bak}")


if __name__ == "__main__":
    main()
