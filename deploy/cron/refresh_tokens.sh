#!/usr/bin/env bash
# Daily one-shot Fyers token refresh, PER USER and IP-bound. Cron: 08:30 IST (03:00 UTC),
# 25 min before the 08:55 bot start so the bots load a fresh token.
#
# Replaces the always-on fyers-auth-* systemd units and the legacy `--enabled-only` cron.
# Per-user (never --enabled-only) so each account's login exits from ITS whitelisted IP:
#   user1 (Rahul)    -> no proxy: the master host IP is his whitelisted IP.
#   user2 (Pratibha) -> her proxy 157.245.108.24 (from accounts/pratibha/account.env).
#   user3 (Piyush)   -> his EC2 proxy 15.252.102.31 (from accounts/piyush/account.env).
# (user4 unused: auto_refresh=false, no token.)
# Then sync user1's fresh token to the FyersFire app (preserves the old cron's behaviour).
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

echo "$(date -u +%FT%TZ) refreshing user1 (rahul, direct)"
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" scripts/fyers_auto_auth.py --auth-file fyers_auth.json --user-key user1 --once

echo "$(date -u +%FT%TZ) refreshing user2 (pratibha, via proxy)"
env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
  "$PY" scripts/fyers_auto_auth.py --auth-file fyers_auth.json --user-key user2 --once

echo "$(date -u +%FT%TZ) refreshing user3 (piyush, via EC2 proxy)"
env $(grep -v '^#' accounts/piyush/account.env | xargs) \
  "$PY" scripts/fyers_auto_auth.py --auth-file fyers_auth.json --user-key user3 --once

echo "$(date -u +%FT%TZ) syncing FyersFire auth"
"$PY" scripts/sync_fyersfire_auth.py
