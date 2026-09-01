#!/usr/bin/env bash
# Daily one-shot Fyers token refresh, PER ACCOUNT and IP-bound. Cron: 08:30 IST
# (03:00 UTC), 25 min before the 08:55 bot start so the bots load a fresh token.
#
# Replaces the always-on fyers-auth-* systemd units and the legacy
# `--enabled-only` cron. Per-account (never --enabled-only) so each login exits
# from ITS whitelisted IP.
#
# The account list comes from deploy/accounts.py, which reads fyers_auth.json —
# the same register deploy/onboard.py and deploy/preflight.sh use. It used to be
# three hardcoded blocks, which meant a newly added account would silently never
# get a token and its agent would simply die at the next expiry.
#
# Then sync user1's fresh token to the FyersFire app (preserves the old cron's
# behaviour).
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

failed=0
count=0

while read -r account user_key; do
  [ -n "$account" ] || continue
  count=$((count + 1))
  env_file="accounts/$account/account.env"
  proxy=$(sed -n 's/^HTTPS_PROXY=//p' "$env_file" | head -1)
  echo "$(date -u +%FT%TZ) refreshing $account ($user_key, ${proxy:-direct})"

  # Each login runs under its own account.env, so it leaves by that account's
  # whitelisted IP. Doing them in one process would share whichever proxy the
  # environment happened to hold.
  if ! env $(grep -v '^#' "$env_file" | xargs) \
        "$PY" scripts/fyers_auto_auth.py --auth-file fyers_auth.json \
              --user-key "$user_key" --once; then
    echo "$(date -u +%FT%TZ) FAILED to refresh $account ($user_key)"
    failed=$((failed + 1))
  fi
done < <("$PY" deploy/accounts.py --refreshable)

if [ "$count" -eq 0 ]; then
  # Silence here would look identical to success, and the first anyone would
  # know is every agent failing authentication the next morning.
  echo "$(date -u +%FT%TZ) ERROR: no refreshable accounts found — check fyers_auth.json"
  exit 1
fi

# The exchanges' instrument list: public CSVs, no account, no proxy, so this
# spends none of the accounts' rate budget. Instruments are added, renamed and
# moved between series constantly, and the order pad treats "not in this list"
# as "does not exist".
echo "$(date -u +%FT%TZ) refreshing the instrument list"
"$PY" scripts/fetch_symbols.py || echo "$(date -u +%FT%TZ) instrument refresh FAILED"

echo "$(date -u +%FT%TZ) syncing FyersFire auth"
"$PY" scripts/sync_fyersfire_auth.py

echo "$(date -u +%FT%TZ) refreshed $((count - failed))/$count account(s)"
[ "$failed" -gt 0 ] && exit 1
exit 0
