#!/usr/bin/env bash
# Daily broker-P&L reconciliation fetch, per account, IP-bound. Cron: after close (~15:40 IST).
# Pulls the day's tradebook, accumulates it, replays realized vs the bot, estimates charges, and
# writes accounts/<acct>/reports/broker_pnl.json + portfolio.json for the dashboard.
# Read-only broker calls.
#
# The account list comes from deploy/accounts.py, the same register refresh_tokens.sh and
# onboard.py use. It used to be two hardcoded blocks (rahul, pratibha), which is why piyush was
# onboarded in August and still had no broker_pnl.json or portfolio.json six weeks later: a new
# account was simply never fetched, and nothing said so. Anything with an account.env is now
# covered automatically.
#
# --refreshable is the token cron's flag, but its meaning is what this needs too: accounts that
# can log in AND have an account.env to egress through. Without one, the call would leave by the
# wrong IP.
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
  echo "$(date -u +%FT%TZ) fetching $account ($user_key, ${proxy:-direct})"

  # Each account under its own account.env, so every broker call leaves by that account's
  # whitelisted IP. One shared process would use whichever proxy the environment happened to hold.
  for script in fetch_broker_pnl.py fetch_broker_portfolio.py; do
    if ! env $(grep -v '^#' "$env_file" | xargs) \
          "$PY" "scripts/$script" --account "$account" --user-key "$user_key"; then
      echo "$(date -u +%FT%TZ) FAILED $script for $account ($user_key)"
      failed=$((failed + 1))
    fi
  done

  # The dashboard store (capital / realised / charges). Was manual-only, so it went a
  # month stale: rahul and pratibha were last fetched 2026-08-31 and the page was still
  # showing August figures on 30 September, missing a 589.00 transfer and a month of P&L.
  # No --daily-realised here: that asks the realised endpoint once per calendar day
  # (~185 calls) and is for backfills. The daily top-up needs one call.
  if ! env $(grep -v '^#' "$env_file" | xargs) \
        "$PY" scripts/fetch_history.py --account "$account" --user-key "$user_key"; then
    echo "$(date -u +%FT%TZ) FAILED fetch_history.py for $account ($user_key)"
    failed=$((failed + 1))
  fi
done < <("$PY" deploy/accounts.py --refreshable)

if [ "$count" -eq 0 ]; then
  # Silence would look exactly like success, and the dashboard would quietly go stale.
  echo "$(date -u +%FT%TZ) ERROR: no accounts found — check fyers_auth.json"
  exit 1
fi

# Bot realized-P&L history from local trade logs (no broker call). Feeds the dashboard.
"$PY" scripts/build_bot_pnl_history.py

echo "$(date -u +%FT%TZ) fetched $count account(s), $failed failure(s)"
[ "$failed" -gt 0 ] && exit 1
exit 0
