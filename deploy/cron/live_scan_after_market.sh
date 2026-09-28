#!/usr/bin/env bash
# 44-SMA scanner: post-market actionable-signal scan + Telegram summary.
# Read-only (cached daily bars + a Fyers login only for the odd cache top-up).
# Writes data/live_scan/watchlist_latest.json for live_scan_poll.sh to track
# during the NEXT session, and sends one Telegram summary of what's actionable.
#
# Cron (server clock is UTC; IST = UTC+5:30):
#   0 13 * * 1-5   deploy/cron/live_scan_after_market.sh   # 18:30 IST, after
#                  refresh_tokens.sh / fetch_universe_topup_cron.sh's own
#                  nightly top-up so the cache is current before this runs.
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" -m sma44_level1_intraday.scripts.live_scan_after_market
