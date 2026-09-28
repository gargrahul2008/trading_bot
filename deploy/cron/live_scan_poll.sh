#!/usr/bin/env bash
# 44-SMA scanner: market-hours poll of the watchlist live_scan_after_market.sh
# wrote the previous evening. Fires a Telegram alert the moment a setup's
# entry triggers, and tracks it through to exit (SL/target/force-exit) with a
# second alert including P&L. See sma44_level1_intraday/scripts/live_scan_poll.py's
# own docstring for the full mechanics and known limitations.
#
# Cron (server clock is UTC; IST = UTC+5:30 -- 09:15-15:30 IST = 03:45-10:00 UTC):
#   */5 3-10 * * 1-5   deploy/cron/live_scan_poll.sh
#     (the script's own _in_market_hours gate is what actually restricts it to
#     09:15-15:30 IST; the wider 3-10 hour range just makes sure both edges of
#     that window fall inside SOME 5-minute tick).
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" -m sma44_level1_intraday.scripts.live_scan_poll
