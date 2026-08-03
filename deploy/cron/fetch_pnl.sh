#!/usr/bin/env bash
# Daily broker-P&L reconciliation fetch, per account, IP-bound. Cron: after close (~15:40 IST).
# Pulls the day's tradebook, accumulates it, replays realized vs the bot, estimates charges, and
# writes accounts/<acct>/reports/broker_pnl.json for the dashboard. Read-only broker calls.
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" scripts/fetch_broker_pnl.py --account rahul --user-key user1
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" scripts/fetch_broker_portfolio.py --account rahul --user-key user1

env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
  "$PY" scripts/fetch_broker_pnl.py --account pratibha --user-key user2
env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
  "$PY" scripts/fetch_broker_portfolio.py --account pratibha --user-key user2

# Bot realized-P&L history from local trade logs (no broker call). Feeds the dashboard.
"$PY" scripts/build_bot_pnl_history.py
