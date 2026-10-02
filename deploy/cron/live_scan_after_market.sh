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
#
# --start-date overrides the notebook's own START_DATE for the live run only
# (the notebook itself is untouched, and still the source of everything else).
# It keeps the nightly scan to a short window: the notebook's 2020-01-01 takes
# ~45min here, this takes ~13min. FETCH_START_DATE still recomputes off it, so
# the 200-SMA warmup stays intact -- but the touch-sequence counter now starts
# at this date, so touch_count is counted from 2026-01-01 and will read LOWER
# than the notebook's own run for a symbol whose sequence began earlier. That
# also feeds scanner.minimum_touch_number, so which setups qualify can differ
# from the research output. Raise this date's history if that matters.
set -u
cd /root/trading_bot || exit 1
PY=/root/trading_bot/env/bin/python

env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" -m sma44_level1_intraday.scripts.live_scan_after_market \
  --start-date 2026-01-01
