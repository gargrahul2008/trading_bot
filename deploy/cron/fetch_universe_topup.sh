#!/usr/bin/env bash
# Nightly top-up of the full cash-segment universe's daily cache
# (data/fyers_daily/), so live_scan_after_market.sh never has to fetch
# anything live at scan time. Instance counterpart of the dev machine's own
# scripts/fetch_universe_topup_cron.sh (same script underneath, just this
# repo's path/venv/account conventions instead of the dev machine's).
#
# Idempotent/gap-aware: only genuinely missing day(s) get fetched per
# symbol (see fetch_fyers_universe_data.py / provider.py's fetched_ranges
# coverage check) -- the 7-day START lookback is just a safety margin for a
# missed/partial prior run, not a full re-fetch. Full universe (~2319
# symbols) still costs ~1 Fyers API call per symbol even for a 1-day
# top-up -- expect roughly 2-2.5 hours, which is why this runs overnight.
#
# Cron (server clock is UTC; IST = UTC+5:30 -- 18:00 IST = 12:30 UTC):
#   30 12 * * *   deploy/cron/fetch_universe_topup.sh
set -euo pipefail
cd /root/trading_bot
PY=/root/trading_bot/env/bin/python

# "today" has no EOD candle yet before market close (15:30 IST) -- target a
# day the market has actually closed for (see fetch_universe_topup_cron.sh's
# own comment for the incident this guards against: every one of ~2319
# symbols burning an API call checking for data that can't exist yet).
if [ "$(date '+%H%M')" -lt "1530" ]; then
    END="$(date -d 'yesterday' '+%Y-%m-%d')"
else
    END="$(date '+%Y-%m-%d')"
fi
START="$(date -d '7 days ago' '+%Y-%m-%d')"

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Starting universe top-up fetch ($START -> $END)..."
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  "$PY" -u scripts/fetch_fyers_universe_data.py \
  --auth-file fyers_auth.json \
  --user-key user1 \
  --universe-file universe_full_cash_segment.json \
  --start "$START" \
  --end "$END" \
  --output-dir data/fyers_daily \
  --format parquet \
  --resolution D \
  --chunk-days 365 \
  --skip-invalid-symbols

# The fetch script also writes a throwaway per-run dated snapshot file
# alongside the real gap-aware master cache for every symbol -- only the
# master cache matters downstream; clean up tonight's snapshots.
rm -f data/fyers_daily/*_"${START}"_"${END}".parquet

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Done."
