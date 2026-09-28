#!/bin/bash
# fetch_universe_topup_cron.sh — Nightly top-up of the full cash-segment
# universe's daily cache (data/fyers_daily/), so the scanner notebook never
# has to fetch anything live at scan time.
#
# Runs well after market close (15:30 IST) so the day's EOD candle is final.
# Idempotent/gap-aware: only the genuinely missing day(s) get fetched per
# symbol (see fetch_fyers_universe_data.py / provider.py's fetched_ranges
# coverage check) -- a --start a week back is just a safety margin in case
# a prior night's run was missed or failed partway, not a full re-fetch.
# Full universe (~2668 symbols) still costs ~1 FYERS API call per symbol
# (one call per account, rate-limited) even for a 1-day top-up -- expect
# this to take roughly 2-2.5 hours; that's why it runs overnight, not
# inside the notebook itself.
#
# Cron (18:00 IST daily): 0 18 * * * /home/rahul/trading2/trading_bot/scripts/fetch_universe_topup_cron.sh >> /home/rahul/trading2/trading_bot/logs/fetch_universe_topup.log 2>&1

set -euo pipefail
cd /home/rahul/trading2/trading_bot

PYTHON=".venv/bin/python"
# If run before market close (15:30 IST), "today" has no EOD candle yet --
# requesting it anyway makes every one of the ~2668 symbols burn an API call
# checking for data that can't exist, for nothing (this happened once, from
# a manual midday run). Target END so it's always a day the market has
# actually closed for -- fine for the scheduled 18:00 run, and safe if this
# is ever triggered manually during market hours too.
if [ "$(date '+%H%M')" -lt "1530" ]; then
    END="$(date -d 'yesterday' '+%Y-%m-%d')"
else
    END="$(date '+%Y-%m-%d')"
fi
START="$(date -d '7 days ago' '+%Y-%m-%d')"

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Starting universe top-up fetch ($START -> $END)..."
"$PYTHON" -u scripts/fetch_fyers_universe_data.py \
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
# (NSE_X_EQ_<start>_<end>.parquet) alongside the real gap-aware master
# cache (NSE_X_EQ.parquet + .meta.json) for every one of the ~2668 symbols.
# Only the master cache matters for anything downstream -- clean up
# tonight's snapshots so they don't pile up run after run.
rm -f data/fyers_daily/*_"${START}"_"${END}".parquet

echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Done."
