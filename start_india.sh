#!/bin/bash
CONFIG="strategies/pct_ladder/config.midcap.mtf.json"
LOG="logs/india_strategy.log"
cd /root/trading_bot
mkdir -p logs

# NSE holidays 2026 — update each year
NSE_HOLIDAYS=(
    "2026-01-15"
    "2026-01-26"
    "2026-03-03"
    "2026-03-26"
    "2026-03-31"
    "2026-04-03"
    "2026-04-14"
    "2026-05-01"
    "2026-05-28"
    "2026-06-26"
    "2026-09-14"
    "2026-10-02"
    "2026-10-20"
    "2026-11-10"
    "2026-11-24"
    "2026-12-25"
)

TODAY=$(date +%Y-%m-%d)
for h in "${NSE_HOLIDAYS[@]}"; do
    if [ "$TODAY" == "$h" ]; then
        echo "$(date): NSE holiday ($TODAY), skipping start." >> $LOG
        exit 0
    fi
done

# Only start if not already running
if pgrep -f "$CONFIG" > /dev/null; then
    echo "$(date): Bot already running, skipping start." >> $LOG
    exit 0
fi

nohup /root/trading_bot/env/bin/python run_strategy.py --config $CONFIG >> $LOG 2>&1 &
echo "$(date): Bot started. PID: $!" >> $LOG
