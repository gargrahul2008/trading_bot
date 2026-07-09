#!/bin/bash
# reliance_hold_stop.sh — STOP the Reliance bot, then cancel all its open orders.
# Use to freeze and hold the position (e.g. ahead of an expected gap-up).
# Stop-first is essential: a running bot re-places cancelled orders within ~5s.
#
#   bash scripts/reliance_hold_stop.sh            # dry run (stops bot, LISTS orders)
#   bash scripts/reliance_hold_stop.sh --execute  # stops bot AND cancels orders
set -u
cd /root/trading_bot
CONFIG="strategies/pct_ladder/config.reliance.mtf.json"
PY="/root/trading_bot/env/bin/python"

echo "[1/3] Stopping Reliance bot..."
pkill -f "run_strategy.py --config $CONFIG"
sleep 2
if pgrep -f "run_strategy.py --config $CONFIG" > /dev/null; then
    echo "  bot still alive, sending SIGKILL..."
    pkill -9 -f "run_strategy.py --config $CONFIG"
    sleep 1
fi
if pgrep -f "run_strategy.py --config $CONFIG" > /dev/null; then
    echo "  ERROR: bot process still running — ABORTING cancel (would get re-armed)."
    exit 1
fi
echo "  bot stopped."

echo "[2/3] Cancelling open Reliance orders..."
if [ "${1:-}" == "--execute" ]; then
    $PY scripts/cancel_reliance_orders.py --execute
else
    $PY scripts/cancel_reliance_orders.py
fi

echo "[3/3] NOTE: cron will restart the bot at 08:55 tomorrow."
echo "      To keep holding across days, disable the start cron, and reset"
echo "      reference_price in state before restarting."
