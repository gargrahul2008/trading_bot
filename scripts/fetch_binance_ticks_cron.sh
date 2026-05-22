#!/bin/bash
# fetch_binance_ticks_cron.sh — Daily cron to download yesterday's Binance ETHUSDT tick data.
#
# Binance Data Vision publishes the previous day's aggTrades file by ~00:30 UTC.
# This cron runs at 02:00 UTC to ensure the file is available.
# The script skips dates that are already downloaded.
#
# Cron: 0 2 * * * /root/trading_bot/scripts/fetch_binance_ticks_cron.sh >> /root/trading_bot/logs/fetch_binance_ticks.log 2>&1

set -euo pipefail
cd /root/trading_bot

PYTHON="env/bin/python"
DIR="backtest/ticks"

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Fetching yesterday's Binance ETHUSDT tick data..."
"$PYTHON" -u scripts/fetch_binance_ticks.py --days 2 --dir "$DIR"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Done."
