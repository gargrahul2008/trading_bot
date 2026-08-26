#!/bin/bash
# mexc_monthly_cron.sh — send the previous calendar month's bot earnings to Telegram.
# Suggested cron (1st of each month, 06:00 IST = 00:30 UTC):
#   30 0 1 * * /root/trading_bot/scripts/mexc_monthly_cron.sh >> /root/trading_bot/logs/mexc_monthly.log 2>&1
cd /root/trading_bot
echo "=== $(date '+%F %T %Z') monthly report ==="
env/bin/python scripts/mexc_monthly_report.py --send
