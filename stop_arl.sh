#!/bin/bash
CONFIG="strategies/pct_ladder/config.arl.json"
LOG="logs/arl_strategy.log"
cd /root/trading_bot
mkdir -p logs
pkill -f "run_strategy.py --config $CONFIG"
echo "$(date): Bot stopped." >> $LOG
