#!/bin/bash
# Daily live-vs-backtest performance comparison report
# Runs after the 8-hour telegram report so both land close together

cd /root/trading_bot

python3 scripts/mexc_backtest_compare.py \
    --config strategies/pct_ladder/config.mexc.json \
    --telegram \
    --secrets strategies/pct_ladder/secrets/telegram.json
