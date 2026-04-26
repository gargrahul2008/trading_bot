#!/bin/bash
cd /root/trading_bot

TRADES="strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl"

python3 scripts/mexc_telegram_report.py \
    --config strategies/pct_ladder/config.mexc.json \
    --trades $TRADES \
    --hours 8

python3 scripts/mexc_pnl_verify.py \
    --config strategies/pct_ladder/config.mexc.json \
    --trades $TRADES
