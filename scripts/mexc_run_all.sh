#!/bin/bash
# Run all MEXC reporting scripts in sequence.
# Called by cron every 2 hours.

set -e
cd /root/trading_bot

TRADES="strategies/pct_ladder/state/mexc_trades.jsonl \
        strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \
        strategies/pct_ladder/state/mexc_trades_2026_03_03.jsonl \
        strategies/pct_ladder/state/mexc_trades_2026_03_03_v1.jsonl \
        strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl"

STATE="strategies/pct_ladder/state/mexc_state_2026_03_05_v1.json"
MANUAL="strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json"
SNAPSHOTS="strategies/pct_ladder/state/mexc_snapshots.csv"
PNL_BREAKDOWN="strategies/pct_ladder/state/mexc_pnl_breakdown.csv"
STYLE_PNL="strategies/pct_ladder/state/mexc_style_pnl.csv"
CAPITAL_FLOWS="strategies/pct_ladder/state/capital_flows_2026_03_05_v1.json"
LEDGER="strategies/pct_ladder/state/mexc_ledger.csv"
LEDGER_OPENING_TS="2026-03-24T23:00"
CYCLE_VERIFY="strategies/pct_ladder/state/mexc_cycle_verify.csv"

echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

python3 scripts/mexc_snapshot.py \
    --state "$STATE" \
    --trades $TRADES \
    --manual-positions "$MANUAL" \
    --csv "$SNAPSHOTS"

python3 scripts/mexc_pnl_breakdown.py \
    --snapshots "$SNAPSHOTS" \
    --out "$PNL_BREAKDOWN"

python3 scripts/mexc_style_pnl.py \
    --snapshots "$SNAPSHOTS" \
    --trades $TRADES \
    --out "$STYLE_PNL"

python3 scripts/mexc_ledger.py \
    --snapshots "$SNAPSHOTS" \
    --opening-ts "$LEDGER_OPENING_TS" \
    --trades $TRADES \
    --manual-positions "$MANUAL" \
    --capital-flows "$CAPITAL_FLOWS" \
    --out "$LEDGER"

python3 scripts/mexc_cycle_verify.py \
    --state "$STATE" \
    --trades $TRADES \
    --out "$CYCLE_VERIFY"
