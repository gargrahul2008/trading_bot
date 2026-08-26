#!/bin/bash
# 8-hour Telegram report — runs once per bucket (2 buckets = 2 messages per cycle)
# Bucket 2's report always sends (even with no trades) so we always see the HODL position.
cd /root/trading_bot

run_report () {
    local B="$1"
    local CONFIG="$2"
    local TRADES="$3"
    local EXTRA_ETH="$4"   # untracked HODL ETH (0 for bucket1, 17.709 for bucket2)
    local SEND_VERIFY="$5" # "1" to also run pnl_verify; "0" to skip (skip when no trades file)
    local SINCE="$6"       # baseline cutoff (IST); "" to use full trade history
    local VER="${7:-2026_05_28_v1}"  # state-file version suffix (bucket3 uses 2026_07_31_v1)
    local INIT_ETH="${8:-0}"   # seed inventory ETH (funded, not grid-bought) — for FIFO seeding
    local INIT_COST="${9:-0}"  # avg cost of that seed inventory

    echo "=== $B report ==="

    local extra_arg=""
    if [ "$EXTRA_ETH" != "0" ]; then
        extra_arg="--extra-eth $EXTRA_ETH"
    fi

    local since_arg=""
    if [ -n "$SINCE" ]; then
        since_arg="--since $SINCE"
    fi

    local init_arg=""
    if [ "$INIT_ETH" != "0" ] && [ "$INIT_COST" != "0" ]; then
        init_arg="--initial-eth $INIT_ETH --initial-cost $INIT_COST"
    fi

    # telegram_report.py handles missing trades file gracefully (treats as zero fills);
    # report still sends with HODL info derived from state file + extra-eth.
    python3 scripts/mexc_telegram_report.py \
        --config "$CONFIG" \
        --trades "$TRADES" \
        --hours 8 \
        $extra_arg $since_arg $init_arg

    # pnl_verify only makes sense when there ARE trades — skip when no trades file.
    if [ "$SEND_VERIFY" = "1" ] && [ -f "$TRADES" ]; then
        local CAPITAL="strategies/pct_ladder/state/${B}/capital_flows_${VER}.json"
        local MANUAL="strategies/pct_ladder/state/${B}/manual_positions_${VER}.json"
        python3 scripts/mexc_pnl_verify.py \
            --config "$CONFIG" \
            --trades "$TRADES" \
            --capital "$CAPITAL" \
            --manual "$MANUAL" \
            --out "strategies/pct_ladder/state/${B}/pnl_verify.csv"
    fi
}

# Bucket 1: tight grid, all ETH bot-tracked → no extra ETH.
# --since baselines cycle/PnL accounting to the clean grid era (excludes pre-2026-06-08 churn).
run_report "bucket1" \
    "strategies/pct_ladder/config.mexc.bucket1.json" \
    "strategies/pct_ladder/state/bucket1/trades_2026_05_28_v1.jsonl" \
    "0" \
    "1" \
    "2026-06-08T10:44:47"

# Bucket 2: wide grid + 17.709 ETH off-bot HODL stack → include HODL in PV
run_report "bucket2" \
    "strategies/pct_ladder/config.mexc.bucket2.json" \
    "strategies/pct_ladder/state/bucket2/trades_2026_05_28_v1.jsonl" \
    "17.709" \
    "1" \
    ""

# Bucket 3: tight 2% grid. Seeded with 19.7094 ETH @ real blended cost 1939.5
# (8.42 bought @1865 + 11.29 moved from bucket1 @1995) — pass as initial inventory so seed sells book PnL.
run_report "bucket3" \
    "strategies/pct_ladder/config.mexc.bucket3.json" \
    "strategies/pct_ladder/state/bucket3/trades_2026_07_31_v1.jsonl" \
    "0" \
    "1" \
    "" \
    "2026_07_31_v1" \
    "19.7094" \
    "1939.5"
