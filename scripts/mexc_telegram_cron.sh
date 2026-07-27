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

    echo "=== $B report ==="

    local extra_arg=""
    if [ "$EXTRA_ETH" != "0" ]; then
        extra_arg="--extra-eth $EXTRA_ETH"
    fi

    local since_arg=""
    if [ -n "$SINCE" ]; then
        since_arg="--since $SINCE"
    fi

    # telegram_report.py handles missing trades file gracefully (treats as zero fills);
    # report still sends with HODL info derived from state file + extra-eth.
    python3 scripts/mexc_telegram_report.py \
        --config "$CONFIG" \
        --trades "$TRADES" \
        --hours 8 \
        $extra_arg $since_arg

    # pnl_verify only makes sense when there ARE trades — skip when no trades file.
    if [ "$SEND_VERIFY" = "1" ] && [ -f "$TRADES" ]; then
        local CAPITAL="strategies/pct_ladder/state/${B}/capital_flows_2026_05_28_v1.json"
        local MANUAL="strategies/pct_ladder/state/${B}/manual_positions_2026_05_28_v1.json"
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
