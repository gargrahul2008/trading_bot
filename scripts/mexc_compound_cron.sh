#!/bin/bash
# Daily compounding at UTC 00:00 (05:30 IST).
# Order is critical to avoid race condition:
# 1. Stop bot first (so its shutdown flush cannot overwrite our new state)
# 2. Compute and write new step size
# 3. Apply or remove weekend mode
# Runner auto-restarts bot after script completes with the clean state.
set -e
cd /root/trading_bot

DOW=$(date -u '+%u')   # 1=Mon … 6=Sat, 7=Sun
echo "=== Compound $(date -u '+%Y-%m-%d %H:%M UTC') (DOW=$DOW) ==="

# 1. Stop the bot FIRST so its shutdown flush cannot overwrite the state we
#    are about to write.  The runner auto-restarts after we are done.
PID=$(pgrep -f "run_strategy.py.*config.mexc.json" 2>/dev/null | head -1 || true)
if [ -n "$PID" ]; then
    echo "Stopping bot (PID $PID) before state update..."
    kill -INT "$PID" 2>/dev/null || true
    for i in $(seq 1 15); do
        kill -0 "$PID" 2>/dev/null || { echo "Bot stopped after ${i}s."; break; }
        sleep 1
    done
    # Force-kill if still alive after 15s
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
else
    echo "Bot not running."
fi

# Small pause to ensure the bot's final state flush has hit disk before we
# overwrite it.
sleep 2

# 2. Compute and write new weekday step size
python3 scripts/mexc_compound.py \
    --config strategies/pct_ladder/config.mexc.json \
    --trades strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl \
    --initial-equity 104491.12 \
    --initial-buy-quote 2512

# 3. Weekend mode switch (safe — bot is already down)
if [ "$DOW" -eq 6 ]; then
    echo "Saturday — applying weekend mode (0.1% grid, 4 levels, half step)..."
    bash scripts/mexc_weekend_start.sh
elif [ "$DOW" -eq 1 ]; then
    echo "Monday — removing weekend mode, restoring weekday settings..."
    bash scripts/mexc_weekend_end.sh
fi

# Runner auto-restarts in ~5s and reads the freshly written state.
echo "Compound done. Runner will restart bot with updated state."
