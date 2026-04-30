#!/bin/bash
# Daily compounding at UTC 00:00.
# 1. Stop the bot (graceful)
# 2. Compute new step size from actual LIFO cycle PnL
# 3. Write to state.json extras
# 4. Restart bot in 'mexc' screen
set -e
cd /root/trading_bot

echo "=== Compound $(date -u '+%Y-%m-%d %H:%M UTC') ==="

# 1. Stop bot
PID=$(pgrep -f "config.mexc.json" 2>/dev/null | head -1 || true)
if [ -n "$PID" ]; then
    echo "Stopping bot (PID $PID)..."
    kill -INT "$PID" 2>/dev/null || true
    for i in $(seq 1 10); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
    sleep 2
    echo "Bot stopped."
fi

# 2+3. Compute and write new step size
python3 scripts/mexc_compound.py \
    --config strategies/pct_ladder/config.mexc.json \
    --trades strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl \
    --initial-equity 100491.12 \
    --initial-buy-quote 2512

# 4. Restart bot in mexc screen
sleep 2
SCREEN_NAME="mexc"
BOT_CMD="python run_strategy.py --config /root/trading_bot/strategies/pct_ladder/config.mexc.json"
if screen -list | grep -q "\.${SCREEN_NAME}"; then
    screen -S "$SCREEN_NAME" -p 0 -X stuff "$BOT_CMD\n"
else
    screen -dmS "$SCREEN_NAME" bash -c "cd /root/trading_bot && $BOT_CMD; exec bash"
fi
echo "Bot restarted in screen '$SCREEN_NAME'."
