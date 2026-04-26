#!/bin/bash
# Restart MEXC bot inside the 'mexc' screen session.
# Runs mexc_bot_runner.sh which auto-restarts on crash + sends Telegram alerts.
#
# Usage: ./scripts/mexc_restart.sh

set -e
cd /root/trading_bot

SCREEN_NAME="mexc"
RUNNER="/root/trading_bot/scripts/mexc_bot_runner.sh"

# Kill existing runner + bot (SIGTERM triggers clean stop in runner)
for PATTERN in "mexc_bot_runner.sh" "run_strategy.py.*config.mexc.json"; do
    PID=$(pgrep -f "$PATTERN" 2>/dev/null | head -1 || true)
    if [ -n "$PID" ]; then
        echo "Stopping PID $PID ($PATTERN)..."
        kill -TERM "$PID" 2>/dev/null || true
        for i in $(seq 1 10); do
            kill -0 "$PID" 2>/dev/null || break
            sleep 1
        done
        kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
    fi
done
sleep 1

# Launch runner inside screen
if screen -list | grep -q "\.${SCREEN_NAME}"; then
    echo "Sending runner to existing '$SCREEN_NAME' screen..."
    screen -S "$SCREEN_NAME" -p 0 -X stuff "$RUNNER\n"
else
    echo "Creating new '$SCREEN_NAME' screen..."
    screen -dmS "$SCREEN_NAME" bash -c "$RUNNER; exec bash"
fi

echo "MEXC bot runner started in screen '$SCREEN_NAME'."
echo "Attach with: screen -r $SCREEN_NAME"
