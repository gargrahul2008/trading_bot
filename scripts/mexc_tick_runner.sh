#!/bin/bash
# Keeps mexc_tick_collector.py running inside a 'ticks' screen session.
# Usage: ./scripts/mexc_tick_runner.sh

cd /root/trading_bot

SCREEN="ticks"
CMD="python3 scripts/mexc_tick_collector.py >> /tmp/mexc_ticks.log 2>&1"

if screen -list | grep -q "\.${SCREEN}"; then
    echo "Screen session '$SCREEN' already exists."
    echo "  Attach with: screen -r $SCREEN"
    echo "  Logs at:     tail -f /tmp/mexc_ticks.log"
else
    echo "Starting tick collector in screen '$SCREEN'..."
    screen -dmS "$SCREEN" bash -c "$CMD; exec bash"
    sleep 1
    echo "Started. Attach with: screen -r $SCREEN"
    echo "Logs at: tail -f /tmp/mexc_ticks.log"
fi
