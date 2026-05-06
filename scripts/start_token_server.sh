#!/bin/bash
# Start the Fyers token server (serves tokens to other VPS bots).
# Run once manually or via cron watchdog.
cd /root/trading_bot

SECRET_FILE="scripts/token_server_secret.txt"
if [ ! -f "$SECRET_FILE" ]; then
    echo "ERROR: secret file not found: $SECRET_FILE"
    echo "Create it with: echo 'your_secret_here' > $SECRET_FILE && chmod 600 $SECRET_FILE"
    exit 1
fi

SECRET=$(cat "$SECRET_FILE" | tr -d '[:space:]')
PORT=8502
LOG="logs/token_server.log"
mkdir -p logs

# Already running?
if pgrep -f "token_server.py" > /dev/null; then
    exit 0
fi

nohup /root/trading_bot/env/bin/python scripts/token_server.py \
    --auth-file fyers_auth.json \
    --secret "$SECRET" \
    --port $PORT >> "$LOG" 2>&1 &

echo "$(date): Token server started. PID: $!" >> "$LOG"
