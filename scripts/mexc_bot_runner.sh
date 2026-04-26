#!/bin/bash
# mexc_bot_runner.sh — Run MEXC bot with auto-restart and Telegram alerts.
#
# Starts the bot in an infinite loop. On any exit (crash, OOM, exception):
#   1. Sends a Telegram alert with the exit code and timestamp.
#   2. Waits RESTART_DELAY seconds.
#   3. Restarts the bot.
#
# Graceful stop: send SIGTERM to this script (kills bot + stops loop).
#   kill $(pgrep -f mexc_bot_runner.sh)
#
# Usage (run inside a screen session):
#   screen -S mexc bash -c "/root/trading_bot/scripts/mexc_bot_runner.sh"

set -euo pipefail
cd /root/trading_bot

CONFIG="strategies/pct_ladder/config.mexc.json"
SECRETS="strategies/pct_ladder/secrets/telegram.json"
PYTHON="env/bin/python"
RESTART_DELAY=5          # seconds to wait before restarting after a crash
BOT_PID=""
STOP_REQUESTED=0

# ── Telegram helper ────────────────────────────────────────────────────────

send_telegram() {
    local message="$1"
    if [ ! -f "$SECRETS" ]; then
        echo "[telegram] secrets not found: $SECRETS"
        return
    fi
    "$PYTHON" - "$SECRETS" "$message" <<'PYEOF'
import sys, json, urllib.request, urllib.parse

secrets_path = sys.argv[1]
text         = sys.argv[2]

with open(secrets_path) as f:
    s = json.load(f)

token    = s["bot_token"]
chat_ids = s["chat_id"]
if isinstance(chat_ids, str):
    chat_ids = [chat_ids]

for chat_id in chat_ids:
    data = urllib.parse.urlencode({
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=data, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            pass
    except Exception as e:
        print(f"[telegram] send failed: {e}", file=sys.stderr)
PYEOF
}

# ── Signal handlers ────────────────────────────────────────────────────────

stop_cleanly() {
    STOP_REQUESTED=1
    echo "[runner] Stop requested — shutting down bot…"
    if [ -n "$BOT_PID" ] && kill -0 "$BOT_PID" 2>/dev/null; then
        kill -INT "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    send_telegram "⏹ *Bot stopped* (manual) — $(date '+%Y-%m-%d %H:%M:%S IST' --date='TZ=\"Asia/Kolkata\"')" || true
    exit 0
}

trap stop_cleanly SIGTERM SIGINT

# ── Main loop ──────────────────────────────────────────────────────────────

echo "[runner] Starting bot runner (PID $$)"
send_telegram "🚀 *Bot started* — $(date '+%Y-%m-%d %H:%M:%S IST' --date='TZ=\"Asia/Kolkata\"')" || true

ATTEMPT=0

while [ "$STOP_REQUESTED" -eq 0 ]; do
    ATTEMPT=$((ATTEMPT + 1))
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

    echo "[runner] Attempt #${ATTEMPT} — starting bot at ${START_TIME}"

    # Run bot in background so we can capture PID for signal forwarding
    "$PYTHON" run_strategy.py --config "$CONFIG" &
    BOT_PID=$!

    # Wait for bot to exit
    wait "$BOT_PID" 2>/dev/null
    EXIT_CODE=$?
    BOT_PID=""

    [ "$STOP_REQUESTED" -eq 1 ] && break

    END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[runner] Bot exited with code ${EXIT_CODE} at ${END_TIME}"

    # Send Telegram alert
    MSG="⚠️ *Bot stopped unexpectedly* (exit=${EXIT_CODE})
Start: ${START_TIME}
Stop:  ${END_TIME}
Run #${ATTEMPT}
Restarting in ${RESTART_DELAY}s…"
    send_telegram "$MSG" || true

    echo "[runner] Waiting ${RESTART_DELAY}s before restart…"
    sleep "$RESTART_DELAY"
done

echo "[runner] Runner exited."
