#!/bin/bash
# mexc_bucket3_runner.sh — Run MEXC Bucket 1 (tight 0.4% grid) with auto-restart and Telegram alerts.
#
# Starts the bot in an infinite loop. On any exit, sends Telegram alert and restarts.
#
# Graceful stop: kill $(pgrep -f mexc_bucket3_runner.sh)

set -euo pipefail
cd /root/trading_bot

BUCKET="bucket3"
CONFIG="strategies/pct_ladder/config.mexc.bucket3.json"
SECRETS="strategies/pct_ladder/secrets/telegram.json"
PYTHON="env/bin/python"
RESTART_DELAY=5
BOT_PID=""
STOP_REQUESTED=0

send_telegram() {
    local message="$1"
    if [ ! -f "$SECRETS" ]; then
        echo "[telegram] secrets not found: $SECRETS"
        return
    fi
    "$PYTHON" - "$SECRETS" "$message" <<'PYEOF'
import sys, json, urllib.request, urllib.parse
secrets_path = sys.argv[1]
text = sys.argv[2]
with open(secrets_path) as f:
    s = json.load(f)
token = s["bot_token"]
chat_ids = s["chat_id"]
if isinstance(chat_ids, str):
    chat_ids = [chat_ids]
for chat_id in chat_ids:
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            pass
    except Exception as e:
        print(f"[telegram] send failed: {e}", file=sys.stderr)
PYEOF
}

stop_cleanly() {
    STOP_REQUESTED=1
    echo "[runner-$BUCKET] Stop requested — shutting down bot…"
    if [ -n "$BOT_PID" ] && kill -0 "$BOT_PID" 2>/dev/null; then
        kill -INT "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    send_telegram "⏹ *Bucket3 stopped* (manual) — $(date '+%Y-%m-%d %H:%M:%S IST' --date='TZ=\"Asia/Kolkata\"')" || true
    exit 0
}

trap stop_cleanly SIGTERM SIGINT

LOCKFILE="/tmp/mexc_bucket3_runner.lock"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    echo "[runner-$BUCKET] Another runner instance already holds the lock — exiting."
    exit 0
fi

echo "[runner-$BUCKET] Starting bot runner (PID $$)"
send_telegram "🚀 *Bucket3 started* — $(date '+%Y-%m-%d %H:%M:%S IST' --date='TZ=\"Asia/Kolkata\"')" || true

ATTEMPT=0
while [ "$STOP_REQUESTED" -eq 0 ]; do
    ATTEMPT=$((ATTEMPT + 1))
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[runner-$BUCKET] Attempt #${ATTEMPT} — starting bot at ${START_TIME}"
    "$PYTHON" run_strategy.py --config "$CONFIG" &
    BOT_PID=$!
    wait "$BOT_PID" 2>/dev/null || true
    EXIT_CODE=$?
    BOT_PID=""
    [ "$STOP_REQUESTED" -eq 1 ] && break
    END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[runner-$BUCKET] Bot exited with code ${EXIT_CODE} at ${END_TIME}"
    MSG="⚠️ *Bucket3 stopped unexpectedly* (exit=${EXIT_CODE})
Start: ${START_TIME}
Stop:  ${END_TIME}
Run #${ATTEMPT}
Restarting in ${RESTART_DELAY}s…"
    send_telegram "$MSG" || true
    echo "[runner-$BUCKET] Waiting ${RESTART_DELAY}s before restart…"
    sleep "$RESTART_DELAY"
done

echo "[runner-$BUCKET] Runner exited."
