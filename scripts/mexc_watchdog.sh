#!/bin/bash
# mexc_watchdog.sh — Cron watchdog: ensures both MEXC bucket bots are always running.
#
# Runs every minute via cron. Checks Bucket 1 and Bucket 2 independently.
# If either is down, restarts only the missing one and sends a Telegram alert.
#
# Crontab entry:
#   * * * * * /root/trading_bot/scripts/mexc_watchdog.sh >> /root/trading_bot/logs/mexc_watchdog.log 2>&1

cd /root/trading_bot

SECRETS="strategies/pct_ladder/secrets/telegram.json"
PYTHON="env/bin/python"
LOCKFILE="/tmp/mexc_watchdog.lock"

exec 9>"$LOCKFILE"
flock -n 9 || exit 0

send_telegram() {
    local message="$1"
    if [ ! -f "$SECRETS" ]; then return; fi
    "$PYTHON" - "$SECRETS" "$message" <<'PYEOF' || true
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
        print(f"telegram failed: {e}", file=sys.stderr)
PYEOF
}

check_and_restart() {
    local bucket="$1"
    local config="$2"
    local runner="$3"
    local log="$4"

    local alive=0
    pgrep -f "run_strategy.py.*${config}" > /dev/null 2>&1 && alive=1
    pgrep -f "$(basename "$runner")" > /dev/null 2>&1 && alive=1

    if [ "$alive" -eq 1 ]; then
        return 0
    fi

    local now
    now=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$now] $bucket down — restarting in background"
    send_telegram "🔴 *${bucket} DOWN* — watchdog restarting\n${now}"
    nohup "$runner" >> "/root/trading_bot/$log" 2>&1 &
    echo "[$now] $bucket runner started in background (PID $!), logging to $log"
}

check_and_restart "Bucket1" "config.mexc.bucket1.json" \
    "/root/trading_bot/scripts/mexc_bucket1_runner.sh" "logs/mexc_bucket1_runner.log"

check_and_restart "Bucket2" "config.mexc.bucket2.json" \
    "/root/trading_bot/scripts/mexc_bucket2_runner.sh" "logs/mexc_bucket2_runner.log"

check_and_restart "Bucket3" "config.mexc.bucket3.json" \
    "/root/trading_bot/scripts/mexc_bucket3_runner.sh" "logs/mexc_bucket3_runner.log"
