#!/bin/bash
# mexc_watchdog.sh — Cron watchdog: ensures MEXC bot is always running.
#
# Runs every minute via cron. If neither the bot nor the runner script is
# alive, it restarts the runner as a background process and sends a Telegram alert.
# Runner output is appended to logs/mexc_runner.log.
#
# Crontab entry (add with: crontab -e):
#   * * * * * /root/trading_bot/scripts/mexc_watchdog.sh >> /root/trading_bot/logs/mexc_watchdog.log 2>&1

cd /root/trading_bot

RUNNER="scripts/mexc_bot_runner.sh"
SECRETS="strategies/pct_ladder/secrets/telegram.json"
PYTHON="env/bin/python"
LOCKFILE="/tmp/mexc_watchdog.lock"
RUNNER_LOG="logs/mexc_runner.log"

# ── Prevent concurrent watchdog runs ──────────────────────────────────────
exec 9>"$LOCKFILE"
flock -n 9 || exit 0

# ── Check if bot or runner is alive ───────────────────────────────────────
BOT_ALIVE=0
pgrep -f "run_strategy.py.*config.mexc.json" > /dev/null 2>&1 && BOT_ALIVE=1
pgrep -f "mexc_bot_runner.sh" > /dev/null 2>&1 && BOT_ALIVE=1

if [ "$BOT_ALIVE" -eq 1 ]; then
    exit 0
fi

# ── Bot is down — alert and restart ───────────────────────────────────────
NOW=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$NOW] MEXC bot not running — restarting in background"

# Send Telegram alert
"$PYTHON" - "$SECRETS" "🔴 *Bot DOWN* — watchdog restarting\n${NOW}" <<'PYEOF' || true
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
        print(f"telegram failed: {e}", file=sys.stderr)
PYEOF

# Restart as background process
nohup /root/trading_bot/scripts/mexc_bot_runner.sh >> "/root/trading_bot/$RUNNER_LOG" 2>&1 &

echo "[$NOW] Runner started in background (PID $!), logging to $RUNNER_LOG"
