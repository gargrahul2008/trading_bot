#!/bin/bash
# mexc_audit_cron.sh — run the PnL/reconciliation audit; alert on Telegram ONLY if it fails.
# Add to crontab (e.g. hourly): 0 * * * * /root/trading_bot/scripts/mexc_audit_cron.sh >> /root/trading_bot/logs/mexc_audit.log 2>&1
cd /root/trading_bot
SECRETS="strategies/pct_ladder/secrets/telegram.json"
PYTHON="env/bin/python"

OUT="$($PYTHON scripts/mexc_audit.py 2>&1)"
RC=$?
echo "$(date '+%F %T') rc=$RC"
echo "$OUT"

if [ "$RC" -ne 0 ]; then
    "$PYTHON" - "$SECRETS" "🚨 *MEXC AUDIT FAILED* — accounting gap detected:
\`\`\`
$OUT
\`\`\`" <<'PYEOF' || true
import sys, json, urllib.request, urllib.parse
s = json.load(open(sys.argv[1])); text = sys.argv[2]
token = s["bot_token"]; chats = s["chat_id"]
if isinstance(chats, str): chats = [chats]
for c in chats:
    data = urllib.parse.urlencode({"chat_id": c, "text": text[:4000], "parse_mode": "Markdown"}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST"), timeout=10)
    except Exception as e:
        print("telegram failed:", e, file=sys.stderr)
PYEOF
fi
