#!/bin/bash
# mexc_weekend_end.sh — Restore MEXC bot to weekday mode (0.2% grid, 2 levels, full step).
# Called from mexc_compound_cron.sh (Monday) which kills the bot first.
# If run standalone, this script kills the bot itself before writing state to
# prevent the shutdown flush from overwriting the new state.
set -e
cd /root/trading_bot

# Kill bot first so its shutdown flush cannot overwrite the state we are about to write.
PID=$(pgrep -f "run_strategy.py.*config.mexc.json" 2>/dev/null | head -1 || true)
if [ -n "$PID" ]; then
    echo "Stopping bot (PID $PID) before writing weekday state..."
    kill -INT "$PID" 2>/dev/null || true
    for i in $(seq 1 15); do
        kill -0 "$PID" 2>/dev/null || { echo "Bot stopped after ${i}s."; break; }
        sleep 1
    done
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID" 2>/dev/null || true
fi
sleep 2

CONFIG="strategies/pct_ladder/config.mexc.json"
STATE=$(python3 -c "
import json, os
cfg = json.load(open('$CONFIG'))
base = os.path.dirname(os.path.abspath('$CONFIG'))
print(os.path.join(base, cfg['paths']['state_path']))
")

echo "=== Weekend End $(date -u '+%Y-%m-%d %H:%M UTC') ==="
echo "State file: $STATE"

python3 - "$STATE" <<'PYEOF'
import json, sys, os

state_path = sys.argv[1]
with open(state_path) as f:
    state = json.load(f)

extras = state.get("extras") or {}

weekday_step = extras.get("weekday_compound_buy_quote")
if weekday_step:
    extras["compound_buy_quote"]  = weekday_step
    extras["compound_sell_quote"] = weekday_step
    print(f"  Restored weekday step: ${weekday_step}")
else:
    print("  WARNING: weekday_compound_buy_quote not found — compound step unchanged")

for key in ("weekend_upper_pct", "weekend_lower_pct", "weekend_pro_levels", "weekday_compound_buy_quote"):
    extras.pop(key, None)

state["extras"] = extras

tmp = state_path + ".weekend_end.tmp"
with open(tmp, "w") as f:
    json.dump(state, f, indent=2, default=str)
os.replace(tmp, state_path)
print("  Weekend mode removed.")
PYEOF

echo "Weekday mode restored. Bot runner will restart with updated settings."
