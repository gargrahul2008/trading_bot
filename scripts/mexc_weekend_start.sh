#!/bin/bash
# mexc_weekend_start.sh — Switch MEXC bot to weekend mode (0.1% grid, 4 levels, half step).
# Called from mexc_compound_cron.sh (Saturday) which kills the bot first.
# If run standalone, this script kills the bot itself before writing state to
# prevent the shutdown flush from overwriting the new state.
set -e
cd /root/trading_bot

# Kill bot first so its shutdown flush cannot overwrite the state we are about to write.
PID=$(pgrep -f "run_strategy.py.*config.mexc.json" 2>/dev/null | head -1 || true)
if [ -n "$PID" ]; then
    echo "Stopping bot (PID $PID) before writing weekend state..."
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

echo "=== Weekend Start $(date -u '+%Y-%m-%d %H:%M UTC') ==="
echo "State file: $STATE"

python3 - "$STATE" <<'PYEOF'
import json, sys, os
from decimal import Decimal, ROUND_HALF_UP

state_path = sys.argv[1]
with open(state_path) as f:
    state = json.load(f)

extras = state.get("extras") or {}

current_step = Decimal(str(extras.get("compound_buy_quote") or extras.get("compound_sell_quote") or "2512"))
half_step = (current_step / Decimal("2")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

print(f"  Current weekday step : ${current_step}")
print(f"  Weekend step (half)  : ${half_step}")
print(f"  Grid pct             : 0.1% (was 0.2%)")
print(f"  Levels               : 4 buy + 4 sell (was 2+2)")

extras["weekday_compound_buy_quote"] = str(current_step)
extras["weekend_upper_pct"]  = "0.1"
extras["weekend_lower_pct"]  = "0.1"
extras["weekend_pro_levels"] = "4"
extras["compound_buy_quote"]  = str(half_step)
extras["compound_sell_quote"] = str(half_step)
state["extras"] = extras

tmp = state_path + ".weekend.tmp"
with open(tmp, "w") as f:
    json.dump(state, f, indent=2, default=str)
os.replace(tmp, state_path)
print("  State updated.")
PYEOF

echo "Weekend mode written to state. Bot runner will restart with updated settings."
