#!/usr/bin/env bash
# BTST '1lg0' paper bot — daily runner for BOTH universes. Arg: exit | entry.
#
#   exit  (morning ~09:22 IST): sell book at 09:20 open + compute & store the signal.
#   entry (afternoon ~15:06 IST): place the pre-decided buys at the 15:05 close.
#
# Cron (server clock is UTC; IST = UTC+5:30):
#   52 3  * * 1-5   deploy/cron/btst_paper.sh exit    # 09:22 IST
#   36 9  * * 1-5   deploy/cron/btst_paper.sh entry   # 15:06 IST
# Mon–Fri only; NSE holidays are skipped below (update the list each year).
set -u
cd /root/trading_bot || exit 1
ACTION="${1:?usage: btst_paper.sh exit|entry}"
PY=/root/trading_bot/env/bin/python
LOG=logs/btst_paper.log
mkdir -p logs

TODAY=$(TZ=Asia/Kolkata date +%Y-%m-%d)
DOW=$(TZ=Asia/Kolkata date +%u)   # 1=Mon .. 7=Sun (IST)
if [ "$DOW" -gt 5 ]; then
    echo "$(date -u): weekend ($TODAY), skip $ACTION" >> "$LOG"; exit 0
fi

# NSE holidays 2026 — update each year (mirror of start_india.sh)
NSE_HOLIDAYS=(
    "2026-01-15" "2026-01-26" "2026-03-03" "2026-03-26" "2026-03-31" "2026-04-03"
    "2026-04-14" "2026-05-01" "2026-05-28" "2026-06-26" "2026-09-14" "2026-10-02"
    "2026-10-20" "2026-11-10" "2026-11-24" "2026-12-25"
)
for h in "${NSE_HOLIDAYS[@]}"; do
    if [ "$TODAY" == "$h" ]; then
        echo "$(date -u): NSE holiday ($TODAY), skip $ACTION" >> "$LOG"; exit 0
    fi
done

# Fyers auth egresses the whitelisted IP via account.env (same as the equity bots).
source <(grep -v '^#' accounts/rahul/account.env | sed 's/^/export /') 2>/dev/null

for UNI in universe_top250.json universe_next250.json; do
    echo "$(date -u): BTST paper $ACTION $UNI ($TODAY IST)" >> "$LOG"
    "$PY" scripts/btst_paper_bot.py --universe "$UNI" --action "$ACTION" >> "$LOG" 2>&1
done
echo "$(date -u): BTST paper $ACTION done" >> "$LOG"
