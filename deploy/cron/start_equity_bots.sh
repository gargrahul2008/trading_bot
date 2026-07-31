#!/usr/bin/env bash
# Start the 6 equity bots for the trading day. Cron: 08:55 IST (03:25 UTC) Mon-Fri.
#
# Why daily start/stop instead of always-on: a Fyers SDK session that idles overnight goes
# stale and gets wedged at HTTP 429 at the 09:00 open-bell burst, and never recovers on that
# same session (needs a fresh one). Starting fresh each morning avoids it — the proven
# pre-cutover pattern. Bots are stopped again after close by stop_equity_bots.sh.
#
# Staggered ~8s apart so all 6 don't fire their first quotes in lock-step at the open
# (belt-and-suspenders with the retry.py 429 back-off). The session guard idles any bot that
# gets started on an NSE holiday, so no holiday logic is needed here.
set -u
cd /root/trading_bot || exit 1
BOTS=(bot-rahul-reliance bot-pratibha-shishind bot-pratibha-indothai \
      bot-rahul-vikaseco bot-pratibha-coolcaps bot-pratibha-arl)
for u in "${BOTS[@]}"; do
  /usr/bin/systemctl start "$u" && echo "$(date -u +%FT%TZ) started $u" || echo "$(date -u +%FT%TZ) FAILED to start $u"
  sleep 8
done
