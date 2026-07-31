#!/usr/bin/env bash
# Stop all 6 equity bots after close. Cron: 15:31 IST (10:01 UTC) Mon-Fri.
#
# 15:31 is after the 15:29:30 EOD-cancel, so open grid orders are already cancelled and state
# is clean (this is the "normal EOD-cancel restart is fine" case — no stale pro_*_oids). The
# bots run only during market hours; nothing is left polling off-hours.
set -u
cd /root/trading_bot || exit 1
/usr/bin/systemctl stop bot-rahul-reliance bot-rahul-vikaseco \
  bot-pratibha-shishind bot-pratibha-indothai bot-pratibha-coolcaps bot-pratibha-arl
echo "$(date -u +%FT%TZ) stopped all equity bots"
