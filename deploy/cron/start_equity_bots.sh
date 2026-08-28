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

# --- Bots held down INDEFINITELY. Listed here = does not start, on any day, until the entry
# is removed from this list. Deliberately open-ended, not dated: a skip that expires by itself
# would silently put a bot back into the market on a morning nobody asked for it.
# ALL FOUR PRATIBHA BOTS are held down from 2026-08-17 on request (rahul): her account places
# no automated orders at all for now. Of Rahul's two, vikaseco joined them on 2026-08-28;
# bot-rahul-reliance is the only equity bot still trading.
#   bot-pratibha-shishind : stopped mid-session 2026-08-12, held down since. It was killed
#                           holding inventory and live sell orders, so check its state.json
#                           against the broker before restarting.
#   bot-pratibha-indothai / -coolcaps / -arl : held down 2026-08-17, stopped normally at EOD.
#   bot-rahul-vikaseco : held down 2026-08-28. It places nothing and cannot: state.json has
#                           traded_qty 0, core_qty 0, cash 0, lots [], while the config still
#                           assumes pro_base_qty 66184. It logged its own diagnosis at start —
#                           "net_sold DRIFT=-66184: tracked=0 but base(66184)-owned(0)=66184
#                           — a fill likely mis-booked" — and then warned "insufficient
#                           inventory" and "insufficient cash" ~12,600 times a day, ~97% of
#                           all equity-bot log volume. Its pnl_summary reports -100% portfolio
#                           P&L, which is an artefact of portfolio_start_value against an
#                           empty book, not a real loss.
#                           It was still polling Fyers every 5s on user1's app — the same rate
#                           budget bot-rahul-reliance and agent-rahul share — to place nothing.
#                           Before restarting: confirm from the broker what VIKASECO the
#                           account actually holds, then either restore the position or reset
#                           pro_base_qty and portfolio_start_value to match reality.
# Before restarting ANY of them, reconcile accounts/<user>/<run>/state/state.json with the
# broker — each day held down is a day its local view drifts from the real position.
HOLD_DOWN=" bot-pratibha-shishind bot-pratibha-indothai bot-pratibha-coolcaps bot-pratibha-arl bot-rahul-vikaseco "

# --- Skip for ONE dated morning only (e.g. the PCA engine is selling that holding at 09:00).
# Expires by itself. Leave SKIP_ON empty when unused.
SKIP_ON=""
SKIP_ONE_DAY=" "

SKIP_UNITS="$HOLD_DOWN"
if [ -n "$SKIP_ON" ] && [ "$(date -u +%F)" = "$SKIP_ON" ]; then
  SKIP_UNITS="$SKIP_UNITS$SKIP_ONE_DAY"
  echo "$(date -u +%FT%TZ) $SKIP_ON: also skipping for today only:$SKIP_ONE_DAY"
fi
[ -n "$(echo "$SKIP_UNITS" | tr -d ' ')" ] && echo "$(date -u +%FT%TZ) holding down:$SKIP_UNITS"

for u in "${BOTS[@]}"; do
  case "$SKIP_UNITS" in
    *" $u "*) echo "$(date -u +%FT%TZ) SKIPPED $u"; continue ;;
  esac
  /usr/bin/systemctl start "$u" && echo "$(date -u +%FT%TZ) started $u" || echo "$(date -u +%FT%TZ) FAILED to start $u"
  sleep 8
done
