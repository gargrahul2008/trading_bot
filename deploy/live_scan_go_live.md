# Bringing the 44-SMA live scanner up on this instance

You are a Claude Code session running on the trading instance
(`/root/trading_bot`), asked to finish deploying the 44-SMA scanner's live
alerting. The code was developed and tested on a separate dev machine and
pushed to `origin`; your job is to pull it here, wire up the two new cron
jobs, and verify they actually work — not to redesign anything.

## What this feature does

Two new scripts under `sma44_level1_intraday/scripts/`:

- `live_scan_after_market.py` — runs once after market close. Recomputes
  the full actionable-setup scan (reading the SAME config the research
  notebook `notebooks/scanner_universe_scan.ipynb` currently has, parsed
  directly from that notebook — see `sma44_level1_intraday/live/notebook_config.py`),
  writes `data/live_scan/watchlist_latest.json`, and sends a Telegram
  summary.
- `live_scan_poll.py` — runs every 5 minutes during market hours. Polls
  Fyers LTP for symbols on that watchlist, checks entry triggers with the
  same rule the backtest engine uses, and tracks a triggered position
  through to exit (SL/target/force-exit), alerting on Telegram both times.

Two matching wrapper scripts already exist and are the ONLY things you
should point cron at: `deploy/cron/live_scan_after_market.sh` and
`deploy/cron/live_scan_poll.sh`. Each has its exact crontab line (server
clock is UTC) documented in its own header comment — do not re-derive the
IST↔UTC conversion yourself, just read it off those files.

State lives under `data/live_scan/` (gitignored, created automatically on
first run): `watchlist_latest.json`, `watchlist_history/`,
`open_positions.json`, `running_bars.json`, `live_trade_log.csv`.

## Steps

1. **Pull.** `cd /root/trading_bot && git status` — refuse to pull onto
   uncommitted changes (same rule `deploy/deploy.sh` itself follows). If
   clean, `git pull`. Confirm `sma44_level1_intraday/live/`,
   `sma44_level1_intraday/scripts/live_scan_after_market.py`,
   `sma44_level1_intraday/scripts/live_scan_poll.py`,
   `deploy/cron/live_scan_after_market.sh`, and
   `deploy/cron/live_scan_poll.sh` all exist afterward.

2. **Telegram secrets.** `sma44_level1_intraday/secrets/telegram.json` is
   gitignored, so the pull will NOT bring it — it needs `{"bot_token":
   "<token>", "chat_id": "<id or [ids]>"}`. **Ask the human operator for the
   actual bot token and chat id — never invent, reuse, or guess one.** They
   may want a fresh bot (so equity alerts don't mix into the MEXC grid
   bot's channel at `strategies/pct_ladder/secrets/telegram.json`) or may
   tell you to point this feature at that same file instead — ask rather
   than assume. Create the file with mode 600.

3. **Confirm the Fyers account.** Both wrapper scripts run under
   `accounts/rahul/account.env` / `user1` (read-only quote polling, same
   account `deploy/cron/fetch_pnl.sh` already uses). If that's not the
   right account for this instance's setup, tell the human what you'd
   change it to before editing — don't silently pick a different one.

4. **Dry-run the after-market scan** (no cron yet, no Telegram spam):
   ```
   cd /root/trading_bot
   env $(grep -v '^#' accounts/rahul/account.env | xargs) \
     env/bin/python -m sma44_level1_intraday.scripts.live_scan_after_market --no-notify
   ```
   It should finish without a traceback and print a line like
   `live scan: N actionable setups across M symbols (...) in Ts`. Then
   check `cat data/live_scan/watchlist_latest.json` — valid JSON with a
   `setups` list (possibly empty, that's fine). If it errors on missing
   cached data, that's expected if this instance doesn't already have the
   `data/fyers_daily/` cache the dev machine used — flag that to the human
   rather than trying to bulk-fetch thousands of symbols yourself; that's a
   separate, larger data question.

5. **Dry-run one poll cycle** (safe outside market hours too — it just
   no-ops if `_in_market_hours` says no, or runs and finds nothing to check
   if the watchlist is empty):
   ```
   env $(grep -v '^#' accounts/rahul/account.env | xargs) \
     env/bin/python -m sma44_level1_intraday.scripts.live_scan_poll
   ```
   Should exit cleanly with no traceback.

6. **Show the human the two exact crontab lines** (copy them verbatim from
   the header comments in `deploy/cron/live_scan_after_market.sh` and
   `deploy/cron/live_scan_poll.sh` — do not retype/recompute them) and get
   their explicit go-ahead before touching crontab — this is a live trading
   instance and a bad cron edit is the kind of mistake that's annoying to
   debug at 9am. Once confirmed:
   ```
   crontab -l > /tmp/crontab.bak   # always keep a copy before editing
   (crontab -l 2>/dev/null; echo "0 13 * * 1-5 deploy/cron/live_scan_after_market.sh"; \
     echo "*/5 3-10 * * 1-5 deploy/cron/live_scan_poll.sh") | crontab -
   crontab -l   # verify both lines landed, nothing else was lost
   ```

7. **Logs.** Both scripts are meant to be run with `>> logs/<name>.log
   2>&1` by cron (see other `deploy/cron/*.sh` callers for the convention);
   `logs/` already exists in this repo. If you add the crontab lines
   yourself in step 6, append the log redirection to each line.

8. **Report back to the human**: what you pulled, whether the telegram
   secrets file is in place, the dry-run output from steps 4–5, and the
   final `crontab -l` output. Don't claim it's "live and alerting" until
   you've actually seen a real poll run during market hours (or the human
   confirms a test Telegram message arrived) — a clean dry run only proves
   the code path works, not that the schedule is correct or that Telegram
   delivery succeeds end to end.

## Guardrails

- Don't touch `deploy/deploy.sh`, systemd units, or any existing bot's
  enable state — this feature is unrelated to the per-account trading
  bots that infrastructure manages.
- Don't fabricate the Telegram bot token or chat id.
- Don't edit crontab without showing the human the exact lines first.
- If `sma44_level1_intraday`'s cached market data isn't present on this
  instance, don't attempt a large bulk fetch on your own initiative —
  that's a rate-limited, potentially hours-long operation the human should
  explicitly ask for.
