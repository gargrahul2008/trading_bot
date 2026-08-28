# Host state — control host survey

Survey taken **2026-08-28, 06:30–06:45 UTC** (11:59–12:14 IST) on `64.227.135.117`
(`hostname: Trading`), repo `/root/trading_bot` at `main` @ `b66558c`.

Read-only: nothing was started, stopped, edited or installed. The only write is this
file. `deploy/preflight.sh` was run (both `--quick` and full, including its outbound IP
checks) — no broker script was run.

Tokens, API secrets, TOTP keys, PINs and password hashes are deliberately absent below.
Where identity matters, the Fyers account id (`fy_id`) is shown — those are already in
the repo docs.

**The five things worth reading first:**

1. **The equity bots did not trade on Thursday 2026-08-27.** A branch checkout left on
   the host removed `deploy/cron/` for 13½ hours; cron's morning jobs failed with "not
   found". Tokens were not refreshed and neither Rahul bot started. §9, §10.
2. **~$25,012 of USDC left the MEXC account on 2026-08-26 and no bucket state was
   updated.** The hourly audit has failed and alerted every hour since — 41 consecutive
   Telegram alerts. §4.
3. **MEXC buckets 1 and 3 are frozen out of the market**, sitting all-cash with
   reference prices 16% and 25% *below* spot. §4.
4. **`bot-rahul-vikaseco` is running but is a no-op**, emitting ~12,600 warnings a day.
   Its config claims 66,184 shares the account does not hold. §3.
5. **Pratibha's four bots have been held down since 2026-08-17**; their state files have
   not moved since and now drift further from the broker every day. §3.

---

## 1. Machine

| | |
|---|---|
| Boot | `2026-03-02 06:07:51 UTC` — up 179 days |
| Kernel | `Linux Trading 6.8.0-101-generic #101-Ubuntu SMP PREEMPT_DYNAMIC x86_64` |
| Load | `0.50 / 0.25 / 0.14` — idle for an 8-CPU-limit box |
| Disk `/` | 116 G total, **51 G used, 65 G free (45%)** |
| Memory | 7.8 G total, 4.1 G used, 3.5 G cache, **3.6 G available** |
| Swap | **none** |
| IPs | `64.227.135.117` (public), `100.109.109.19` (Tailscale), `10.47.0.5`, `10.122.0.2` |

Nothing is close to a hard limit today, but two things are worth knowing:

- **No swap.** A memory spike has nowhere to go but the OOM killer, and the biggest
  processes on the box are live trading bots.
- **Disk grows monotonically and nothing rotates it.** `/root/trading_bot` is 39 G of
  the 51 G used. The largest items:

  | Path | Size | Growth |
  |---|---|---|
  | `strategies/pct_ladder/state/bucket2/pnl_points.csv` | 7.7 GiB | since 2026-05-28 |
  | `logs/mexc_bucket2_runner.log` | 5.3 GiB | since 2026-06-08 |
  | `strategies/pct_ladder/state/pnl_points.csv` | 2.1 GiB | dead since 2026-07-28 |
  | `strategies/pct_ladder/state/bucket1/pnl_points.csv` | 1.8 GiB | live |
  | `logs/mexc_bucket1_runner.log` | 1.6 GiB | since 2026-08-13 |
  | `logs/mexc_bucket3_runner.log` | 0.5 GiB | since 2026-08-13 |
  | `fyersRequests.log` (repo root) | 297 MB | live |
  | `logs/india_strategy.log` | 54 MB | dead since 2026-07-30 |

  `logrotate.timer` runs daily but covers none of these — they are not in
  `/etc/logrotate.d`. Combined write rate works out to roughly **0.3 GB/day**, so 65 G
  is on the order of six months of headroom. Not urgent; not indefinite either.

## 2. Everything that runs

### systemd

`systemctl list-unit-files 'bot-*' 'agent-*' 'fyers-*' 'mexc-*'` returns 11 units. There
are **no `mexc-*` units at all** — the crypto side does not use systemd (§4).

| Unit | Enabled | Active | Started by |
|---|---|---|---|
| `agent-rahul.service` | enabled | **active** (06:19:54 today) | systemd (boot + `Restart=always`) |
| `agent-pratibha.service` | enabled | **active** (06:19:54 today) | systemd |
| `agent-piyush.service` | enabled | **active** (06:19:54 today) | systemd |
| `bot-rahul-reliance.service` | disabled | **active** (03:25:01 today) | cron → `start_equity_bots.sh` |
| `bot-rahul-vikaseco.service` | disabled | **active** (03:25:09 today) | cron → `start_equity_bots.sh` |
| `bot-pratibha-shishind.service` | disabled | inactive | held down since 2026-08-17 |
| `bot-pratibha-indothai.service` | disabled | inactive | held down since 2026-08-17 |
| `bot-pratibha-coolcaps.service` | disabled | inactive | held down since 2026-08-17 |
| `bot-pratibha-arl.service` | disabled | inactive | held down since 2026-08-17 |
| `fyers-pca-engine.service` | disabled | inactive | one-shot timer, last fired 2026-08-10 |
| `fyers-pca-engine.timer` | disabled | inactive | dated `OnCalendar=2026-08-10`, spent |

Non-trading units running: `tailscaled`, `do-agent`, `droplet-agent`, `sshd`, `cron`,
`systemd-resolved`.

**Finding — the "impossible to enable" bot units are not the ones installed.** Commit
`9bc3a4a` removed the `[Install]` section from the generated bot units so that
`systemctl enable` would refuse them. `deploy/systemd/generated/bot-*.service` were
regenerated today at 06:16 and do carry that change, but the units actually installed in
`/etc/systemd/system/` are the *old* ones and still contain:

```
[Install]
WantedBy=multi-user.target
```

The agent units are identical between generated and installed; only the six bot units
have drifted. Today they are all `disabled`, so nothing starts at boot regardless — but
the safety property the commit was written to guarantee is not in force on this host. It
needs a `cp` + `daemon-reload` in a non-trading window to become real.

### cron

Full `crontab -l` for `root` (the only crontab on the box; `/etc/cron.d` holds only
distro defaults). Times are UTC; IST = UTC+5:30.

| Schedule | Job | What it does | State |
|---|---|---|---|
| `0 3 * * *` | `deploy/cron/refresh_tokens.sh` | Mints the day's Fyers access tokens for user1/2/3, each through its own egress; then syncs user1's token into FyersFire | **active** |
| `25 3 * * 1-5` | `deploy/cron/start_equity_bots.sh` | `systemctl start` the 6 equity bots, 8s apart, minus `HOLD_DOWN` | **active** |
| `1 10 * * 1-5` | `deploy/cron/stop_equity_bots.sh` | `systemctl stop` all 6 | **active** |
| `10 10 * * 1-5` | `deploy/cron/fetch_pnl.sh` | Pulls broker tradebook/charges into `accounts/*/reports/` | **active** |
| `52 3 * * 1-5` | `deploy/cron/btst_paper.sh exit` | BTST paper: sell book at 09:20 open, compute signal | **active** |
| `36 9 * * 1-5` | `deploy/cron/btst_paper.sh entry` | BTST paper: place the pre-decided 15:05 buys | **active** |
| `* * * * *` | `scripts/mexc_watchdog.sh` | Restarts any of the 3 MEXC bucket runners that is down | **active** |
| `* * * * *` | `scripts/mexc_alerts.py` | Per-bucket health alerts to Telegram | **active** |
| `* * * * *` | `scripts/mexc_trade_verify.py` | Appends new trades to a verify CSV | **active** (operating on 2026-03/2026-05 era files) |
| `0 */2 * * *` | `scripts/mexc_run_all.sh` | Portfolio snapshot + PnL breakdown | **active** |
| `0 0,8,16 * * *` | `scripts/mexc_telegram_cron.sh` | 8-hourly MEXC Telegram report | **active** |
| `0 * * * *` | `scripts/mexc_audit_cron.sh` | PnL/reconciliation audit; Telegram **only on failure** | **active — failing every hour, see §4** |
| `30 0 1 * *` | `scripts/mexc_monthly_cron.sh` | Monthly earnings report | **active** |
| `0 0 * * *` | `scripts/mexc_compare_cron.sh` | Live-vs-backtest comparison | **active but self-disabled** — logs `mexc_compare_cron disabled — see script header` and exits |
| `*/5 * * * *` | `scripts/start_token_server.sh` | Watchdog: starts `token_server.py` on :8502 if absent | **active** |
| `0 2 * * *` | `scripts/fetch_binance_ticks_cron.sh` | Downloads yesterday's Binance ETHUSDT ticks | **active** |
| `0 0 * * *` | `scripts/mexc_compound_cron.sh` | Daily compounding | **commented out** (`#DISABLED_FOR_DEPLOY`) |
| `44 3` / `1 10 * * 1-5` | `start_shishind.sh` / `stop_shishind.sh` | Old single-bot lifecycle | **commented out** |

Two comment-block leftovers: the header comments still describe `start_india.sh` /
`stop_india.sh` at 08:55/15:31 and a "VIKASECO sell-first grid" cron, but **no such
entries exist** — those runs moved to `accounts/` and are covered by
`start_equity_bots.sh`. The `start_*.sh` / `stop_*.sh` scripts are still present at the
repo root (§7).

### timers

`systemctl list-timers --all` shows 18 timers, all distro maintenance
(`apt-daily`, `logrotate`, `fstrim`, `man-db`, `sysstat`, `motd-news`,
`droplet-agent-update`, …) plus the spent `fyers-pca-engine.timer`. Nothing else
trading-related is timer-driven.

### processes

Trading-related processes, oldest first:

| PID | Started | Command | Explained by |
|---|---|---|---|
| 2542869 | 2026-05-05 | `scripts/token_server.py --port 8502` | `*/5` cron watchdog |
| 1244507/1244516 | 2026-06-08 | `mexc_bucket2_runner.sh` → `run_strategy.py --config …bucket2.json` | `mexc_watchdog.sh` |
| 3374103 | 2026-08-12 | `scripts/live_fib_bot.py --config configs/fib_live_dualfeed.json` | **nothing** — manual `nohup` |
| 3440810/3440816 | 2026-08-13 | `mexc_bucket3_runner.sh` → `…bucket3.json` | `mexc_watchdog.sh` |
| 3445419/3445429 | 2026-08-13 | `mexc_bucket1_runner.sh` → `…bucket1.json` | `mexc_watchdog.sh` |
| 3931295/97/98 | 2026-08-17 | `streamlit run dashboard/streamlit_app.py` on `100.109.109.19:8501` | **nothing** — manual, inside `screen -dmS dashboard` |
| 935758 | today 03:25 | `run_strategy.py --config accounts/rahul/reliance/config.json` | `bot-rahul-reliance.service` |
| 935787 | today 03:25 | `run_strategy.py --config accounts/rahul/vikaseco/config.json` | `bot-rahul-vikaseco.service` |
| 953321/22/24 | today 06:19 | `webapp.agent.main --user {piyush,rahul,pratibha}` | `agent-*.service` |

**Stray process:** PID `1806407`, alive since **2026-06-14** (74 days), is a leftover
Claude Code shell:

```
bash -c … eval 'cd /root/trading_bot/permanent_grid/notebooks;
  while pgrep -f "nbconvert.*permanent_grid_backtest" >/dev/null; do sleep 15; done; …'
```

It is polling for an `nbconvert` job that finished long ago and its parent session is
gone. Harmless (one `sleep` per 15s), but it is a process no unit or cron explains and
should be reaped.

**Secret on a command line:** `token_server.py` is invoked with `--secret <value>`, so
the shared secret is visible to anything that can run `ps aux`. Everything on this box
runs as root, so the practical exposure is low, but it also lands in any `ps` output
pasted into a ticket or a chat. `scripts/token_server_secret.txt` already exists — the
server could read the file directly instead.

### screen / tmux

No tmux. Five detached `screen` sessions, four of them abandoned:

| Session | Since | Contents |
|---|---|---|
| `3931295.dashboard` | 2026-08-17 | **live** — the Streamlit dashboard |
| `2597615.fastclaude` | 2026-08-05 | abandoned |
| `1813970.migration` | 2026-07-28 | abandoned (the account migration work) |
| `3416595.fibclaude` | 2026-06-30 | abandoned |
| `319300.claude` | 2026-03-10 | abandoned |

Only `dashboard` holds a running process. The other four are empty shells.

## 3. The equity side

### `accounts/` inventory

| User | `fy_id` | Run | Symbol | Product | Sizing | Held down? |
|---|---|---|---|---|---|---|
| rahul | XR11308 | `reliance` | `NSE:RELIANCE-EQ` | **MTF** (3×) | 140 sh/level, step ₹14, 1 pro level | no — **running** |
| rahul | XR11308 | `vikaseco` | `NSE:VIKASECO-EQ` | CNC | 50,000 sh/level, step ₹0.09 | no — **running (no-op, see below)** |
| pratibha | XP12698 | `shishind` | `BSE:SHISHIND-X` | CNC | 10,000 sh/level, step ₹0.18, 3 levels | **YES** |
| pratibha | XP12698 | `indothai` | `NSE:INDOTHAI-EQ` | CNC | 500 sh/level, step ₹8, 3 levels | **YES** |
| pratibha | XP12698 | `coolcaps` | `NSE:COOLCAPS-ST` | CNC | 5,000 sh/level, step ₹0.80 | **YES** |
| pratibha | XP12698 | `arl` | `BSE:ARL-B` | CNC | 1,900 sh/level, step ₹2.25 | **YES** |
| piyush | FAK31683 | — | — | — | — | **no runs configured at all** |

`HOLD_DOWN` in `deploy/cron/start_equity_bots.sh` is `" bot-pratibha-shishind
bot-pratibha-indothai bot-pratibha-coolcaps bot-pratibha-arl "` — deliberately open-ended
and well documented in the script. `SKIP_ON` (the one-day skip) is empty.

**Piyush is half-onboarded.** `accounts/piyush/account.env` exists, his token refreshes
daily, and `agent-piyush` runs — but he has no run directory, no config, no unit, and his
`reports/` holds only `bot_pnl_history.json`. He is visible on the dashboard and invisible
everywhere else. That may well be intended (dashboard-only account), but nothing in the
repo says so.

### `state/` per run

| Run | `state.json` last modified | Files | Drift? |
|---|---|---|---|
| `rahul/reliance` | **2026-08-28 06:30** (live) | 11 | no |
| `rahul/vikaseco` | **2026-08-28 06:30** (live) | 10 | no — but the contents are wrong, below |
| `pratibha/shishind` | 2026-08-17 09:59 | 10 | **7 trading days stale** |
| `pratibha/indothai` | 2026-08-17 09:59 | 9 | **7 trading days stale** |
| `pratibha/coolcaps` | 2026-08-17 09:59 | 9 | **7 trading days stale** |
| `pratibha/arl` | 2026-08-17 09:59 | 11 | **7 trading days stale** |

Pratibha's four are stale *because* they are held down, which is expected — but it is the
drift the hold-down comment itself warns about: "each day held down is a day its local
view drifts from the real position." That has now been 7 trading days (2026-08-18 →
2026-08-28) with no reconciliation recorded anywhere on the host. `shishind` is the
sharp one: it was killed mid-session on 2026-08-12 holding inventory and live sell
orders, and its `trades.jsonl` and `rejects.jsonl` both stop on 2026-08-12 while
`state.json` ran to 2026-08-17.

**`rahul/vikaseco` is running and doing nothing.** Its `state.json` says
`traded_qty: 0`, `core_qty: 0`, `cash: "0"`, `lots: []`, `portfolio_value: "0.00"`,
while `extras.pro_base_qty_NSE:VIKASECO-EQ` is `66184`. Every 5 seconds it logs both:

```
WARNING | runner | PRO NSE:VIKASECO-EQ SELL L1 @ 1.30: insufficient inventory, skipping remaining
WARNING | runner | PRO NSE:VIKASECO-EQ BUY  L1 @ 1.12: insufficient cash, skipping remaining
```

— 6,290 and 6,289 times respectively on 2026-08-27 alone. On start it logged the
diagnosis itself:

```
PRO NSE:VIKASECO-EQ net_sold DRIFT=-66184: tracked=0 but base(66184)-owned(0)=66184
  — a fill likely mis-booked; verify before trusting the sell cap
```

So the account holds no VIKASECO, the config still says 66,184 shares, and the bot has
nothing to sell and no cash to buy with. Two consequences beyond the noise: its
`pnl_summary.json` reports `portfolio_pnl: "-79420.80"` at `-1.0` (i.e. −100%), which is
an artefact of `portfolio_start_value: 79420.8` against a zero book and **not** a real
loss; and the last reject recorded is `CIRCUIT_LIMIT`. This bot is burning a Fyers rate
budget every 5 seconds to place nothing.

`rahul/reliance` by contrast is trading normally: `traded_qty 1808 @ avg ₹1383.83`,
16 lots, `cash ₹747,889`, `realized_pnl ₹51,014.60`, ref ₹1290.90, mark ₹1281.50, one
buy and one sell order working. At today's mark the open position carries roughly
**−₹185,000 unrealized** against +₹51,015 realized. Note `initialized: false` in its
symbol state despite a live position — worth a look, though it does not appear to stop
it trading.

### `fyers_auth.json`

Repo root, 7,782 bytes, rewritten today at 03:00:02 UTC.

| Key | Label | `accounts/` dir | `auto_refresh` | `token_updated_at` | Egress |
|---|---|---|---|---|---|
| `user1` | Rahul | `accounts/rahul` | true | 2026-08-28T03:00:02Z | **host IP** `64.227.135.117`, no proxy |
| `user2` | Pratibha | `accounts/pratibha` | true | 2026-08-28T03:00:04Z | `http://157.245.108.24:3128` |
| `user3` | Piyush | `accounts/piyush` | true | 2026-08-28T03:00:06Z | `http://15.252.102.31:3128` (EC2 EIP) |
| `user4` | User 4 | — | false | never | placeholder (`YOUR_APP_ID`) |

All three live access tokens expire **2026-08-29 00:30 UTC**; refresh tokens expire
2026-09-12 00:30 UTC. MTF is enabled on user1 only — consistent with reliance being the
only MTF run. All three `redirect_uri`s point at `http://100.109.109.19:8501/fyers-auth`,
i.e. the Streamlit dashboard on the Tailscale IP — so the *manual* auth path depends on
that screen session being up (the automated path does not).

**Finding — file permissions.** `fyers_auth.json` is mode **644** and holds, in
plaintext, each account's API secret key, TOTP seed and login PIN alongside the tokens.
`webapp/agent.env` and `webapp/dashboard.env` are correctly `600`; so should this be.
`dashboard_access.json` (repo root) and `strategies/pct_ladder/secrets/*.json` (MEXC API
keys, Telegram bot tokens) are also 644. Everything runs as root so this is not an
escalation path today, but it is the difference between "a secret" and "a secret one
`scp` away".

### `reports/`

| Account | Contents |
|---|---|
| `rahul` | `agent_claims.json` (today 04:52), `bot_pnl_history.json`, `broker_pnl.json`, `pnl_seed.json`, `portfolio.json`, `trades_all.jsonl` — all refreshed 2026-08-27 10:10 |
| `pratibha` | same six files, all 2026-08-27 10:10 (`agent_claims.json` 2026-08-26) |
| `piyush` | **only** `bot_pnl_history.json` (183 bytes) |

`fetch_pnl.sh` produced no 2026-08-28 output yet — it runs at 10:10 UTC, still ahead.
In `rahul/broker_pnl.json`, several symbols (`ANTHEM`, `DMART`) show
`broker_realized` non-zero against `bot_realized: 0.0` with the difference booked as
`discrepancy` — that is manual trading in the same account, not a bug, but the field
name reads like an error.

## 4. The crypto side

Three MEXC bucket bots, all live, all on `ETHUSDC`, **none under systemd**:

| | Bucket 1 | Bucket 2 | Bucket 3 |
|---|---|---|---|
| Config | `strategies/pct_ladder/config.mexc.bucket1.json` | `…bucket2.json` | `…bucket3.json` |
| Runner | `scripts/mexc_bucket1_runner.sh` | `…bucket2_runner.sh` | `…bucket3_runner.sh` |
| Grid | 3% | 10% | 2% |
| Mode | proactive, 2 pro levels | reactive | proactive, 4 pro levels |
| Sizing | `banded_qty`, quote 4,875 | `fixed_quote` 4,700 | `banded_qty`, quote 9,236 |
| State | `state/bucket1/state_2026_05_28_v1.json` | `state/bucket2/state_2026_05_28_v1.json` | `state/bucket3/state_2026_07_31_v1.json` |
| Up since | 2026-08-13 10:40 | 2026-06-08 19:23 | 2026-08-13 09:48 |

All three share `broker.secrets_file: secrets/mexc_spot.json`, `isolated_cash: true` and
`adopt_broker_inventory: false`.

**How they start.** Each runner is a bash respawn loop holding a `flock` on
`/tmp/mexc_bucketN_runner.lock`; it restarts the bot 5s after any exit and Telegrams each
transition. `mexc_watchdog.sh` (every minute, all three buckets) restarts a runner that is
absent entirely. There are no units and no `[Install]`-equivalent — the watchdog *is* the
supervisor.

**Relationship to `strategies/pct_ladder/`.** This is the one part of the tree that is
still genuinely live rather than superseded by `accounts/`. The bucket configs, state and
secrets all live under `strategies/pct_ladder/`, and `common/engine/generic_runner.py` is
shared with the equity bots. Everything *else* in that directory is historical:
`config.arl.json`, `config.coolcaps.json`, `config.indothai.json`, `config.shishind.json`,
`config.vikaseco.json`, `config.reliance.mtf.json` and `config.midcap.mtf.json` are the
pre-migration originals — each `accounts/*/*/config.json` records its ancestor in
`_migrated_from`. `config.mexc.json` (the pre-2-bucket single bot) is kept for rollback.
The top level of `strategies/pct_ladder/state/` holds the 2026-03 → 2026-05 era files,
including the dead 2.1 GiB `pnl_points.csv`.

### The USDC reconciliation has been failing since 2026-08-26

`scripts/mexc_audit.py` runs hourly and alerts only on failure. It last passed at
**2026-08-26 12:00 UTC** and has returned `rc=1` on **every run since — 41 consecutive
hourly Telegram alerts.** The gap appeared in two steps:

```
2026-08-26 12:00   USDC tracked $93,767.78  vs account $93,767.78   PASS
2026-08-26 13:00   USDC tracked $93,767.78  vs account $79,575.98   FAIL   (−$14,191.80)
2026-08-26 14:00   USDC tracked $93,767.78  vs account $68,755.98   FAIL   (−$10,820.00)
… unchanged through 2026-08-28 06:00
```

Total **−$25,011.80**. ETH reconciles exactly (`21.1111` tracked vs account) throughout,
and every per-bucket invariant is `OK`. Two withdrawals an hour apart, with ETH untouched,
reads as a deliberate transfer of USDC off the exchange — but no bucket's `cash` was
reduced to match, so the buckets collectively believe they hold $25 k that is not there
(bucket1 $38,076 + bucket2 $9,405 + bucket3 $46,286 = $93,767.78 against $68,755.98
actual). Per `docs/mexc_pnl_model.md` the fix after any manual movement is to edit the
bucket's cash *and* reset `portfolio_start_value` — neither happened.

There is also a **13-hour hole in the audit itself**: no run between 2026-08-26 15:00 and
2026-08-27 05:00, because `scripts/mexc_audit_cron.sh` did not exist in the working tree
during that window (§9).

Latest audit (2026-08-28 06:00, ETH $2,494.31):

```
bucket        ETH       cash   realized   unreal   start_val   invariant
bucket1    0.6256     38,076    2,582.4    312.4      36,742          OK
bucket2    1.7817      9,405    1,270.9    889.6      11,688          OK
bucket3    0.9948     46,286      753.3    551.9      47,462          OK
HODL      17.7090   (cost basis not set — HODL PnL excluded)
Buckets: realized $4,606.7 + unrealized $1,753.9 = $6,360.6
Account value: $121,413.71  (invested baseline: $115,053.12)
```

### Buckets 1 and 3 are frozen out of the market

ETH is at **$2,502.89**. The buckets' reference prices are not:

| Bucket | Ref | Spot vs ref | ETH held | Status |
|---|---|---|---|---|
| 1 | $2,151.83 | **+16.3%** | 0.6256 | all-cash; wants to sell 4.33 ETH, has 0.63 |
| 2 | $2,417.18 | +3.5% | 1.7817 | inside its 10% band, still cycling |
| 3 | $2,008.58 | **+24.6%** | 0.9948 | all-cash; wants to sell 17.60 ETH, has 0.99 |

Buckets 1 and 3 log this on every tick:

```
PRO ETHUSDC SELL: insufficient ETH (need=17.602356 have=0.99481) — waiting for pending BUY to fill
```

Their resting buys sit 2–3% below a reference that is itself 16–25% below spot, so ETH
would have to fall ~$350 (bucket1) or ~$490 (bucket3) before either trades again. Both run
`pro_drift_recenter: false`, so they will never re-anchor on their own — the repo's stated
intent is that a freeze is a "fund me" signal for a human. That design is working as
written; what is new is the *direction* and the *size*. This is not a downside freeze
awaiting a top-up, it is an upside run-away: both buckets sold their inventory into the
rally and are now sitting on ~$84 k of idle cash. Worth an explicit decision rather than
letting it ride, and worth noting that the $25 k that left the account is exactly the
kind of thing a human does when a grid has gone all-cash.

**Also unresolved:** the ~17.71 ETH `HODL` position outside all buckets still has no cost
basis set (`HODL_COST_PRICE` in `scripts/mexc_audit.py`), so account-level PnL excludes it.

## 5. Anything else trading-related

| Thing | Running? | Started by | What it does | Depended on by |
|---|---|---|---|---|
| **Token server** (`scripts/token_server.py`, :8502) | **yes**, since 2026-05-05 | `*/5` cron watchdog | Serves Fyers tokens to off-host bots | Pratibha's old VPS — the log shows `served token for user='user2' to 157.245.108.24` continuing today, so **something is still pulling tokens from that box** |
| **`fyers-pca-engine`** (C#, `/root/FyersPcaEngine`) | no | one-shot timer, spent | Fires pre-decided limit orders at 09:00:00.000 IST through the per-account proxy | Last fired 2026-08-10 (2 Pratibha sells, both PLACED). Dormant until the timer is re-dated |
| **BTST paper bot** (`scripts/btst_paper_bot.py`) | between runs | `deploy/cron/btst_paper.sh` | Paper-live BTST over top250 + next250; 09:22 exit+signal, 15:06 entry | Telegram + Streamlit dashboard. Today: +37,748.65 overnight, 52,023.10 cumulative, 4 tranches open |
| **Fib bot** (`scripts/live_fib_bot.py`) | **yes**, since 2026-08-12 | **manual `nohup`** | Dual-feed paper scalper, ETHUSDT + SOLUSDT, signals from Binance / fills from MEXC, $5,000/trade | Nothing. Writes `artifacts/fib_live_dual/`. Both legs FLAT; cumPnL **ETH −$18.45, SOL −$234.75** |
| **Streamlit dashboard** (`dashboard/streamlit_app.py`) | **yes**, since 2026-08-17 | **manual, in `screen -dmS dashboard`** | The old dashboard, bound to `100.109.109.19:8501` (Tailscale only) | The three `redirect_uri`s in `fyers_auth.json` point at it. Its stdout (`/tmp/streamlit.log`) has not moved since 2026-08-25 — nobody has loaded a page in 3 days |
| **FyersFire** (`/root/trade/FyersFire`) | no | — | C# app; its `fyers.json` is rewritten daily by `refresh_tokens.sh` | Auth sync runs whether or not the app does |

The Streamlit log living in `/tmp` is a small trap: `systemd-tmpfiles-clean.timer` runs
daily and `/tmp` does not survive a reboot.

## 6. The dashboard we are building

### Unit status

All three agents are **enabled and active**, restarted **today at 06:19:54 UTC**:

```
agent-piyush    active (running)  user_key=user3  egress=http://15.252.102.31:3128   :9103
agent-rahul     active (running)  user_key=user1  egress=host IP (no proxy set)      :9102
agent-pratibha  active (running)  user_key=user2  egress=http://157.245.108.24:3128  :9101
```

All three log `listening on 127.0.0.1:<port> (trading disabled)` — no `--allow-trading`,
as intended. Ports match `deploy/agent_ports.json` exactly.

**Finding — every agent restart takes 90 seconds and ends in SIGKILL.** The journal shows
the same shape at each of today's three restarts:

```
06:08:40  agent-piyush: poller stopped
06:10:10  agent-piyush.service: Main process exited, code=killed, status=9/KILL
06:10:10  agent-piyush.service: Failed with result 'timeout'.
```

Exactly 90 s — systemd's default `TimeoutStopSec` — between the poller stopping and the
kill. The agent shuts its poller down on SIGTERM but the process never exits, so systemd
kills it and records the stop as a **failure**. Three restarts today, four in seven days,
all identical. It is not a restart loop (nothing is crashing; these are operator-driven
restarts) and no state is lost, but every `systemctl restart agent-*` costs 90 seconds of
blank dashboard and leaves a `Failed with result 'timeout'` in the journal that will
mislead whoever next reads it.

### `deploy/preflight.sh` — full output

```
── Environment
  repo     /root/trading_bot
  branch   main @ b66558c
  PASS  interpreter /root/trading_bot/env/bin/python
  PASS  working tree clean

── Secrets
  PASS  /root/trading_bot/webapp/agent.env (AGENT_TOKEN set, mode 600)
  PASS  /root/trading_bot/webapp/dashboard.env (DASHBOARD_PASSWORD_HASH set, mode 600)

── Units
  WARN  bot-pratibha-arl.service not installed
  WARN  bot-pratibha-coolcaps.service not installed
  WARN  bot-pratibha-indothai.service not installed
  WARN  bot-pratibha-shishind.service not installed
  WARN  bot-rahul-reliance.service not installed
  WARN  bot-rahul-vikaseco.service not installed
  PASS  agent-piyush.service enabled and active
  PASS  agent-pratibha.service enabled and active
  PASS  agent-rahul.service enabled and active
  PASS  no fyers-auth units installed (cron owns token refresh)

── Cron
  PASS  cron runs refresh_tokens.sh
  PASS  cron runs start_equity_bots.sh
  PASS  cron runs stop_equity_bots.sh

── Tokens
  PASS  Rahul: token refreshed 3.6h ago
  PASS  Pratibha: token refreshed 3.6h ago
  PASS  Piyush: token refreshed 3.6h ago
  SKIP  User 4: auto_refresh off, ignored

── Agents
  PASS  piyush: live, phase live, token reloads 1
  PASS  pratibha: live, phase live, token reloads 1
  PASS  rahul: live, phase live, token reloads 1

── Egress IPs
  PASS  piyush: 15.252.102.31 (via its proxy)
  PASS  pratibha: 157.245.108.24 (via its proxy)
  PASS  rahul: 64.227.135.117 (no proxy — host IP is its whitelisted IP)

── 20 pass  6 warn  0 fail
```

(`--quick` gives the same result with `WARN skipped (--quick)` for egress: 17 pass, 7 warn.)

**The six `not installed` warnings are wrong and worth fixing before they cost someone an
hour.** All six unit files exist in `/etc/systemd/system/`, and two of them —
`bot-rahul-reliance` and `bot-rahul-vikaseco` — were **actively trading real money while
preflight printed this**. The check is inferring "installed" from `systemctl is-enabled`,
but these units are *deliberately* `disabled` (cron owns their lifecycle, and commit
`9bc3a4a` went out of its way to make enabling them impossible). Preflight is warning
about exactly the state the design requires. It should test for the unit file's presence
and assert `disabled`, and separately report whether each bot is active vs held down.

### Env files

Both exist, both correct mode (contents not read):

```
-rw------- 1 root root  56 2026-08-26 14:43 webapp/agent.env
-rw------- 1 root root 223 2026-08-26 19:12 webapp/dashboard.env
```

### Has the API or UI ever been started here?

**Yes, by hand only — never as a service, and nothing is running now.** Evidence:

- `webapp/api/.venv` exists (built 2026-08-26 18:43); `webapp/api/app` was touched today
  at 05:44.
- `webapp/web/dist/` holds a built UI (`index.html` + `assets/`, 2026-08-26 19:03), so
  `npm run build` has been run.
- Shell history shows `uvicorn app.main:app --app-dir webapp/api --port 8000` (×4),
  `--port 8093` (×2), `npm run dev` (×3), `npm run build` (×1).
- No `api-*` or `dashboard-*` unit exists, nothing listens on 8000/8093/5173, and there
  is no API log in `logs/`.

So the API and UI have only ever run in the foreground of an SSH session. There is no
supervised deployment of either.

## 7. Repo state

```
branch : main
commit : b66558c535635fde6a7c74de903362dc8c9b4d2c
         "Add a host survey brief for the instance's Claude"  (2026-08-28 11:53 +0530)
remote : git@github.com:gargrahul2008/trading_bot.git
status : clean (git status --short is empty)
```

Working tree is clean and matches `origin/main`, which was pulled three times this
morning (05:44, 06:16, 06:27).

**Untracked / gitignored files that matter** — the host cannot be rebuilt from git alone
without these:

| File | Why it matters |
|---|---|
| `fyers_auth.json` | All three accounts' credentials + live tokens. Mode 644 |
| `accounts/{rahul,pratibha,piyush}/account.env` | Identity + proxy binding per account |
| `accounts/*/*/state/` | Every bot's resume state — the real position of record |
| `accounts/*/reports/` | P&L history, agent sticky claims |
| `webapp/agent.env`, `webapp/dashboard.env` | Agent shared secret, dashboard password hash |
| `strategies/pct_ladder/secrets/{mexc_spot,telegram,telegram_btst}.json` | MEXC API keys, Telegram bot tokens. Mode 644 |
| `strategies/pct_ladder/state/bucket{1,2,3}/` | The MEXC buckets' position of record |
| `scripts/token_server_secret.txt` | Token server shared secret |
| `dashboard_access.json` | Streamlit dashboard credentials. Mode 644 |
| `configs/fib_live_dualfeed.json`, `artifacts/fib_live_dual/` | Fib bot config + its entire paper book |

**Hand-edited units:** the six installed `bot-*.service` files no longer match their
generator output (§2). Nothing else in `/etc/systemd/system/` diverges.

**Repo-root scripts the cron no longer calls:** `start_india.sh`, `stop_india.sh`,
`start_shishind.sh`, `stop_shishind.sh`, `start_vikaseco.sh`, `stop_vikaseco.sh`,
`start_arl.sh`, `stop_arl.sh`, `start_coolcaps.sh`, `stop_coolcaps.sh`,
`start_indothai.sh`, `stop_indothai.sh`. These are the pre-migration lifecycle and are
now superseded by `deploy/cron/start_equity_bots.sh`. They are still executable and
would start a *second*, unsupervised copy of a bot against the same state files if
anyone ran one out of habit.

**Outside the repo, under `/root`:** `ladder_algo` (816 M), `xts` (209 M),
`option_ladder` (151 M), `FyersPcaEngine` (3.5 M, live-ish — §5), `TopAskSeller` (2.9 M),
`pca_engine` (2.0 M), `pace` (1.4 M), `trade` (560 K, contains FyersFire whose auth is
written daily by our cron). Only `FyersPcaEngine` and `trade/FyersFire` are wired to
anything on this host.

## 8. Ports

```
LISTEN  100.109.109.19:8501   streamlit (pid 3931298)     Tailscale IP only
LISTEN  127.0.0.1:9101        agent-pratibha (953324)     loopback only
LISTEN  127.0.0.1:9102        agent-rahul    (953322)     loopback only
LISTEN  127.0.0.1:9103        agent-piyush   (953321)     loopback only
LISTEN  0.0.0.0:8502          token_server.py (2542869)   ALL interfaces
LISTEN  0.0.0.0:22 / [::]:22  sshd
LISTEN  127.0.0.53:53, 127.0.0.54:53   systemd-resolved
LISTEN  100.109.109.19:54613  tailscaled
```

The agents are correctly loopback-only — the dashboard API is expected to reach them over
localhost, so nothing needs to change there.

`token_server.py` binds `0.0.0.0`, but **ufw saves it**: port 8502 is allowed only from
`157.245.108.24` (Pratibha's proxy). ufw default is `deny (incoming)` and the full ruleset
is:

```
22/tcp (OpenSSH)   ALLOW IN   Anywhere
8888               ALLOW IN   Anywhere
8502               ALLOW IN   157.245.108.24
```

**`8888` is open to the entire internet and nothing is listening on it.** It is a hole
left from something that no longer runs. Closing it is free; leaving it means the next
process that happens to bind 8888 is world-reachable by accident.

## 9. Logs

`ls -la logs/` — 7.6 G total, 26 files. Sizes and mtimes in §1; the ones that matter:

| File | Size | Last modified | Note |
|---|---|---|---|
| `mexc_bucket2_runner.log` | 5.3 G | live | unrotated |
| `mexc_bucket1_runner.log` | 1.6 G | live | unrotated |
| `mexc_bucket3_runner.log` | 480 M | live | unrotated |
| `mexc_runner.log` | 172 M | 2026-05-28 | dead (pre-2-bucket) |
| `india_strategy.log` | 52 M | 2026-07-30 | dead (pre-migration) |
| `vikaseco_strategy.log` | 16 M | 2026-07-30 | dead (pre-migration) |
| `fib_dual.log` | 11 M | 2026-07-30 | superseded by `artifacts/fib_live_dual/` |
| `mexc_alerts.log` | 16 K | 2026-06-29 | runs every minute, silent when healthy |
| `equity_bots.log`, `fyers_auto_auth.log`, `broker_pnl.log`, … | small | today 06:05 | see the crontab incident below |

The `accounts/*/*/logs/` directories exist but the bots log to journald via
`SyslogIdentifier`, so per-run error analysis below comes from `journalctl`.

### Equity bots — last two trading days (2026-08-27, 2026-08-28)

Explicitly checked for `-429`, `-16`, `Could not authenticate`, and restart loops:

| Pattern | reliance | vikaseco |
|---|---|---|
| `-429` (raw error code) | **0** | **0** |
| `-16` | **0** | **0** |
| `Could not authenticate` | **0** | **0** |
| Unit `Started` events | 2 (one per day, as scheduled) | 2 — **no restart loop** |

What is there instead:

**reliance** — 18 retryable `BrokerError`s across the two days, 2 of which surfaced as
`WARNING | fyers | RATE_LIMIT (429): backing off Ns then aborting call` and 2 as
`ERROR | runner | PRO loop error: Failed after 4 retries. last_error=BrokerError("Quotes
error: …")`. Four transient quote failures in two days on a 5-second poll is background
noise, and the retry/back-off is doing its job — worth knowing the 429s exist as
back-off events even though the literal string `-429` does not appear. Also two expected
one-offs on 2026-08-27: `PRO CAS freeze at 15:15:00` and `PRO CAS auction pass done`,
i.e. the closing-auction handling working as designed.

**vikaseco** — 12,579 warnings on 2026-08-27 alone (6,290 `insufficient inventory` +
6,289 `insufficient cash`), plus the single `net_sold DRIFT=-66184` diagnosis at startup,
3 rate-limit back-offs and 3 `PRO loop error` retry exhaustions. See §3. This one bot
produces ~97% of all equity bot log volume and none of it is informative.

### Agent logs

No `-429`, no `-16`, no `Could not authenticate` in either day. A single cluster of
transient failures at **06:06:50–06:07:07 today**, hitting all three agents at once:

```
WARNING fyers Retryable error (BrokerError). attempt=1/2/3 …
WARNING agent.poller rahul:    orders refresh failed:   Failed after 3 retries
WARNING agent.poller rahul:    funds refresh failed:    Failed after 3 retries
WARNING agent.poller pratibha: orders refresh failed:   Failed after 3 retries
WARNING agent.poller pratibha: holdings refresh failed: Failed after 3 retries
WARNING agent.poller piyush:   orders refresh failed:   Failed after 3 retries
WARNING agent.poller piyush:   trades refresh failed:   Failed after 3 retries
```

All three accounts, all three egress paths, one ~17-second window — that is Fyers being
briefly unavailable, not a per-account problem. The agents kept their last good payload
and recovered, which is the designed behaviour.

**Finding — the agents log every line twice.** Each message appears in two formats:

```
2026-08-28 06:35:00,806 WARNING fyers Retryable error (BrokerError). attempt=1 sleep=0.47s
2026-08-28 06:35:00 | WARNING | fyers | Retryable error (BrokerError). attempt=1 sleep=0.47s
```

Two handlers are attached to the same logger. Each agent writes ~87,000 journal lines a
day, so roughly half of ~260,000 lines/day across the three is pure duplication — it
fills the journal ring buffer twice as fast and halves how far back an incident can be
read.

### The 2026-08-27 outage — the most important thing in this section

`logs/equity_bots.log`:

```
2026-08-26T10:01:01Z stopped all equity bots
/bin/sh: 1: /root/trading_bot/deploy/cron/start_equity_bots.sh: not found
2026-08-27T10:01:01Z stopped all equity bots
2026-08-28T03:25:01Z holding down: …
2026-08-28T03:25:01Z started bot-rahul-reliance
```

`logs/fyers_auto_auth.log`:

```
/bin/sh: 1: /root/trading_bot/deploy/cron/refresh_tokens.sh: not found
2026-08-28T03:00:01Z refreshing user1 (rahul, direct)
```

`git reflog` explains it:

```
318f387 2026-08-26 14:40:05  checkout: moving from main to dashboard-agent
cf5cc36 2026-08-27 04:04:08  checkout: moving from dashboard-agent to main
725b8d8 2026-08-27 04:17:14  commit (merge): Merge branch 'dashboard-agent' into main
```

The working tree sat on `dashboard-agent` from **2026-08-26 14:40 to 2026-08-27 04:04
UTC**, and that branch predates `deploy/cron/`. Checking which cron-referenced scripts
existed at `318f387`:

| Script | On `dashboard-agent`? |
|---|---|
| `deploy/cron/refresh_tokens.sh` | **absent** |
| `deploy/cron/start_equity_bots.sh` | **absent** |
| `deploy/cron/stop_equity_bots.sh` | **absent** |
| `deploy/cron/fetch_pnl.sh` | **absent** |
| `deploy/cron/btst_paper.sh` | **absent** |
| `scripts/mexc_audit_cron.sh` | **absent** |
| `scripts/mexc_monthly_cron.sh` | **absent** |
| `scripts/mexc_watchdog.sh`, `mexc_alerts.py`, `mexc_run_all.sh`, `mexc_telegram_cron.sh`, `mexc_trade_verify.py`, `start_token_server.sh`, `fetch_binance_ticks_cron.sh`, `mexc_compare_cron.sh` | present |

So on **Thursday 2026-08-27**:

- **03:00 UTC** — no token refresh. The bots ran the day on Wednesday's tokens.
- **03:25 UTC** — **`bot-rahul-reliance` and `bot-rahul-vikaseco` never started. Rahul's
  equity book was not traded for the entire session.**
- **03:52 UTC** — BTST paper's exit leg did not run. The 09:36 entry then correctly
  refused (`RuntimeError: phase=overnight: exit hasn't run since the last entry —
  refusing to double-enter`) — the guard worked, but the day was skipped.
- **Hourly** — the MEXC audit went dark for 13 hours, straddling the withdrawal in §4.
- The MEXC bucket bots were **unaffected** (their scripts existed on both branches) and
  traded normally throughout.

The 10:01 stop worked because by then the tree was back on `main`. Everything self-healed
on 2026-08-28.

### One more thing in the logs, unrelated

At **06:05:21–06:05:23 today**, eleven log files each gained a line like
`0: command not found`, `25: command not found`, `README.md: command not found`,
`-bash: */5: No such file or directory`. These are cron *schedule fields* being
interpreted as shell commands: someone executed the crontab as a shell script from
`/root/trading_bot` (the `README.md` comes from `*` glob-expanding in that directory).
Each line's redirect still pointed at its log, which is why the errors landed there.

**Nothing was executed** — bash failed on the schedule field before reaching the `&&` or
the script path in every case. The effect is cosmetic log pollution, but it is worth
knowing that these lines are not evidence of a real failure, and worth not repeating: a
crontab with different leading fields could have fired live jobs.

## 10. My own read

### Wrong, fragile, or undocumented

1. **`bot-rahul-vikaseco` should not be running.** It holds no inventory and no cash,
   places nothing, logs 12,600 warnings a day, and reports a fictitious −100% P&L. It is
   consuming Fyers rate budget on user1's app — the same app `bot-rahul-reliance` and
   `agent-rahul` share, and the agent README is explicit that this budget is already
   tight. Either the 66,184 shares need restoring/reconciling or the run belongs in
   `HOLD_DOWN`. As it stands it is pure cost.
2. **The $25 k USDC gap has been alerting hourly for 41 hours with no response.** Whatever
   the cause, the current state is that three bots are trading against a cash figure that
   is $25 k too high, and the alarm that exists to catch exactly this is being ignored.
   The secondary damage is alert fatigue: 41 identical Telegram messages train everyone
   to swipe past the next one, which will be a different problem.
3. **Buckets 1 and 3 are frozen 16% and 25% below spot** with ~$84 k idle and
   `pro_drift_recenter: false`. Working as designed, but the design assumes a human
   answers the signal, and nobody has.
4. **Preflight warns about the correct state** (§6) and stays silent about the actual
   problems. A check that cries wolf six times on every run is worse than no check.
5. **Pratibha's four runs drift a little further every day**, and the reconciliation the
   hold-down comment demands has not been recorded anywhere.
6. **`fyers_auth.json`, `dashboard_access.json` and `strategies/pct_ladder/secrets/*` are
   mode 644**, holding API secrets, TOTP seeds and PINs in plaintext.
7. **Nothing rotates the MEXC logs or `pnl_points.csv`** (§1).
8. **ufw port 8888 is open to the world** with nothing behind it.
9. **The agents' 90-second SIGKILL shutdown** and **double-logging** (§6, §9).
10. **Twelve superseded `start_*.sh`/`stop_*.sh` scripts** sit executable at the repo root,
    each capable of launching an unsupervised second copy of a bot against live state.
11. **The stray 74-day-old Claude polling shell** (§2).

### Contradictions with the docs

- **`docs/master_host_runbook.md` is a month stale.** Its "Current state (as of
  2026-07-28)" lists "cut over Pratibha, then Rahul" as *pending your work*. Both are
  done: all six bot units exist, Rahul's two trade daily, Pratibha's four are
  deliberately held down. Anyone following the runbook literally would try to perform a
  cutover that already happened.
- **The runbook and `docs/multi_account_architecture.md` §3 both describe
  `fyers-auth-<user>` systemd units** (runbook step 4: "install+start
  `fyers-auth-pratibha`"). Those units no longer exist and are no longer generated —
  commit `9bc3a4a` stopped emitting them, and preflight now explicitly asserts
  `no fyers-auth units installed (cron owns token refresh)`. The docs describe the
  opposite of the enforced state.
- **Neither doc knows about `piyush` (user3).** Both are written for user1 + user2. The
  architecture doc's process diagram shows a generic "user3" placeholder, but the real
  Piyush account — EC2 proxy `15.252.102.31`, agent on 9103, no runs — is undocumented.
- **`webapp/agent/README.md` gives install commands that do not match this host:**
  `INSTALL_DIR=/opt/trading_bot` (host is `/root/trading_bot`) and `.venv/bin/python`
  (host uses `env/bin/python`). Its description of the agents' behaviour is accurate; only
  the paths are wrong.
- **The crontab's own comments describe jobs that no longer exist** — `start_india.sh` /
  `stop_india.sh` at 08:55/15:31 and a VIKASECO grid entry. Only comment headers remain.
- **`docs/multi_account_architecture.md` §7** is cited by the runbook as "do not touch
  the MEXC crypto bots or the shared engine files". Worth noting that the equity bots and
  the MEXC buckets *do* share `common/engine/generic_runner.py` on this host, so that rule
  is load-bearing and not merely advisory.

### Single points of failure — what breaks if this box reboots right now?

It is 12:15 IST, mid-session, so this is not hypothetical.

| Comes back by itself | How |
|---|---|
| `agent-rahul` / `-pratibha` / `-piyush` | `enabled`, `Restart=always` |
| MEXC buckets 1, 2, 3 | `mexc_watchdog.sh` restarts each within 60 s |
| Token server (:8502) | `*/5` cron watchdog |

| Does **not** come back |  |
|---|---|
| **`bot-rahul-reliance` and `bot-rahul-vikaseco`** | Units are `disabled` and only cron starts them, at 03:25 UTC on weekdays. **A reboot at any point during the session leaves Rahul's MTF book unmanaged until the next morning** — holding 1,808 RELIANCE shares on 3× leverage with no grid and no EOD cancel. This is the single biggest exposure on the host. |
| **Streamlit dashboard** | `screen -dmS`, manual. Also the `redirect_uri` target for all three accounts' manual auth flow, and `/tmp/streamlit.log` is wiped anyway. |
| **Fib bot** | manual `nohup` since 2026-08-12. Paper-only, so the loss is 15 days of continuous book. |
| **The dashboard API/UI** | never had a unit; only ever run in an SSH foreground. |

Secondary: no swap means a memory spike resolves as an OOM kill, and the OOM killer will
prefer the largest RSS — which is the Streamlit process (2.3%), then a bot. And a reboot
during 03:00–03:25 UTC would race the token refresh against the bot start.

The mitigation for the big one is not "enable the units" — that would defeat the
deliberate daily-restart design and would start held-down bots. It is a
`start_equity_bots.sh` invocation guarded by a session check, run once at boot.

### Open questions I could not answer read-only

1. **Where did the $25,011.80 of USDC go on 2026-08-26?** A withdrawal, a transfer to
   another MEXC wallet, and a conversion all look identical from here. Answering it needs
   the MEXC account history, which means a broker call.
2. **Does Rahul's Fyers account still hold any VIKASECO?** The bot says the account owns
   0 against a configured 66,184. Only the broker's holdings can settle it — and the
   answer decides whether the run is restored or retired.
3. **Are Pratibha's four positions still what her `state.json` files from 2026-08-17
   claim?** Seven trading days of possible manual activity, and `shishind` was killed
   mid-session holding live sell orders.
4. **What is still pulling tokens from `157.245.108.24`?** The token server has served
   `user2` to that IP as recently as today. The migration doc says the old VPS is a hot
   standby that soaks for three trading days — that was a month ago. Is a bot still
   running there, and could it be trading Pratibha's account in parallel with the
   held-down units here?
5. **Is Piyush meant to have runs?** Or is he a dashboard-only account?
6. **Was the 2026-08-27 outage noticed?** Nothing on the host records a human reacting to
   a missed trading day.
7. **Who restarted the agents three times between 06:08 and 06:20 today?** Two SSH
   sessions from `122.172.53.245` were open during the survey; that is almost certainly
   the other Claude or Rahul, but the journal cannot say.
8. **Is the fib bot still wanted?** 16 days running, cumulative −$253 across both legs,
   nothing supervising it, and its results are not in any report.
9. **Should `fyers-pca-engine` fire again?** Its timer is a spent one-shot dated
   2026-08-10; whether that is "done" or "needs re-dating each time" is not recorded
   anywhere on the host.
