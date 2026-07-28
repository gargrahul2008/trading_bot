# Multi-Account Migration Framework

Goal: consolidate the existing per-VPS Fyers bots onto **one control host**, each account
keeping its own whitelisted static IP via a per-account proxy, with **zero disruption to
live trading** and **no changes to the MEXC bots**.

Read `multi_account_architecture.md` first. This document is the *how*, in ordered phases.

## Guiding rules (do not violate)

1. **One account at a time.** Never migrate two accounts in the same window.
2. **Only during a non-trading window** — after 15:30 IST, on a weekend, or an NSE holiday.
3. **The old VPS stays as hot standby** until the account has run cleanly on the host for
   the agreed soak period (default: 3 trading days). Do not decommission early.
4. **State files move by hand.** `state/` is gitignored and exists only on the source VPS.
5. **MEXC is out of scope.** Do not move, restart, or edit any crypto bot (see
   architecture doc §7).
6. **Verify before you trust.** Every cutover ends with the verification checklist. If any
   check fails, roll back (§ Rollback) — do not "fix it live."

---

## Phase 0 — Inventory & provisioning (no disruption)

Done once, before touching anything. Nothing here affects running bots.

1. **Inventory each existing account.** For every live account, record:
   - source VPS + its current public IP (the one whitelisted in Fyers today)
   - `user_key`, `client_id`
   - config path on the VPS (e.g. `strategies/pct_ladder/config.shishind.json`)
   - `paths.state_path`, `trades_path`, `rejects_path`
   - which strategy + symbols + product type it trades
   - auth mode (json / http / db) and, if http, the token service URL
2. **Provision the control host** — one machine (or your existing dev box promoted to prod),
   clone of this repo, Python venv installed, no bots running yet.
3. **Provision one static-IP proxy per account.** Options (architecture doc): a small
   dedicated-IP VPS running tinyproxy/3proxy, or a dedicated-static-IP proxy service. The IP
   **must be static** (whitelistable). Record each proxy URL, e.g. `http://10.0.0.6:3128`.
4. **Whitelist each proxy IP in that account's Fyers API app.** This is the gate — until
   the new IP is registered, orders from it will be rejected. You can whitelist the new IP
   *in addition to* the old VPS IP if Fyers permits multiple, which makes cutover safer.
5. **Confirm the proxy tunnels HTTPS (CONNECT).** Test:
   `HTTPS_PROXY=http://10.0.0.6:3128 curl -s https://api.ipify.org` → must print the
   proxy's IP, not the host's.

**Exit criteria:** host ready, N proxies live, each proxy IP whitelisted and egress-verified.

---

## Phase 1 — Build the account layout on the host (no disruption)

Additive only. Live bots on the old VPSs keep running untouched.

1. **Generate the per-run configs** from the live `strategies/` configs. The mapping of
   which runs belong to which user lives in `deploy/build_account_configs.py :: MAPPING`
   (currently rahul→reliance,vikaseco; pratibha→shishind,indothai,coolcaps,arl):
   ```
   python deploy/build_account_configs.py
   ```
   This writes `accounts/<user>/<strat>/config.json` (auth converged to local `json`,
   `paths` repointed into each run's `state/`, SDK `log_path` isolated) and creates each
   run's `state/` and `logs/`. Re-run right before cutover to capture the latest live params.
2. **Write each `accounts/<user>/account.env`** — the account's identity + its IP:
   ```
   ACCOUNT_ID=<user>
   FYERS_USER_KEY=<user_key>
   # HOME account (IP == master host): omit the proxy. Otherwise its static-IP proxy:
   HTTPS_PROXY=http://<whitelisted-ip>:3128
   HTTP_PROXY=http://<whitelisted-ip>:3128
   ```
3. **Provision each non-home proxy** per `deploy/proxy/README.md` and verify egress from the
   master host.
4. **Generate the systemd units** (do **not** start them yet):
   ```
   INSTALL_DIR=/opt/trading_bot python deploy/gen_systemd_units.py
   sudo cp deploy/systemd/generated/*.service /etc/systemd/system/ && sudo systemctl daemon-reload
   ```
5. **Verify `.gitignore`** ignores `accounts/**/state/`, `accounts/**/logs/`,
   `accounts/**/*.env` (already set in this repo).

**Exit criteria:** each run has a config + state/ + logs/, each user an account.env, proxies
verified, units installed but stopped. Old VPSs still own live trading.

---

## Phase 2 — Per-account cutover (one account, one window)

Repeat this whole phase for **one** account per non-trading window.

### 2a. Move the resume state (point 1 — what must move)

A run resumes smoothly **only if** its state comes across. Each **strategy run** has its own
state, so copy **per run** from the source VPS into `accounts/<user>/<strat>/state/`:

| File                     | Contains                                                        | Needed to resume? |
|--------------------------|----------------------------------------------------------------|-------------------|
| `state.json`             | positions, lots, avg price, cash, `pending_order_id`, cooldowns, `session_date`, `last_eod_cancel_date` | **Yes — critical** |
| `trades.jsonl`           | trade history / audit trail                                    | For continuity    |
| `rejects.jsonl`          | reject history                                                 | For continuity    |
| `fyers_auth.json` entry  | the account's `client_id` + tokens                             | **Yes** |

The source files are named per strategy (e.g. `shishind_state.json`); rename to the new flat
names on copy:
```
# on the source VPS, after 15:30 with the bot stopped:
scp /root/trading_bot/strategies/pct_ladder/state/shishind_state.json \
    host:/opt/trading_bot/accounts/pratibha/shishind/state/state.json
scp .../shishind_trades.jsonl  host:/opt/trading_bot/accounts/pratibha/shishind/state/trades.jsonl
scp .../shishind_rejects.jsonl host:/opt/trading_bot/accounts/pratibha/shishind/state/rejects.jsonl
```
Repeat for every run of the account being migrated.

> Copy state only **after** the source bot is stopped and has written its final state
> (post-EOD-cancel), so you capture a clean, flat end-of-day state.

### 2b. Migrate auth for this account (IP-bound)

Add/confirm the account in `fyers_auth.json` on the host, then do a one-shot
refresh **through the account's proxy** to prove the whitelisted IP works end to end:
```
env $(grep -v '^#' accounts/<user>/account.env | xargs) \
  python scripts/fyers_auto_auth.py --user-key <user_key> --once
```
The login + token exchange must succeed and exit via the proxy IP.

### 2c. Stop the old, start the new

```
# source VPS — stop ALL of this account's runs:
./stop_<name>.sh                       # or systemctl stop, whatever runs it today
# control host:
systemctl start fyers-auth-<user>              # daily token refresh, IP-bound
systemctl start bot-<user>-<strategy>          # start each run of this account
```

### 2d. Verification checklist (must all pass)

- [ ] `journalctl -u bot-<user>-<strategy>` shows a clean start, config loaded, broker built.
- [ ] Log line confirms the token was fetched for the right `user_key`.
- [ ] **Egress IP check:** the bot's Fyers calls originate from the whitelisted proxy IP
      (confirm via a proxy-routed `get_profile`/`funds` call or proxy access logs).
- [ ] **Position reconciliation:** the bot's adopted positions match the broker's actual
      positions/holdings and match the pre-move `state.json` (no phantom or missing lots).
- [ ] Order placement works: a controlled test order (or the first live signal) is accepted
      — no `Invalid IP` / auth reject.
- [ ] `pending_order_id` / open orders reconcile — no duplicate of an order the old
      instance already placed.

**If every box is checked**, the account is live on the host. **If any fails → Rollback.**

---

## Phase 3 — Soak & decommission

1. Let the account run on the host for the soak period (default 3 trading days) with the
   **old VPS kept as standby** (bot stopped there, but the box and its whitelisted IP intact).
2. Watch daily: clean starts, correct EOD cancel, state written, PnL sane.
3. Only after a clean soak: remove the old VPS's IP from the Fyers whitelist (if it was
   kept alongside the new one) and decommission the box.
4. Move to the next account (back to Phase 2).

---

## Rollback (any failed verification)

Because the old VPS is untouched and still whitelisted, rollback is fast and safe:

1. `systemctl stop 'bot-<user>-*' fyers-auth-<user>` on the host (stops all the account's runs).
2. Restart the bot on the **source VPS** (`./start_<name>.sh`). Its `state.json` there is
   still the last-known-good — you copied it, you didn't delete it.
3. The account is back exactly where it was. Diagnose the failure offline, then retry the
   cutover in the next window.

Never attempt to fix a broken cutover by editing live state — roll back to the standby.

---

## What this migration does **not** change

- **No strategy logic changes.** Same `strategies/` modules, same params.
- **No broker/order-code changes.** Same `common/broker/fyers_client.py`. The IP binding is
  purely `HTTPS_PROXY` in `account.env`.
- **No MEXC changes.** Crypto bots and shared engine files are untouched (architecture §7).
- **No behavior change per account** — same config values, just relocated + IP-bound.

## Optional follow-up: repo tidy-up (separate track)

Restructuring the repo (point 3) is worthwhile but **must not block or risk this
migration**. Do it as a separate, later track once all accounts are consolidated:

- Move per-account configs out of `strategies/<strat>/config.<user>.json` into
  `accounts/<user>/<strat>/config.json` (already the target here).
- Consider grouping the loose top-level backtest/research scripts under `research/`.
- Keep `common/` (shared) and `strategies/` (shared logic) stable — they are the contract.

Treat repo tidy-up as cosmetic-until-proven-safe: one move per PR, tests green, no live bot
running against the moved path.
