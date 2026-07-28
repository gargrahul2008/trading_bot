# New Account Setup Guide

How to onboard a **new user** onto the control host. Assumes the host is already running
(see `multi_account_migration.md` for the initial consolidation). Read
`multi_account_architecture.md` for the *why*.

Each new account needs three things: **a whitelisted static IP (proxy)**, **credentials in
the auth file**, and **an account folder (config + env + state)**. Then two systemd units
run it. No code changes — ever.

---

## Step 1 — Get the account a whitelisted static IP

1. Provision a dedicated **static-IP** proxy for this account (a small VPS running
   tinyproxy/3proxy, or a dedicated-static-IP proxy service). Note its URL, e.g.
   `http://10.0.0.9:3128`.
2. In the user's **Fyers API app**, whitelist that proxy's IP.
3. Verify egress:
   ```
   HTTPS_PROXY=http://10.0.0.9:3128 curl -s https://api.ipify.org
   ```
   Must print the proxy IP (the whitelisted one), not the host's IP.

> One IP per demat is mandatory (SEBI/Fyers). Never reuse another account's proxy.

## Step 2 — Add credentials to the auth file

Add the user to `fyers_auth.json` under a new `user_key` (see
`fyers_auth.example.json` for the field set: `client_id`, `secret_key`, `redirect_uri`,
`fy_id`, `totp_key`, `pin`, ...). This file is gitignored — never commit it.

## Step 3 — Create the account folder + its strategy runs

An account is a folder with **one shared `account.env`** and **one subfolder per strategy
run** (`accounts/<user>/<strategy>/config.json` + `state/` + `logs/`). All of a user's runs
share the one `account.env`, because the whitelisted IP is per-demat, not per-strategy.

**a) The account env (identity + IP), once per user:**
```
mkdir -p accounts/<user>
cp accounts/_template/account.env.example accounts/<user>/account.env
```
Edit `accounts/<user>/account.env`:
```
ACCOUNT_ID=<user>
FYERS_USER_KEY=<user_key from step 2>
# HOME account (its IP == the master host): omit the proxy lines entirely.
# Otherwise, its dedicated static-IP proxy:
HTTPS_PROXY=http://10.0.0.9:3128
HTTP_PROXY=http://10.0.0.9:3128
```

**b) Each strategy run.** Two paths:

- *Migrating an existing `strategies/` config* → add it to the `MAPPING` in
  `deploy/build_account_configs.py` and run it. That converges auth to `json`, repoints
  `paths`, and isolates the SDK `log_path` automatically:
  ```
  python deploy/build_account_configs.py
  ```
- *A brand-new run* → create it by hand:
  ```
  mkdir -p accounts/<user>/<strategy>/state accounts/<user>/<strategy>/logs
  cp accounts/_template/config.example.json accounts/<user>/<strategy>/config.json
  ```
  then edit `config.json`: `broker.user_key` = the user_key; `broker.auth_file` =
  `../../../fyers_auth.json`; `broker.log_path` = `logs`; `strategy_name` / `strategy` /
  `symbols`; and leave `paths` pointing into `state/`.

> New state starts empty — the bot initializes on first run and (with
> `adopt_broker_inventory`) reconciles against the broker's actual holdings.

## Step 4 — Verify auth through the account's IP (one-shot)

Prove the whitelisted IP works before going live:
```
env $(grep -v '^#' accounts/<user>/account.env | xargs) \
  python scripts/fyers_auto_auth.py --auth-file fyers_auth.json \
    --user-key <user_key> --once
```
Expect a successful token refresh. If it fails with an IP/auth error, the proxy IP isn't
correctly whitelisted (recheck step 1–2).

## Step 5 — Dry-run a strategy (optional but recommended)

Start one run manually and watch it before installing services:
```
env $(grep -v '^#' accounts/<user>/account.env | xargs) \
  python run_strategy.py --config accounts/<user>/<strategy>/config.json
```
Confirm: config loads, broker builds, token fetched for the right `user_key`, positions
reconcile. `Ctrl-C` to stop.

## Step 6 — Generate, install, and start the services

Units are **generated** from the `accounts/` layout (one `bot-<user>-<strategy>.service` per
run + one `fyers-auth-<user>.service` per user), not hand-written:
```
INSTALL_DIR=/opt/trading_bot python deploy/gen_systemd_units.py   # set to the host's repo path
sudo cp deploy/systemd/generated/*.service /etc/systemd/system/
sudo systemctl daemon-reload
```
Enable + start this user's token refresh and each of its runs:
```
sudo systemctl enable --now fyers-auth-<user>
sudo systemctl enable --now bot-<user>-<strategy>       # repeat per run
```

## Step 7 — Confirm it's live

```
systemctl status bot-<user>-<strategy>
journalctl -u bot-<user>-<strategy> -f          # this run
journalctl -u 'bot-<user>-*' -f                 # all of this user's runs (user-level view)
```
Check: clean start, correct `user_key`, egress from the whitelisted IP, positions match the
broker, first order accepted (no `Invalid IP` reject).

## Logging model

- **App logs** → journald. Strategy-level: `journalctl -u bot-<user>-<strategy>`. User-level:
  `journalctl -u 'bot-<user>-*'`. Everything: `journalctl -u 'bot-*'`.
- **Fyers SDK logs** (`fyersApi.log`, `fyersRequests.log`) → each run's own
  `accounts/<user>/<strategy>/logs/` (via `broker.log_path=logs`), so no cross-process
  collision.
- **Trades / rejects** → each run's `accounts/<user>/<strategy>/state/*.jsonl`.

---

## Quick reference — files per account

| Path                                              | Committed? | Purpose                                   |
|---------------------------------------------------|-----------|--------------------------------------------|
| `accounts/<user>/account.env`                     | **no**    | ACCOUNT_ID, FYERS_USER_KEY, (HTTPS_PROXY)  |
| `accounts/<user>/<strat>/config.json`             | yes*      | strategy + broker(json) + execution + paths|
| `accounts/<user>/<strat>/state/state.json`        | **no**    | resume state (positions/lots/cash/pending) |
| `accounts/<user>/<strat>/state/trades.jsonl`      | **no**    | trade history                              |
| `accounts/<user>/<strat>/logs/`                   | **no**    | Fyers SDK logs (app logs go to journald)   |
| `fyers_auth.json` (user entry)                    | **no**    | credentials + tokens (repo root)           |

\* Config holds no secrets, so it's committed for versioning. `account.env`, `state/`,
`logs/`, and `fyers_auth.json` are gitignored.

## Removing / pausing an account

```
# one run:
sudo systemctl disable --now bot-<user>-<strategy>
# whole user (all runs + auth):
sudo systemctl disable --now 'bot-<user>-*' fyers-auth-<user>
```
Folders and state remain, so runs can be re-enabled later. To fully offboard, also remove the
user from `fyers_auth.json` and de-whitelist / decommission its proxy IP.
