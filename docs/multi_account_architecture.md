# Multi-Account Fyers Trading — Architecture Reference

> **Read this first when working on anything involving live Fyers accounts, per-user
> config/state, the control host, or IP/proxy binding.** This is the source of truth for
> *how* multiple users' bots run off one codebase.

## 1. The problem this solves

SEBI's algo-trading framework requires a **unique, registered static IP per demat** for
API order placement. Fyers enforces this: each account's API app whitelists one static IP,
and every order must originate from it.

We run **several users' accounts, each with its own strategy/config**, off **one shared
codebase**. The naive solution (one full VPS per user) scatters code, logs, and deploys
across N machines that drift apart. Instead:

- **One control host** runs every account as a separate OS process.
- **Each account's traffic exits through its own static-IP proxy** (its whitelisted IP).
- **One codebase, per-account config** — nothing account-specific lives in the code.

The key insight: *separate egress IP does not require separate deployment.* We decouple
"where the IP lives" (a per-account proxy) from "where the logic runs" (one host).

## 2. How one host serves many users

Fyers identifies an account by the **access token** in each request, not by the machine.
So one host runs N processes, each with a different token + a different egress IP:

```
Control host (one machine, one repo)
├── process: india    → token(user1) + proxy → Fyers sees IP whitelisted for user1
├── process: shishind → token(user2) + proxy → Fyers sees IP whitelisted for user2
└── process: user3    → token(user3) + proxy → Fyers sees IP whitelisted for user3
```

The processes never talk to each other. Each reacts only to its own market data and its
own strategy signals. The only shared things are the **code**, the **host**, and the
**log destination** — the trading identities stay fully isolated.

### Why the IP binding needs no code change

`common/broker/fyers_client.py` wraps the `fyers-apiv3` SDK, which makes all HTTP calls
through Python `requests`. `requests` honors the `HTTPS_PROXY` environment variable. So
**setting `HTTPS_PROXY` per process routes that account's Fyers traffic through its proxy**
— order placement *and* token refresh both go out through the account's whitelisted IP.
This is why each account has its own `account.env`.

> Caveat: the env-proxy covers REST calls (orders, orderbook, positions, quotes) and the
> auth flow — everything that needs the whitelisted IP. A market-data websocket, if added,
> would need its proxy set explicitly in code. Order placement is REST, so this is covered.

## 3. Repo layout

```
trading_bot/
  run_strategy.py            # SHARED generic entry: python run_strategy.py --config <cfg>
  common/                    # SHARED — DO NOT fork per user
    broker/                  #   fyers_client.py, mexc_spot_client.py, auth_json.py, ...
    engine/                  #   generic_runner.py, execution.py, state.py, pnl.py
    utils/
  strategies/                # SHARED strategy LOGIC (behavior comes from config params)
    pct_ladder/  fix_levels/  sell_first/  ...
  accounts/                  # PER-ACCOUNT isolation root (one folder per user)
    _template/               #   skeleton copied for each new account
    rahul/                   #   user1 — HOME account (host IP, no proxy)
      account.env            #     ACCOUNT_ID + FYERS_USER_KEY (+ HTTPS_PROXY for non-home)
      reliance/              #     one folder per strategy RUN
        config.json          #       strategy + broker(json auth) + execution + paths
        state/               #       resume state (gitignored): state.json/trades/rejects
      vikaseco/  ...
    pratibha/                #   user2 — proxied to its own whitelisted IP
      account.env            #     ...+ HTTPS_PROXY=<pratibha's static IP>
      shishind/  indothai/  coolcaps/  arl/   # each: config.json + state/
  fyers_auth.json            # all users keyed by user_key (repo root), gitignored
  deploy/
    build_account_configs.py # regenerate accounts/*/*/config.json from live strategies/ configs
    gen_systemd_units.py     # emit per-run bot + per-user auth units into systemd/generated/
    systemd/generated/       # bot-<user>-<strat>.service, fyers-auth-<user>.service
    proxy/                   # proxy setup notes
  docs/
    multi_account_architecture.md   ← this file
    multi_account_migration.md      ← how to migrate existing users onto the host
    new_account_setup.md            ← how to onboard a new user
```

**Rule of thumb:** logic goes in `common/` and `strategies/` (shared, versioned in git);
identity, config, and state go in `accounts/<name>/` (per-user, mostly gitignored).

**Multi-run per account.** A user runs several strategies, each its own
`accounts/<user>/<strategy>/` folder (config + state) and its own bot process/unit. **All of
a user's runs share that user's single `account.env`** — because the whitelisted IP is
per-demat, not per-strategy. Change the IP once, every run of that user follows.

- `deploy/build_account_configs.py` — regenerates the per-run configs from the live
  `strategies/` configs (converging auth to `json`, repointing `paths`). Re-run before a
  cutover to snapshot current live params. The `MAPPING` at its top is the live-run registry.
- `deploy/gen_systemd_units.py` — scans `accounts/` and emits one `bot-<user>-<strat>.service`
  per run + one `fyers-auth-<user>.service` per user. Re-run after adding/removing a run.

Current live runs: **rahul** → reliance, vikaseco; **pratibha** → shishind, indothai,
coolcaps, arl. (The many other `strategies/*/config.*.json` are dead/legacy or MEXC — not
part of the accounts layout.)

## 4. Config-driven behavior (point: common order handling)

Everything account-specific is expressed in `accounts/<user>/<strategy>/config.json`. The same
`run_strategy.py` + `common/broker` + `strategies/` code serves every account. A config
declares:

| Section      | Purpose                                                                 |
|--------------|-------------------------------------------------------------------------|
| `strategy_name` | which module under `strategies/` to load                             |
| `runner_type`   | engine mode (e.g. `proactive`)                                       |
| `broker`        | `type: fyers`, `auth_mode`, `user_key` (selects the token)          |
| `strategy`      | per-run params (symbols, sizing, step %, ...)                       |
| `execution`     | product type, poll intervals, EOD cancel time, market hours, ...   |
| `paths`         | `state_path`, `trades_path`, `rejects_path` (per-account state)      |

Because one account can run the same strategy on different symbols *and* different
strategies, an account is free to have multiple configs/processes if needed — but the
common case is **one config = one process = one account**.

**Order placement is common for all users**: every account routes through
`FyersClient.place_order()` (`common/broker/fyers_client.py`). There is no per-user order
code — only per-user config and per-user token/IP. Do not add account-specific branches to
the broker layer; add config fields instead.

## 5. Authentication

Auth is selected by `broker.auth_mode` in the config:

- **`json`** — reads `client_id` + `access_token` from `fyers_auth.json` keyed by
  `user_key`. Tokens refreshed by `scripts/fyers_auto_auth.py` (automated TOTP/PIN login).
- **`http`** — fetches the token from a central token service (`token_url`) by `user_key`.
  This is the current cross-VPS distribution method.
- **`db`** — legacy traderealm MySQL lookup by `user_id`.

**Per-user IP-bound auth:** run token refresh **per user** (`fyers_auto_auth.py --user-key
<u>`) in a process that carries that user's `HTTPS_PROXY`, so the login/token exchange also
exits through the account's whitelisted IP. **Do not use `--enabled-only`** (it refreshes
all users in one process → one shared IP). See the generated `fyers-auth-<user>.service`.

## 6. State & resume (why the bot picks up where it left off)

`common/engine/state.py :: GlobalState` is persisted to `paths.state_path` per account. It
holds: cash, per-symbol lots/qty/avg-price/realized-pnl, `pending_order_id`, cooldowns,
`session_date`, `last_eod_cancel_date`. **This file is what makes the next trading day
resume smoothly.** It is gitignored and exists only on the host running the account.

On start, configs with `execution.sync_on_start` / `adopt_broker_inventory` also reconcile
against the broker's actual positions — a safety net — but `state.json` carries the richer
local view (lot structure, realized PnL, cooldowns) that the broker can't return.

## 7. Do-not-touch surface (crypto / MEXC)

The MEXC crypto bots are independent and **must not be modified** during account work:

- Clients/scripts: `common/broker/mexc_spot_client.py`, `scripts/live_fib_bot.py`,
  `scripts/mexc_*.py`, `scripts/fetch_binance_*.py`, `bottom_zone_grid/`, `permanent_grid/`.
- **Shared engine files** used by *both* Fyers and MEXC — change only with extreme care and
  full test coverage: `common/engine/generic_runner.py`, `common/engine/execution.py`,
  `common/engine/pnl.py`, `common/engine/overlay_pnl.py`.

Multi-account work touches **only**: `accounts/`, `deploy/`, `docs/`, per-account configs,
`.gitignore`, and (if needed) additive config fields — never MEXC logic.

## 8. Design principles

- **Config over code.** New behavior = new config field or new value, not a per-user fork.
- **Thin per-account layer.** An account is a folder of config + env + state, nothing more.
- **Common broker/order path.** All order handling flows through `common/broker`.
- **Isolation by process + folder + proxy.** One process, one folder, one IP per account.
- **Additive migration.** The `accounts/` layout and systemd units are added alongside the
  running bots; nothing is cut over until verified. See `multi_account_migration.md`.
