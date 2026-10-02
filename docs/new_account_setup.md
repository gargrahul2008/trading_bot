# Adding an account (and how the egress IP works)

Three parts: **the new proxy instance** (§2), **the control host** (§3), and **reporting**
(§4). Copy-paste them and an account is live in about twenty minutes, most of it waiting for
Fyers to whitelist the IP.

> §4 is easy to skip and silently costs you data. `piyush` was onboarded in August and still
> had an empty `reports/` folder six weeks later: no P&L, no portfolio, nothing on the
> dashboard. Nothing failed — the step had just never been run.

For the architecture reasoning behind all of this, see `multi_account_architecture.md`.
For the proxy box in more depth, `../deploy/proxy/README.md`.

---

## 1. How the egress IP works

SEBI requires one registered static IP per demat, and Fyers enforces it **per API app**:
every order must appear to come from the IP whitelisted on that account's app. Orders from
anywhere else are rejected.

We run every account off **one control host**, so each account needs its own exit address.
That is what the proxy boxes are for: the control host makes the request, the account's proxy
makes the outbound call, and Fyers sees the proxy's IP.

```
control host 64.227.135.117 (one repo, one process per run)
├── rahul     → no proxy  → Fyers sees 64.227.135.117   (the host IP is his whitelisted IP)
├── pratibha  → proxy     → Fyers sees 157.245.108.24
└── piyush    → proxy     → Fyers sees 15.252.102.31
```

**Exactly one account can be the "home" account** — the one whose whitelisted IP is the
host's own address. Everyone else needs a proxy.

The binding needs no code: `fyers-apiv3` calls through Python `requests`, which honours
`HTTPS_PROXY`. Each account's value lives in `accounts/<name>/account.env`, and systemd loads
that file per unit. So one process = one account = one exit IP.

> **`account.env` is the live truth for an account's egress** — it is what systemd loads. The
> `proxy` field in `fyers_auth.json` is consumed *only* by `deploy/onboard.py`, at the single
> moment it creates a missing `account.env`. Changing the auth file afterwards does nothing.
>
> Do not be surprised to find `proxy` absent from every existing account in `fyers_auth.json`:
> they predate the field and their `account.env` files were written directly. That is fine and
> nothing needs backfilling. It only matters for an account that has **no `account.env` yet**,
> where leaving it out makes `onboard.py` write a proxy-less (direct) env — see §3 Step 2.

Ask the register rather than trusting any list in a doc:

```bash
deploy/accounts.py            # account, user_key, status, egress, agent port
deploy/preflight.sh           # actually checks each account's egress IP end-to-end
```

```
account    user_key  status       egress           port
rahul      user1     configured   host IP          9102
pratibha   user2     configured   157.245.108.24   9101
piyush     user3     configured   15.252.102.31    9103
```

### What the env-var proxy does and does not cover

It covers everything that goes through `requests`, whose `Session.trust_env` defaults to
`True`. Verified per account by making a `requests` call under each `account.env`:

```
rahul     -> 64.227.135.117 (direct/home)    piyush  -> 15.252.102.31
pratibha  -> 157.245.108.24                  jhalak  -> 159.65.149.60
```

**Token acquisition is covered too**, which matters because a login from the wrong IP is
rejected. `scripts/fyers_auto_auth.py` uses bare `requests` for the TOTP/PIN flow, and the SDK's
`fyersModel.SessionModel` — which performs the final auth_code → access_token exchange — uses
`requests` only, no aiohttp. `deploy/cron/refresh_tokens.sh` runs each account in its own
process under its own `account.env`, so every login leaves by that account's IP. Never refresh
all users in one process (`--enabled-only`): they would share whichever proxy the environment
happened to hold.

Two things are **not** covered, both silent if they ever start being used:

- **Websockets.** A socket does not pick up the env proxy and needs it set explicitly in code.
  Order placement is REST, so the bots are fine; `FyersPcaEngine` hits this directly and binds
  per-account in code (see its README).
- **The SDK's async methods** (`post_async_call`, `get_async_call`, `delete_async_call`,
  `patch_async_call`, `put_async_call`). Those use `aiohttp.ClientSession()`, whose `trust_env`
  defaults to **`False`** — so they ignore `HTTPS_PROXY` entirely and would egress from the host
  IP. Nothing in this repo calls them today (checked). If anything ever does, it must pass the
  proxy explicitly or that account's orders will be rejected from the wrong IP.

---

## 2. On the NEW INSTANCE (the proxy box)

Provision a small VPS. Its **public IP is the account's identity** — never change or reuse it.
Everything below runs as root on that box.

```bash
# 1. tinyproxy
apt-get update && apt-get install -y tinyproxy

# 2. Edit the SHIPPED config in place — don't replace the file, it carries defaults
#    (DefaultErrorFile, StatFile, ...) that tinyproxy expects to exist.
cp /etc/tinyproxy/tinyproxy.conf /etc/tinyproxy/tinyproxy.conf.orig
sed -i 's/^Port .*/Port 3128/'        /etc/tinyproxy/tinyproxy.conf
sed -i 's/^#\?Listen .*/Listen 0.0.0.0/' /etc/tinyproxy/tinyproxy.conf

# Who may USE the proxy: ONLY the control host. Drop the default localhost allows.
# An open proxy will be found and abused within days.
sed -i '/^Allow /d' /etc/tinyproxy/tinyproxy.conf
echo 'Allow 64.227.135.117' >>/etc/tinyproxy/tinyproxy.conf

# There must be NO `Upstream` line — tinyproxy has to egress from THIS box's own
# public IP, which is the whole point.
grep -q '^Upstream' /etc/tinyproxy/tinyproxy.conf && echo 'WARNING: remove the Upstream line'

# 3. Start it
systemctl enable --now tinyproxy
systemctl restart tinyproxy
systemctl is-active tinyproxy
grep -E '^(Port|Listen|Allow)' /etc/tinyproxy/tinyproxy.conf   # sanity-check what took effect

# 4. Firewall: port 3128 reachable only from the control host
ufw allow from 64.227.135.117 to any port 3128 proto tcp
ufw deny 3128
ufw --force enable
ufw status

# 5. Note this box's public IP — this is what gets whitelisted at Fyers
curl -s https://api.ipify.org; echo
```

If the provider has its own firewall/security group, mirror rule 4 there: inbound TCP 3128
from `64.227.135.117` only.

**Then, in the Fyers dashboard for that user's API app, whitelist the IP from step 5.**
Whitelists are per *app*, so a user with two apps needs it set on the one this account uses.

### Verify from the control host (not your laptop — 3128 is firewalled to the host)

```bash
HTTPS_PROXY=http://<NEW_IP>:3128 curl -s https://api.ipify.org; echo
# must print <NEW_IP>. Connection refused => tinyproxy down, or Allow/ufw is blocking.
```

> **Fyers enforces the IP whitelist on order placement only — not on reads.** A token reads
> `profile`, `holdings` and `orders` happily from any address; verified by reading an account
> whose *orders* definitely require its proxy, straight from the host IP. So a passing profile
> check proves the token, never the egress binding. Only the ipify check above (and
> `deploy/preflight.sh`, which does the same) proves where traffic actually leaves from, and
> only a real order proves the whitelist end-to-end. Keep the first live order small.

---

## 3. On the CONTROL HOST

### Step 1 — add the account to the register

`fyers_auth.json` (gitignored, repo root) is the single register. Add one entry under a new
`user_key`:

```jsonc
"user4": {
  "label": "Someone",
  "account": "someone",                      // -> accounts/someone/
  "proxy": "http://<NEW_IP>:3128",           // REQUIRED unless this is the home account
  "auto_refresh": true,                      // false = ignored everywhere
  "client_id": "XXXXXXXXXX-100",
  "secret_key": "...",
  "fy_id": "...",
  "pin": "1989",
  "totp_key": "...",
  "app_id_type": 2,
  "redirect_uri": "http://100.109.109.19:8501/fyers-auth"
}
```

`client_id`, `secret_key`, `totp_key`, `pin`, `redirect_uri` are all required — a record
missing any of them shows as `incomplete` and is skipped everywhere, silently but visibly in
`deploy/accounts.py`.

> **Do not forget `proxy`.** It is not in the required list, because omitting it is how the
> home account is declared — so an entry without it is created as a *direct* account sharing
> the host's IP, and its orders are rejected because that IP is whitelisted for a different
> app. `deploy/accounts.py` warns when more than one account resolves to the host IP; that
> warning means someone left `proxy` out.

### Step 2 — create the account directory

```bash
deploy/onboard.py            # dry run: shows exactly what it would create
deploy/onboard.py --apply    # creates accounts/<name>/, its account.env, its agent port
deploy/accounts.py           # confirm: status should now be `configured`
```

Two safety properties worth knowing, because they shape what you can and cannot fix later:

**It refuses to create a second direct account.** If the entry has no `proxy` and some other
account already egresses directly, it blocks rather than creating anything:

```
  BLOCK  jhalak     (user5) — no proxy, and rahul already goes out directly.
         Add "proxy": "http://<its-ip>:3128" to its fyers_auth.json record.

Nothing can be created until the above is resolved.
```

It exits non-zero and creates *nothing* — not even the accounts it could have. Add the `proxy`
field and re-run. (Only one account may legitimately have no proxy: the home account, whose
whitelisted IP is the host's own address.)

**It never touches an account that already has an `account.env`.** That file names a live
account's whitelisted IP, and rewriting it from a stale auth-file field would silently redirect
a real account's orders. The corollary: it can only set the proxy on the *first* run for an
account. To change an egress afterwards, edit `account.env` directly — that is the live truth.

Then confirm:

```bash
deploy/accounts.py     # the new account's `egress` must be its proxy IP, not `host IP`
```

### Step 3 — prove the token works through that IP

```bash
env $(grep -v '^#' accounts/<name>/account.env | xargs) \
  env/bin/python scripts/fyers_auto_auth.py --auth-file fyers_auth.json \
    --user-key <user_key> --once
```

Expect a successful refresh. An IP/auth error here means the whitelist isn't right — fix that
before going further; everything downstream depends on it.

**Daily refresh needs no edit.** `deploy/cron/refresh_tokens.sh` (03:00 UTC / 08:30 IST) reads
the same register and picks the account up on its own, refreshing each one under its own
`account.env` so every login exits from its own IP.

### Step 4 — add strategy runs (optional; an account can exist with none)

One subfolder per run. All of a user's runs share the one `account.env`, because the
whitelisted IP is per-demat, not per-strategy.

```bash
mkdir -p accounts/<name>/<strategy>/state accounts/<name>/<strategy>/logs
cp accounts/_template/config.example.json accounts/<name>/<strategy>/config.json
```

In that `config.json` set: `broker.user_key` = the user_key, `broker.auth_file` =
`../../../fyers_auth.json`, `broker.log_path` = `logs`, plus `strategy_name` / `strategy` /
`symbols`. Leave `paths` pointing into `state/`.

Dry-run it before installing anything:

```bash
env $(grep -v '^#' accounts/<name>/account.env | xargs) \
  env/bin/python run_strategy.py --config accounts/<name>/<strategy>/config.json
```

Confirm the config loads, the token resolves for the right `user_key`, and positions
reconcile against the broker. `Ctrl-C` to stop.

### Step 5 — generate and install units

Units are **generated** from the `accounts/` layout, never hand-written:

```bash
env/bin/python deploy/gen_systemd_units.py        # INSTALL_DIR defaults to /root/trading_bot
cp deploy/systemd/generated/*.service /etc/systemd/system/
systemctl daemon-reload
```

This emits `bot-<user>-<strategy>.service` per run, `agent-<user>.service` per account, and
`dashboard.service`. There is **no** `fyers-auth-*` unit — token refresh is the cron above.

> **Do not `systemctl enable` the bot units.** Their lifecycle is the daily cron pair
> (`start_equity_bots.sh` 03:25 UTC / `stop_equity_bots.sh` 10:01 UTC) — a fresh SDK session
> each morning avoids the open-bell 429 wedge. Enabling them would also restart held-down bots
> on the next reboot, which `preflight.sh` explicitly checks for.

```bash
systemctl enable --now agent-<name>      # agents DO stay enabled
```

**The generated agent unit carries `--allow-trading`**, which exposes the place / modify /
cancel / exit routes — the dashboard can place real orders in that account. Without the flag the
agent is read-only. It comes from the host-wide `deploy/trading_enabled` marker, so a new
account gets it by default, matching the others. Decide deliberately for a brand-new account;
to start read-only, drop the flag from the unit and regenerate later.

The agent also needs `AGENT_TOKEN` (from `webapp/agent.env`) — `/health` returns
`{"error": "unauthorised"}` without it, which is correct, not a fault.

Finally, commit `deploy/agent_ports.json` so the account's port is recorded.

### Step 6 — put its bots in the daily lifecycle

Edit `deploy/cron/start_equity_bots.sh` and add the new units to the `BOTS` array. Note the
`HOLD_DOWN` list in the same file: anything listed there does not start on any day until the
entry is removed — that is how an account is paused without touching cron.

### Step 7 — confirm

```bash
deploy/preflight.sh                      # the real check: egress IPs, tokens, unit states
journalctl -u bot-<name>-<strategy> -f   # one run
journalctl -u 'bot-<name>-*' -f          # everything for this account
```

Look for: clean start, correct `user_key`, egress from the whitelisted IP, positions matching
the broker, and a first order accepted with no `Invalid IP` rejection.

---

## 4. Reporting and P&L

Onboarding creates no `reports/` directory. Until these run, the account is invisible on the
dashboard — no holdings, no P&L, no trade history — and nothing warns you.

| File | Written by | Contains |
|---|---|---|
| `portfolio.json` | `fetch_broker_portfolio.py` | holdings, positions, funds — pure snapshot |
| `trades_all.jsonl` | `fetch_broker_pnl.py` | every fill seen so far, deduped by `tradeNumber` |
| `pnl_seed.json` | `fetch_broker_pnl.py` | **write-once** starting cost basis (see below) |
| `broker_pnl.json` | `fetch_broker_pnl.py` | broker vs bot realized, charges, discrepancy |
| `bot_pnl_history.json` | `build_bot_pnl_history.py` | bot realized over time (local logs, no broker call) |

Run all three for the new account — under its `account.env`, so the broker calls leave by its
own IP:

```bash
env $(grep -v '^#' accounts/<name>/account.env | xargs) \
  env/bin/python scripts/fetch_broker_pnl.py --account <name> --user-key <user_key>

env $(grep -v '^#' accounts/<name>/account.env | xargs) \
  env/bin/python scripts/fetch_broker_portfolio.py --account <name> --user-key <user_key>

env/bin/python scripts/opening_positions.py --seed <name>    # no broker call, no env needed
```

### Backfill the dashboard store — do this at onboarding, not later

`fetch_history.py` fills the dashboard's own store (`webapp/data/dashboard.db`): capital flows,
realised P&L per scrip-day, charges, and opening positions. **Nothing does this automatically** —
it is not in cron and takes `--account` explicitly, so a new account shows `capital in 0` and no
return figures until it is run. Both `piyush` and `jhalak` were found this way.

```bash
env $(grep -v '^#' accounts/<name>/account.env | xargs) \
  env/bin/python scripts/fetch_history.py --account <name> --from 2026-04-01 --daily-realised
```

Read-only — three GET endpoints, no order/modify/cancel. About 185 calls with
`--daily-realised` (it asks the realised endpoint once per calendar day, because that endpoint
carries no date field and only a one-day window yields a one-day figure). Without the flag one
call returns the same total attributed to a single day.

Rate budget is **per account**, so backfilling an account with no bots running contends with
nothing. Backfilling an account whose own bots are live is what to avoid during market hours.

### Then set the opening-securities base

The ledger gives opening *cash* and every transfer since — **not the securities the account
already owned at the start of the financial year**. That money went in during earlier years and
no endpoint in this year records it, so the capital base comes out short and every return
percentage measured against it is wrong.

```bash
env/bin/python scripts/capital.py                     # show every account's base
env/bin/python scripts/capital.py --set <name> <amount> \
    --on 2026-04-01 --note "securities held at FY start, at cost"
```

Idempotent on its reference, so re-running replaces rather than adds. The symptom of a missing
base is `deployed_exceeds_capital` on the portfolio page, or — if enough was withdrawn during
the year — a **negative** `capital in`, as jhalak showed (opening cash 220,133.80 against
890,309.29 withdrawn). The page renders no return at all rather than a wrong one when the base
is not positive, so a blank return figure means this step is outstanding.

> Leverage is a separate, legitimate cause of the same `deployed_exceeds_capital` symptom —
> rahul's RELIANCE ladder is 3x MTF, so its deployed exceeds its cash by design.

`opening_positions.py` records what the account held *before* our fill history begins. The
broker gives each holding's buy average, which is exactly the cost basis a future sale is
matched against — so this is the step that makes the account's P&L correct going forward.
Verify with `opening_positions.py` (no arguments) and correct any individual basis with
`--set <acct> <symbol> <qty> <price> --as-of <date> --note "..."`; a manual entry deliberately
outranks a re-seed.

**The daily cron needs no edit.** `deploy/cron/fetch_pnl.sh` iterates `deploy/accounts.py`, the
same register as the token cron, so a new account is picked up automatically. It used to be
hardcoded to two accounts, which is exactly why `piyush` was skipped for six weeks.

### `pnl_seed.json` is written once

It is the starting basis for replaying every later trade, so it is created on first run and
never rewritten. Getting it wrong is persistent — to redo it, move the file aside and re-run
`fetch_broker_pnl.py`.

Its `seed_source` tells you which basis was used:

- **`bot_state`** — the preferred path: lots, borrowed quantity and realized taken from the
  account's `*/state/state.json`.
- **`broker_holdings`** — the fallback for an account with no strategy runs. One weighted-average
  lot per holding, rewound behind the trades already accumulated (`pre_qty = remaining + sells −
  buys`) and dated a day earlier so those trades replay. Without that rewind the seed is taken
  *after* the day's trades while the replay skips them, and the day's realized silently vanishes
  — measured on jhalak as `0.0` where the truth was `−5,185.55`.

### Limits worth knowing before someone asks for "all the history"

- **`/tradebook` is today-only** — that is the *fill-level* feed behind `trades_all.jsonl`, so
  the individual-fill history in `reports/` starts the day you onboard. It does **not** mean
  history is unavailable: `fetch_history.py` above uses `/ledger-history` and the realised
  endpoint, both of which go back across the financial year, and recovered capital flows,
  realised P&L per scrip-day and charges for jhalak and piyush back to 2026-04-01. Run it.
- **A position fully closed before the first run cannot be reconstructed.** No holdings row means
  no recoverable cost basis, so its realized P&L is simply not available from the API.
- **Only fill-by-fill history before onboarding needs a broker statement**, and nothing here
  parses one yet — there is no importer for a Fyers CSV or contract note. Aggregate history
  (capital, realised, charges) does not need one; `fetch_history.py` covers it.
- **A bot-less account always shows a P&L "discrepancy".** That field is broker realized minus
  *bot* realized, and with no bot the bot side is 0, so the discrepancy just mirrors realized.
  It is noise, not divergence; `seed_source: broker_holdings` is the flag to suppress it by.

## 5. Files per account

| Path | Committed? | Purpose |
|---|---|---|
| `accounts/<name>/account.env` | **no** | `ACCOUNT_ID`, `FYERS_USER_KEY`, `HTTPS_PROXY` — the live egress |
| `accounts/<name>/<strat>/config.json` | yes | strategy + broker + execution + paths (no secrets) |
| `accounts/<name>/<strat>/state/state.json` | **no** | resume state: positions, lots, cash, pending |
| `accounts/<name>/<strat>/state/*.jsonl` | **no** | trades / rejects |
| `accounts/<name>/<strat>/logs/` | **no** | Fyers SDK logs (app logs go to journald) |
| `fyers_auth.json` | **no** | the register: credentials + tokens + `account`/`proxy` |

## 6. Pausing and offboarding

**Pause** — add the unit to `HOLD_DOWN` in `deploy/cron/start_equity_bots.sh`, then
`systemctl stop bot-<name>-<strategy>`. Deliberately open-ended rather than dated: a skip that
expires by itself would put a bot back into the market on a morning nobody asked for.

> A stopped bot's open orders stay live at the broker, unmanaged — no repricing, and its own
> EOD cancel won't run. Check the orderbook after stopping one mid-session.

**Restarting after a pause** — reconcile `accounts/<name>/<strat>/state/state.json` against
the broker first. Every day held down is a day its local view of lots and realized PnL drifts
from the real position.

**Offboard** — remove the units, set `auto_refresh: false` (or delete the entry) in
`fyers_auth.json`, de-whitelist the IP at Fyers, and decommission the proxy box. Keep the
`accounts/<name>/` folder if the history matters; it is inert without units.
