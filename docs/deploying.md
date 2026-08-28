# Deploying

One command, after you have pushed:

```bash
cd /root/trading_bot
deploy/deploy.sh            # show what would happen
deploy/deploy.sh --apply    # do it
```

Always dry-run first. The dry run prints every command it would execute.

## What it does

1. **Update** — refuses to pull onto uncommitted changes, and never force-pulls.
2. **Accounts** — reconciles `accounts/` against `fyers_auth.json` (below).
3. **Units** — regenerates, and installs only the ones that differ.
4. **Agents** — restarts them *only* if something under `webapp/` changed or a
   unit was reinstalled. An unchanged deploy leaves them alone.
5. **Preflight** — `deploy/preflight.sh`, then a summary of the store.

Its exit code is preflight's, so it can gate anything downstream.

## What it will never do

- **Touch a bot unit's enable state.** `deploy/cron/start_equity_bots.sh` owns
  the bots' daily lifecycle and holds individual bots down. A unit enabled here
  would start every bot on the next boot, held down or not — including any whose
  local state has drifted from the broker.
- **Stop or start a bot.** Restarting a live bot mid-session is an operator
  decision, never a deploy step.
- **Overwrite an existing `account.env`.** That file names a live account's
  whitelisted IP; rewriting it from a stale field would redirect real orders.

## Adding an account

`fyers_auth.json` is the register. Add the account there and everything else
follows.

```json
"user5": {
  "label": "Meera",
  "account": "meera",
  "proxy": "http://51.20.11.9:3128",

  "client_id": "…", "secret_key": "…", "totp_key": "…",
  "pin": "…", "redirect_uri": "http://100.109.109.19:8501/fyers-auth",
  "auto_refresh": true, "fy_id": "…"
}
```

Two fields beyond what Fyers itself needs:

| | |
|---|---|
| `account` | the `accounts/<name>` directory. Defaults to the label, lower-cased. |
| `proxy` | its whitelisted egress. **Omit only for the one account whose whitelisted IP is the host's own** — the script refuses a second one, because Fyers would reject its orders and the failure looks like bad credentials. |

Then `deploy/deploy.sh --apply` creates `accounts/meera/account.env` (mode 600),
assigns an agent port, generates and installs the unit, and preflight confirms
the account leaves by the right IP.

Two things follow on their own, because they read the same register:

- **Token refresh.** `deploy/cron/refresh_tokens.sh` iterates
  `deploy/accounts.py --refreshable`. It used to hardcode three user keys, which
  meant a new account silently never got a token and its agent simply died at
  the next expiry.
- **The dashboard.** The API reads `deploy/agent_ports.json`, so the new account
  appears on the Overview page as soon as its agent is up.

Afterwards, commit `deploy/agent_ports.json` so the port is recorded — the
script prints the reminder.

### Before it can trade

Onboarding sets up the *account*. Giving it a strategy is separate and
deliberate: create `accounts/<name>/<run>/config.json`, regenerate units, and add
the run to `deploy/cron/start_equity_bots.sh`. Nothing places an order until you
do that.

## Checking without deploying

```bash
deploy/accounts.py                 # who exists, their egress, their ports
deploy/onboard.py                  # what onboarding would create
deploy/preflight.sh                # is the host in its intended state
env/bin/python -m webapp.store.status
```

All read-only.
