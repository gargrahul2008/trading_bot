# Master-Host Runbook — for the agent running ON the master host

You are an agent running **on the master trading host** (`64.227.135.117`, Rahul's box). Your
job is to finish consolidating the Fyers accounts onto this host, following the framework that
was already designed. **Read these first, in order, and treat them as the source of truth:**

1. `docs/multi_account_architecture.md` — the design (why/how).
2. `docs/multi_account_migration.md` — the phased cutover procedure.
3. `docs/new_account_setup.md` — onboarding shape (multi-run layout).
4. `deploy/proxy/README.md` — proxy setup.

Before doing anything, **summarize your understanding of the current state and the next step,
and wait for the operator to confirm.**

## Golden rules (do not violate)

- **Live trading must not be disrupted.** Do not start/stop/modify any running bot without the
  operator's explicit go-ahead for that specific account.
- **One account at a time**, only in a **non-trading window** (after 15:30 IST / weekend /
  NSE holiday).
- **Do not touch the MEXC crypto bots** or the shared engine files they use
  (`common/engine/generic_runner.py`, `execution.py`, `pnl.py`). See architecture doc §7.
- **The old VPS stays as hot standby** for each account until it soaks cleanly (default 3
  trading days). Rollback = restart the bot on the source VPS (its state is untouched).
- After every cutover, run the **verification checklist** (migration doc Phase 2d). If any
  check fails, **roll back** — never fix live state by hand.

## Current state (as of 2026-07-28) — done vs pending

**Done (in git, arrives via `git pull`):**
- Multi-run account layout under `accounts/`: `rahul` (user1) → reliance, vikaseco;
  `pratibha` (user2) → shishind, indothai, coolcaps, arl. Each run has `config.json` (auth
  converged to local `json`, `log_path` isolated).
- Generators: `deploy/build_account_configs.py` (regenerate configs from live `strategies/`),
  `deploy/gen_systemd_units.py` (emit bot + auth units).
- P&L pipeline: `common/broker/fyers_client.py` (tradebook/charges/realized methods),
  `scripts/fetch_broker_pnl.py`, `common/reporting/pnl.py`, `dashboard/views/pnl_page.py`.
- Pratibha's proxy (`157.245.108.24:3128`) validated: HTTPS egress confirmed from the
  whitelisted IP.

**Pending (your work):**
1. Host prerequisites (see below — several files are gitignored and must exist on the host).
2. Harden Pratibha's proxy to allow only the master host; confirm `157.245.108.24` is
   whitelisted in Pratibha's Fyers app.
3. Cut over **Pratibha** (Phase 2), then **Rahul** (Phase 2), one at a time.
4. Calibrate the broker-P&L parser (see below).

## ⚠️ Gitignored files — NOT delivered by `git pull`; create/verify them on the host

- `accounts/<user>/account.env` — create per user. `rahul`: `ACCOUNT_ID`, `FYERS_USER_KEY=user1`,
  **no proxy** (host IP is his whitelisted IP). `pratibha`: same + `HTTPS_PROXY`/`HTTP_PROXY=http://157.245.108.24:3128`, `FYERS_USER_KEY=user2`.
  Templates: `accounts/_template/account.env.example`.
- `fyers_auth.json` — must exist at the repo root with both `user1` and `user2` entries and
  valid tokens. Configs reference `../../../fyers_auth.json`.
- `accounts/<user>/<strat>/state/` — empty until you copy each run's state from the source VPS
  at cutover (migration doc Phase 2a). These files are what make the bot resume.

Also: `deploy/gen_systemd_units.py` uses `INSTALL_DIR` — set it to this host's actual repo path
(e.g. `/root/trading_bot`) when generating units.

## Ordered steps

1. **Sanity check the host.** Confirm repo path, the venv Python, that `fyers_auth.json` has
   both users, and that `strategies/` still has the live configs. Run
   `python deploy/build_account_configs.py` and `INSTALL_DIR=<repo> python deploy/gen_systemd_units.py`;
   review `deploy/systemd/generated/`. **Do not install/start units yet.**
2. **Create `account.env` for rahul and pratibha** (see gitignored section).
3. **Harden Pratibha's proxy** (remove any temporary Allow, keep only `64.227.135.117`) and
   **verify egress from this host**: `HTTPS_PROXY=http://157.245.108.24:3128 curl -s https://api.ipify.org`
   → must print `157.245.108.24`. Confirm that IP is whitelisted in Pratibha's Fyers app.
4. **Cut over Pratibha** (migration doc Phase 2, non-trading window): stop her bots on the old
   VPS → copy each run's state → install+start `fyers-auth-pratibha` and each `bot-pratibha-*`
   → run the verification checklist → soak.
5. **Cut over Rahul** the same way (he needs no proxy).
6. **Calibrate broker-P&L parser.** Run once under an account's env, e.g.
   `env $(grep -v '^#' accounts/rahul/account.env | xargs) python scripts/fetch_broker_pnl.py --account rahul --user-key user1`,
   inspect the `raw` block in `accounts/rahul/reports/broker_pnl.json`, and fix the field
   names in `scripts/fetch_broker_pnl.py :: normalize()` to match the real responses.

At each step: show the exact commands you intend to run and the expected result, then wait for
approval before anything that stops/starts a live process or places/reads broker orders.
