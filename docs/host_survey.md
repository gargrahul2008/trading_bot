# Host survey — for the Claude running on the control host

You are on the **live trading control host** (`64.227.135.117`, `/root/trading_bot`).
A second Claude is working on the same repo from a local machine and needs an
accurate picture of what actually runs here. Guessing has already cost several
rounds of back-and-forth.

Produce a report. **Change nothing.**

## Hard rules

- **Read-only.** No `systemctl start/stop/restart/enable/disable`, no `kill`, no
  edits to config, state, cron or units, no `git checkout`/`reset`/`pull`, no
  package installs. The only write is the report file itself.
- **Real money is trading right now.** Rahul's two equity bots run 08:55–15:31
  IST, MEXC bots run continuously. Nothing you do may interrupt them.
- **Redact secrets.** Never print an access token, refresh token, TOTP key, API
  secret, password, or password hash. Show the last 4-6 characters at most where
  identity matters. Proxy IPs and account IDs are fine — they are already in the
  repo docs.
- **Don't call brokers.** No script that places orders or hits a broker API.
  `deploy/preflight.sh --quick` is safe and worth running; the full version makes
  outbound IP checks, which is also fine, but nothing beyond that.
- If a command would take more than ~30s or produce thousands of lines, sample
  it instead (`head`, `tail`, counts) and say you did.

## What to report

Write to `docs/host_state.md`, replacing any previous version. Use headings in
this order. Where something surprises you or contradicts the repo's docs, say so
explicitly — that is the most valuable part of this.

### 1. Machine
`uptime -s`, `uname -a`, disk free, memory, load. Anything close to a limit.

### 2. Everything that runs
The complete inventory, however it is started:

- **systemd**: `systemctl list-unit-files 'bot-*' 'agent-*' 'fyers-*' 'mexc-*'`
  plus anything else trading-related; for each, enabled state, active state, and
  what starts it if not systemd itself.
- **cron**: the full `crontab -l`, annotated — for each entry, what the script
  does and whether it is currently active or commented out.
- **timers**: `systemctl list-timers --all`.
- **processes**: `ps aux` filtered to python/trading processes, with start times.
  Flag anything running that no unit or cron entry explains — a stray manual
  process is exactly the kind of thing we need to know about.
- Anything in `screen`/`tmux` sessions.

### 3. The equity side
- `accounts/` inventory: user -> runs, each run's symbol, product type, and
  whether it is in `HOLD_DOWN` in `deploy/cron/start_equity_bots.sh`.
- For each run's `state/`: files present, size, last modified. Flag any whose
  `state.json` is older than its last trading day — that is drift.
- `fyers_auth.json`: for each user key, its label, matching `accounts/` dir,
  `auto_refresh`, `token_updated_at`, and the proxy from that account's
  `account.env`. **No tokens.**
- Which accounts have `reports/` and what is in them.

### 4. The crypto side
What MEXC bots/buckets are live, how they are started, where their state and
config live, and how they relate to `strategies/pct_ladder/`. Note anything in
`strategies/` that is still live rather than superseded by `accounts/`.

### 5. Anything else trading-related
`fyers-pca-engine`, the token server, the BTST paper bot, the fib bot, the old
Streamlit dashboard — for each: is it running, what starts it, what does it do,
does anyone depend on it.

### 6. The dashboard we are building
- `systemctl status agent-piyush agent-pratibha agent-rahul`
- `deploy/preflight.sh` output in full
- Whether `webapp/agent.env` and `webapp/dashboard.env` exist (not their contents)
- Whether the API or UI has ever been started here, and how

### 7. Repo state
Current branch and commit, `git status --short`, and — importantly — any
**untracked or locally-modified file that matters**: config the repo does not
know about, hand-edited units, scripts in `/root` outside the repo.

### 8. Ports
`ss -ltnp` — what listens, on which interface, and what owns it.

### 9. Logs
`ls -la logs/` with sizes and last-modified. For the last two trading days, any
ERROR/WARNING patterns in the equity bot logs and the agent logs, summarised —
counts and examples, not raw dumps. Specifically check for `-429`, `-16`,
`Could not authenticate`, and any restart loops.

### 10. Your own read
Close with:
- Anything that looks wrong, fragile, or undocumented.
- Anything that contradicts `docs/multi_account_architecture.md`,
  `docs/master_host_runbook.md`, or `webapp/agent/README.md`.
- Single points of failure: what breaks if this box reboots right now?
- Open questions you could not answer read-only.

## When done

```bash
git add docs/host_state.md
git commit -m "Add a survey of the control host's actual state"
git push
```

Committing this one file is expected — it is documentation, not configuration.
Do not commit anything else, and do not merge or rebase.
