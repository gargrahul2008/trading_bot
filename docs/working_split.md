# How the two Claudes divide the work

There are two: one on Rahul's **local machine** with the repo, and one on the
**control host** where the money is. This is the standing arrangement, so it does
not have to be explained per task.

## The rule

> **The local Claude writes code. The host Claude writes `docs/*.md`.**

That one line is what makes it safe for both to work on the same repository at
once. Divergent commits to the same files is the failure this prevents, and it
has already been close: an agent port was recorded locally while the host had
generated the same one independently.

| | Local | Control host |
|---|---|---|
| Writes | code, tests, docs | `docs/*.md` reports only |
| Runs | the test suite, stub agents, a local UI | the real thing |
| Sees | no live data | three live accounts |
| Can break | a test | a trading day |

## Why development stays off the host

Not caution for its own sake — it has already cost a day. Checking out a branch
on the host removed `deploy/cron/` for 13 hours: no token refresh, neither of
Rahul's bots started, the BTST leg skipped, and the MEXC audit went dark across a
$25k withdrawal. Development means branches, stashes and half-written files, and
`deploy/deploy.sh` refuses to deploy from a dirty tree by design.

The host pulls, verifies and deploys. It does not develop.

## What the host Claude is for

It is the only one that can see reality, and it has repeatedly found things no
amount of local reasoning would have:

- that the bot units were deliberately disabled because cron owns their daily
  lifecycle — after the local Claude had wrongly advised enabling them
- that Fyers stamps every order with the channel that placed it, which replaced
  an inference with broker truth
- that the host's SDK lacks all three history methods the plan depended on
- that `/ledger-history` silently truncates at 100 rows and looks complete
- that `exch_id` disagrees with `symbol_name` on real rows

Its standing job:

1. **Verify.** Run what the local Claude built, against real accounts, and report
   what actually happened — including when it contradicts what was expected.
2. **Investigate.** Read-only questions about the host: what runs, what changed,
   why something failed.
3. **Probe.** Fetch real payloads before an importer or parser is written against
   guessed field names. This has paid for itself every time.
4. **Deploy.** `deploy/deploy.sh --apply`, then report the output.

## Standing rules for the host Claude

- **Read-only unless the task says otherwise.** No `systemctl start/stop/enable/
  disable`, no edits to config, state, cron or units, no `git checkout`, no
  package installs.
- **Never touch a bot.** Their lifecycle is `deploy/cron/start_equity_bots.sh`,
  which also holds individual bots down. Starting one that is held down puts a
  position back in the market that somebody deliberately took out.
- **Redact.** No tokens, secrets, TOTP keys, PINs or password hashes in any
  report. Account ids and proxy IPs are already in these docs and are fine.
- **Commit only `docs/*.md`**, one file, and say what changed. Never merge, never
  rebase, never force.
- **Say when the docs are wrong.** Reality beats the repo's description of it,
  and the correction is the most valuable thing in any report.

## Standing rules for the local Claude

- **Never advise a change to live infrastructure without reading what governs
  it.** `deploy/cron/` was the file that made "enable the bot units" wrong, and
  it was not read first.
- **Do not guess a broker payload's shape.** Ask for a probe. Three separate bugs
  came from guessed field names: carry-forward quantities dropped, a delivery
  sale reported as a short, and an importer nearly written against SDK methods
  that do not exist on the host.
- **Push before asking the host to pull.** It cannot read unpushed commits, and
  a brief that has not landed looks exactly like a task not done.

## The loop

```
local:  write, test, commit, push
host:   pull, run, report to docs/, push
local:  pull, read, fix
```

## Where things are written down

| | |
|---|---|
| `docs/dashboard_plan.md` | what is being built, and what every figure means |
| `docs/host_state.md` | what actually runs on the host — the host Claude's report |
| `docs/deploying.md` | the one deploy command, and adding an account |
| `docs/dashboard_https.md` | putting the dashboard behind TLS |
| `webapp/*/README.md` | why each component is shaped the way it is |
