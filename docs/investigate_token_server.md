# Who is still pulling Pratibha's tokens?

For the Claude on the control host. **Read-only** — the same rules as
`docs/host_survey.md`: change nothing, stop nothing, print no secrets.

## The question

`docs/host_state.md` §5 records that `scripts/token_server.py` on `:8502` is
still serving `user2` (Pratibha) tokens to **157.245.108.24**, as recently as
today.

That address is Pratibha's squid proxy — the box whose IP is whitelisted with
Fyers for her account. Our own agents and bots do **not** use the token server;
they read `fyers_auth.json` directly. So something else is asking for her live
access token, daily, and getting it.

Pratibha's four bots have been held down here since 2026-08-17 precisely so her
account places no automated orders. **If a bot survives on that box, it has a
valid token and her whitelisted IP — everything it needs to be trading her
account right now, and nothing here would show it.** That is the thing to settle.

`docs/multi_account_migration.md` says the old VPS stays as a hot standby until
it soaks for three trading days. That was a month ago.

## What to find out, in order

### 1. What the token server has actually served

```bash
grep -c "user2" logs/token_server.log
grep "user2" logs/token_server.log | tail -30
grep "user2" logs/token_server.log | awk '{print $1}' | sort | uniq -c | tail -20
```

Establish: how often, since when, and whether the cadence looks like a bot
starting each morning or something polling continuously. Note whether `user1` or
`user3` are being served to anything too.

### 2. What the requests look like

Read `scripts/token_server.py` and report what it logs per request — does it
record a user agent, a path, a process identity, anything that would distinguish
a Python bot from a person with `curl`?

### 3. Is there a second listener

```bash
grep -rn "8502\|token_server" --include='*.py' --include='*.sh' --include='*.json' \
  scripts/ deploy/ accounts/ strategies/ common/ 2>/dev/null | grep -v '\.pyc'
```

Anything in this repo that *fetches* from the token server rather than serving
it. If something on this host is the client, the answer is boring and we stop.

### 4. Whether the old VPS is still running anything

**Do not connect to it** — it holds a live trading identity and a whitelisted IP.
Only report what this host knows:

```bash
ss -tnp | grep 157.245.108.24
grep -rn "157.245.108.24" --include='*.sh' --include='*.py' --include='*.json' \
  --include='*.env' . 2>/dev/null | grep -v '\.git/'
cat ~/.ssh/config 2>/dev/null | sed 's/IdentityFile.*/IdentityFile <redacted>/'
last -20
```

Establish whether it is only a proxy to us, or whether anything here treats it as
a host we deploy to.

### 5. The decisive evidence

The one thing that settles it: **is anyone other than us placing orders in
Pratibha's account?**

`agent-pratibha` already polls her order book every few seconds and is running
now. Ask it — no broker call of your own, no new session:

```bash
TOKEN=$(sed -n 's/^AGENT_TOKEN=//p' webapp/agent.env)
curl -s -H "Authorization: Bearer $TOKEN" localhost:9101/orders \
  | python3 -m json.tool | head -60
```

Every order in her account today, whatever placed it, with a `source` of `bot`,
`manual` or `pending`. Her units are held down here, so **any order attributed to
a bot run is one this host did not place.**

Report: how many orders today, their symbols, product types, timestamps and
attribution. Cross-check the symbols against her four held-down runs
(`BSE:SHISHIND-X`, `NSE:INDOTHAI-EQ`, `NSE:COOLCAPS-ST`, `BSE:ARL-B`) — an order
on one of those, today, is the answer.

Do the same for `rahul` on `:9102` and, while you are there, settle a second
open question:

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:9102/holdings \
  | python3 -c "import json,sys; d=json.load(sys.stdin)
rows=[r for r in (d.get('data') or []) if 'VIKASECO' in r['symbol']]
print(rows or 'no VIKASECO holding')"
```

`bot-rahul-vikaseco` was held down today on the assumption the account holds no
VIKASECO. Confirm or correct that.

## Report

Append a section to `docs/host_state.md` titled
`## 11. Token server — who is pulling Pratibha's tokens`, then commit and push
that one file. Lead with the answer, not the method.

If §5 shows orders in her account that this host did not place, **say so in the
first line and stop** — that is an operator decision, not something to act on.
