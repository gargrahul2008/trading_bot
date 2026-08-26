# Dashboard agent — install & test on the control host

Bringing up `webapp/agent` (one per Fyers account) on the master host. Follow in
order; each step says what you should see before moving on.

The agent is **read-only** as installed here — it cannot place an order. Turning
trading on is a separate, deliberate step at the end.

## 0. Before you start

- The agent shares each app's Fyers rate limit with that account's bots. It is
  capped at 60 req/min and yields on `-429` (see `webapp/agent/README.md`), but
  the first run should still be in a **non-trading window** so a mistake cannot
  disturb a live session.
- Then repeat step 3 *during* a session to confirm real positions and orders
  come back — an empty book after close proves the plumbing, not the parsing.
- Nothing here touches bot state. The agent only ever **reads**
  `accounts/<user>/<run>/state/`, to tell bot orders from manual ones.

## 1. Get the code onto the host

From your workstation:

```bash
git push
```

On the host:

```bash
cd /root/trading_bot          # adjust if the repo lives elsewhere
git pull
ls webapp/agent/              # expect: main.py poller.py budget.py ...
```

## 2. Create the agent secret (gitignored — `git pull` will not bring it)

Like `account.env` and `fyers_auth.json`, this must be made on the host.

```bash
cd /root/trading_bot
python3 -c "import secrets; print('AGENT_TOKEN=' + secrets.token_urlsafe(32))" > webapp/agent.env
chmod 600 webapp/agent.env
cat webapp/agent.env          # note the value; you need it to curl the agent
```

The agent refuses to start without it. It binds loopback only, but anything on
the host could otherwise reach it and place a real order.

## 3. Confirm egress, then smoke-test each account

**First check the account leaves by its whitelisted IP.** Pratibha is proxied;
Rahul uses the host IP directly.

```bash
# every proxied account
env $(grep -v '^#' accounts/<user>/account.env | xargs) curl -s https://api.ipify.org; echo
# expect that account's whitelisted IP, e.g. 157.245.108.24 for pratibha

# the home account, which uses the host IP directly
curl -s https://api.ipify.org; echo
# expect: 64.227.135.117
```

Do this for **every** account under `accounts/`, not just the first two. If any
is wrong, stop — everything below would leave from the wrong IP.

**Now the read-only smoke test.** Five requests per account, no orders, nothing
written:

```bash
env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
  .venv/bin/python -m webapp.agent.smoke --user pratibha

env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  .venv/bin/python -m webapp.agent.smoke --user rahul
```

Expected shape:

```
account   : pratibha (user_key=user2)
egress    : http://157.245.108.24:3128
session   : live  intervals={'positions': 3.0, ...}

bot runs claiming orders: ['pratibha/arl', 'pratibha/coolcaps', ...]

funds      available=123456.78 utilised=0.00 realised=0.00
positions  2 row(s)
    NSE:ARL-EQ             LONG        500 @ 92.40      ltp 93.10      unreal    350.00  [positional]
orders     4 row(s)
    2510...      NSE:ARL-EQ             BUY       0/500   PENDING   CNC        pratibha/arl
holdings   6 row(s)
trades     3 row(s)

OK — all five sections read
```

**What to check in that output**, because this is the step that validates the
field mapping against your real accounts:

- `egress` matches the IP you confirmed above.
- `bot runs claiming orders` lists the runs — empty means state files were not
  found and every order will be mislabelled `manual`.
- Numbers are right: quantities, average prices, LTP, unrealised. A `0.00` where
  you expect a figure means a Fyers field name we did not map.
- The last column on orders shows the owning run for bot orders, `manual` for
  anything you placed by hand.

A `FAILED` line naming a section is usually an expired token (re-run the auth
unit) or the wrong IP.

## 4. Run one agent by hand and look at it

```bash
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
    $(grep -v '^#' webapp/agent.env | xargs) \
    .venv/bin/python -m webapp.agent.main --user rahul --port 9102
```

From a second shell on the host:

```bash
TOKEN=$(sed -n 's/^AGENT_TOKEN=//p' /root/trading_bot/webapp/agent.env)

curl -s -H "Authorization: Bearer $TOKEN" localhost:9102/health | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" localhost:9102/positions | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" localhost:9102/book | python3 -m json.tool | head -40
```

In `/health`, confirm:

- `"live": true` — positions and orders are both fresh.
- `"allow_trading": false`.
- `poller.phase` matches the actual market phase (`live`, `closed`, `holiday`…).
- `poller.budget.rate_limited` is `0`. If it climbs, the agent is competing with
  the bots — lower `--per-min`.
- every section's `age_s` is below its `stale_after_s`.

Check the gates while you are here:

```bash
curl -s localhost:9102/health                                    # -> 401 unauthorised
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
     -d '{"symbol":"NSE:RELIANCE-EQ","side":"BUY","qty":1,"order_type":"MARKET"}' \
     localhost:9102/orders                                       # -> 403 read-only
```

Ctrl-C to stop.

## 5. Install as services

```bash
INSTALL_DIR=/root/trading_bot python3 deploy/gen_systemd_units.py
cat deploy/systemd/generated/agent-pratibha.service    # review before installing

sudo cp deploy/systemd/generated/agent-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now $(ls deploy/systemd/generated/agent-*.service \
                              | xargs -n1 basename | sed 's/.service//')
```

Each account's loopback port is recorded in `deploy/agent_ports.json` and never
changes, including when you add an account that sorts before an existing one.
If the generator prints "Recorded new agent port(s)", **commit that file** so the
host and your workstation agree on which port is which account.

```bash
cat deploy/agent_ports.json
```

```bash
systemctl status agent-pratibha agent-rahul
journalctl -u agent-pratibha -f
```

## 6. Soak, and watch the bots

For the first live session, the thing to watch is whether the agent has cost the
bots anything.

```bash
# no rate limiting on either agent
for p in 9101 9102; do
  curl -s -H "Authorization: Bearer $TOKEN" localhost:$p/health \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['user'], d['live'], d['poller']['budget'])"
done

# and nothing new in the bots' logs
journalctl -u 'bot-*' --since "1 hour ago" | grep -i "429\|rate" | tail
```

If `-429` appears in a **bot** log, stop the agents and lower `--per-min`:

```bash
sudo systemctl stop agent-pratibha agent-rahul
```

That is the whole rollback — the agents hold no state and the bots never depended
on them.

## 7. Enabling trading (later, deliberately)

Only after the read-only agents have soaked cleanly, and only when you actually
want to place orders from the dashboard:

```bash
ALLOW_TRADING=1 INSTALL_DIR=/root/trading_bot python3 deploy/gen_systemd_units.py
sudo cp deploy/systemd/generated/agent-*.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl restart agent-pratibha agent-rahul
```

`/health` will then show `"allow_trading": true`. Every place/modify/cancel/exit
is logged at WARNING with account, side, symbol and quantity **before** the call,
so `journalctl -u agent-<user>` is the record even if the broker call fails.
