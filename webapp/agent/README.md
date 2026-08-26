# Per-account Fyers agent

One process per Fyers account. It is the **only** thing in the dashboard stack
that talks to a broker.

## Why it exists

SEBI requires a registered static IP per demat, and Fyers enforces it: every
call must leave from the IP whitelisted for that account. The SDK takes its
proxy from the **process environment**, which is global — so a single web server
cannot serve two accounts without racing on that variable, and with order
placement in scope that race means orders leaving through the wrong IP.

One process per account makes the binding structural: the agent is started from
`accounts/<user>/account.env`, so its proxy, its token and its identity are all
fixed for the life of the process and cannot be confused with another account's.

```
dashboard API  ──localhost──▶  agent: rahul     ──proxy──▶  Fyers (rahul's IP)
   (no token,                  agent: pratibha  ──proxy──▶  Fyers (pratibha's IP)
    no proxy,
    never calls a broker)
```

## The rate budget

Limits are per **app**, and the bots are already spending them: about 24
requests/minute per run, so ~96/min on pratibha's app across its four runs. We
have hit `-429` in production before — see the comment at
`common/engine/generic_runner.py:2431`.

So the agent is capped (`--per-min`, default 60) and, when quota is short, it is
the side that yields. A bot missing a poll can miss a fill; the agent missing one
shows a figure three seconds old. Two mechanisms enforce it:

* a leaky bucket (`budget.py`) that refuses rather than queues, and
* priority order in `poller.py` — a squeeze costs the holdings refresh long
  before it costs the order book.

On a `-429` the agent stops for 30s and then runs at half rate for five minutes.
Allowance does not accrue during that pause, so it does not answer a rate limit
with a burst.

## What it polls, and how often

Cadence follows the market session (`session.py`), like the bots'
`closed_poll_seconds`. During the live session:

| Section | Interval | Cost/min | Why |
|---|---|---|---|
| `positions` | 3s | 20 | net qty, average, **and LTP + unrealised** |
| `orders` | 3s | 20 | every order — bot and manual — with status and fills |
| `funds` | 30s | 2 | only moves on fills and margin changes |
| `holdings` | 60s | 1 | the CNC book barely moves intraday |
| `trades` | on fill, swept at 60s | ~2 | exact traded price, needed for P&L matching |

**≈45/min.** Quotes are never polled: the Fyers positions and holdings payloads
already carry `ltp`, so live MTM comes free with a call we make anyway.

Outside the session everything slows down, and on a weekend or NSE holiday it
drops to a 15-minute heartbeat — enough to surface a dead token before Monday
rather than during it.

## Staleness

Every section is served with `as_of`, `age_s` and `stale`. A failed refresh keeps
the last good payload and attaches the error, rather than blanking the screen —
but the timestamp does not move, so the figure visibly ages towards stale.

This matters because the dashboard is used to act. A stale position rendered as
current is how you place the wrong trade, so the UI must grey out anything
flagged `stale` rather than showing it as live.

## Bot or manual?

`attribution.py` reads each run's own files off the same host — no broker call —
using three sources, in descending order of certainty:

1. **Live claims** — order ids a run currently has working, from `state.json`.
2. **Sticky claims** — every id we have ever seen claimed, kept in
   `accounts/<user>/reports/agent_claims.json`.
3. **Configured symbol** — a run trades one symbol with one product type, both
   in its `config.json`. An order matching that pair is almost certainly its.

Sticky claims exist because a bot order **cancelled without filling disappears
from both of the bot's own records**: `_clear_pro_oids` empties the live list,
and `trades.jsonl` is written only on a fill (`generic_runner.py:667`). After the
EOD cancel, every unfilled bot order of the day would otherwise read as manual —
which is exactly what the first live run showed.

The third source is an **inference, not a claim**: the same symbol traded by
hand, in the same account, on the same product would look identical. So the
label carries `matched_by` (`order_id` or `symbol`) and the UI must distinguish
them — and warn before acting either way. Product type has to agree, so buying
RELIANCE as CNC by hand is not the MTF ladder's order. Two runs on the same
symbol and product are not guessed between: a wrong run name is worse than none.

A bot places an order and writes its state a moment later, so an order younger
than 15 seconds that nobody has claimed yet is reported as `pending`, not
`manual`, and settles on a later poll.

## Running it

```bash
# once, on the host
cp webapp/agent.env.example webapp/agent.env
python3 -c "import secrets; print('AGENT_TOKEN=' + secrets.token_urlsafe(32))" > webapp/agent.env
chmod 600 webapp/agent.env

# generate the units (read-only unless ALLOW_TRADING=1)
INSTALL_DIR=/opt/trading_bot python3 deploy/gen_systemd_units.py
sudo cp deploy/systemd/generated/agent-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now $(ls deploy/systemd/generated/agent-*.service \
                              | xargs -n1 basename | sed 's/.service//')
```

Ports come from `deploy/agent_ports.json`, which is tracked and append-only, so
an account keeps its port even when a new one is added ahead of it alphabetically.

By hand, for one account:

```bash
env $(grep -v '^#' accounts/pratibha/account.env | xargs) \
  AGENT_TOKEN=$(sed -n 's/^AGENT_TOKEN=//p' webapp/agent.env) \
  .venv/bin/python -m webapp.agent.main --user pratibha --port 9101
```

## Trading is off by default

Without `--allow-trading` the place/modify/cancel/exit routes return 403 whatever
they are sent. Turn it on per account, deliberately.

Every write is logged at WARNING with the account, side, symbol and quantity
before the call is made, so the journal survives even if the broker call fails.

## Endpoints

All require `Authorization: Bearer $AGENT_TOKEN`.

| Method | Path | |
|---|---|---|
| `GET` | `/health` | liveness, section ages, poller phase, budget state |
| `GET` | `/book` | every section at once — what the dashboard polls |
| `GET` | `/positions` `/orders` `/holdings` `/funds` `/trades` | one section |
| `POST` | `/orders` | place *(needs `--allow-trading`)* |
| `PATCH` | `/orders/{id}` | modify *(ditto)* |
| `DELETE` | `/orders/{id}` | cancel *(ditto)* |
| `POST` | `/positions/{id}/exit` | square off *(ditto)* |

## Tests

```bash
.venv/bin/python -m pytest tests/webapp/ -q
```

The polling schedule, the budget's refusals and the HTTP surface are all tested
without a broker; `tests/webapp/fakes.py` stands in for one.
