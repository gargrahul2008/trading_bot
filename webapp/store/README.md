# The store

One SQLite file, written by the agents and read by the API.

```
agent-rahul     ─┐
agent-pratibha  ─┼─ write ─▶  webapp/data/dashboard.db  ─ read ─▶  API ─▶ browser
agent-piyush    ─┘                                          ▲
                                                            └─ live agents, preferred
```

## Why it exists

Two reasons, and the second is the one that shows up daily.

**History.** Fyers wipes the order book and tradebook every day. A day not
ingested is a day of trade-level detail that cannot be recovered — the per-trade
P&L work has nothing to stand on without this.

**Continuity.** Before the store, an agent being down meant that account had *no*
row on the dashboard — indistinguishable from an account holding nothing. Now the
API falls back to what the agent last wrote, marked as from the store and
visibly ageing. "We cannot reach this account" and "here is what it was four
minutes ago" are very different things to be looking at.

## What it is not

Not the live path. The API prefers the agents and only reads the store when one
does not answer. A stored figure is stale **by definition** — `source: "store"`,
`stale: true`, always with an age — and the UI greys the row and says so.

## Write on change, not on poll

Positions are read every 3 seconds and change a few times an hour. Storing every
read would be 20 near-identical rows a minute per account; storing the changes is
the same information. `Writer.snapshot` hashes the payload and skips a write when
it matches the last one.

Orders are upserted — one row per order whose status moves, not a row per poll.
Fills are insert-once, since a trade never changes and the broker keeps returning
it all session.

Attribution is allowed to *improve* but never to regress: an order first matched
by its configured symbol becomes matched by order id once the bot records its
claim, and a later poll with no attribution cannot blank a run that is already
known.

## Persisting must never break polling

Every write path swallows its own errors and counts them. A locked database or a
full disk is a reason to lose history — never a reason for the dashboard to go
blank or for an agent to stop reading the broker. An agent that cannot open the
store at all logs it once and runs without persistence.

## Concurrency

Three agents write while the API reads:

- **WAL**, so a read never waits behind a write.
- **busy_timeout 5s**, because three writers will occasionally collide and
  waiting is better than losing a fill to a lock.
- The API opens **read-only** connections — a guarantee, not a convention — and a
  fresh one per request, since `sqlite3` handles are not thread-safe and FastAPI
  serves on many.

## Is it filling?

```bash
env/bin/python -m webapp.store.status
```

Read-only. Per account: when the agent was last heard from, how old each
snapshot is, and how many orders and fills are recorded. The last line is the
one that matters — if nothing has been written for over five minutes, the agents
are not running, because status alone is written every ~15 seconds.

## Layout

| | |
|---|---|
| `schema.py` | DDL, `connect()`, `migrate()`. `PRAGMA user_version` gates upgrades and refuses to run against a newer schema. |
| `writer.py` | Agent side. Upserts orders, insert-ignores fills, snapshots on change, records agent health. |
| `reader.py` | API side. Rebuilds a book in the *same shape* an agent's `/book` returns, so the aggregation does not need to know which source it read. |

The database lives at `webapp/data/dashboard.db` (gitignored), overridable with
`DASHBOARD_DB` or `--db`. `--db none` disables persistence entirely.
