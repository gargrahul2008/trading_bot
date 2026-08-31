# Account history

Capital, realised P&L and charges from 1 April 2026 — the financial year this
dashboard measures from.

## Why REST and not the SDK

The control host runs `fyers-apiv3` **3.1.10**, which has none of
`ledger_history`, `realised_profit_history` or `charges_history`. Those two
wrappers in `FyersClient` raise `AttributeError` there and have never been
called.

Upgrading the SDK is the wrong fix: it is the library the live bots trade
through, and this is a reporting feature. So `client.py` calls the endpoints
directly, using the SDK's own base URL and header format.

## What the probe established

Shapes, traps and reconciliations were measured against the three live accounts
on 2026-08-31 — `docs/host_state.md` §12. Three findings shape this code:

**The ledger silently truncates.** A plain call returns exactly 100 rows and
looks complete; rahul's first page held one month of the five requested.
Everything pages until a *short* page — not an empty one, since a full final page
followed by an empty one is indistinguishable from truncation if you stop early.

**`exch_id`, `exchange_name` and `segment_name` are wrong on real rows.**
`BSE:SHISHIND-X` comes back as NSE, and the store has 50 fills of it on BSE. Only
`symbol_name` is carried through; the others are not stored at all, because
storing them wrong is worse than not storing them.

**Realised P&L has no date field, but the window is a free parameter** and the
endpoint is additive over it — two half-windows summed to the full window
exactly. So `realised_by_day` recovers per-day figures by asking one day at a
time. That is the difference between day-level history and one number per symbol
for the year.

## Which realised figure is authoritative

The **broker's**. `realised_history` is the headline; our own FIFO matching
supplies per-trade detail. They are never added, so they cannot double-count —
and where both cover a period they can be compared, which makes a disagreement a
finding rather than a nuisance.

## Capital is transfers only

A ledger row of type `Trading` is the day's P&L settling into the balance, not
money put in. Counting it as capital would make every rupee earned look like a
rupee deposited and drive the return figure towards zero. Only `Funds added` and
`Funds withdrawn` count.

## Running it

Per account, under its own `account.env` so the call leaves by its whitelisted
IP:

```bash
# routine: the last week
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  env/bin/python scripts/fetch_history.py --account rahul

# first time: the whole financial year, with per-day realised
env $(grep -v '^#' accounts/rahul/account.env | xargs) \
  env/bin/python scripts/fetch_history.py --account rahul \
  --from 2026-04-01 --daily-realised
```

`--dry-run` fetches and reports without writing.

The backfill is ~150 calls per account, because per-day realised needs one call
per calendar day. These share a rate budget with the live bots, so **run a full
backfill outside market hours**. The routine weekly window is a handful of calls
and is safe any time.

Everything is idempotent on re-import: capital on
`(account, source, reference)`, realised on `(account, day, symbol)`, charges on
`(account, day)`. Re-fetching the last few days each evening is the intended way
to stay current.
