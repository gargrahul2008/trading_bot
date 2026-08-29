# P&L matching

The broker reports realised P&L **per symbol, not per trade**. Every figure the
dashboard shows about a *trade* — what it made, how long it was held, whether it
was long or short, intraday or positional — is computed here.

```
fills (store) ──▶ matcher.py ──▶ closed round trips + what is still open
```

## Signed lots

A buy is a positive lot, a sell a negative one, and a fill first consumes open
lots of the opposite sign before opening one of its own.

That single rule covers everything awkward:

- **Shorts are not a special case.** A short is a position that happens to be
  negative. Sold at 12, bought back at 10 is a 200 gain, and one expression
  gives that for both directions because the lot's sign already carries it.
- **A sell larger than the position** closes the long and opens a short in the
  same fill, with no branch for it.
- **Partial exits** split the lot and leave the remainder open at its original
  cost — FIFO, not average cost, so each match carries its own entry price.

## Books are (account, symbol, product_type)

The same symbol held as CNC and traded as INTRADAY are different positions to
the broker. Netting them would invent a round trip that never happened.

## Intraday is what happened, not what was allowed

`kind` comes from comparing the entry and exit **days**, not from
`product_type`. A CNC buy sold the same afternoon was an intraday trade whatever
it was booked as. `product_type` is reported alongside, not instead.

## Match over all history, then filter

A position opened last week and closed today is a *today* trade, and its entry
is only findable by replaying the fills before it. Matching one day's fills in
isolation would leave that exit unmatched and report a day's P&L quietly missing
its positional trades. So `service.py` always replays everything and filters the
output by `closed_day`.

## Decimal, not float

These are money figures summed over thousands of fills. `10.4 - 10.1` is
`0.30000000000000004` in float, and that error compounds.

## Gross, not net

Charges are excluded here. The broker reports them per day and per segment, not
per trade, so they are apportioned separately — mixing an exact per-trade figure
with an apportioned estimate in one number would make neither trustworthy.

## Seeing it

```bash
env/bin/python -m webapp.pnl.report
env/bin/python -m webapp.pnl.report --account rahul --day 2026-08-28
```

`--day` filters which closed trades are *shown*, never which fills are matched.

## Tests

`tests/webapp/test_matcher.py` states the money outcome of each case explicitly
rather than asserting a shape: the short that wins and the short that loses, the
position flip in both directions, FIFO across several entries, execution order
against input order, a day boundary beating a timestamp, and an order filled in
27 pieces sharing one timestamp — which is how Fyers actually reported
pratibha's SHRINGARMS sale.
