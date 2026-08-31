# The dashboard: what we are building

Agreed 2026-08-29. This is the shared understanding — if something here is wrong,
fix this file before writing code against it.

## What it is for

Rahul runs **several Fyers accounts** (three today, one being set up, growing).
The dashboard answers, per account and for all of them together:

- how much capital went in
- how much is deployed, and how much is free
- what has been made or lost — realised and unrealised
- how the bots are performing, separately from what was traded by hand

and lets orders be placed from the same screen.

The consolidated view is the point. Anyone can read one account in the broker's
own app; nobody can read six at once.

## Definitions

Most dashboards go wrong here rather than in the arithmetic, so these are fixed:

| Term | Means | Source |
|---|---|---|
| **Capital in** | opening cash + securities already held + net transfers | `/ledger-history` + a one-time manual entry |
| **Free** | cash available to trade now | `funds` → Available Balance |
| **Deployed** | cost basis of everything open — positions and holdings, at what was paid, not at market | matched lots + holdings |
| **Market value** | the same at today's marks | positions/holdings `ltp` |
| **Realised** | closed round trips, **net of charges** | matcher + charges |
| **Unrealised** | (mark − cost) × qty on what is still open | positions/holdings |
| **Intraday / positional** | opened and closed on the same day, or not | matcher, from the days |
| **Long / short** | the sign of the position | matcher, signed lots |

**Capital needs one figure the API cannot give.** `/ledger-history` records the
opening *cash* balance and every transfer since — but not the securities the
account already owned on 1 April. That money went in during earlier years and
nothing in this financial year records it. Without it the base is too small and
every return measured against it is too large: pratibha showed 17,07,978 in
against 50,07,446 deployed.

`scripts/capital.py --set <account> <amount>` records it, once. The dashboard
flags any account where deployed exceeds capital, because that is the visible
symptom — though leverage causes it legitimately too, and rahul's 3x MTF ladder
does.

Two figures that look alike and are not, and must never share a column:

- A **position's realised** covers the whole life of the trade.
- The **account's realised** from `funds` is *today's* mark-to-market from the
  previous close. For anything carried overnight these differ — a TATAELXSI
  short showed −9,275 and −3,750, and both were right.

## History

Starting point: **1 April 2026**, the financial year.

| What | How | Granularity |
|---|---|---|
| Capital in/out | `/ledger-history` | per transaction |
| Realised P&L | `/realised-pnl-history` | per symbol (shape to confirm) |
| Charges | `/charges-history` | per day, or per segment |
| Per-trade fills | the store + `reports/trades_all.jsonl` | **from 2026-08-01** |
| Realised per scrip **per day** | `/realised-pnl-history`, one call per day | per scrip per day |
| Positions open before 1 Apr | one-time manual entry | per symbol |

Probed against the live accounts on 2026-08-31 — `docs/host_state.md` §12 has the
field names, the traps and the reconciliations. What that established:

- **FY-to-date totals are exact.** Capital comes from `/ledger-history`'s
  `summary_data` in one call; realised and charges reconcile to the paisa across
  two endpoints.
- **Realised is recoverable per scrip *per day*.** The endpoint has no date field
  and looks like one row per symbol for the whole window — but the window is a
  free parameter and the endpoint is additive over it, so calling it once per day
  gives per-day granularity. ~100 calls per account for the year.
- **Per-trade P&L exists from 1 August**, from the store and the seeded
  `reports/trades_all.jsonl`. Before that the finest truth available is
  per-scrip-per-day, and no combination of these endpoints goes finer — two round
  trips in one scrip on one day collapse into a single averaged row.
- **Bot-versus-hand attribution cannot be reconstructed** for the back period. No
  history endpoint carries an order id or a channel.

So the dashboard shows per-trade detail from 1 August and day-level truth before
it, and says which is which rather than blurring them. A Fyers back-office export
is the only way to get per-round-trip detail for April–July, loaded once by hand.

## Charges

Per **round trip**, not per fill: buy 1000 TCS and sell 1000 TCS is one trade,
and its charges are one number.

The broker reports charges per day and per segment — never per symbol — so they
are apportioned to fills by turnover share, and a match takes a pro-rata slice of
its entry and its exit. Day-wise and segment-wise totals reconcile exactly, which
gives a hard control total to apportion against rather than a guess.
Every net figure is therefore marked **estimated** where it came from
apportionment, and the exact day-level total is always available beside it. A
number that is exact and one that is apportioned must never be added without
saying so.

## Pages

In build order.

1. **Portfolio** — the primary screen. Per account and consolidated: capital in,
   free, deployed, market value, realised FY, unrealised, return. Scales to
   twenty accounts, not just three.
2. **Positions** — everything open now, long and short, intraday and positional,
   with the delivery-sale distinction.
3. **Trades** — every closed round trip with its own P&L, net of charges, tagged
   bot or manual, filterable by account, symbol, day, FY.
4. **Orders** — full order book including cancelled and rejected, with parsed
   reject reasons.
5. **Holdings** — the delivery book with cost, mark and days held.
6. **Bots** — per run: its trades, its P&L, whether it is running, its state.
7. **Order pad** — place, modify, cancel, exit. Market, limit, SL, SL-M, and
   orders carrying a target and stop-loss.

Out of scope for now: MEXC, and the old Streamlit pages other than Fyers auth.

## Order placement

Required from the start, not bolted on later. Beyond the plain types, orders
must be able to carry a **target and a stop-loss**.

The safety rules already agreed:

- Off unless the agent runs with `--allow-trading`.
- Bot-owned orders are marked, and touching one warns and offers to pause the
  run first.
- Every action is logged before the call is made, so the record survives a
  failure.
- The account is chosen explicitly, never defaulted.

## What has to be true before each phase

- **History** — done. `docs/host_state.md` §12 records the shapes. Three
  constraints it found that any importer must respect: the installed SDK on the
  host lacks all three methods (so call REST directly, do not upgrade the SDK the
  bots trade through); `/ledger-history` paginates and **silently truncates** at
  100 rows; and `exch_id` / `exchange_name` / `segment_name` disagree with
  `symbol_name` on real rows — key on the symbol.
- **Charges per trade** needs the charges shape and a decision on segment
  granularity.
- **Order pad** needs `place_order` extended for SL/SL-M and the target/stop-loss
  fields, and a confirm flow.
- **Anything relied on daily** needs the API under systemd with TLS. It runs in
  an SSH foreground today.

## Non-goals

- Not a backtester, not a research tool. Those live elsewhere in this repo.
- Not a replacement for the broker's terminal for charting or market depth.
- Not multi-user: one operator, one password. Roles can come later if that
  changes.
