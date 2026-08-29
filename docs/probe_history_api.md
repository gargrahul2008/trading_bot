# Probe the three history endpoints

For the Claude on the control host. **Read-only** — same rules as
`docs/host_survey.md`: change nothing, print no secrets.

## Why

`docs/dashboard_plan.md` needs history from 1 April 2026, and three Fyers
endpoints should provide it:

| Endpoint | Wanted for |
|---|---|
| `/ledger-history` | capital in and out — "how much fund was there" |
| `/realised-pnl-history` | realised P&L for the year to date |
| `/charges-history` | charges, to net off per round trip |

**Nobody here knows what they return.** The SDK documents `charges_history`'s
parameters and says nothing about the other two, and no response shape is
recorded anywhere in this repo.

Guessing at field names has already cost real time on this project: the position
parser looked right and was quietly dropping the carry-forward quantities and
reporting a delivery sale as a short. It was only fixed by dumping a raw payload
and reading it. Do that first here, before any importer is written against a
guess.

## Rules

- **Read-only.** These three endpoints only. No order, modify, cancel or exit.
- Run each account's call **under its own `account.env`**, so it leaves by that
  account's whitelisted IP. A call from the wrong IP fails and tells you nothing.
- **A few calls, not a sweep.** The agents share these accounts' rate budget with
  the bots. One call per endpoint per account is enough; the market is closed, so
  there is headroom, but there is no reason to use it.
- **Redact.** No tokens. Client ids and `fy_id` are already in the repo docs and
  are fine.

## What to run

For each of rahul, pratibha, piyush:

```bash
cd /root/trading_bot
env $(grep -v '^#' accounts/<user>/account.env | xargs) env/bin/python - <<'PY'
import json, os, sys
sys.path.insert(0, '.')
from common.broker.auth_json import get_fyers_creds_from_json
from common.broker.fyers_client import FyersClient

client_id, token = get_fyers_creds_from_json('fyers_auth.json',
                                             user_key=os.environ['FYERS_USER_KEY'])
fy = FyersClient(client_id=client_id, access_token=token)._fyers

RANGE = {"from_date": "2026-04-01", "to_date": "2026-08-29"}

for name, call, data in (
    ("ledger_history",          fy.ledger_history,          dict(RANGE)),
    ("realised_profit_history", fy.realised_profit_history, dict(RANGE)),
    # report_type 1 is date-wise, 2 is segment-wise. Both are wanted: one says
    # what to apportion, the other is the exact total to check it against.
    ("charges_history_daywise", fy.charges_history,
        dict(RANGE, segment_type="0", exchange_type="0", report_type="1")),
    ("charges_history_segment", fy.charges_history,
        dict(RANGE, segment_type="0", exchange_type="0", report_type="2")),
):
    try:
        resp = call(data=data)
    except TypeError:
        resp = call(data)
    except Exception as exc:
        print("=== %s FAILED: %s" % (name, exc)); continue
    print("=== %s" % name)
    print(json.dumps(resp, indent=2, default=str)[:4000])
    print()
PY
```

## What to report

Append `## 12. History endpoints` to `docs/host_state.md`, then commit and push
that file only.

For each endpoint, per account:

1. **Did it work?** An error code and message is a useful answer — some of these
   need a permission the app may not have.
2. **The exact field names**, with one real row as an example. Amounts may be
   rounded; names must be verbatim.
3. **The granularity.** Per transaction, per symbol, per day, per segment? Is
   realised P&L per *trade* or per *scrip* — that decides whether per-trade
   history before 28 Aug is recoverable at all.
4. **Is it paginated?** `charges_history` takes `page_size` and `page_no`; say
   whether the others do, and what a full year's volume looks like.
5. **Do the totals reconcile?** Does day-wise charges sum to the segment-wise
   total, and does realised history agree with what the positions and the
   dashboard show for today.

Then, in your own words: **what can and cannot be reconstructed from 1 April
2026 with these three endpoints alone**, and what would still need a manual entry
or an export.

That last question is the one the plan turns on.
