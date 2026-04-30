# MEXC Capital Management Guide

## What to do when adding or withdrawing USDC

### Step 1 — Stop the bot
Kill the bot BEFORE making any state changes, otherwise the bot will overwrite your edits.
Also temporarily comment out the watchdog cron if needed:
```
crontab -e   # comment out the watchdog line
```

### Step 2 — Record the capital flow
Add an entry to the capital flows file:
`strategies/pct_ladder/state/capital_flows_2026_03_05_v1.json`

```json
{"ts": "YYYY-MM-DD HH:MM", "delta": 31251.12, "note": "added"}
{"ts": "YYYY-MM-DD HH:MM", "delta": -5000.00, "note": "withdrawn"}
```

Use positive delta for deposits, negative for withdrawals.

### Step 3 — Update state file
File: `strategies/pct_ladder/state/mexc_state_2026_04_13_v1.json`

Update two values under `extras` by adding (deposit) or subtracting (withdrawal) the amount:

| Field | Purpose |
|---|---|
| `portfolio_start_value` | baseline for portfolio PnL display |
| `compound_initial_equity` | baseline for step size compounding |

```bash
python3 -c "
import json
path = 'strategies/pct_ladder/state/mexc_state_2026_04_13_v1.json'
with open(path) as f:
    s = json.load(f)
injection = 1000.0   # use negative for withdrawal
extras = s['extras']
extras['portfolio_start_value']  = str(float(extras['portfolio_start_value'])  + injection)
extras['compound_initial_equity'] = str(float(extras['compound_initial_equity']) + injection)
with open(path, 'w') as f:
    json.dump(s, f, indent=2)
print('portfolio_start_value: ', extras['portfolio_start_value'])
print('compound_initial_equity:', extras['compound_initial_equity'])
"
```

### Step 4 — Recalculate step size
The step size (buy_quote) should always be ~5% of total initial equity to maintain consistent runway:

```
buy_quote = compound_initial_equity × 5%
```

**Also adjust for grid pct:**
- 0.4% grid → buy_quote = initial_equity × 5%
- 0.2% grid → buy_quote = initial_equity × 2.5%  (half, to keep same price runway)

General formula:
```
buy_quote = initial_equity × (grid_pct / 0.4) × 5%
```

Run dry-run to preview compounded step:
```bash
env/bin/python scripts/mexc_compound.py \
    --config strategies/pct_ladder/config.mexc.json \
    --trades strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl \
    --dry-run
```

Then update step in state file:
```bash
python3 -c "
import json
path = 'strategies/pct_ladder/state/mexc_state_2026_04_13_v1.json'
with open(path) as f:
    s = json.load(f)
new_step = '2113.0'   # your new step
extras = s['extras']
extras['compound_initial_buy_quote'] = new_step
extras['compound_buy_quote']         = new_step
extras['compound_sell_quote']        = new_step
with open(path, 'w') as f:
    json.dump(s, f, indent=2)
print('Step updated to:', new_step)
"
```

### Step 5 — Update grid pct in config (if changing)
File: `strategies/pct_ladder/config.mexc.json`
```json
"upper_pct": 0.2,
"lower_pct": 0.2,
```

### Step 6 — Restart the bot
```bash
screen -S mexc bash -c "/root/trading_bot/scripts/mexc_bot_runner.sh"
```
Re-enable the watchdog cron if you commented it out.

---

## Capital history

| Date | Delta (USDC) | Note | initial_equity after |
|---|---|---|---|
| 2026-01-20 | +3100 | added | — |
| 2026-01-29 | +3051 | added | — |
| 2026-02-24 | -980 | adjusted | — |
| 2026-03-02 | +20976 | added | — |
| 2026-03-07 | +10571 | added | — |
| 2026-03-18 | +15790 | added | 53233.00 |
| 2026-04-27 | +31251.12 | added | 84484.12 |

---

## Current config snapshot (2026-04-27)

| Parameter | Value |
|---|---|
| grid pct | 0.2% |
| buy_quote (config) | 2662 |
| compound_initial_buy_quote | 2113 |
| compound_buy_quote | 2113 |
| compound_initial_equity | 84484.12 |
| Step as % of equity | ~2.5% (= 5% at 0.4% grid equivalent) |

---

## Key rules

- **Always stop the bot before editing state** — bot dumps state every tick and will overwrite changes.
- **Both `portfolio_start_value` and `compound_initial_equity` must be updated** on every capital change.
- **Step size ratio**: maintain ~5% of equity per step at 0.4% grid. Halve step if halving grid pct.
- **Runway**: `(cash / buy_quote) × pct` = price % the bot can absorb in one direction. Keep ~4%.
- After updating state, run compound dry-run to confirm new step looks correct before restarting.
