# Percent Ladder (Reactive)

This is the refactored, drop-in *percentage ladder* built to address:

- **Sellable inventory mismatches** due to `T1/T2/T0` holdings in FYERS holdings response.
- **"You can sell only X quantity"** rejects: we clamp SELL to broker-reported sellable qty and also parse qty from reject messages.
- **Adopt existing holdings as tradable inventory** (so strategy sells what you already have; no "core").

## Your Rajoo config (example)

- `ladder.upper_pct = 1` and `ladder.lower_pct = 1`
- `sizing.mode = fixed_qty`
- `fixed_buy_qty = fixed_sell_qty = 800`
- `behaviour.adopt_broker_inventory = traded` (take current holdings as tradable)
- `behaviour.include_t_settled_for_eq = true` (counts T1/T2 in sellable for NSE:*-EQ and BSE:*-A/-EQ)

Copy the example and edit auth:

```bash
cp strategies/pct_ladder/config.example.json strategies/pct_ladder/config.json
```

Run:

```bash
python run.py live --config strategies/pct_ladder/config.json
```

## How we avoid the SELL reject

Runner calls `broker.get_inventory()` which uses FYERS `holdings()` and:
- treats **HLD** as sellable
- for NSE:*-EQ and BSE:*-A/-EQ it also includes **T0/T1/T2** lots (BTST-like behavior)
- uses `remainingQuantity` to avoid overselling

Then strategy clamps SELL qty to `min(traded_qty, sellable_qty)`.

If FYERS still rejects, we log the message and parse any quantity embedded in it.
