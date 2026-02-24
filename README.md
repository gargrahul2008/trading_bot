# Modular FYERS Strategy Runner (Common Broker + Per-Strategy Folders)

## What you get
- `common/` shared layer:
  - FYERS client wrapper (quotes/orders/orderbook/positions/holdings/funds/history)
  - DB auth helper (reads `tr_db`)
  - Sellable quantity computation with **T1/BTST auto**:
    - Treat T1 as sellable automatically for `NSE:* -EQ` and `BSE:* -A` (BTST eligible)
  - Reject parser + adaptive SELL qty retry on "insufficient qty/holdings" type rejects

- `strategies/` per-strategy folders:
  - each strategy has `strategy.py`, config, README
  - examples:
    - `pct_ladder` (reactive; market orders)
    - `order_grid_template` (managed; pre-place limit orders)

## Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run example (pct ladder)
1) Copy/modify config:
`strategies/pct_ladder/config.example.json`

2) Ensure `tr_db` file is present (for DB auth) or switch broker.auth_mode to env.

3) Run:
```bash
python run_strategy.py --config strategies/pct_ladder/config.example.json
```

## Proactive strategies
For strategies that pre-place orders and react to fills:
- set `"runner_type": "managed"`
- implement `desired_actions()` in strategy

If you need something more advanced (modify order, complex state machine),
it's fine to keep a custom runner inside the strategy folder while still reusing
`common/` modules.


## Crypto (MEXC Spot)
- Configure broker.type = "mexc_spot" and set broker.secrets_file to a repo-root secrets json.
- Example config: strategies/pct_ladder/config.mexc.example.json
- Reactive runner supports order_mode=marketable_limit with slippage_bps and limit_ttl_seconds.


## Proactive levels from previous close
- Strategy: strategies/prevclose_levels (runner_type: managed)
- Example config: strategies/prevclose_levels/config.fyers.rajoo.example.json
