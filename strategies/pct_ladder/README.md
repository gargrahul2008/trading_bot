# Percentage Ladder (with optional Fixed-Qty steps)

This strategy maintains a **reference price** per symbol and trades when price moves ±X%:

- BUY when `ltp <= reference * (1 - lower_pct/100)`
- SELL when `ltp >= reference * (1 + upper_pct/100)`

After every **fill**, the reference is reset to the fill price.

## Key features
- Uses shared FYERS execution layer (`common/`) for:
  - DB auth via `tr_db`
  - Quotes, orders, orderbook polling
  - **Sellable quantity calculation** using Positions + Holdings (T1/BTST aware)
  - Adaptive SELL quantity on “insufficient qty/holdings” style rejects
- Strategy config is provided via JSON config file

## Strategy config variables (under `strategy`)
- `symbols` (list[str]): e.g. `["NSE:RAJOOENG-EQ"]`
- `upper_pct`, `lower_pct` (float): ladder threshold percentages

### Sizing
- `sizing_mode`: `"fixed_qty"` or `"pct"`
  - `"fixed_qty"`: always use fixed qty per step
    - `fixed_qty_buy` (int)
    - `fixed_qty_sell` (int)
  - `"pct"`: compute qty from base * pct
    - `sizing_base`: `"strategy_equity"`, `"cash"`, `"fixed"`
    - `fixed_capital` (float): used if `sizing_base="fixed"`
    - `buy_trade_pct`, `sell_trade_pct` (float)

### Quantity rounding
- `qty_step` (int): round down to multiples of step
- `min_qty` (int): if result below this, skip trade

## Execution settings (under `execution`)
- `product_type` (default CNC)
- `allow_btst_auto` (true): Treat T1 holdings as sellable automatically for **NSE *-EQ** and **BSE *-A**
- `sync_on_start` (true): pull cash and adopt broker inventory
- `adopt_broker_inventory` (true): set `state.traded_qty` from broker (positions + holdings)
- `poll_seconds`, `closed_poll_seconds`
- market timings / EOD cancel

## Running
1. Create a config JSON (see `config.example.json`)
2. Run:
   ```bash
   python run_strategy.py --config strategies/pct_ladder/config.example.json
   ```


## Crypto sizing modes
- fixed_quote: set buy_quote_usdt and sell_quote_usdt
- fixed_percent_of_portfolio: set buy_percent and sell_percent where 0.25 means 25%
