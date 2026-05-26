# Capital-Fixed Rotating Bottom-Zone Grid Strategy - Final Specification

**Version:** Final discussion version 1.0  
**Purpose:** Implementation-ready strategy specification for Codex/developer handoff  
**Persistence model:** JSON files only, no database  
**Trading style:** Long-only spot inventory grid with optional MTF mode  

---

## 1. Purpose of this document

This document defines a full-fledged algo-trading setup for a **Capital-Fixed Rotating Bottom-Zone Grid Strategy**.

The strategy is designed for spot/cash-market trading where positional short selling is not available. It buys stock inventory near the lower part of a validated range, then runs a fixed-point grid to sell higher and buy back lower. Capital is not married to one stock. Capital rotates across eligible stocks whenever a campaign exits.

This document should be given to Codex/developer as the baseline specification for building scanner, backtester, paper-trading system, and live execution system.

Important: this is a system-design document, not financial advice. All values must be backtested, paper traded, and validated with actual brokerage charges, taxes, slippage, and broker API behavior before live deployment.

---

## 2. Final strategy concept

Traditional spot grid often starts with:

```text
50% stock inventory + 50% cash
```

That is balanced when price is near the middle of range, but it wastes cash when the stock is already near the bottom of a valid range.

The final strategy is different:

```text
1. Find liquid stocks/ETFs that are trading in the lower 10% of a validated range.
2. Enter only if price is close to range bottom, not far from it.
3. Buy most of the allotted capital as stock inventory.
4. Keep only enough cash for the remaining downside grid levels until range bottom.
5. Use a fixed absolute point grid per stock, not a moving percentage grid.
6. Place limited advance orders around current grid level.
7. Exit when inventory is fully sold on upside, when bottom is revisited with net profit, or when breakdown/risk rules trigger.
8. Rotate released capital to the next eligible candidate.
9. If no eligible candidate exists, keep capital idle.
```

Short internal name:

```text
Bottom-Zone Grid
```

Full internal name:

```text
Capital-Fixed Rotating Bottom-Zone Grid Strategy
```

---

## 3. Core principles

### 3.1 Capital-fixed, not stock-fixed

Capital should search for the best current opportunity. The system should not keep trading a stock just because it was previously selected.

```text
Wrong: I will trade only one stock forever.
Right: I will trade capital across whichever stocks currently satisfy the bottom-zone grid setup.
```

### 3.2 Bottom-zone entry only

Fresh entry is allowed only when:

```text
Range Position <= 10%
```

where:

```text
Range Position = (CMP - Range Low) / (Range High - Range Low)
```

The stock must be in the lower 10% of its validated range.

### 3.3 Do not enter if price is far from bottom

Even if price is in lower 10%, it must not be too many grid levels above bottom.

```text
Grids To Bottom <= max_grids_to_bottom
```

Recommended starting value:

```text
max_grids_to_bottom = 3 to 5
```

### 3.4 Fixed-point grid, not moving percentage grid

The final strategy should use **fixed absolute point levels per stock**.

Example:

```text
Range Low = 100
Range High = 110
Grid Gap Points = 0.50

Grid ladder:
100.00
100.50
101.00
101.50
102.00
...
110.00
```

This is better for live execution because pending orders remain useful after fills. The system does not need to cancel and replace all orders after every execution.

### 3.5 Strategy must be diversified live

Live capital should be spread across multiple active stocks/ETFs. The system should not concentrate all capital into one symbol.

### 3.6 Performance must be judged by net portfolio value

Do not judge the campaign only by booked grid profit.

Correct metric:

```text
Net Campaign Value = cash_available + market_value_of_remaining_shares
Net Campaign P&L = Net Campaign Value - initial_allocated_capital
```

A campaign may have positive booked grid profit but negative total P&L because remaining shares are in MTM loss.

---

## 4. Supported instruments

### 4.1 Stocks

Allowed by default if they pass liquidity, spread, volatility, and range-quality filters.

### 4.2 ETFs

ETFs are allowed.

ETFs were not disallowed due to strategy logic. They should be treated as a separate instrument type with their own filters. ETFs can be useful because they often have lower single-stock event risk, but they may have lower volatility and therefore require smaller fixed-point grid gaps and larger order values.

ETF requirements:

```text
1. Must be liquid.
2. Must have low bid-ask spread.
3. Must have enough range movement after costs.
4. Must have reliable intraday and historical data.
5. Avoid illiquid ETFs.
6. Avoid leveraged/inverse ETFs unless separately approved.
7. MTF on ETFs should be disabled unless broker confirms eligibility.
```

### 4.3 MTF / Margin Trading Facility

MTF support should be included as an **optional mode**, disabled by default.

Reason: MTF can increase buying power, but it adds margin-call risk, interest cost, and broker/security eligibility restrictions.

Default:

```json
{
  "mtf": {
    "enabled": false
  }
}
```

If enabled, system must validate:

```text
1. Symbol is broker-approved for MTF.
2. Required margin is available.
3. Interest cost is included in P&L.
4. Margin buffer is maintained.
5. Campaign drawdown limits are stricter.
6. Portfolio drawdown limits are stricter.
7. MTF position can be squared off if margin risk is breached.
8. No MTF trade should be placed without explicit config approval.
```

Recommended MTF safety defaults:

```text
mtf_enabled = false
max_mtf_leverage = 1.0 when disabled
max_mtf_leverage = 1.5 to 2.0 only after testing
min_margin_buffer_pct = 25%
force_reduce_if_margin_buffer_below_pct = 15%
```

---

## 5. Strategy parameters

All parameters must be configurable in JSON.

Example `config/strategy_config.json`:

```json
{
  "strategy_name": "capital_fixed_rotating_bottom_zone_grid",
  "mode": "backtest_or_paper_or_live",
  "total_strategy_capital": 1000000,

  "portfolio": {
    "base_active_stocks": 5,
    "active_stocks_auto_scale": true,
    "capital_per_slot": 200000,
    "min_capital_per_slot": 100000,
    "max_active_stocks": 10,
    "max_single_symbol_allocation_pct": 20,
    "max_sector_allocation_pct": 40,
    "allow_idle_cash": true
  },

  "universe": {
    "review_frequency_days": 15,
    "allow_stocks": true,
    "allow_etf": true,
    "allow_penny_stocks": false,
    "min_avg_daily_turnover": 50000000,
    "min_avg_daily_volume": 500000,
    "max_bid_ask_spread_pct": 0.10
  },

  "range": {
    "lookback_days": 60,
    "alternative_lookback_days": 120,
    "range_method": "percentile_or_confirmed_support_resistance",
    "lower_zone_pct": 0.10,
    "min_range_width_pct": 1.0,
    "max_range_width_pct": 20.0,
    "min_support_touches": 2,
    "min_resistance_touches": 2,
    "min_price_containment_pct": 80
  },

  "grid": {
    "use_fixed_point_grid": true,
    "target_grid_count": 20,
    "min_grid_gap_points": null,
    "max_grid_gap_points": null,
    "round_to_tick_size": true,
    "max_grids_to_bottom": 5,
    "advance_buy_orders": 2,
    "advance_sell_orders": 2,
    "min_expected_net_profit_per_cycle": 50,
    "reserve_charges_buffer_pct": 0.25
  },

  "order_sizing": {
    "order_value_mode": "adaptive",
    "base_order_value": 10000,
    "min_order_value": 5000,
    "max_order_value": 50000,
    "increase_order_value_when_grid_gap_small": true
  },

  "entry": {
    "require_lower_10pct_zone": true,
    "reject_if_below_range_low": true,
    "reject_if_breakdown_confirmed": true,
    "max_gap_down_pct_at_entry": 3.0,
    "avoid_event_days_before": 2,
    "avoid_event_days_after": 1
  },

  "exit": {
    "exit_when_inventory_sold_out": true,
    "allow_reentry_after_profit_exit": true,
    "profit_exit_cooldown_days": 1,
    "breakdown_cooldown_days": 20,
    "min_profit_to_exit_at_bottom_pct": 0.20,
    "breakdown_buffer_pct": 2.0,
    "breakdown_confirm_days": 2,
    "max_campaign_drawdown_pct": 8.0,
    "max_portfolio_drawdown_pct": 5.0,
    "no_trade_timeout_days": 10
  },

  "mtf": {
    "enabled": false,
    "max_leverage": 1.0,
    "include_interest_cost": true,
    "min_margin_buffer_pct": 25,
    "force_reduce_if_margin_buffer_below_pct": 15,
    "require_broker_mtf_eligibility": true
  },

  "execution": {
    "order_type": "limit",
    "use_advance_orders": true,
    "max_order_api_calls_per_minute": 60,
    "cancel_replace_only_when_required": true,
    "slippage_assumption_pct": 0.05,
    "include_brokerage_and_taxes": true
  },

  "storage": {
    "type": "json_files",
    "use_database": false,
    "atomic_writes": true,
    "backup_before_overwrite": true,
    "file_locking": true
  }
}
```

---

## 6. Active stock count and diversification

The number of active stocks should not be permanently fixed. It should be capital-based.

Example:

```text
capital_per_slot = 2,00,000
max_active_stocks = 10
```

Then:

```text
active_stock_slots = floor(total_strategy_capital / capital_per_slot)
active_stock_slots = min(active_stock_slots, max_active_stocks)
```

Examples:

```text
Total capital = 10,00,000 -> 5 active slots
Total capital = 16,00,000 -> 8 active slots
Total capital = 25,00,000 -> capped at 10 active slots if max_active_stocks = 10
```

Important rule:

```text
When capital increases, prefer increasing diversification first.
Do not simply increase size in existing stocks unless max_active_stocks is already reached.
```

Diversification rules:

```text
1. Keep multiple active symbols in live trading.
2. Do not exceed max_single_symbol_allocation_pct.
3. Avoid too much sector concentration.
4. If not enough valid candidates exist, keep remaining capital idle.
5. Do not force low-quality entries just to fill all slots.
```

---

## 7. Universe creation

The system should not scan the whole market blindly every day. It should maintain an allowed universe.

### 7.1 Universe sources

Possible universe choices:

```text
1. Nifty 100
2. Nifty 200
3. F&O stocks
4. Custom high-liquidity stock list
5. Liquid ETFs
```

### 7.2 Universe filters

A symbol is allowed only if it satisfies:

```text
1. High average daily turnover.
2. High average volume.
3. Low bid-ask spread.
4. Reliable historical data.
5. Reliable live quote/order book data.
6. Not a penny stock.
7. Not frequently in circuit.
8. Not under special trading restriction.
9. Event/corporate-action risk is manageable.
10. For MTF: broker confirms symbol eligibility.
```

### 7.3 Universe review frequency

Universe review should happen:

```text
Every 15 days or monthly
```

Daily scanner is different. The daily scanner runs on the already-approved universe.

---

## 8. How the system identifies a valid range

This is critical. Do not simply assume every 60-day high/low is a valid range.

Range discovery should have two stages:

```text
Stage 1: Identify possible range.
Stage 2: Validate that the range is tradable.
```

### 8.1 Basic range calculation

Simple first version:

```text
Range Low = lowest low over lookback period
Range High = highest high over lookback period
Range Width = Range High - Range Low
Range Width % = (Range Width / Range Low) * 100
Range Position = (CMP - Range Low) / Range Width
```

### 8.2 Better range calculation

Raw highest high / lowest low can be distorted by one-day wicks.

Better approach:

```text
Range Low = 5th percentile of lows or validated support zone
Range High = 95th percentile of highs or validated resistance zone
```

This ignores abnormal one-day spikes and gives a more realistic tradable range.

### 8.3 Range validation rules

A stock is considered to be trading in a valid range only if:

```text
1. Price containment is good.
2. Support has multiple touches.
3. Resistance has multiple touches.
4. Range width is acceptable.
5. Price is not in confirmed breakdown.
6. Price is not in strong one-sided trend.
7. Volume/liquidity is sufficient.
```

Suggested checks:

```text
price_containment_pct >= 80%
support_touches >= 2
resistance_touches >= 2
min_range_width_pct <= range_width_pct <= max_range_width_pct
```

### 8.4 Range width interpretation

Final understanding:

```text
Range width should not directly decide that a stock is bad because the grid gap is adaptive.
```

Earlier fixed-percentage grid logic would reject small ranges. Final strategy does not do that.

For small range-width stocks:

```text
1. Use smaller fixed point grid gap.
2. Increase order value if needed.
3. Trade more frequent cycles.
4. Allow the stock if expected net cycle profit is still meaningful after cost.
```

Recommended starting values:

```text
min_range_width_pct = 1%
max_range_width_pct = 20%
```

Reason:

```text
Below 1%: movement may be too small after costs.
Above 20%: range may be too wide/unstable for this controlled bottom-zone model.
```

These values should be optimized by backtest.

---

## 9. Bottom-zone entry rule

Fresh entry is allowed only when:

```text
Range Low <= CMP <= Lower Zone Upper Price
```

where:

```text
Lower Zone Upper Price = Range Low + lower_zone_pct * (Range High - Range Low)
lower_zone_pct = 0.10
```

Equivalent:

```text
Range Position <= 0.10
```

Example:

```text
Range Low = 100
Range High = 150
Range Width = 50
Lower 10% zone = 100 to 105

If CMP = 103 -> allowed
If CMP = 108 -> not allowed
If CMP = 99  -> reject as below range/breakdown watch
```

Additional rule:

```text
CMP should not be below Range Low unless a separate recovery setup is explicitly created.
```

---

## 10. Fixed-point grid ladder

### 10.1 Why fixed-point grid is preferred

In percentage grids, every execution shifts reference price and recalculates new percentage-based levels. This may require many cancel/replace operations.

Live trading issues with percentage grid:

```text
1. Existing orders become invalid after every fill.
2. System must cancel and replace many orders.
3. API calls increase.
4. Latency increases.
5. Broker throttling risk increases.
6. Order mismatch/reconciliation risk increases.
```

Fixed-point grid solves this.

### 10.2 Fixed grid per stock

Each stock gets its own fixed grid gap in absolute points.

Examples:

```text
Stock A grid gap = 0.50 points
Stock B grid gap = 2.00 points
ETF grid gap = 0.10 points
```

Once calculated for a campaign, the grid ladder remains fixed.

### 10.3 Grid gap calculation

```text
Range Width Points = Range High - Range Low
Raw Grid Gap Points = Range Width Points / target_grid_count
Grid Gap Points = round_to_valid_tick_size(Raw Grid Gap Points)
```

Example:

```text
Range Low = 100
Range High = 110
Range Width = 10
target_grid_count = 20
Raw Grid Gap Points = 10 / 20 = 0.50
Final Grid Gap Points = 0.50
```

### 10.4 Cost-safe grid validation

Even though grid is fixed in points, it must pass cost validation.

```text
Grid Gap % at CMP = (Grid Gap Points / CMP) * 100
```

Allowed only if:

```text
Grid Gap % at CMP > round_trip_cost_pct + expected_slippage_pct + required_profit_buffer_pct
```

Also check expected absolute profit:

```text
Expected Gross Cycle Profit = order_quantity * grid_gap_points
Expected Net Cycle Profit = Expected Gross Cycle Profit - charges - slippage
```

Allowed only if:

```text
Expected Net Cycle Profit >= min_expected_net_profit_per_cycle
```

### 10.5 Grid ladder generation

```text
Level 0 = Range Low
Level 1 = Range Low + 1 * Grid Gap Points
Level 2 = Range Low + 2 * Grid Gap Points
...
Level N <= Range High
```

Each level should have:

```text
level_index
level_price
```

Every order should be linked to a level index. This makes order reconciliation easier.

### 10.6 Grids to bottom

For fixed-point grid:

```text
Current Level Index = floor((CMP - Range Low) / Grid Gap Points)
Grids To Bottom = Current Level Index
```

Or more directly:

```text
Grids To Bottom = ceil((CMP - Range Low) / Grid Gap Points)
```

Entry allowed only if:

```text
Grids To Bottom <= max_grids_to_bottom
```

Example:

```text
Range Low = 100
CMP = 101
Grid Gap Points = 0.50
Grids To Bottom = ceil((101 - 100) / 0.50) = 2
```

---

## 11. Advance order logic to reduce API calls

The live system should support proactive advance orders.

Recommended starting config:

```text
advance_buy_orders = 2
advance_sell_orders = 2
```

Example:

```text
Current level = 10
Buy orders below: level 9 and level 8
Sell orders above: level 11 and level 12
```

When one order fills, do not cancel all orders. Instead:

```text
1. Recompute required open order levels.
2. Compare with existing open orders.
3. Keep orders that are still valid.
4. Cancel only redundant orders.
5. Place only missing required orders.
```

This reduces:

```text
API calls
Latency
Cancel/replace noise
Order throttling risk
Live execution lag
```

Important implementation requirement:

```text
Order manager should be level-index based, not reference-price based.
```

---

## 12. Order sizing

Order size should be stock/campaign-specific.

### 12.1 Base order value

Start from:

```text
base_order_value = configurable amount, for example 10,000
```

```text
order_quantity = floor(order_value / CMP)
```

### 12.2 Adaptive order value

If grid gap is small, order value may need to increase so that absolute net profit per cycle remains meaningful.

```text
Expected Gross Cycle Profit = order_quantity * grid_gap_points
Expected Net Cycle Profit = gross_profit - costs - slippage
```

If expected net profit is too small:

```text
1. Increase order_value within max_order_value.
2. If still too small, reject candidate.
```

### 12.3 Cost rule

A candidate should be rejected if the grid cannot generate positive net cycle profit after:

```text
1. Brokerage
2. STT/taxes/fees
3. Slippage
4. Safety buffer
5. MTF interest cost if applicable
```

---

## 13. Cash reserve and initial buy

Cash reserve is based on remaining downside grid levels until range bottom.

### 13.1 Downside reserve

```text
Cash Reserve = sum(order_quantity * buy_level_price for all downside levels until bottom)
Cash Reserve With Buffer = Cash Reserve * (1 + reserve_charges_buffer_pct / 100)
```

Simple first version:

```text
Cash Reserve = grids_to_bottom * order_value * (1 + buffer)
```

### 13.2 Initial stock buy

```text
Initial Stock Buy Capital = capital_per_slot - cash_reserve
Initial Stock Quantity = floor(Initial Stock Buy Capital / CMP)
```

Validation:

```text
Initial Stock Quantity >= order_quantity
Initial Stock Buy Capital > 0
Cash reserve is enough for planned downside buys
```

If validation fails:

```text
Reject candidate or reduce order size.
```

### 13.3 Example

```text
Capital per stock = 2,00,000
Range Low = 100
Range High = 110
CMP = 101
Grid Gap Points = 0.50
Order Value = 10,000
Order Quantity = floor(10,000 / 101) = 99
Grids To Bottom = 2

Approx Cash Reserve = 2 * 10,000 = 20,000 plus buffer
Initial Buy Capital = 1,80,000
Initial Stock Quantity = floor(1,80,000 / 101) = 1,782
```

---

## 14. Candidate scanner

The scanner should run daily before market open and optionally again when capital becomes free during the day.

### 14.1 Required data

For each symbol:

```text
1. Adjusted historical OHLCV.
2. Latest price / previous close.
3. Intraday or latest quote if scanning live.
4. Average daily volume.
5. Average daily turnover.
6. Bid-ask spread if available.
7. Event/corporate-action data if available.
8. Broker eligibility information if MTF mode is enabled.
```

### 14.2 Scanner flow

```text
For each symbol in allowed universe:
    1. Validate liquidity and spread.
    2. Calculate/validate range.
    3. Check range_width_pct between min and max.
    4. Check CMP is in lower 10% zone.
    5. Calculate fixed grid_gap_points.
    6. Validate grid gap after costs.
    7. Calculate grids_to_bottom.
    8. Check grids_to_bottom <= max_grids_to_bottom.
    9. Reject confirmed breakdowns.
    10. Reject strong downtrend/falling knife cases.
    11. Score candidate.
    12. Rank all candidates.
    13. Select top candidates based on free capital slots.
```

### 14.3 Scoring model

```text
Candidate Score =
    Liquidity Score
  + Spread Score
  + Range Quality Score
  + Bottom Zone Score
  + Expected Cycle Profit Score
  + Upside Grid Availability Score
  - Breakdown Penalty
  - Trend Penalty
  - Event Risk Penalty
  - MTF Risk Penalty, if applicable
```

Prefer:

```text
1. CMP close to bottom but not below bottom.
2. Clean support zone with multiple touches.
3. Enough upside room.
4. Low spread.
5. Good liquidity.
6. Good expected net profit per cycle.
```

Reject:

```text
1. CMP below range low.
2. Confirmed breakdown.
3. High-volume gap-down.
4. Strong one-sided downtrend.
5. Illiquid symbols.
6. Expected cycle profit too small after costs.
```

---

## 15. Entry rules

A campaign can start only if all conditions are true:

```text
1. Symbol is in allowed universe.
2. Symbol is not already active.
3. Portfolio has free capital slot.
4. Range is valid.
5. Range Position <= 10%.
6. CMP >= Range Low.
7. Grids To Bottom <= max_grids_to_bottom.
8. Grid gap is cost-safe.
9. Expected net cycle profit is acceptable.
10. No confirmed breakdown.
11. No major event/corporate-action risk.
12. For MTF, symbol is MTF-eligible and margin rules pass.
```

When entry is confirmed:

```text
1. Allocate one capital slot.
2. Reserve downside cash.
3. Buy initial stock inventory.
4. Build fixed grid ladder.
5. Place advance buy/sell orders around current level.
6. Save campaign state to JSON.
```

---

## 16. Live grid execution

### 16.1 Campaign starts with inventory

Because the strategy buys shares immediately, first normal action is sell on upside.

### 16.2 Required orders around current level

At any time, for each active campaign:

```text
Required Buy Orders = next N levels below current level
Required Sell Orders = next N levels above current level
```

where:

```text
N = advance_buy_orders / advance_sell_orders
```

### 16.3 Fill handling

When a buy order fills:

```text
1. Increase shares held.
2. Decrease cash.
3. Update current level.
4. Update P&L.
5. Recompute required level orders.
6. Keep valid existing orders.
7. Cancel redundant orders.
8. Place missing orders.
9. Save state and order event JSON.
```

When a sell order fills:

```text
1. Decrease shares held.
2. Increase cash.
3. Update current level.
4. Update realized/grid P&L if cycle closes.
5. Recompute required level orders.
6. Keep valid existing orders.
7. Cancel redundant orders.
8. Place missing orders.
9. Save state and order event JSON.
```

### 16.4 Stop buying near/below bottom

If a required buy level would be below range low:

```text
Do not place that buy order.
```

If price moves below range low:

```text
Move campaign to BREAKDOWN_WATCH.
Stop fresh buying.
Do not average blindly.
```

---

## 17. Campaign states

Recommended states:

```text
CANDIDATE
READY_TO_ENTER
ACTIVE
PAUSED_AT_BOTTOM
BREAKDOWN_WATCH
WIND_DOWN
PROFIT_EXITED_UPSIDE
PROFIT_EXITED_AT_BOTTOM
BREAKDOWN_EXITED
DRAWDOWN_EXITED
MANUAL_EXITED
```

### 17.1 ACTIVE

Grid orders are active and campaign is trading.

### 17.2 PAUSED_AT_BOTTOM

Price is near bottom and no more downside buys are allowed. Continue monitoring for recovery, profitable exit, or breakdown.

### 17.3 BREAKDOWN_WATCH

Price is below range low, but breakdown confirmation is pending. No fresh buys.

### 17.4 WIND_DOWN

Exit condition has triggered. System should reduce/exit position based on configured exit style.

### 17.5 EXITED states

Capital is released after final reconciliation.

---

## 18. Exit rules

Exit rules are mandatory.

### 18.1 Upside inventory sold-out exit

If price moves up and grid sells all or nearly all holdings:

```text
Exit campaign.
Mark reason = PROFIT_EXITED_UPSIDE.
Release capital.
```

Conditions:

```text
shares_held < order_quantity
```

or:

```text
shares_held_value < minimum_position_value
```

### 18.2 Re-entry after upside exit

If price goes up, all holdings are sold, and later the same stock comes back down, the stock is not banned.

Rule:

```text
After profit exit, stock can be re-entered only as a fresh campaign.
```

Fresh re-entry requires:

```text
1. Same stock is still in allowed universe.
2. Range is still valid or newly validated.
3. CMP is again in lower 10% zone.
4. Grids to bottom are within limit.
5. No confirmed breakdown.
6. Capital slot is available.
7. Profit-exit cooldown, if any, has passed.
```

This means a stock can be traded repeatedly if it keeps respecting the range.

### 18.3 Profitable return-to-bottom exit

If stock bounces, generates grid cycles, and later returns near bottom, exit can be taken if total campaign P&L is positive.

Use:

```text
Net Campaign Value = cash_available + market_value_of_remaining_shares
Net Campaign P&L = Net Campaign Value - initial_allocated_capital
Net Campaign P&L % = Net Campaign P&L / initial_allocated_capital * 100
```

Rule example:

```text
If Range Position <= 10% and Net Campaign P&L % >= min_profit_to_exit_at_bottom_pct:
    Exit campaign
    Reason = PROFIT_EXITED_AT_BOTTOM
```

Reason: if price revisits bottom but campaign is profitable due to grid cycles, capital can be released and rotated.

### 18.4 Breakdown exit

If bottom breaks, stop buying first. Then exit if breakdown confirms.

Example:

```text
Breakdown Level = Range Low * (1 - breakdown_buffer_pct / 100)
```

Confirm breakdown if:

```text
Daily close < Breakdown Level for breakdown_confirm_days consecutive sessions
```

Then:

```text
Exit full position or move to wind-down mode.
Reason = BREAKDOWN_EXITED
```

After breakdown exit:

```text
Do not re-enter immediately using old range.
Block symbol for breakdown_cooldown_days or until new range is formed and validated.
```

### 18.5 Max drawdown exit

If total campaign P&L breaches max loss:

```text
If Net Campaign P&L % <= -max_campaign_drawdown_pct:
    Exit or require manual approval
```

### 18.6 Portfolio drawdown rule

If portfolio drawdown crosses limit:

```text
1. Pause new entries.
2. Review all active campaigns.
3. Optionally reduce risk.
4. Alert operator.
```

### 18.7 No-trade timeout review

If no grid trade happens for configured sessions:

```text
Mark campaign for review.
```

Possible actions:

```text
1. Continue if range remains valid.
2. Exit if capital has better opportunity.
3. Pause if price is outside active zone.
```

---

## 19. Capital rotation

When a campaign exits:

```text
1. Cancel remaining open orders for that campaign.
2. Reconcile holdings/cash with broker.
3. Calculate final P&L.
4. Save closed campaign JSON.
5. Release capital slot.
6. Run scanner.
7. Start new campaign only if valid candidate exists.
8. Otherwise keep capital idle.
```

Important:

```text
Idle cash is acceptable.
Bad deployment is not acceptable.
```

---

## 20. JSON-only persistence model

No database should be used.

All state, config, scanner output, orders, trades, P&L, and campaign history should be stored in JSON files.

### 20.1 Folder structure

```text
strategy_data/
│
├── config/
│   ├── strategy_config.json
│   ├── universe_config.json
│   ├── broker_config.json
│   └── risk_config.json
│
├── universe/
│   ├── allowed_universe.json
│   ├── rejected_symbols.json
│   ├── etf_universe.json
│   └── mtf_eligible_symbols.json
│
├── scanner/
│   ├── latest_scan.json
│   ├── selected_candidates.json
│   └── history/
│       └── scan_history_YYYYMMDD.json
│
├── live_state/
│   ├── active_campaigns.json
│   ├── capital_state.json
│   ├── pending_orders.json
│   ├── symbol_state_SYMBOL.json
│   └── broker_reconciliation.json
│
├── orders/
│   ├── orders_YYYYMMDD.json
│   └── order_events_YYYYMMDD.json
│
├── trades/
│   ├── trades_YYYYMMDD.json
│   └── completed_cycles.json
│
├── pnl/
│   ├── daily_pnl_YYYYMMDD.json
│   ├── symbol_pnl.json
│   └── portfolio_pnl.json
│
├── archive/
│   └── closed_campaigns/
│
└── logs/
    └── strategy_log_YYYYMMDD.jsonl
```

### 20.2 JSON write safety

Live trading state must be restart-safe.

Every write should use:

```text
1. File lock.
2. Write to temporary file.
3. fsync temporary file.
4. Atomic rename to final file.
5. Backup previous file before overwrite for important state files.
```

Do not directly overwrite critical files with simple open/write without atomic protection.

### 20.3 Restart recovery

On restart, the system should:

```text
1. Load active_campaigns.json.
2. Load each symbol_state_SYMBOL.json.
3. Load pending_orders.json.
4. Fetch broker holdings/cash/open orders.
5. Reconcile internal state with broker state.
6. Pause campaigns with mismatch.
7. Resume only after safe reconciliation.
```

---

## 21. JSON file examples

### 21.1 `live_state/capital_state.json`

```json
{
  "total_strategy_capital": 1000000,
  "capital_per_slot": 200000,
  "active_stock_slots": 5,
  "max_active_stocks": 10,
  "deployed_capital": 800000,
  "free_capital": 200000,
  "reserved_cash": 100000,
  "active_symbols": ["RELIANCE", "HDFCBANK", "SBIN", "NIFTYBEES"],
  "last_updated": "2026-05-25T09:20:00+05:30"
}
```

### 21.2 `scanner/latest_scan.json`

```json
{
  "scan_date": "2026-05-25",
  "lookback_days": 60,
  "candidates": [
    {
      "symbol": "RELIANCE",
      "instrument_type": "stock",
      "cmp": 2825,
      "range_low": 2800,
      "range_high": 3050,
      "range_width_points": 250,
      "range_width_pct": 8.93,
      "range_position": 0.10,
      "grid_gap_points": 12.5,
      "grids_to_bottom": 2,
      "expected_net_profit_per_cycle": 140,
      "eligible": true,
      "score": 86,
      "reason": "CMP in lower 10%, valid range, no breakdown"
    }
  ]
}
```

### 21.3 `live_state/symbol_state_RELIANCE.json`

```json
{
  "campaign_id": "RELIANCE_20260525_001",
  "symbol": "RELIANCE",
  "instrument_type": "stock",
  "state": "ACTIVE",
  "entry_date": "2026-05-25",
  "allocated_capital": 200000,
  "range_low": 2800,
  "range_high": 3050,
  "range_width_points": 250,
  "range_width_pct": 8.93,
  "entry_price": 2825,
  "cmp_at_entry": 2825,
  "grid_gap_points": 12.5,
  "tick_size": 0.05,
  "current_level_index": 2,
  "grids_to_bottom_at_entry": 2,
  "order_quantity": 7,
  "initial_stock_buy_value": 175000,
  "reserved_cash": 25000,
  "cash_available": 25000,
  "shares_held": 62,
  "avg_holding_price": 2825,
  "realized_grid_profit": 0,
  "unrealized_pnl": 0,
  "net_campaign_pnl": 0,
  "mtf_enabled": false,
  "exit_reason": null,
  "last_updated": "2026-05-25T09:20:00+05:30"
}
```

### 21.4 `live_state/pending_orders.json`

```json
{
  "RELIANCE": [
    {
      "campaign_id": "RELIANCE_20260525_001",
      "order_id": "internal_001",
      "broker_order_id": "2505250001",
      "symbol": "RELIANCE",
      "side": "BUY",
      "level_index": 1,
      "price": 2812.5,
      "quantity": 7,
      "status": "OPEN",
      "created_at": "2026-05-25T09:20:00+05:30"
    },
    {
      "campaign_id": "RELIANCE_20260525_001",
      "order_id": "internal_002",
      "broker_order_id": "2505250002",
      "symbol": "RELIANCE",
      "side": "SELL",
      "level_index": 3,
      "price": 2837.5,
      "quantity": 7,
      "status": "OPEN",
      "created_at": "2026-05-25T09:20:00+05:30"
    }
  ]
}
```

### 21.5 `orders/order_events_YYYYMMDD.json`

```json
[
  {
    "event_time": "2026-05-25T10:14:02+05:30",
    "campaign_id": "RELIANCE_20260525_001",
    "symbol": "RELIANCE",
    "broker_order_id": "2505250002",
    "side": "SELL",
    "price": 2837.5,
    "quantity": 7,
    "status": "COMPLETE",
    "level_index": 3,
    "broker_message": "Order filled"
  }
]
```

### 21.6 `trades/completed_cycles.json`

```json
[
  {
    "cycle_id": "RELIANCE_20260525_001_C001",
    "campaign_id": "RELIANCE_20260525_001",
    "symbol": "RELIANCE",
    "buy_order_id": "2505250003",
    "sell_order_id": "2505250002",
    "buy_price": 2812.5,
    "sell_price": 2837.5,
    "quantity": 7,
    "gross_profit": 175,
    "charges": 18,
    "net_profit": 157,
    "opened_at": "2026-05-25T10:14:02+05:30",
    "closed_at": "2026-05-25T11:02:41+05:30"
  }
]
```

---

## 22. P&L accounting

Track four P&L views:

```text
1. Realized grid P&L
2. Unrealized stock P&L
3. MTF interest/carry cost, if applicable
4. Total campaign P&L
```

Main formula:

```text
Net Campaign Value = cash_available + market_value_of_remaining_shares - mtf_liability_if_any
Net Campaign P&L = Net Campaign Value - initial_allocated_capital
```

For portfolio:

```text
Portfolio Value = free_cash + sum(net_campaign_value for all active campaigns)
Portfolio P&L = Portfolio Value - total_strategy_capital
```

Exit decisions must use total campaign P&L, not only grid P&L.

---

## 23. Backtesting requirements

Before live trading, build a backtester.

### 23.1 Data requirements

```text
1. Adjusted daily OHLCV for scanner/range calculation.
2. Intraday OHLCV, preferably 1-minute, for execution simulation.
3. Corporate-action adjusted prices.
4. Realistic brokerage/tax model.
5. Slippage model.
6. MTF interest model if testing MTF.
7. ETF data if ETFs are enabled.
```

### 23.2 Backtest flow

```text
For each trading day:
    1. Update allowed universe on review date.
    2. Run scanner before market open using only previous day data.
    3. Start new campaigns if free slots and valid candidates exist.
    4. Generate fixed-point ladder for each campaign.
    5. Simulate intraday fills using candle high/low.
    6. Apply transaction costs and slippage.
    7. Update JSON-like campaign state in simulation.
    8. Apply exit rules.
    9. Rotate capital to next candidates.
    10. Record daily portfolio value and metrics.
```

### 23.3 Avoid lookahead bias

When scanning before market open, use only data available up to previous close.

Do not use today's high/low to decide today's entry.

### 23.4 Same-candle ambiguity

If both buy and sell levels are touched in the same candle, define conservative logic.

Recommended:

```text
Assume less favorable fill sequence.
```

Better:

```text
Use tick data where possible.
```

### 23.5 Backtest metrics

Track:

```text
1. Total net profit.
2. CAGR / annualized return.
3. Max drawdown.
4. Win rate by campaign.
5. Average campaign duration.
6. Average capital idle time.
7. Number of grid cycles.
8. Realized grid P&L.
9. Unrealized P&L.
10. Total campaign P&L.
11. Slippage and cost impact.
12. Breakdown exits.
13. Profitable bottom exits.
14. Full upside exits.
15. Re-entry success rate.
16. Capital utilization.
17. Stock-wise and ETF-wise performance.
18. Sector exposure.
19. API order count estimate.
20. Benefit of fixed-point grid versus percentage grid.
```

---

## 24. Paper trading requirements

Before live deployment:

```text
1. Run paper trading for at least 1-3 months.
2. Validate scanner quality.
3. Compare expected fills with actual market movement.
4. Measure slippage.
5. Validate advance order management.
6. Validate cancel/replace minimization.
7. Validate JSON state recovery after restart.
8. Validate broker reconciliation.
9. Validate no duplicate orders.
10. Validate behavior on partial fills.
11. Validate exit rules.
12. Validate MTF only in paper mode first if enabled.
```

---

## 25. Live execution risk checks

### 25.1 Pre-order checks

Before any order:

```text
1. Campaign state allows order.
2. No duplicate open order exists at same level/side.
3. Sell quantity <= shares held.
4. Buy amount <= cash available or MTF buying power.
5. Price is inside allowed ladder/range rules.
6. Stock is not halted/circuit/restricted.
7. Order value is within configured min/max.
8. Portfolio drawdown rules are not breached.
9. MTF margin buffer is safe if MTF is enabled.
10. JSON state and broker state are reconciled.
```

### 25.2 Post-fill checks

After every fill:

```text
1. Update shares.
2. Update cash.
3. Update campaign level.
4. Update P&L.
5. Record order event.
6. Update pending orders.
7. Recompute required advance orders.
8. Save JSON atomically.
9. Reconcile with broker if needed.
```

### 25.3 Reconciliation

At minimum:

```text
1. At startup.
2. Before market open.
3. Periodically during market.
4. After every order fill.
5. After market close.
```

If mismatch:

```text
Pause affected campaign.
Alert operator.
Do not place new orders until resolved.
```

---

## 26. Edge cases

### 26.1 Gap below bottom

If stock opens below range low:

```text
1. Do not buy more.
2. Move to BREAKDOWN_WATCH.
3. Cancel unsafe buy orders.
4. Apply breakdown confirmation or emergency exit rule.
```

### 26.2 Gap above multiple sell levels

Because fixed ladder exists, price may gap above multiple sell levels.

Possible modes:

```text
Mode A: Sell one grid quantity per event.
Mode B: Sell multiple grid quantities for crossed levels.
```

Recommended first implementation:

```text
Sell only one grid quantity per event/fill cycle.
```

Reason: avoids over-selling in sudden spikes.

### 26.3 Partial fills

If partial fill occurs:

```text
1. Update only filled quantity.
2. Keep/cancel remaining quantity based on timeout rule.
3. Do not create duplicate replacement order.
4. Shift level state only after configured minimum fill threshold.
```

Simpler first version:

```text
Use limit orders with timeout.
Update state only after confirmed execution.
```

### 26.4 Corporate actions

If split/bonus/dividend adjustment affects price:

```text
1. Pause campaign.
2. Adjust range, ladder, quantities, and prices.
3. Reconcile with broker.
4. Resume only after verification.
```

### 26.5 Trading halt / circuit

```text
Pause campaign.
Do not place new orders.
Alert operator.
```

### 26.6 JSON corruption or missing file

```text
1. Stop live order placement.
2. Load latest backup.
3. Reconcile with broker.
4. Resume only after manual/operator approval.
```

---

## 27. Suggested project structure

```text
bottom_zone_grid/
│
├── config/
│   ├── strategy_config.json
│   ├── universe_config.json
│   ├── broker_config.json
│   └── risk_config.json
│
├── data/
│   ├── historical_data.py
│   ├── live_feed.py
│   ├── corporate_actions.py
│   └── market_calendar.py
│
├── scanner/
│   ├── universe_builder.py
│   ├── range_calculator.py
│   ├── range_validator.py
│   ├── candidate_filters.py
│   ├── candidate_scoring.py
│   └── daily_scanner.py
│
├── strategy/
│   ├── campaign.py
│   ├── state_machine.py
│   ├── fixed_grid_ladder.py
│   ├── allocation.py
│   ├── risk_manager.py
│   └── pnl.py
│
├── execution/
│   ├── broker_adapter.py
│   ├── order_manager.py
│   ├── advance_order_manager.py
│   ├── fill_handler.py
│   └── reconciliation.py
│
├── storage/
│   ├── json_store.py
│   ├── atomic_writer.py
│   ├── file_lock.py
│   └── recovery.py
│
├── backtest/
│   ├── backtest_engine.py
│   ├── simulator.py
│   ├── cost_model.py
│   ├── mtf_cost_model.py
│   └── reports.py
│
├── monitoring/
│   ├── dashboard.py
│   ├── alerts.py
│   └── logs.py
│
├── scripts/
│   ├── run_daily_scan.py
│   ├── start_paper_trading.py
│   ├── start_live_trading.py
│   ├── stop_strategy.py
│   ├── reconcile_broker.py
│   └── generate_report.py
│
└── tests/
    ├── test_range_calculator.py
    ├── test_range_validator.py
    ├── test_scanner.py
    ├── test_fixed_grid_ladder.py
    ├── test_cash_reserve.py
    ├── test_order_manager.py
    ├── test_json_store.py
    ├── test_exit_rules.py
    └── test_backtest_engine.py
```

---

## 28. Pseudocode: scanner

```python
def run_scanner(universe, market_data, config):
    candidates = []

    for symbol in universe:
        data = market_data.get_adjusted_history(symbol, config["range"]["lookback_days"])
        cmp_price = market_data.get_latest_price(symbol)

        if not passes_liquidity_filters(symbol, data, config):
            continue

        range_info = calculate_validated_range(symbol, data, config)
        if not range_info.valid:
            continue

        range_low = range_info.low
        range_high = range_info.high
        range_width = range_high - range_low
        range_width_pct = (range_width / range_low) * 100

        if range_width_pct < config["range"]["min_range_width_pct"]:
            continue
        if range_width_pct > config["range"]["max_range_width_pct"]:
            continue

        range_position = (cmp_price - range_low) / range_width

        if range_position < 0:
            continue
        if range_position > config["range"]["lower_zone_pct"]:
            continue

        grid_gap_points = calculate_fixed_grid_gap_points(range_low, range_high, cmp_price, config)
        grid_gap_points = round_to_tick(symbol, grid_gap_points)

        if not is_grid_gap_cost_safe(symbol, cmp_price, grid_gap_points, config):
            continue

        grids_to_bottom = math.ceil((cmp_price - range_low) / grid_gap_points)
        if grids_to_bottom > config["grid"]["max_grids_to_bottom"]:
            continue

        if is_breakdown_candidate(symbol, data, cmp_price, range_low, config):
            continue

        score = calculate_candidate_score(
            symbol=symbol,
            data=data,
            cmp_price=cmp_price,
            range_info=range_info,
            grid_gap_points=grid_gap_points,
            config=config,
        )

        candidates.append({
            "symbol": symbol,
            "cmp": cmp_price,
            "range_low": range_low,
            "range_high": range_high,
            "range_width_pct": range_width_pct,
            "range_position": range_position,
            "grid_gap_points": grid_gap_points,
            "grids_to_bottom": grids_to_bottom,
            "score": score,
            "eligible": True,
        })

    return sorted(candidates, key=lambda x: x["score"], reverse=True)
```

---

## 29. Pseudocode: start campaign

```python
def start_campaign(candidate, capital_slot, config, broker, json_store):
    symbol = candidate["symbol"]
    cmp_price = candidate["cmp"]
    range_low = candidate["range_low"]
    range_high = candidate["range_high"]
    grid_gap_points = candidate["grid_gap_points"]
    grids_to_bottom = candidate["grids_to_bottom"]

    order_value = calculate_adaptive_order_value(candidate, config)
    order_qty = int(order_value // cmp_price)
    if order_qty <= 0:
        raise ValueError("Order quantity is zero")

    downside_levels = build_downside_levels(
        cmp_price=cmp_price,
        range_low=range_low,
        grid_gap_points=grid_gap_points,
    )

    required_cash = sum(order_qty * price for price in downside_levels)
    cash_reserve = required_cash * (1 + config["grid"]["reserve_charges_buffer_pct"] / 100)

    initial_buy_capital = capital_slot - cash_reserve
    if initial_buy_capital <= 0:
        raise ValueError("Not enough capital after reserve")

    initial_qty = int(initial_buy_capital // cmp_price)
    if initial_qty < order_qty:
        raise ValueError("Initial quantity too small")

    entry_order = broker.place_limit_buy(symbol, initial_qty, cmp_price)
    fill = broker.wait_for_fill_or_handle_pending(entry_order)

    ladder = build_fixed_grid_ladder(range_low, range_high, grid_gap_points, symbol)
    current_level = find_nearest_level_index(ladder, fill.avg_price)

    campaign = {
        "campaign_id": create_campaign_id(symbol),
        "symbol": symbol,
        "state": "ACTIVE",
        "allocated_capital": capital_slot,
        "entry_price": fill.avg_price,
        "range_low": range_low,
        "range_high": range_high,
        "grid_gap_points": grid_gap_points,
        "current_level_index": current_level,
        "order_quantity": order_qty,
        "shares_held": fill.quantity,
        "cash_available": capital_slot - fill.total_cost,
        "reserved_cash": cash_reserve,
        "realized_grid_profit": 0,
        "net_campaign_pnl": 0,
    }

    json_store.save_symbol_state(symbol, campaign)
    place_required_advance_orders(campaign, ladder, broker, json_store, config)
    return campaign
```

---

## 30. Pseudocode: advance order reconciliation

```python
def reconcile_required_orders(campaign, ladder, broker, json_store, config):
    current_level = campaign["current_level_index"]
    required = []

    for i in range(1, config["grid"]["advance_buy_orders"] + 1):
        level_index = current_level - i
        if level_index >= 0:
            price = ladder[level_index]
            if price >= campaign["range_low"]:
                required.append(("BUY", level_index, price))

    for i in range(1, config["grid"]["advance_sell_orders"] + 1):
        level_index = current_level + i
        if level_index < len(ladder):
            price = ladder[level_index]
            required.append(("SELL", level_index, price))

    open_orders = json_store.load_pending_orders(campaign["symbol"])

    # Keep valid existing orders.
    # Cancel orders not in required set.
    # Place orders in required set that do not already exist.
    sync_orders_with_required_levels(campaign, required, open_orders, broker, json_store)
```

---

## 31. Pseudocode: exit decision

```python
def should_exit_campaign(campaign, ltp, daily_data, config):
    net_value = campaign["cash_available"] + campaign["shares_held"] * ltp
    if campaign.get("mtf_enabled"):
        net_value -= campaign.get("mtf_liability", 0)

    net_pnl = net_value - campaign["allocated_capital"]
    net_pnl_pct = net_pnl / campaign["allocated_capital"] * 100

    range_width = campaign["range_high"] - campaign["range_low"]
    range_position = (ltp - campaign["range_low"]) / range_width

    if campaign["shares_held"] < campaign["order_quantity"]:
        return True, "PROFIT_EXITED_UPSIDE"

    if range_position <= config["range"]["lower_zone_pct"]:
        if net_pnl_pct >= config["exit"]["min_profit_to_exit_at_bottom_pct"]:
            return True, "PROFIT_EXITED_AT_BOTTOM"

    if net_pnl_pct <= -config["exit"]["max_campaign_drawdown_pct"]:
        return True, "DRAWDOWN_EXITED"

    if is_breakdown_confirmed(campaign, daily_data, config):
        return True, "BREAKDOWN_EXITED"

    return False, None
```

---

## 32. Implementation phases

### Phase 1: Scanner and JSON foundation

Build:

```text
1. JSON config loader.
2. Atomic JSON writer.
3. Universe loader.
4. Historical data loader.
5. Range calculator.
6. Range validator.
7. Fixed grid ladder calculator.
8. Candidate scanner.
9. Scanner JSON output.
```

Deliverable:

```text
Daily scanner report with eligible candidates and all calculated values.
```

### Phase 2: Backtester

Build:

```text
1. Campaign simulator.
2. Fixed-point grid execution simulator.
3. Advance order simulation.
4. Cost/slippage model.
5. Exit-rule simulator.
6. Capital rotation engine.
7. JSON-like state simulation.
8. Reports.
```

Deliverable:

```text
Backtest report proving whether this model works after costs.
```

### Phase 3: Paper trading

Build:

```text
1. Live quote feed.
2. Paper broker.
3. JSON state persistence.
4. Advance order manager.
5. Reconciliation simulator.
6. Alerts.
```

Deliverable:

```text
Paper trading system that behaves like live system but places no real orders.
```

### Phase 4: Live small-capital trading

Build:

```text
1. Real broker adapter.
2. Broker order reconciliation.
3. Pre-order risk checks.
4. Emergency stop.
5. Manual override.
6. Live alerts.
```

Deliverable:

```text
Small-capital live deployment with strict monitoring.
```

### Phase 5: Scale-up

Only after stable performance:

```text
1. Increase capital gradually.
2. Increase active stock slots as capital grows.
3. Add ETFs if validated.
4. Add MTF only after separate paper/live validation.
5. Improve scoring and range detection.
```

---

## 33. MVP scope

The first implementation should be simple.

MVP includes:

```text
1. JSON-only config and state.
2. Fixed universe.
3. Daily scanner.
4. Validated range detection.
5. Lower 10% entry condition.
6. Range width filter: 1% to 20%.
7. Fixed-point grid ladder.
8. Max grids to bottom rule.
9. Adaptive order size basic logic.
10. Sell-first grid.
11. Advance orders: 2 buy and 2 sell.
12. Stop fresh buying below range low.
13. Full upside exit.
14. Profitable bottom return exit.
15. Breakdown exit.
16. Capital rotation.
17. Backtest report.
```

MVP should not include:

```text
1. Machine learning.
2. Over-optimized indicators.
3. Complex predictive models.
4. MTF live trading.
5. Aggressive capital scaling.
```

---

## 34. Final strategy summary

```text
Trade only liquid stocks/ETFs from an approved universe.
Scan daily for symbols in lower 10% of a validated range.
Use range validation, not just raw high/low.
Keep range width generally between 1% and 20%.
Use stock-specific fixed point grid ladders, not moving percentage grids.
Calculate grid gap from range width and target grid count.
Validate that grid gap and order size are profitable after costs.
Enter only if CMP is close to bottom and grids_to_bottom is within limit.
Buy most allotted capital as inventory.
Reserve only enough cash for remaining downside grid levels to bottom.
Place limited advance buy/sell orders around current level.
After fills, keep valid orders and only cancel/place what is required.
Stop buying if price reaches or breaks bottom.
Exit if inventory is sold on upside, if bottom return is profitable, or if breakdown/drawdown rules trigger.
Allow re-entry after profit exit only as a fresh setup.
After breakdown exit, block re-entry until cooldown/new range validation.
Rotate free capital into next eligible candidate.
Keep capital idle if no valid candidate exists.
Use JSON files only for all persistence.
Judge performance by total campaign and portfolio P&L, not booked grid P&L alone.
```

---

## 35. Open questions for backtesting, not for initial coding blockage

These should be optimized using data:

```text
1. Best universe: Nifty 100, Nifty 200, F&O, custom list, ETFs.
2. Best lookback: 60 days, 90 days, 120 days, or adaptive.
3. Best range method: raw high/low, percentile, support/resistance validation.
4. Best target_grid_count.
5. Best min/max range_width_pct values.
6. Best order value logic for small grid gaps.
7. Whether to sell one grid or multiple grids on gap-up.
8. Best breakdown confirmation rule.
9. Best profitable-bottom-exit threshold.
10. Best cooldown period after profit exit and breakdown exit.
11. Whether ETFs improve risk-adjusted return.
12. Whether MTF improves return after interest and margin risk.
```

---

## 36. Recommended next action for Codex

Start with scanner + backtester before live broker integration.

First deliverables:

```text
1. Implement JSON config and atomic JSON store.
2. Implement universe loader.
3. Implement range calculator and validator.
4. Implement lower 10% scanner.
5. Implement fixed-point grid ladder calculation.
6. Implement grid gap cost validation.
7. Implement capital slot and cash reserve calculation.
8. Generate latest_scan.json.
9. Build backtester using fixed-point grid.
10. Produce backtest report comparing:
    a. Bottom-zone grid
    b. Normal 50/50 grid
    c. Simple buy-and-hold
```

Only after scanner/backtester validates the idea should paper/live execution be built.
