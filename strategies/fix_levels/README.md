# PrevClose Levels Strategy (Proactive)

This strategy places **limit orders at predefined % levels from the previous close** (anchor).

## Core behavior
- Anchor price = **previous trading day close** (refreshed once per market day).
- For each symbol, you provide:
  - `buy_levels_pct`: e.g. `[-2.5, -4.5]`
  - `sell_levels_pct`: e.g. `[2.5, 4.5]`
- The strategy keeps **at most one BUY and one SELL** order live (depending on `mode`).
- Initially, it places the **nearest buy and sell** levels. If price already jumped across multiple levels, it starts at the **last crossed** level.
- When a level order fills:
  - it advances to the next level on the same side (e.g. +2.5 -> +4.5)
  - it keeps the opposite side order unchanged
- If a side is exhausted (last level filled), that side is **reset after the opposite side fills**.

## Modes
- `both`: keep one BUY and one SELL live
- `buy_only`: only BUY levels
- `sell_only`: only SELL levels

## Tick rounding
Limit prices are rounded to the nearest `price_tick` (default `0.05` for NSE EQ).

## Notes
- Use `runner_type: "managed"` in config.
- Use `execution.sync_on_start` + `execution.adopt_broker_inventory` if you want the strategy to adopt current holdings as tradable inventory.
