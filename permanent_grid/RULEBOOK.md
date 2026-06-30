# Permanent Grid Strategy — Rulebook v1.0

## Overview
Run 1% grid on 10 diversified Nifty50 blue-chip stocks permanently.
Each stock managed independently. Diversification enforced at portfolio level.

---

## Capital Structure (80/20)

| Pool | Allocation | Purpose |
|------|-----------|---------|
| Grid Capital | 80% | 10 stocks × 8% each |
| Crash Reserve | 20% | Deploy ONLY during Rule 4 crash event |

- No separate withdrawal buffer — withdrawals come from realized profits or UPSIDE_OUT capital
- Crash Reserve stays in liquid fund / savings earning ~7% until needed

---

## Grid Parameters (per stock)

| Parameter | Value |
|-----------|-------|
| Grid step | 1% of price |
| Downside runway | 25% (25 levels below entry) |
| Upside runway | 25% (25 levels above entry) |
| Shares per level | capital_per_stock / (price × 25) |

---

## Rule 1: Entry — When to Start a Stock

A stock qualifies for entry when ALL conditions met:
1. **Within ±10% of 200-DMA** (mean reversion zone)
2. **NOT within 5% of 52-week high** (avoid buying tops)
3. **Above 52-week low by at least 15%** (avoid falling knives)
4. **Market state is NORMAL** (see Rule 4)

If fewer than 10 stocks qualify → run fewer grids, keep excess in Crash Reserve.
Never force an entry.

Initial position: buy shares for the first level at current price. Grid builds positions
as price moves.

---

## Rule 2: Upside Breakout — Stock Rallies Past Grid Top

When all grid inventory is sold (price went +25% above entry):

1. Mark stock as `UPSIDE_OUT`
2. Capital + realized profit is now free:
   - Profit portion → available for withdrawal
   - Original capital → available for re-deployment
3. **Wait** for stock to pull back to within **5% of 50-DMA**
4. When pullback happens AND Rule 1 criteria met → re-anchor grid at new price
5. If stock never pulls back (keeps rallying) → capital stays free, look for replacement

---

## Rule 3: Downside Breakdown — Per-Stock Stop-Loss

### Layer A — Stock-Specific (market is normal)

| Level | Trigger | Action |
|-------|---------|--------|
| Grid bottom | -25% from entry | Fully loaded. Stop buying. Hold. |
| Hard stop | **-35% from entry** | EXIT all inventory. ~15-20% realized loss. |

After stop-loss exit:
- Mark stock as `BLOCKED` for 60 days
- Capital goes to Crash Reserve
- After 60 days: re-evaluate with Rule 1 entry criteria
- If triggered **twice in 6 months**: flag for basket replacement (Rule 6)

### Layer B — Market is in STRESS/CRASH (Rule 4 active)

- **Do NOT trigger per-stock stop-loss** during market-wide stress
- Rule 4 overrides Rule 3
- Rationale: in a crash, ALL stocks are down — stop-lossing them crystallizes losses
  right before recovery

---

## Rule 4: Market Crash — Systemic Risk Management

Monitor Nifty50 index level relative to its 52-week high.

| Market State | Trigger | Action |
|-------------|---------|--------|
| **NORMAL** | Default | All grids run normally |
| **CAUTION** | Nifty drops **>8%** from 52-week high OR India VIX **>25** | No new stock entries. Active grids continue normally. |
| **STRESS** | Nifty drops **>15%** from 52-week high | **Sell-only mode** — grids process sell orders only, no new buys. Preserves runway for deeper drop. |
| **CRASH_DEPLOY** | Nifty drops **>25%** from 52-week high | Deploy Crash Reserve (20%) into 5 most-beaten-down basket stocks. Resume buying on these. |
| **RECOVERY** | Nifty recovers to within **10%** of 52-week high | Resume NORMAL mode. All grids active. |

### State transitions:
```
NORMAL → CAUTION → STRESS → CRASH_DEPLOY
                                  ↓
NORMAL ← ─ ─ ─ ─ RECOVERY ← ─ ─ ┘
```

### Crash Reserve deployment:
- Split 20% reserve equally among 5 stocks with largest drawdown from grid entry
- These get additional runway (effectively 35-40% total runway)
- When RECOVERY triggers, excess inventory from crash deployment sells into the rally = big profit

---

## Rule 5: Capital Add / Withdraw

### Adding Capital
- Minimum add: 1 stock unit (8% of total account target)
- Options:
  a) Add new stock to basket (if <10 active) — must pass Rule 1
  b) Increase allocation to existing stocks proportionally
- Maintain 80/20 split on new capital
- No additions during STRESS/CRASH states (wait for RECOVERY or use as Crash Reserve)

### Withdrawing Capital
- **Source 1:** Accumulated realized grid profits (safest)
- **Source 2:** UPSIDE_OUT capital (stock fully sold, grid idle)
- **NEVER withdraw from active grids** — breaks the grid
- **NEVER withdraw from Crash Reserve** — that's insurance
- If no profits or UPSIDE_OUT capital available → no withdrawal possible

### Scaling to Multiple Accounts
- Each account is fully independent (own 80/20 split, own grid states)
- All accounts run same 10-stock basket, same rules
- Only difference: capital per stock

---

## Rule 6: Quarterly Basket Review

### Schedule: 1st week of Jan / Apr / Jul / Oct

### Process:
1. Pull current Nifty50 constituent list
2. Fetch 1-year daily data for all 50 stocks
3. Run correlation + volatility analysis
4. Score all stocks: grid_score = 0.6 × vol_norm - 0.4 × corr_norm
5. Compare with current basket

### Replace a stock when ANY of:
- **Dropped from Nifty50** index
- **Volatility collapsed**: annualized vol < 15% (insufficient grid fills)
- **Correlation spiked**: pairwise corr > 0.6 with another basket stock
- **2× stop-loss in 6 months**: fundamentally impaired

### Replacement process:
1. Do NOT force-exit mid-grid
2. Wait for natural exit (UPSIDE_OUT or next re-anchor point)
3. If stock in active grid and not stressed — let it run to completion, don't re-enter
4. Replace with highest-scoring stock from new scan (different sector, corr < 0.5 with all basket stocks)
5. New stock enters only when Rule 1 criteria met

### No basket changes during:
- CAUTION / STRESS / CRASH_DEPLOY market states
- Wait for NORMAL to execute replacements

---

## Rule 7: Portfolio Diversification Check

**At all times: minimum 4 different sectors must be active.**

- If exiting a stock would break this → delay exit or find replacement first
- When re-entering stocks, prioritize underrepresented sectors
- Each stock managed independently (no paired entry/exit)
- Diversification is a portfolio-level guardrail, not a per-stock rule

---

## Initial Basket (as of June 2026)

| # | Stock | Sector | Ann. Vol | Avg Corr | CMP |
|---|-------|--------|---------|----------|-----|
| 1 | ETERNAL | Consumer Services | 34.4% | 0.28 | 248 |
| 2 | SHRIRAMFIN | Financial Services | 34.4% | 0.38 | 919 |
| 3 | ADANIENT | Metals & Mining | 34.1% | 0.37 | 2909 |
| 4 | INFY | Information Technology | 25.7% | 0.16 | 1202 |
| 5 | ONGC | Oil Gas & Fuels | 22.7% | 0.09 | 264 |
| 6 | MAXHEALTH | Healthcare | 26.3% | 0.23 | 938 |
| 7 | BEL | Capital Goods | 26.0% | 0.27 | 407 |
| 8 | TATACONSUM | FMCG | 22.5% | 0.22 | 1143 |
| 9 | EICHERMOT | Automobile | 25.7% | 0.33 | 7100 |
| 10 | ASIANPAINT | Consumer Durables | 23.8% | 0.29 | 2632 |

Avg pairwise correlation: 0.20 | Max: 0.48 | 10 different sectors.

---

## Backtest Validation Required

Before going live, backtest must cover:
- Normal market (2024-2025): steady grid returns
- Correction (2022, Oct): 10-15% drop and recovery
- Crash (2020 COVID, Mar): 37% Nifty drop, full recovery in 10 months
- Minimum 2-year backtest period to capture all regimes
