# BOS+Fib Scalping Strategy — Research & Live Bot

## What this is

A 1-minute Break-of-Structure (BOS) + Fibonacci retracement scalping strategy, researched and live-traded on crypto (MEXC: ETHUSDT, SOLUSDT, BTCUSDT). Originally prototyped on NIFTY intraday data, then swept across crypto pairs at zero cost (MEXC maker fee = 0%).

The core idea: when price breaks a recent swing high/low (BOS), it often retraces to the 61.8% Fibonacci level of the impulse leg before continuing. We enter a limit order at that level, with the stop behind the impulse origin and target beyond the BOS level.

---

## Relevant files

| File | Purpose |
|---|---|
| `notebooks/multi_ticker_fib_research.ipynb` | Full sweep notebook — backtests all param combos across ETHUSDT, SOLUSDT, BTCUSDT |
| `scripts/live_fib_bot.py` | Live/paper bot — runs the chosen config on MEXC in real time |
| `scripts/run_fib_backtest.py` | CLI backtest runner — single symbol or full NIFTY50 sweep |
| `scripts/fetch_binance_1m.py` | Fetches 1-min OHLCV from Binance public API into `data/binance/` |
| `src/intraday_research/strategies/nifty_fib_scalping.py` | Strategy implementation |
| `src/intraday_research/features.py` | Feature engine (VWAP, EMA, pivots, etc.) |
| `src/intraday_research/backtester.py` | Backtesting engine |
| `src/intraday_research/costs.py` | Cost models — `EQUITY_COSTS`, `MEXC_ZERO_COSTS` |
| `configs/fib_live.json` | Live bot config — symbols, trade size, strategy params |

---

## Strategy logic (step by step)

1. **Pivot detection** — confirm swing highs/lows using `pivot_lookback=2` bars on each side. A pivot at bar N is confirmed at bar N+2.

2. **Micro-trend** — requires at least 2 confirmed highs and 2 confirmed lows. A series of Higher Highs + Higher Lows = BULLISH. Lower Highs + Lower Lows = BEARISH.

3. **BOS detection** — price breaks below the last confirmed swing low (bearish BOS → SHORT setup) or above the last confirmed swing high (bullish BOS → LONG setup).

4. **Fib levels** — the impulse leg is the move from the last swing high to the new low (SHORT) or last swing low to the new high (LONG). Key levels:
   - **Fib 0.0** — BOS level (impulse endpoint) = target area
   - **Fib 61.8%** = entry limit price
   - **Fib 100%** = impulse origin = stop zone

5. **Entry** — `limit_618` mode: place a GTC limit order exactly at the 61.8% retracement. Order expires if not filled within 3 bars (~3 minutes).

6. **Stop** — at fib 100% ± `stop_buffer_points` (scaled as `price × stop_buffer_pct / 100`). Default `stop_buffer_pct = 0.009%`.

7. **Target** — extended beyond the BOS level by `target_extension_ratio`:
   - `1.0` → at BOS level (RR ≈ 1.5:1)
   - `1.618` → 161.8% extension (RR ≈ 3.2:1) ← **chosen config**

8. **Trail stop** — milestones set at intermediate Fibonacci extension levels. When price hits a milestone, stop moves up to that level (applied with a one-bar delay to match backtester). A partial exit (50% of position) is booked at the first milestone.

9. **EMA filter** (`use_ema_filter=True`) — entry only allowed if the candle close is above the EMA (LONG) or below the EMA (SHORT). Reduces counter-trend entries.

10. **BOS extension** — if the active setup's impulse extreme is broken further (e.g. price makes a new lower low during a SHORT), the setup updates its fib levels to the new extreme, keeping the entry fresh.

11. **Time exit** — if the position is still open after `time_exit_bars` bars, it closes at the bar's close price. Set to 90 bars (minutes) for the 1.618 extension config.

---

## Chosen live config

After sweeping 179 parameter combinations across ETHUSDT, SOLUSDT, BTCUSDT on 6 months of 1-min data (Jan–Jun 2026) with `MEXC_ZERO_COSTS` (0% fee):

| Parameter | Value | Reason |
|---|---|---|
| Variant | EMA | More trades × positive expectancy beats VWAP filter at 0% fee |
| `min_impulse_pct` | 0.25% | Filters noise while keeping enough setups |
| `target_ext` | 1.618 | Best net P&L across all three symbols |
| `use_vwap_filter` | False | VWAP adds value only when fees eat marginal trades |
| `use_ema_filter` | True | Reduces counter-trend losers |
| `stop_buffer_pct` | 0.009% | Scaled per symbol to median price |

**Symbols traded**: ETHUSDT + SOLUSDT (live); BTCUSDT researched but not live yet.

**Trade size**: $5,000 per trade → qty = `round(5000 / median_price, 6)` (fractional, e.g. ~2.3 ETH, ~58 SOL).

---

## MEXC zero-cost model

MEXC charges **0% maker fee**. All orders in this strategy are placed as limit orders:

- **Entry**: GTC limit at fib 61.8% → sits on book → maker → 0% fee, 0 slippage
- **Target exit**: GTC limit at target price → same, 0% fee, 0 slippage
- **Stop exit**: marketable limit (limit price set 0.1% beyond stop) → fills immediately at bid/ask, MEXC still classifies as maker → 0% fee, slippage = bid-ask spread only (~0.01–0.03% for ETH/SOL)

This is why the backtest uses `MEXC_ZERO_COSTS` and why net P&L ≈ gross P&L in the crypto runs.

---

## Data

Market data is stored in `data/binance/<SYMBOL>_1m.parquet`. Not committed to git — fetch with:

```bash
python scripts/fetch_binance_1m.py --symbol ETHUSDT --days 180
python scripts/fetch_binance_1m.py --symbol SOLUSDT --days 180
python scripts/fetch_binance_1m.py --symbol BTCUSDT --days 180
```

The live bot fetches bars directly from MEXC REST API (`GET /api/v3/klines`) — no local data needed for live.

---

## Running the research notebook

```bash
# Install deps first
pip install -e .   # or: pip install pandas numpy requests pyarrow

# Launch Jupyter
jupyter notebook notebooks/multi_ticker_fib_research.ipynb
```

The notebook sweeps all combinations of:
- `IMPULSE_PCT_SWEEP`: `[0.15, 0.20, 0.25, 0.30]` (min impulse as % of price)
- `TARGET_EXT_SWEEP`: `[1.0, 1.272, 1.618]`
- Strategy variants: BASE, VWAP, EMA, VWAP_EMA

Uses `ProcessPoolExecutor` (up to 8 cores) — full sweep takes ~5–10 minutes depending on CPU.

---

## Running a single backtest (CLI)

```bash
# Crypto
python scripts/run_fib_backtest.py --symbols ETHUSDT --crypto --min-impulse-pct 0.25 --target-ext 1.618

# NSE equity
python scripts/run_fib_backtest.py --symbols RELIANCE HDFCBANK

# NIFTY index
python scripts/run_fib_backtest.py --symbols NIFTY50 --index
```

Output goes to `artifacts/<RUN_NAME>/`.

---

## Running the live paper bot

```bash
# Paper mode (no real orders, simulates fills from bar OHLC)
python scripts/live_fib_bot.py --config configs/fib_live.json

# Live mode (real MEXC orders — needs mexc_secrets.json)
python scripts/live_fib_bot.py --config configs/fib_live.json --live
```

### Config (`configs/fib_live.json`)

```json
{
  "symbols": ["ETHUSDT", "SOLUSDT"],
  "trade_value_usd": 5000,
  "strategy": {
    "variant": "EMA",
    "min_impulse_pct": 0.25,
    "target_ext": 1.618,
    "stop_buffer_pct": 0.009,
    "use_trailing_stop": true
  },
  "mexc": {
    "secrets_file": "mexc_secrets.json"
  },
  "output_dir": "artifacts/fib_live"
}
```

For live mode, create `configs/mexc_secrets.json`:
```json
{
  "api_key": "YOUR_MEXC_API_KEY",
  "api_secret": "YOUR_MEXC_API_SECRET"
}
```

### What the live bot does each minute

1. Sleeps until 2 seconds after the 1-minute bar close (`:02` past each minute)
2. Fetches last 350 closed 1-min bars from MEXC public klines API (no auth)
3. For each symbol:
   - If **in position**: checks bar OHLC against stop/milestones/target/time-exit — same logic as backtester
   - If **pending entry**: checks if bar low (LONG) or high (SHORT) touched the limit price
   - If **flat**: runs `FeatureEngine` + `NiftyBOSFibScalpStrategy` on the bars, checks if last bar generated a signal
4. Saves state to `artifacts/fib_live/bot_state.json` (survives restarts)
5. Logs paper trades to `artifacts/fib_live/paper_trades.jsonl`

### Bot outputs

| File | Contents |
|---|---|
| `artifacts/fib_live/bot_state.json` | Open position + pending entry state per symbol |
| `artifacts/fib_live/paper_trades.jsonl` | One JSON line per closed trade (entry, exit, P&L) |
| `artifacts/fib_live/bot.log` | Full timestamped log of all signals and actions |

---

## Production deployment (cloud)

Run with nohup or a systemd service:

```bash
nohup python scripts/live_fib_bot.py --config configs/fib_live.json >> logs/fib_bot.log 2>&1 &
```

Stop cleanly:
```bash
kill -INT $(pgrep -f live_fib_bot)
```

The bot catches `KeyboardInterrupt` / SIGINT — saves state and prints final P&L summary before exiting.

**Requirements**: Python 3.10+, `pandas`, `numpy`, `requests`, `pyarrow`. Install with `pip install -e .` (see `pyproject.toml`).

**No WebSocket needed** — the strategy only needs one REST call per minute (at bar close). MEXC's public klines endpoint is rate-limit-friendly at this cadence.
