"""
Regime-aware grid backtest.
Compares: blind grid vs ADX vs Hurst vs BB vs HMM vs Combined.

Regime detection methods:
  1. ADX — trend strength
  2. Hurst exponent — mean-reversion vs trending
  3. Bollinger Band width — volatility squeeze/expansion
  4. HMM — probabilistic regime states
  5. Combined — all four vote together
"""

import math
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from hmmlearn.hmm import GaussianHMM

warnings.filterwarnings("ignore")

# ─── Config ──────────────────────────────────────────────────────────
GRID_PCT = 0.40
BUY_QUOTE = 5000.0
SELL_QUOTE = 5000.0
TOTAL_EQUITY = 100_000.0
BAND_WIDTH = 100.0
QTY_STEP = 0.000001
MIN_QTY = 0.000001
REBAL_TARGET_STEPS = 1

# Regime indicator params
ADX_PERIOD = 14
ADX_TREND_THRESH = 25        # above = trending
HURST_WINDOW = 100           # candles for rolling Hurst
HURST_MR_THRESH = 0.45       # below = mean-reverting (grid friendly)
BB_PERIOD = 20
BB_STD = 2.0
BB_SQUEEZE_THRESH = 0.03     # BB width below this = squeeze (range)
HMM_LOOKBACK = 500           # candles to train HMM
HMM_N_STATES = 3             # bull, bear, range

# How much to scale grid in unfavorable regime
REGIME_SCALE_DOWN = 0.0      # 0 = grid OFF, 0.5 = half size
REGIME_SCALE_UP = 1.0        # 1 = full grid

DATA_FILE = "/root/trading_bot/backtest/cache/binance_ETHUSDT_1h_1704067200000_1779062400000.parquet"

# ─── Indicator functions ─────────────────────────────────────────────

def compute_adx(df, period=14):
    """Average Directional Index."""
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    n = len(df)

    tr = np.zeros(n)
    plus_dm = np.zeros(n)
    minus_dm = np.zeros(n)

    for i in range(1, n):
        h_l = high[i] - low[i]
        h_pc = abs(high[i] - close[i - 1])
        l_pc = abs(low[i] - close[i - 1])
        tr[i] = max(h_l, h_pc, l_pc)

        up = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        plus_dm[i] = up if (up > down and up > 0) else 0
        minus_dm[i] = down if (down > up and down > 0) else 0

    # Smoothed with Wilder's EMA
    alpha = 1.0 / period
    atr = np.zeros(n)
    atr[period] = np.mean(tr[1 : period + 1])
    s_plus = np.zeros(n)
    s_plus[period] = np.mean(plus_dm[1 : period + 1])
    s_minus = np.zeros(n)
    s_minus[period] = np.mean(minus_dm[1 : period + 1])

    for i in range(period + 1, n):
        atr[i] = atr[i - 1] * (1 - alpha) + tr[i] * alpha
        s_plus[i] = s_plus[i - 1] * (1 - alpha) + plus_dm[i] * alpha
        s_minus[i] = s_minus[i - 1] * (1 - alpha) + minus_dm[i] * alpha

    di_plus = np.where(atr > 0, s_plus / atr * 100, 0)
    di_minus = np.where(atr > 0, s_minus / atr * 100, 0)
    di_sum = di_plus + di_minus
    dx = np.where(di_sum > 0, np.abs(di_plus - di_minus) / di_sum * 100, 0)

    adx = np.zeros(n)
    start = 2 * period
    if start < n:
        adx[start] = np.mean(dx[period : start + 1])
        for i in range(start + 1, n):
            adx[i] = adx[i - 1] * (1 - alpha) + dx[i] * alpha

    return adx


def compute_hurst(prices, window=100):
    """Rolling Hurst exponent using R/S analysis."""
    n = len(prices)
    hurst = np.full(n, 0.5)
    log_prices = np.log(prices)

    for i in range(window, n):
        ts = log_prices[i - window : i]
        returns = np.diff(ts)
        mean_r = np.mean(returns)
        deviate = np.cumsum(returns - mean_r)
        R = np.max(deviate) - np.min(deviate)
        S = np.std(returns, ddof=1)
        if S > 1e-10 and R > 1e-10:
            hurst[i] = np.log(R / S) / np.log(window)
    return hurst


def compute_bb_width(close, period=20, std=2.0):
    """Bollinger Band width as fraction of mid."""
    n = len(close)
    bb_width = np.zeros(n)
    for i in range(period, n):
        window = close[i - period : i]
        mid = np.mean(window)
        s = np.std(window, ddof=1)
        if mid > 0:
            bb_width[i] = (2 * std * s) / mid
    return bb_width


def compute_hmm_regimes(df, n_states=3, lookback=500):
    """
    Fit HMM on rolling windows of returns + volatility.
    Returns array of regime labels and probabilities.
    State mapping: lowest-mean-return = bear, highest = bull, middle = range.
    """
    close = df["close"].values
    n = len(close)
    returns = np.zeros(n)
    returns[1:] = np.diff(np.log(close))

    # Rolling volatility (20-period)
    vol = np.zeros(n)
    for i in range(20, n):
        vol[i] = np.std(returns[i - 20 : i])

    regimes = np.full(n, 1)  # default = range (1)
    regime_probs = np.full((n, n_states), 1.0 / n_states)

    # Fit HMM on expanding/rolling window
    min_train = max(lookback, 200)
    for i in range(min_train, n, 24):  # re-fit every 24h for speed
        start = max(0, i - lookback)
        X = np.column_stack([returns[start:i], vol[start:i]])
        # Remove any NaN/inf
        mask = np.isfinite(X).all(axis=1)
        X_clean = X[mask]
        if len(X_clean) < 100:
            continue

        try:
            model = GaussianHMM(
                n_components=n_states,
                covariance_type="full",
                n_iter=50,
                random_state=42,
            )
            model.fit(X_clean)

            # Predict for next 24 candles (or until next refit)
            end = min(i + 24, n)
            X_pred = np.column_stack([returns[i:end], vol[i:end]])
            mask_pred = np.isfinite(X_pred).all(axis=1)
            if mask_pred.sum() == 0:
                continue

            # Map states: sort by mean return
            state_means = model.means_[:, 0]  # mean return per state
            sorted_states = np.argsort(state_means)
            # sorted_states[0] = bear (lowest return), [-1] = bull, [1] = range
            state_map = {}
            state_map[sorted_states[0]] = 0   # bear
            state_map[sorted_states[-1]] = 2  # bull
            for s in sorted_states[1:-1]:
                state_map[s] = 1              # range

            pred_states = model.predict(X_pred)
            pred_probs = model.predict_proba(X_pred)

            for j in range(len(pred_states)):
                if i + j < n and mask_pred[j]:
                    regimes[i + j] = state_map[pred_states[j]]
                    # Remap probabilities too
                    for orig, mapped in state_map.items():
                        regime_probs[i + j, mapped] = pred_probs[j, orig]
        except Exception:
            continue

    return regimes, regime_probs


# ─── Grid engine ─────────────────────────────────────────────────────

def _band_mid(price):
    return math.floor(price / BAND_WIDTH) * BAND_WIDTH + BAND_WIDTH / 2.0

def _round_qty(qty):
    q = math.floor(qty / QTY_STEP) * QTY_STEP
    return q if q >= MIN_QTY else 0.0

def _qty(level_price, quote):
    return _round_qty(quote / _band_mid(level_price))

def _lifo_match(stack, sell_px, sell_qty):
    pnl = 0.0
    remaining = sell_qty
    i = len(stack) - 1
    while i >= 0 and remaining > 0:
        entry = stack[i]
        take = min(remaining, entry[0])
        if sell_px > entry[1]:
            pnl += take * (sell_px - entry[1])
        entry[0] -= take
        remaining -= take
        if entry[0] <= 0:
            stack.pop(i)
        i -= 1
    return pnl


def run_grid(hourly_df, regime_scale, label=""):
    """
    Run grid backtest with per-candle regime scaling.
    regime_scale: array of floats [0, 1] per hourly candle.
    1.0 = full grid, 0.0 = grid paused.
    """
    close = hourly_df["close"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0

    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / close[0]
    ref = close[0]
    lifo_stack = [[eth, close[0]]]

    buy_count = 0
    sell_count = 0
    rebal_count = 0
    lifo_pnl = 0.0
    snapshots = []
    grid_net_usdc = 0.0
    grid_net_eth = 0.0
    rebal_net_usdc = 0.0
    rebal_net_eth = 0.0

    for i in range(1, n):
        price = close[i]
        scale = regime_scale[i] if i < len(regime_scale) else 1.0

        # Daily snapshot
        if i % 24 == 0 or i == n - 1:
            snapshots.append({
                "idx": i,
                "price": price,
                "cash": cash,
                "eth": eth,
                "pv": cash + eth * price,
                "scale": scale,
            })

        # Scaled quote amounts
        buy_q = BUY_QUOTE * scale
        sell_q = SELL_QUOTE * scale

        if buy_q < 1 and sell_q < 1:
            continue  # grid paused

        sell_lvl = ref * (1.0 + pct_frac)
        if price >= sell_lvl:
            qty = _qty(ref, sell_q)
            qty = min(qty, eth)
            if qty <= 0:
                continue
            cash += qty * sell_lvl
            eth -= qty
            lifo_pnl += _lifo_match(lifo_stack, sell_lvl, qty)
            sell_count += 1
            grid_net_usdc += qty * sell_lvl
            grid_net_eth -= qty
            ref = sell_lvl

            # Rebalance after sell
            if REBAL_TARGET_STEPS > 0:
                next_sell_qty = _qty(ref * (1.0 + pct_frac), SELL_QUOTE)
                if next_sell_qty > eth:
                    target_eth = _round_qty(REBAL_TARGET_STEPS * SELL_QUOTE / sell_lvl)
                    deficit = target_eth - eth
                    if deficit > 0:
                        buy_qty = _round_qty(deficit)
                        cost = buy_qty * sell_lvl
                        if cost > cash:
                            buy_qty = _round_qty(cash / sell_lvl)
                            cost = buy_qty * sell_lvl
                        if buy_qty > 0:
                            cash -= cost
                            eth += buy_qty
                            lifo_stack.append([buy_qty, sell_lvl])
                            rebal_count += 1
                            rebal_net_usdc -= cost
                            rebal_net_eth += buy_qty
            continue

        buy_lvl = ref * (1.0 - pct_frac)
        if price <= buy_lvl:
            qty = _qty(buy_lvl, buy_q)
            cost = qty * buy_lvl
            if qty <= 0 or cost > cash:
                continue
            cash -= cost
            eth += qty
            lifo_stack.append([qty, buy_lvl])
            buy_count += 1
            grid_net_usdc -= cost
            grid_net_eth += qty
            ref = buy_lvl

            # Rebalance after buy
            if REBAL_TARGET_STEPS > 0:
                next_buy_qty = _qty(ref * (1.0 - pct_frac), BUY_QUOTE)
                next_buy_cost = next_buy_qty * (ref * (1.0 - pct_frac))
                if next_buy_cost > cash:
                    next_lvl = buy_lvl * (1.0 - pct_frac)
                    next_cost = _qty(next_lvl, BUY_QUOTE) * next_lvl
                    target_cash = REBAL_TARGET_STEPS * max(next_cost, BUY_QUOTE)
                    deficit = target_cash - cash
                    if deficit > 0:
                        sell_qty = min(_round_qty(deficit / buy_lvl) + QTY_STEP, eth)
                        if sell_qty > 0:
                            cash += sell_qty * buy_lvl
                            eth -= sell_qty
                            _lifo_match(lifo_stack, buy_lvl, sell_qty)
                            rebal_count += 1
                            rebal_net_usdc += sell_qty * buy_lvl
                            rebal_net_eth -= sell_qty

    snap = pd.DataFrame(snapshots)
    snap["pv"] = snap["cash"] + snap["eth"] * snap["price"]

    final_pv = snap["pv"].iloc[-1]
    first_price = close[0]
    last_price = close[-1]
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_price) * last_price
    bot_mtm = final_pv - TOTAL_EQUITY
    hodl_mtm = hodl_pv - TOTAL_EQUITY

    grid_pnl = grid_net_usdc + grid_net_eth * last_price
    rebal_pnl_val = rebal_net_usdc + rebal_net_eth * last_price
    trading_pnl = grid_pnl + rebal_pnl_val

    # Sharpe-like on daily PV changes
    snap["daily_ret"] = snap["pv"].pct_change()
    sharpe = (snap["daily_ret"].mean() / snap["daily_ret"].std() * np.sqrt(365)
              if snap["daily_ret"].std() > 0 else 0)

    return {
        "label": label,
        "buys": buy_count,
        "sells": sell_count,
        "rebals": rebal_count,
        "lifo_pnl": lifo_pnl,
        "grid_pnl": grid_pnl,
        "rebal_pnl": rebal_pnl_val,
        "trading_pnl": trading_pnl,
        "final_pv": final_pv,
        "bot_mtm": bot_mtm,
        "hodl_mtm": hodl_mtm,
        "alpha": final_pv - hodl_pv,
        "sharpe": sharpe,
        "max_dd": (snap["pv"] / snap["pv"].cummax() - 1).min(),
        "snap": snap,
    }


# ─── Main ────────────────────────────────────────────────────────────

def main():
    print("Loading 1h candle data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    # Use data from 2025-01-01 onwards (matches tick backtest period)
    df["dt"] = pd.to_datetime(df["ts"])
    df = df[df["dt"] >= "2025-01-01"].reset_index(drop=True)
    n = len(df)
    print(f"  {n} hourly candles: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}")

    # ── Compute all indicators ──
    print("\nComputing indicators...", flush=True)

    print("  ADX...", flush=True)
    adx = compute_adx(df, ADX_PERIOD)

    print("  Hurst exponent...", flush=True)
    hurst = compute_hurst(df["close"].values, HURST_WINDOW)

    print("  Bollinger Band width...", flush=True)
    bb_width = compute_bb_width(df["close"].values, BB_PERIOD, BB_STD)

    print("  HMM regimes...", flush=True)
    hmm_regimes, hmm_probs = compute_hmm_regimes(df, HMM_N_STATES, HMM_LOOKBACK)

    # ── Build regime scales ──
    print("\nBuilding regime scales...", flush=True)

    # 1. Blind (always full grid)
    scale_blind = np.ones(n)

    # 2. ADX only: grid ON when ADX < threshold (no trend)
    scale_adx = np.where(adx < ADX_TREND_THRESH, REGIME_SCALE_UP, REGIME_SCALE_DOWN)

    # 3. Hurst only: grid ON when Hurst < threshold (mean-reverting)
    scale_hurst = np.where(hurst < HURST_MR_THRESH, REGIME_SCALE_UP, REGIME_SCALE_DOWN)

    # 4. BB only: grid ON when BB width < threshold (squeeze/range)
    scale_bb = np.where(bb_width < BB_SQUEEZE_THRESH, REGIME_SCALE_UP, REGIME_SCALE_DOWN)

    # 5. HMM only: grid ON when HMM says range (state 1), scale by probability
    scale_hmm = hmm_probs[:, 1]  # P(range)
    # Normalize: P(range) > 0.4 = full grid, < 0.2 = grid off
    scale_hmm = np.clip((scale_hmm - 0.2) / 0.4, 0, 1)

    # 6. Combined: average of all four signals
    scale_combined = (scale_adx + scale_hurst + scale_bb + scale_hmm) / 4.0

    # 7. Majority vote: grid ON if 3+ signals agree
    votes = ((adx < ADX_TREND_THRESH).astype(float) +
             (hurst < HURST_MR_THRESH).astype(float) +
             (bb_width < BB_SQUEEZE_THRESH).astype(float) +
             (hmm_probs[:, 1] > 0.4).astype(float))
    scale_majority = np.where(votes >= 3, REGIME_SCALE_UP, REGIME_SCALE_DOWN)

    # ── Run all backtests ──
    strategies = [
        ("1. Blind Grid (no regime)", scale_blind),
        ("2. ADX filter", scale_adx),
        ("3. Hurst filter", scale_hurst),
        ("4. BB Width filter", scale_bb),
        ("5. HMM filter", scale_hmm),
        ("6. Combined (avg)", scale_combined),
        ("7. Majority (3/4 vote)", scale_majority),
    ]

    results = []
    for label, scale in strategies:
        print(f"\nRunning: {label}...", flush=True)
        r = run_grid(df, scale, label)
        results.append(r)

    # ── Print comparison ──
    print(f"\n{'='*100}")
    print(f"  REGIME-AWARE GRID BACKTEST COMPARISON")
    print(f"  Data: ETH 1h candles, {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}")
    print(f"  Config: {GRID_PCT}% grid, ${BUY_QUOTE:,.0f} step, ${TOTAL_EQUITY:,.0f} equity")
    print(f"{'='*100}")

    header = (f"{'Strategy':<30} {'Buys':>6} {'Sells':>6} {'Rebals':>6} "
              f"{'LIFO':>10} {'Grid PnL':>10} {'Rebal PnL':>10} "
              f"{'Bot MTM':>10} {'vs HODL':>10} {'Sharpe':>7} {'MaxDD':>7}")
    print(header)
    print("-" * len(header))

    for r in results:
        print(f"{r['label']:<30} {r['buys']:>6,} {r['sells']:>6,} {r['rebals']:>6,} "
              f"${r['lifo_pnl']:>+9,.0f} ${r['grid_pnl']:>+9,.0f} ${r['rebal_pnl']:>+9,.0f} "
              f"${r['bot_mtm']:>+9,.0f} ${r['alpha']:>+9,.0f} {r['sharpe']:>+7.2f} {r['max_dd']:>6.1%}")

    # ── Indicator stats ──
    print(f"\n{'='*60}")
    print("  INDICATOR STATISTICS")
    print(f"{'='*60}")
    print(f"  ADX:   mean={np.mean(adx[ADX_PERIOD*2:]):.1f}  "
          f"% time trending (>{ADX_TREND_THRESH}): {np.mean(adx[ADX_PERIOD*2:] > ADX_TREND_THRESH)*100:.0f}%")
    print(f"  Hurst: mean={np.mean(hurst[HURST_WINDOW:]):.3f}  "
          f"% time mean-reverting (<{HURST_MR_THRESH}): {np.mean(hurst[HURST_WINDOW:] < HURST_MR_THRESH)*100:.0f}%")
    print(f"  BB:    mean={np.mean(bb_width[BB_PERIOD:]):.4f}  "
          f"% time squeeze (<{BB_SQUEEZE_THRESH}): {np.mean(bb_width[BB_PERIOD:] < BB_SQUEEZE_THRESH)*100:.0f}%")
    hmm_range_pct = np.mean(hmm_regimes[HMM_LOOKBACK:] == 1) * 100
    print(f"  HMM:   % time range: {hmm_range_pct:.0f}%  "
          f"bull: {np.mean(hmm_regimes[HMM_LOOKBACK:]==2)*100:.0f}%  "
          f"bear: {np.mean(hmm_regimes[HMM_LOOKBACK:]==0)*100:.0f}%")

    # Grid active % for each strategy
    print(f"\n  Grid active time:")
    for label, scale in strategies:
        active_pct = np.mean(scale > 0.5) * 100
        print(f"    {label:<30} {active_pct:5.1f}%")


if __name__ == "__main__":
    main()
