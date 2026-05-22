"""
Regime-aware grid + trend-follow backtest (v2).

Two-mode system:
  - RANGE regime (Hurst < threshold): Run grid strategy
  - TREND regime (Hurst >= threshold): Run trend-following strategy

Trend-following options tested:
  A. EMA crossover (fast/slow)
  B. Breakout (Donchian channel)
  C. Simple momentum (price vs EMA, go long above / flat below)

Also tests combinations of regime detection.
"""

import math
import warnings
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM

warnings.filterwarnings("ignore")

# ─── Grid Config ─────────────────────────────────────────────────────
GRID_PCT = 0.40
BUY_QUOTE = 5000.0
SELL_QUOTE = 5000.0
TOTAL_EQUITY = 100_000.0
BAND_WIDTH = 100.0
QTY_STEP = 0.000001
MIN_QTY = 0.000001
REBAL_TARGET_STEPS = 1

# ─── Regime detection params ─────────────────────────────────────────
HURST_WINDOW = 100
HURST_GRID_THRESH = 0.45     # below = grid ON
HURST_TREND_THRESH = 0.55    # above = trend-follow ON
# In between = reduced grid, no trend

ADX_PERIOD = 14
ADX_TREND_THRESH = 25

BB_PERIOD = 20
BB_STD = 2.0
BB_SQUEEZE_THRESH = 0.03

HMM_LOOKBACK = 500
HMM_N_STATES = 3

# ─── Trend-follow params ────────────────────────────────────────────
EMA_FAST = 20                 # hours (~1 day)
EMA_SLOW = 80                 # hours (~3.3 days)
DONCHIAN_PERIOD = 48          # hours (2 days)
MOMENTUM_EMA = 50             # hours (~2 days)
TREND_POSITION_PCT = 0.5      # use 50% of equity for trend position
TREND_STOP_PCT = 0.03         # 3% trailing stop

DATA_FILE = "/root/trading_bot/backtest/cache/binance_ETHUSDT_1h_1704067200000_1779062400000.parquet"

# ─── Indicator functions ─────────────────────────────────────────────

def compute_adx(df, period=14):
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
    alpha = 1.0 / period
    atr = np.zeros(n); atr[period] = np.mean(tr[1:period+1])
    s_plus = np.zeros(n); s_plus[period] = np.mean(plus_dm[1:period+1])
    s_minus = np.zeros(n); s_minus[period] = np.mean(minus_dm[1:period+1])
    for i in range(period + 1, n):
        atr[i] = atr[i-1]*(1-alpha) + tr[i]*alpha
        s_plus[i] = s_plus[i-1]*(1-alpha) + plus_dm[i]*alpha
        s_minus[i] = s_minus[i-1]*(1-alpha) + minus_dm[i]*alpha
    di_plus = np.where(atr > 0, s_plus / atr * 100, 0)
    di_minus = np.where(atr > 0, s_minus / atr * 100, 0)
    di_sum = di_plus + di_minus
    dx = np.where(di_sum > 0, np.abs(di_plus - di_minus) / di_sum * 100, 0)
    adx = np.zeros(n)
    start = 2 * period
    if start < n:
        adx[start] = np.mean(dx[period:start+1])
        for i in range(start + 1, n):
            adx[i] = adx[i-1]*(1-alpha) + dx[i]*alpha
    return adx, di_plus, di_minus


def compute_hurst(prices, window=100):
    n = len(prices)
    hurst = np.full(n, 0.5)
    log_prices = np.log(prices)
    for i in range(window, n):
        ts = log_prices[i - window:i]
        returns = np.diff(ts)
        mean_r = np.mean(returns)
        deviate = np.cumsum(returns - mean_r)
        R = np.max(deviate) - np.min(deviate)
        S = np.std(returns, ddof=1)
        if S > 1e-10 and R > 1e-10:
            hurst[i] = np.log(R / S) / np.log(window)
    return hurst


def compute_bb_width(close, period=20, std=2.0):
    n = len(close)
    bb_width = np.zeros(n)
    for i in range(period, n):
        window = close[i - period:i]
        mid = np.mean(window)
        s = np.std(window, ddof=1)
        if mid > 0:
            bb_width[i] = (2 * std * s) / mid
    return bb_width


def compute_hmm_regimes(df, n_states=3, lookback=500):
    close = df["close"].values
    n = len(close)
    returns = np.zeros(n)
    returns[1:] = np.diff(np.log(close))
    vol = np.zeros(n)
    for i in range(20, n):
        vol[i] = np.std(returns[i-20:i])
    regimes = np.full(n, 1)
    regime_probs = np.full((n, n_states), 1.0 / n_states)
    min_train = max(lookback, 200)
    for i in range(min_train, n, 24):
        start = max(0, i - lookback)
        X = np.column_stack([returns[start:i], vol[start:i]])
        mask = np.isfinite(X).all(axis=1)
        X_clean = X[mask]
        if len(X_clean) < 100:
            continue
        try:
            model = GaussianHMM(n_components=n_states, covariance_type="full",
                                n_iter=50, random_state=42)
            model.fit(X_clean)
            end = min(i + 24, n)
            X_pred = np.column_stack([returns[i:end], vol[i:end]])
            mask_pred = np.isfinite(X_pred).all(axis=1)
            if mask_pred.sum() == 0:
                continue
            state_means = model.means_[:, 0]
            sorted_states = np.argsort(state_means)
            state_map = {sorted_states[0]: 0, sorted_states[-1]: 2}
            for s in sorted_states[1:-1]:
                state_map[s] = 1
            pred_states = model.predict(X_pred)
            pred_probs = model.predict_proba(X_pred)
            for j in range(len(pred_states)):
                if i + j < n and mask_pred[j]:
                    regimes[i + j] = state_map[pred_states[j]]
                    for orig, mapped in state_map.items():
                        regime_probs[i + j, mapped] = pred_probs[j, orig]
        except Exception:
            continue
    return regimes, regime_probs


def compute_ema(values, period):
    ema = np.zeros(len(values))
    ema[0] = values[0]
    alpha = 2.0 / (period + 1)
    for i in range(1, len(values)):
        ema[i] = alpha * values[i] + (1 - alpha) * ema[i - 1]
    return ema


def compute_donchian(high, low, period):
    n = len(high)
    upper = np.zeros(n)
    lower = np.zeros(n)
    for i in range(period, n):
        upper[i] = np.max(high[i-period:i])
        lower[i] = np.min(low[i-period:i])
    return upper, lower


# ─── Grid engine ─────────────────────────────────────────────────────

def _band_mid(price):
    return math.floor(price / BAND_WIDTH) * BAND_WIDTH + BAND_WIDTH / 2.0

def _round_qty(qty):
    q = math.floor(qty / QTY_STEP) * QTY_STEP
    return q if q >= MIN_QTY else 0.0

def _qty(level_price, quote):
    return _round_qty(quote / _band_mid(level_price))

def _lifo_match(stack, sell_px, sell_qty):
    pnl = 0.0; remaining = sell_qty; i = len(stack) - 1
    while i >= 0 and remaining > 0:
        entry = stack[i]; take = min(remaining, entry[0])
        if sell_px > entry[1]: pnl += take * (sell_px - entry[1])
        entry[0] -= take; remaining -= take
        if entry[0] <= 0: stack.pop(i)
        i -= 1
    return pnl


# ─── Combined strategy engine ───────────────────────────────────────

def run_strategy(df, regime_signal, trend_mode="ema_crossover", label=""):
    """
    regime_signal: per-candle array
      'grid'  = run grid
      'trend' = run trend-follower
      'flat'  = do nothing (hold current)
    trend_mode: 'ema_crossover', 'breakout', 'momentum'
    """
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0

    # Pre-compute trend signals
    ema_fast = compute_ema(close, EMA_FAST)
    ema_slow = compute_ema(close, EMA_SLOW)
    don_upper, don_lower = compute_donchian(high, low, DONCHIAN_PERIOD)
    mom_ema = compute_ema(close, MOMENTUM_EMA)

    # Portfolio state
    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / close[0]
    total_eth_start = eth

    # Grid state
    ref = close[0]
    lifo_stack = [[eth, close[0]]]
    grid_buys = 0; grid_sells = 0; grid_rebals = 0
    lifo_pnl = 0.0
    grid_net_usdc = 0.0; grid_net_eth = 0.0
    rebal_net_usdc = 0.0; rebal_net_eth = 0.0

    # Trend state
    trend_pos = 0.0        # ETH held for trend (separate from grid)
    trend_entry_px = 0.0
    trend_stop_px = 0.0
    trend_trades = 0
    trend_pnl = 0.0
    trend_direction = 0    # 1=long, -1=short, 0=flat
    prev_regime = 'grid'

    snapshots = []

    for i in range(1, n):
        price = close[i]
        regime = regime_signal[i] if i < len(regime_signal) else 'grid'

        # --- Regime transition: close trend position when switching to grid ---
        if regime != prev_regime:
            if prev_regime == 'trend' and trend_pos > 0:
                # Close trend position
                cash += trend_pos * price
                trend_pnl += trend_pos * (price - trend_entry_px)
                trend_trades += 1
                eth_for_grid = trend_pos
                trend_pos = 0.0
                trend_direction = 0
            if regime == 'grid' and prev_regime == 'trend':
                # Re-initialize grid ref at current price
                ref = price
        prev_regime = regime

        # Daily snapshot
        if i % 24 == 0 or i == n - 1:
            total_eth = eth + trend_pos
            snapshots.append({
                "idx": i, "price": price, "cash": cash,
                "eth": eth, "trend_pos": trend_pos,
                "pv": cash + total_eth * price,
                "regime": regime,
            })

        # === GRID MODE ===
        if regime == 'grid':
            sell_lvl = ref * (1.0 + pct_frac)
            if price >= sell_lvl:
                qty = _qty(ref, SELL_QUOTE)
                qty = min(qty, eth)
                if qty > 0:
                    cash += qty * sell_lvl; eth -= qty
                    lifo_pnl += _lifo_match(lifo_stack, sell_lvl, qty)
                    grid_sells += 1
                    grid_net_usdc += qty * sell_lvl; grid_net_eth -= qty
                    ref = sell_lvl
                    # Rebalance
                    if REBAL_TARGET_STEPS > 0:
                        next_sell_qty = _qty(ref * (1.0 + pct_frac), SELL_QUOTE)
                        if next_sell_qty > eth:
                            target = _round_qty(REBAL_TARGET_STEPS * SELL_QUOTE / sell_lvl)
                            deficit = target - eth
                            if deficit > 0:
                                bqty = min(_round_qty(deficit), _round_qty(cash / sell_lvl))
                                if bqty > 0:
                                    cost = bqty * sell_lvl
                                    cash -= cost; eth += bqty
                                    lifo_stack.append([bqty, sell_lvl])
                                    grid_rebals += 1
                                    rebal_net_usdc -= cost; rebal_net_eth += bqty
                    continue

            buy_lvl = ref * (1.0 - pct_frac)
            if price <= buy_lvl:
                qty = _qty(buy_lvl, BUY_QUOTE)
                cost = qty * buy_lvl
                if qty > 0 and cost <= cash:
                    cash -= cost; eth += qty
                    lifo_stack.append([qty, buy_lvl])
                    grid_buys += 1
                    grid_net_usdc -= cost; grid_net_eth += qty
                    ref = buy_lvl
                    # Rebalance
                    if REBAL_TARGET_STEPS > 0:
                        next_buy_cost = _qty(ref*(1-pct_frac), BUY_QUOTE) * (ref*(1-pct_frac))
                        if next_buy_cost > cash:
                            next_lvl = buy_lvl * (1 - pct_frac)
                            nc = _qty(next_lvl, BUY_QUOTE) * next_lvl
                            target_cash = REBAL_TARGET_STEPS * max(nc, BUY_QUOTE)
                            deficit = target_cash - cash
                            if deficit > 0:
                                sqty = min(_round_qty(deficit / buy_lvl) + QTY_STEP, eth)
                                if sqty > 0:
                                    cash += sqty * buy_lvl; eth -= sqty
                                    _lifo_match(lifo_stack, buy_lvl, sqty)
                                    grid_rebals += 1
                                    rebal_net_usdc += sqty * buy_lvl; rebal_net_eth -= sqty

        # === TREND MODE ===
        elif regime == 'trend':
            # Determine trend direction from selected mode
            want_long = False

            if trend_mode == "ema_crossover":
                want_long = ema_fast[i] > ema_slow[i]

            elif trend_mode == "breakout":
                if price >= don_upper[i] and don_upper[i] > 0:
                    want_long = True
                elif price <= don_lower[i] and don_lower[i] > 0:
                    want_long = False
                else:
                    want_long = trend_direction > 0  # hold current

            elif trend_mode == "momentum":
                want_long = price > mom_ema[i]

            # Position management
            trend_equity = TREND_POSITION_PCT * (cash + (eth + trend_pos) * price)
            target_qty = _round_qty(trend_equity / price) if want_long else 0.0

            # Trailing stop
            if trend_pos > 0:
                trend_stop_px = max(trend_stop_px, price * (1 - TREND_STOP_PCT))
                if price <= trend_stop_px:
                    # Stop hit — close position
                    cash += trend_pos * price
                    trend_pnl += trend_pos * (price - trend_entry_px)
                    trend_trades += 1
                    trend_pos = 0.0
                    trend_direction = 0
                    continue

            # Adjust position
            if want_long and trend_pos < target_qty * 0.8:
                # Enter or add to long
                add_qty = target_qty - trend_pos
                cost = add_qty * price
                if cost <= cash * 0.9:  # keep 10% cash buffer
                    cash -= cost
                    if trend_pos == 0:
                        trend_entry_px = price
                        trend_stop_px = price * (1 - TREND_STOP_PCT)
                    else:
                        # Average entry
                        trend_entry_px = (trend_entry_px * trend_pos + price * add_qty) / (trend_pos + add_qty)
                    trend_pos += add_qty
                    trend_direction = 1
                    trend_trades += 1

            elif not want_long and trend_pos > 0:
                # Exit long
                cash += trend_pos * price
                trend_pnl += trend_pos * (price - trend_entry_px)
                trend_trades += 1
                trend_pos = 0.0
                trend_direction = 0

    # Final snapshot
    total_eth_end = eth + trend_pos
    final_pv = cash + total_eth_end * close[-1]
    # Close any remaining trend position for accounting
    if trend_pos > 0:
        trend_pnl += trend_pos * (close[-1] - trend_entry_px)

    snap = pd.DataFrame(snapshots)
    snap["pv"] = snap["cash"] + (snap["eth"] + snap["trend_pos"]) * snap["price"]

    first_price = close[0]; last_price = close[-1]
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_price) * last_price
    bot_mtm = final_pv - TOTAL_EQUITY
    hodl_mtm = hodl_pv - TOTAL_EQUITY

    grid_pnl = grid_net_usdc + grid_net_eth * last_price
    rebal_pnl = rebal_net_usdc + rebal_net_eth * last_price

    snap["daily_ret"] = snap["pv"].pct_change()
    sharpe = (snap["daily_ret"].mean() / snap["daily_ret"].std() * np.sqrt(365)
              if snap["daily_ret"].std() > 0 else 0)

    # Regime time breakdown
    grid_time = np.sum(np.array(regime_signal) == 'grid') / len(regime_signal) * 100
    trend_time = np.sum(np.array(regime_signal) == 'trend') / len(regime_signal) * 100
    flat_time = 100 - grid_time - trend_time

    return {
        "label": label,
        "grid_buys": grid_buys, "grid_sells": grid_sells, "grid_rebals": grid_rebals,
        "lifo_pnl": lifo_pnl, "grid_pnl": grid_pnl, "rebal_pnl": rebal_pnl,
        "trend_trades": trend_trades, "trend_pnl": trend_pnl,
        "final_pv": final_pv, "bot_mtm": bot_mtm, "hodl_mtm": hodl_mtm,
        "alpha": final_pv - hodl_pv,
        "sharpe": sharpe,
        "max_dd": (snap["pv"] / snap["pv"].cummax() - 1).min(),
        "grid_time_pct": grid_time, "trend_time_pct": trend_time,
        "snap": snap,
    }


# ─── Main ────────────────────────────────────────────────────────────

def main():
    print("Loading 1h candle data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    df["dt"] = pd.to_datetime(df["ts"])
    df = df[df["dt"] >= "2025-01-01"].reset_index(drop=True)
    n = len(df)
    first_px = df["close"].iloc[0]
    last_px = df["close"].iloc[-1]
    print(f"  {n} candles: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}")
    print(f"  ETH: ${first_px:,.0f} → ${last_px:,.0f} ({(last_px/first_px-1)*100:+.1f}%)")

    # ── Compute indicators ──
    print("\nComputing indicators...", flush=True)
    adx, di_plus, di_minus = compute_adx(df, ADX_PERIOD)
    hurst = compute_hurst(df["close"].values, HURST_WINDOW)
    bb_width = compute_bb_width(df["close"].values, BB_PERIOD, BB_STD)
    print("  Computing HMM...", flush=True)
    hmm_regimes, hmm_probs = compute_hmm_regimes(df, HMM_N_STATES, HMM_LOOKBACK)

    # ── Build regime signals ──
    print("Building regime signals...\n", flush=True)

    # Hurst-based regime (best from v1)
    def hurst_regime(h):
        signals = []
        for v in h:
            if v < HURST_GRID_THRESH:
                signals.append('grid')
            elif v > HURST_TREND_THRESH:
                signals.append('trend')
            else:
                signals.append('flat')
        return signals

    # ADX-based: low ADX = grid, high ADX = trend (use DI+/DI- for direction)
    def adx_regime(adx_arr):
        signals = []
        for v in adx_arr:
            if v < ADX_TREND_THRESH:
                signals.append('grid')
            else:
                signals.append('trend')
        return signals

    # Combined: Hurst + ADX agree
    def combined_regime(h, adx_arr):
        signals = []
        for i in range(len(h)):
            h_grid = h[i] < HURST_GRID_THRESH
            adx_grid = adx_arr[i] < ADX_TREND_THRESH
            if h_grid and adx_grid:
                signals.append('grid')
            elif not h_grid and not adx_grid:
                signals.append('trend')
            else:
                signals.append('flat')  # disagreement = cautious
        return signals

    # HMM-based: range state = grid, bull/bear = trend
    def hmm_regime(regimes):
        return ['grid' if r == 1 else 'trend' for r in regimes]

    # All 4 vote
    def vote_regime(h, adx_arr, bb, hmm_r):
        signals = []
        for i in range(len(h)):
            grid_votes = 0
            if h[i] < HURST_GRID_THRESH: grid_votes += 1
            if adx_arr[i] < ADX_TREND_THRESH: grid_votes += 1
            if bb[i] < BB_SQUEEZE_THRESH: grid_votes += 1
            if hmm_r[i] == 1: grid_votes += 1
            if grid_votes >= 3:
                signals.append('grid')
            elif grid_votes <= 1:
                signals.append('trend')
            else:
                signals.append('flat')
        return signals

    regime_builders = {
        "Hurst": lambda: hurst_regime(hurst),
        "ADX": lambda: adx_regime(adx),
        "Hurst+ADX": lambda: combined_regime(hurst, adx),
        "HMM": lambda: hmm_regime(hmm_regimes),
        "4-Vote": lambda: vote_regime(hurst, adx, bb_width, hmm_regimes),
    }

    trend_modes = ["ema_crossover", "breakout", "momentum"]

    # ── Run all combinations ──
    results = []

    # 1. Baseline: blind grid (no regime, no trend)
    blind_signal = ['grid'] * n
    r = run_strategy(df, blind_signal, "ema_crossover", "Blind Grid (baseline)")
    results.append(r)

    # 2. HODL reference
    hodl_mtm = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px - TOTAL_EQUITY

    # 3. Trend-only (no grid at all)
    trend_only_signal = ['trend'] * n
    for tm in trend_modes:
        r = run_strategy(df, trend_only_signal, tm, f"Trend-only ({tm})")
        results.append(r)

    # 4. Regime-aware: each regime detector × each trend mode
    # Focus on best combinations to keep output readable
    for regime_name, regime_fn in regime_builders.items():
        regime_sig = regime_fn()
        for tm in trend_modes:
            label = f"{regime_name} + {tm}"
            r = run_strategy(df, regime_sig, tm, label)
            results.append(r)

    # ── Print results ──
    print(f"\n{'='*130}")
    print(f"  REGIME + TREND-FOLLOW BACKTEST COMPARISON")
    print(f"  ETH: ${first_px:,.0f} → ${last_px:,.0f} ({(last_px/first_px-1)*100:+.1f}%)  |  HODL MTM: ${hodl_mtm:+,.0f}")
    print(f"  Config: {GRID_PCT}% grid, ${BUY_QUOTE:,.0f} step, ${TOTAL_EQUITY:,.0f} equity")
    print(f"{'='*130}")

    header = (f"{'Strategy':<35} {'Grid%':>5} {'Trnd%':>5} "
              f"{'GBuys':>5} {'GSell':>5} {'Rebal':>5} {'TTrds':>5} "
              f"{'Grid PnL':>10} {'Rebal PnL':>9} {'Trend PnL':>10} "
              f"{'Bot MTM':>10} {'vs HODL':>10} {'Sharpe':>7} {'MaxDD':>7}")
    print(header)
    print("-" * len(header))

    for r in results:
        print(f"{r['label']:<35} {r['grid_time_pct']:>4.0f}% {r['trend_time_pct']:>4.0f}% "
              f"{r['grid_buys']:>5,} {r['grid_sells']:>5,} {r['grid_rebals']:>5,} {r['trend_trades']:>5,} "
              f"${r['grid_pnl']:>+9,.0f} ${r['rebal_pnl']:>+8,.0f} ${r['trend_pnl']:>+9,.0f} "
              f"${r['bot_mtm']:>+9,.0f} ${r['alpha']:>+9,.0f} {r['sharpe']:>+7.2f} {r['max_dd']:>6.1%}")

    # Best result
    best = max(results, key=lambda r: r['alpha'])
    print(f"\n  BEST: {best['label']}  →  Alpha: ${best['alpha']:+,.0f}  "
          f"Sharpe: {best['sharpe']:+.2f}  MaxDD: {best['max_dd']:.1%}")

    worst = min(results, key=lambda r: r['alpha'])
    print(f"  WORST: {worst['label']}  →  Alpha: ${worst['alpha']:+,.0f}")

    # Show the best strategy's regime breakdown
    print(f"\n  Best strategy time split: "
          f"Grid {best['grid_time_pct']:.0f}% | Trend {best['trend_time_pct']:.0f}% | "
          f"Flat {100-best['grid_time_pct']-best['trend_time_pct']:.0f}%")
    print(f"  Grid earned: ${best['grid_pnl']:+,.0f} | Rebal cost: ${best['rebal_pnl']:+,.0f} | "
          f"Trend earned: ${best['trend_pnl']:+,.0f}")


if __name__ == "__main__":
    main()
