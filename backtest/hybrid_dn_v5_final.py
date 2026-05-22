"""
Hybrid DN v5 — Final focused sweep
Combine best window (192h) with best thresholds (0.75-0.90 high, 0.40-0.60 low)
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

TOTAL_EQUITY   = 100_000.0
GRID_PCT       = 0.40
GRID_RANGE_PCT = 4.0
BUY_QUOTE      = 5_000.0
SELL_QUOTE     = 5_000.0
QTY_STEP       = 0.001
TAPER_FACTOR   = 0.10

FUTURES_FEE_PCT   = 0.02
FUNDING_RATE_8H   = 0.005
FUNDING_POSITIVE_PCT = 0.60
FUTURES_MARGIN_PCT = 0.10

MOMENTUM_EMA   = 50
TREND_STOP_PCT = 0.03

DATA_FILE = "/root/trading_bot/backtest/cache/binance_ETHUSDT_1h_1704067200000_1779062400000.parquet"


def compute_ema(arr, period):
    out = np.full_like(arr, np.nan, dtype=float)
    if len(arr) < period: return out
    out[period - 1] = np.mean(arr[:period])
    k = 2.0 / (period + 1)
    for i in range(period, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def compute_realized_vol(close, window):
    out = np.full_like(close, np.nan, dtype=float)
    log_ret = np.diff(np.log(close))
    for i in range(window, len(close)):
        out[i] = np.std(log_ret[i - window:i]) * np.sqrt(8760)
    return out


def _round_qty(q):
    return round(q / QTY_STEP) * QTY_STEP

def _qty(price, quote):
    return _round_qty(quote / price)


def count_ema_crosses(close_arr, ema_arr, end_idx, lookback):
    start = max(1, end_idx - lookback)
    crosses = 0
    for i in range(start + 1, end_idx + 1):
        if np.isnan(ema_arr[i]) or np.isnan(ema_arr[i - 1]): continue
        if (close_arr[i] > ema_arr[i]) != (close_arr[i - 1] > ema_arr[i - 1]):
            crosses += 1
    return crosses


def price_range_pct(close_arr, end_idx, lookback):
    start = max(0, end_idx - lookback)
    window = close_arr[start:end_idx + 1]
    if len(window) < 2: return 999.0
    return (window.max() - window.min()) / window.mean() * 100


def run_strategy(df, vol_high, vol_low, vol_window, min_trend_hours=168):
    close = df["close"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)

    ema50 = compute_ema(close, MOMENTUM_EMA)
    vol = compute_realized_vol(close, vol_window)

    ema_cross_lookback = 168
    ema_cross_min = 3
    max_range_pct_val = 6.0
    range_lookback = 168

    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / close[0]

    futures_short_qty = 0.0
    futures_pnl = 0.0; futures_fees_paid = 0.0
    funding_received = 0.0; futures_margin_locked = 0.0
    hedge_entry_price = close[0]

    def open_hedge(qty, price):
        nonlocal futures_short_qty, futures_fees_paid, futures_margin_locked, cash
        if qty <= 0: return
        fee = qty * price * FUTURES_FEE_PCT / 100.0
        futures_fees_paid += fee; cash -= fee
        futures_short_qty += qty
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    def close_hedge_fn(qty, entry_avg, price):
        nonlocal futures_short_qty, futures_pnl, futures_fees_paid, futures_margin_locked, cash
        if qty <= 0 or futures_short_qty <= 0: return
        qty = min(qty, futures_short_qty)
        pnl = qty * (entry_avg - price)
        fee = qty * price * FUTURES_FEE_PCT / 100.0
        futures_pnl += pnl; futures_fees_paid += fee
        cash += pnl - fee
        futures_short_qty -= qty
        if futures_short_qty < QTY_STEP: futures_short_qty = 0.0
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    mode = "grid"
    ref = close[0]; grid_start_ref = ref
    steps_up = 0; steps_down = 0
    trend_entered_at = 0
    trend_pos = 0.0; trend_entry_px = 0.0; trend_stop_px = 0.0

    grid_pnl_usdc = 0.0; grid_pnl_eth = 0.0
    trend_pnl_total = 0.0
    grid_trades = 0; trend_trades = 0; grid_restarts = 0; vol_switches = 0
    regime_log = []
    snapshots = []

    open_hedge(eth, close[0])

    for i in range(1, n):
        price = close[i]
        regime_log.append(mode)
        cur_vol = vol[i] if not np.isnan(vol[i]) else 0

        if futures_short_qty > 0 and i % 8 == 0:
            fa = futures_short_qty * price * FUNDING_RATE_8H / 100.0
            if np.random.random() < FUNDING_POSITIVE_PCT:
                funding_received += fa; cash += fa
            else:
                funding_received -= fa; cash -= fa

        # VOL CHECK in trend
        if mode == "trend" and cur_vol > 0 and cur_vol > vol_high:
            if trend_pos > 0:
                cash += trend_pos * price
                trend_pnl_total += trend_pos * (price - trend_entry_px)
                trend_trades += 1; trend_pos = 0.0
            mode = "flat"; vol_switches += 1

        if mode == "flat":
            if cur_vol > 0 and cur_vol < vol_low:
                mode = "trend"; trend_entered_at = i
            elif cur_vol > 0 and cur_vol < vol_high:
                crosses = count_ema_crosses(close, ema50, i, ema_cross_lookback)
                pr = price_range_pct(close, i, range_lookback)
                if crosses >= ema_cross_min and pr <= max_range_pct_val:
                    mode = "grid"; ref = price; grid_start_ref = price
                    steps_up = 0; steps_down = 0
                    hedge_entry_price = price; open_hedge(eth, price)
                    grid_restarts += 1

        elif mode == "grid":
            sell_lvl = ref * (1.0 + pct_frac)
            buy_lvl = ref * (1.0 - pct_frac)

            if cur_vol > 0 and cur_vol < vol_low:
                if futures_short_qty > 0:
                    close_hedge_fn(futures_short_qty, hedge_entry_price, price)
                mode = "trend"; trend_entered_at = i; vol_switches += 1
            elif price >= sell_lvl:
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_up)
                qty = min(_qty(sell_lvl, SELL_QUOTE * taper_mult), eth)
                if qty > 0:
                    cash += qty * sell_lvl; eth -= qty
                    grid_pnl_usdc += qty * sell_lvl; grid_pnl_eth -= qty
                    grid_trades += 1; ref = sell_lvl
                    steps_up += 1; steps_down = max(0, steps_down - 1)
                    if futures_short_qty > 0:
                        close_hedge_fn(qty, hedge_entry_price, sell_lvl)
                move = (ref - grid_start_ref) / grid_start_ref
                if steps_up >= max_steps or move >= range_frac:
                    if futures_short_qty > 0:
                        close_hedge_fn(futures_short_qty, hedge_entry_price, price)
                    mode = "trend"; trend_entered_at = i
            elif price <= buy_lvl:
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_down)
                qty = _qty(buy_lvl, BUY_QUOTE * taper_mult)
                cost = qty * buy_lvl
                avail = cash - futures_margin_locked
                if qty > 0 and cost <= avail:
                    cash -= cost; eth += qty
                    grid_pnl_usdc -= cost; grid_pnl_eth += qty
                    grid_trades += 1; ref = buy_lvl
                    steps_down += 1; steps_up = max(0, steps_up - 1)
                    open_hedge(qty, buy_lvl)
                    if futures_short_qty > qty:
                        hedge_entry_price = (
                            hedge_entry_price * (futures_short_qty - qty) + buy_lvl * qty
                        ) / futures_short_qty
                    else:
                        hedge_entry_price = buy_lvl
                move = (grid_start_ref - ref) / grid_start_ref
                if steps_down >= max_steps or move >= range_frac:
                    if futures_short_qty > 0:
                        close_hedge_fn(futures_short_qty, hedge_entry_price, price)
                    mode = "trend"; trend_entered_at = i

        elif mode == "trend":
            want_long = not np.isnan(ema50[i]) and price > ema50[i]
            if want_long and trend_pos == 0:
                pv = cash + eth * price
                alloc = min(cash * 0.9, pv * 0.5)
                qty = _round_qty(alloc / price)
                if qty > 0:
                    cash -= qty * price; trend_pos = qty
                    trend_entry_px = price; trend_stop_px = price * (1 - TREND_STOP_PCT)
                    trend_trades += 1
            elif not want_long and trend_pos > 0:
                cash += trend_pos * price
                trend_pnl_total += trend_pos * (price - trend_entry_px)
                trend_trades += 1; trend_pos = 0.0
            elif trend_pos > 0:
                trend_stop_px = max(trend_stop_px, price * (1 - TREND_STOP_PCT))
                if price <= trend_stop_px:
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1; trend_pos = 0.0

            hours_in_trend = i - trend_entered_at
            if hours_in_trend >= min_trend_hours:
                crosses = count_ema_crosses(close, ema50, i, ema_cross_lookback)
                pr = price_range_pct(close, i, range_lookback)
                if crosses >= ema_cross_min and pr <= max_range_pct_val:
                    if trend_pos > 0:
                        cash += trend_pos * price
                        trend_pnl_total += trend_pos * (price - trend_entry_px)
                        trend_trades += 1; trend_pos = 0.0
                    mode = "grid"; ref = price; grid_start_ref = price
                    steps_up = 0; steps_down = 0
                    hedge_entry_price = price; open_hedge(eth, price)
                    grid_restarts += 1

        if i % 24 == 0 or i == n - 1:
            te = eth + trend_pos
            fu = futures_short_qty * (hedge_entry_price - price) if futures_short_qty > 0 else 0
            snapshots.append({"idx": i, "price": price, "pv": cash + te * price + fu})

    if trend_pos > 0:
        trend_pnl_total += trend_pos * (close[-1] - trend_entry_px)
    if futures_short_qty > 0:
        close_hedge_fn(futures_short_qty, hedge_entry_price, close[-1])

    total_eth = eth + trend_pos
    final_pv = cash + total_eth * close[-1]
    first_px = close[0]; last_px = close[-1]
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY
    bot_mtm = final_pv - TOTAL_EQUITY
    alpha = bot_mtm - hodl_mtm
    grid_mtm = grid_pnl_usdc + grid_pnl_eth * last_px

    snap = pd.DataFrame(snapshots)
    snap["daily_ret"] = snap["pv"].pct_change()
    sharpe = (snap["daily_ret"].mean() / snap["daily_ret"].std() * np.sqrt(365)
              if len(snap) > 1 and snap["daily_ret"].std() > 0 else 0)
    max_dd = 0.0
    if len(snap) > 0:
        peak = snap["pv"].cummax()
        max_dd = ((snap["pv"] - peak) / peak).min()

    grid_time = regime_log.count("grid") / len(regime_log) * 100 if regime_log else 0
    trend_time = regime_log.count("trend") / len(regime_log) * 100 if regime_log else 0
    flat_time = regime_log.count("flat") / len(regime_log) * 100 if regime_log else 0

    return {
        "alpha": alpha, "bot_mtm": bot_mtm, "hodl_mtm": hodl_mtm,
        "sharpe": sharpe, "max_dd": max_dd,
        "grid_mtm": grid_mtm, "trend_pnl": trend_pnl_total,
        "futures_pnl": futures_pnl, "futures_fees": futures_fees_paid,
        "funding": funding_received,
        "grid_time": grid_time, "trend_time": trend_time, "flat_time": flat_time,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts, "vol_switches": vol_switches,
        "final_pv": final_pv,
    }


def main():
    print("Loading data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    df["dt"] = pd.to_datetime(df["ts"])
    print(f"Data: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}, {len(df):,} candles\n")

    periods = [
        ("2024-01-01", "2026-05-19", "FULL"),
        ("2024-01-01", "2024-07-01", "2024H1"),
        ("2024-07-01", "2025-01-01", "2024H2"),
        ("2025-01-01", "2025-07-01", "2025H1"),
        ("2025-07-01", "2026-01-01", "2025H2"),
        ("2026-01-01", "2026-05-19", "2026"),
    ]

    period_dfs = {}
    for start, end, label in periods:
        sub = df[(df["dt"] >= start) & (df["dt"] < end)].reset_index(drop=True)
        if len(sub) >= 200:
            period_dfs[label] = sub

    # ═══════════════════════════════════════════════════════════════
    # SWEEP: 192h window × vol_high × vol_low grid
    # ═══════════════════════════════════════════════════════════════
    print("=" * 130)
    print("  SWEEP: 192h window — vol_high × vol_low (flat action)")
    print("=" * 130)

    vol_highs = [0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
    vol_lows  = [0.35, 0.40, 0.45, 0.50, 0.55, 0.60]

    # Collect all results for ranking
    all_results = []

    hdr = f"{'vh':>5} {'vl':>5}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8}"
    hdr += f" {'Sh_FULL':>8}"
    print(hdr)
    print("-" * len(hdr))

    for vh in vol_highs:
        for vl in vol_lows:
            if vl >= vh: continue  # nonsensical
            line = f"{vh:>4.2f} {vl:>4.2f}"
            period_results = {}
            for label, sub_df in period_dfs.items():
                np.random.seed(42)
                r = run_strategy(sub_df, vol_high=vh, vol_low=vl, vol_window=192)
                period_results[label] = r
                line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%}"
            line += f" {period_results['FULL']['sharpe']:>+7.2f}"
            print(line)
            all_results.append((vh, vl, period_results))

    # ═══════════════════════════════════════════════════════════════
    # Also test window 120h and 168h with best combos for comparison
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*130}")
    print("  COMPARISON: Best combos across window sizes")
    print(f"{'='*130}")

    best_combos = [
        (0.80, 0.50), (0.85, 0.50), (0.80, 0.45),
        (0.85, 0.45), (0.75, 0.45), (0.90, 0.50),
    ]
    windows = [120, 144, 168, 192, 216, 240]

    hdr = f"{'vh':>5} {'vl':>5} {'win':>5}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10}"
    hdr += f" {'Sh_FULL':>8} {'DD_FULL':>8}"
    print(hdr)
    print("-" * len(hdr))

    for vh, vl in best_combos:
        for w in windows:
            line = f"{vh:>4.2f} {vl:>4.2f} {w:>4}h"
            full_r = None
            for label, sub_df in period_dfs.items():
                np.random.seed(42)
                r = run_strategy(sub_df, vol_high=vh, vol_low=vl, vol_window=w)
                if label == "FULL": full_r = r
                line += f" ${r['alpha']:>+8,.0f}"
            line += f" {full_r['sharpe']:>+7.2f} {full_r['max_dd']:>7.1%}"
            print(line)
        print()  # blank line between combos

    # ═══════════════════════════════════════════════════════════════
    # TOP 10 by full-period alpha — detailed view
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*130}")
    print("  TOP 10 configs (192h window) ranked by FULL alpha")
    print(f"{'='*130}")

    ranked = sorted(all_results, key=lambda x: x[2]["FULL"]["alpha"], reverse=True)

    for rank, (vh, vl, pr) in enumerate(ranked[:10], 1):
        r = pr["FULL"]
        print(f"\n  #{rank}: vol_high={vh:.2f}, vol_low={vl:.2f}, window=192h")
        print(f"  {'Period':<12} {'Alpha':>10} {'BotMTM':>10} {'Sharpe':>8} {'MaxDD':>8} "
              f"{'Grid%':>6} {'Trend%':>7} {'Flat%':>6} {'GTrd':>6} {'TTrd':>6} {'Rstr':>5}")
        for label in period_dfs:
            r = pr[label]
            print(f"  {label:<12} ${r['alpha']:>+9,.0f} ${r['bot_mtm']:>+9,.0f} "
                  f"{r['sharpe']:>+7.2f} {r['max_dd']:>7.1%} "
                  f"{r['grid_time']:>5.0f}% {r['trend_time']:>6.0f}% {r['flat_time']:>5.0f}% "
                  f"{r['grid_trades']:>6} {r['trend_trades']:>6} {r['grid_restarts']:>5}")

    # ═══════════════════════════════════════════════════════════════
    # CONSISTENCY SCORE: rank by worst-period alpha (robustness)
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*130}")
    print("  TOP 10 by CONSISTENCY (best worst-case period alpha)")
    print(f"{'='*130}")

    sub_periods = ["2024H1", "2024H2", "2025H1", "2025H2", "2026"]

    def worst_alpha(pr):
        return min(pr[p]["alpha"] for p in sub_periods)

    ranked_robust = sorted(all_results, key=lambda x: worst_alpha(x[2]), reverse=True)

    hdr = f"{'#':>3} {'vh':>5} {'vl':>5} {'α_FULL':>10} {'worst_α':>10} {'Sh_FULL':>8} {'DD_FULL':>8}"
    for p in sub_periods:
        hdr += f" {'α_'+p:>10}"
    print(hdr)
    print("-" * len(hdr))

    for rank, (vh, vl, pr) in enumerate(ranked_robust[:10], 1):
        r = pr["FULL"]
        wa = worst_alpha(pr)
        line = f"{rank:>3} {vh:>4.2f} {vl:>4.2f} ${r['alpha']:>+9,.0f} ${wa:>+9,.0f} {r['sharpe']:>+7.2f} {r['max_dd']:>7.1%}"
        for p in sub_periods:
            line += f" ${pr[p]['alpha']:>+9,.0f}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # BALANCED SCORE: alpha * (1 - |max_dd|) — rewards both return and safety
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*130}")
    print("  TOP 10 by BALANCED SCORE (alpha × (1 - |maxDD|))")
    print(f"{'='*130}")

    def balanced_score(pr):
        r = pr["FULL"]
        return r["alpha"] * (1 - abs(r["max_dd"]))

    ranked_balanced = sorted(all_results, key=lambda x: balanced_score(x[2]), reverse=True)

    hdr = f"{'#':>3} {'vh':>5} {'vl':>5} {'Score':>10} {'α_FULL':>10} {'Sh_FULL':>8} {'DD_FULL':>8}"
    for p in sub_periods:
        hdr += f" {'α_'+p:>10}"
    print(hdr)
    print("-" * len(hdr))

    for rank, (vh, vl, pr) in enumerate(ranked_balanced[:10], 1):
        r = pr["FULL"]
        sc = balanced_score(pr)
        line = f"{rank:>3} {vh:>4.2f} {vl:>4.2f} ${sc:>+9,.0f} ${r['alpha']:>+9,.0f} {r['sharpe']:>+7.2f} {r['max_dd']:>7.1%}"
        for p in sub_periods:
            line += f" ${pr[p]['alpha']:>+9,.0f}"
        print(line)


if __name__ == "__main__":
    main()
