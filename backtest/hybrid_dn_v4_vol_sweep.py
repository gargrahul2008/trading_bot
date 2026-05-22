"""
Hybrid DN v4 — Vol threshold sweep
Fine-tune the RVol>X→flat, <Y→trend strategy with granular threshold search.
Also sweep vol window, and test asymmetric entry/exit thresholds.
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
from itertools import product

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
    if len(arr) < period:
        return out
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
        if np.isnan(ema_arr[i]) or np.isnan(ema_arr[i - 1]):
            continue
        if (close_arr[i] > ema_arr[i]) != (close_arr[i - 1] > ema_arr[i - 1]):
            crosses += 1
    return crosses


def price_range_pct(close_arr, end_idx, lookback):
    start = max(0, end_idx - lookback)
    window = close_arr[start:end_idx + 1]
    if len(window) < 2:
        return 999.0
    return (window.max() - window.min()) / window.mean() * 100


def run_strategy(df, vol_high, vol_low, vol_window, high_vol_action,
                  min_trend_hours=168, ema_cross_min=3):
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)

    ema50 = compute_ema(close, MOMENTUM_EMA)
    vol = compute_realized_vol(close, vol_window)

    ema_cross_lookback = 168
    max_range_pct_val = 6.0
    range_lookback = 168

    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / close[0]

    futures_short_qty = 0.0
    futures_pnl = 0.0
    futures_fees_paid = 0.0
    funding_received = 0.0
    futures_margin_locked = 0.0
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

        # VOL CHECK in trend mode
        if mode == "trend" and cur_vol > 0 and cur_vol > vol_high:
            if high_vol_action == "flat":
                if trend_pos > 0:
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1; trend_pos = 0.0
                mode = "flat"; vol_switches += 1
            elif high_vol_action == "grid":
                if trend_pos > 0:
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1; trend_pos = 0.0
                mode = "grid"; ref = price; grid_start_ref = price
                steps_up = 0; steps_down = 0
                hedge_entry_price = price; open_hedge(eth, price)
                vol_switches += 1

        # FLAT MODE
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

        # GRID MODE
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

        # TREND MODE
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
        "grid_time": grid_time, "trend_time": trend_time, "flat_time": flat_time,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts, "vol_switches": vol_switches,
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
    # SWEEP 1: vol_high threshold (vol_low = vol_high - 0.30)
    # ═══════════════════════════════════════════════════════════════
    print("=" * 120)
    print("  SWEEP 1: vol_high threshold (flat action, window=168h, vol_low = vol_high - 0.30)")
    print("=" * 120)

    vol_highs = [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00, 1.10, 1.20]

    hdr = f"{'vol_high':>8} {'vol_low':>8}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8}"
    print(hdr)
    print("-" * len(hdr))

    for vh in vol_highs:
        vl = vh - 0.30
        line = f"{vh:>7.2f}% {vl:>7.2f}%"
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=vh, vol_low=vl, vol_window=168,
                              high_vol_action="flat")
            line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # SWEEP 2: vol_low threshold (vol_high fixed at best from sweep 1)
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*120}")
    print("  SWEEP 2: vol_low threshold (flat action, vol_high=0.80, window=168h)")
    print(f"{'='*120}")

    vol_lows = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]

    hdr = f"{'vol_high':>8} {'vol_low':>8}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8}"
    print(hdr)
    print("-" * len(hdr))

    for vl in vol_lows:
        line = f"{'0.80':>7}% {vl:>7.2f}%"
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=0.80, vol_low=vl, vol_window=168,
                              high_vol_action="flat")
            line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # SWEEP 3: vol window (vol_high=0.80, vol_low=0.50)
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*120}")
    print("  SWEEP 3: vol window hours (flat action, vol_high=0.80, vol_low=0.50)")
    print(f"{'='*120}")

    windows = [48, 72, 96, 120, 144, 168, 192, 240, 336]

    hdr = f"{'window':>8}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8}"
    print(hdr)
    print("-" * len(hdr))

    for w in windows:
        line = f"{w:>7}h"
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=0.80, vol_low=0.50, vol_window=w,
                              high_vol_action="flat")
            line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # SWEEP 4: flat vs grid action during high vol
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*120}")
    print("  SWEEP 4: high_vol_action comparison (best thresholds from above)")
    print(f"{'='*120}")

    combos = [
        (0.80, 0.50, 168, "flat"),
        (0.80, 0.50, 168, "grid"),
        (0.80, 0.45, 168, "flat"),
        (0.80, 0.45, 168, "grid"),
        (0.85, 0.50, 168, "flat"),
        (0.85, 0.50, 168, "grid"),
        (0.75, 0.45, 168, "flat"),
        (0.75, 0.45, 168, "grid"),
    ]

    hdr = f"{'vh':>5} {'vl':>5} {'win':>4} {'action':>6}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8} {'Sh_'+label:>8}"
    print(hdr)
    print("-" * len(hdr))

    for vh, vl, w, action in combos:
        line = f"{vh:>4.2f} {vl:>4.2f} {w:>4} {action:>6}"
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=vh, vol_low=vl, vol_window=w,
                              high_vol_action=action)
            line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%} {r['sharpe']:>+7.2f}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # SWEEP 5: min_trend_hours interaction with vol
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*120}")
    print("  SWEEP 5: min_trend_hours with best vol config (0.80/0.50, 168h, flat)")
    print(f"{'='*120}")

    trend_hours = [48, 120, 168, 240, 336, 504]

    hdr = f"{'min_hrs':>8}"
    for label in period_dfs:
        hdr += f" {'α_'+label:>10} {'DD_'+label:>8} {'Sh_'+label:>8}"
    print(hdr)
    print("-" * len(hdr))

    for mth in trend_hours:
        line = f"{mth:>7}h"
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=0.80, vol_low=0.50, vol_window=168,
                              high_vol_action="flat", min_trend_hours=mth)
            line += f" ${r['alpha']:>+8,.0f} {r['max_dd']:>7.1%} {r['sharpe']:>+7.2f}"
        print(line)

    # ═══════════════════════════════════════════════════════════════
    # FINAL: Top 5 configs — detailed breakdown
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*120}")
    print("  FINAL: Top configs — detailed breakdown")
    print(f"{'='*120}")

    top_configs = [
        (0.80, 0.50, 168, "flat", 168, "RVol 80/50 168h flat 1wk"),
        (0.80, 0.45, 168, "flat", 168, "RVol 80/45 168h flat 1wk"),
        (0.85, 0.50, 168, "flat", 168, "RVol 85/50 168h flat 1wk"),
        (0.75, 0.45, 168, "flat", 168, "RVol 75/45 168h flat 1wk"),
        (0.80, 0.50, 168, "flat", 336, "RVol 80/50 168h flat 2wk"),
        (0.80, 0.50, 168, "grid", 168, "RVol 80/50 168h grid 1wk"),
    ]

    for vh, vl, w, action, mth, name in top_configs:
        print(f"\n  --- {name} ---")
        print(f"  {'Period':<12} {'Alpha':>10} {'BotMTM':>10} {'Sharpe':>8} {'MaxDD':>8} "
              f"{'Grid%':>6} {'Trend%':>7} {'Flat%':>6} {'GTrd':>6} {'TTrd':>6} {'Rstr':>5} {'VSw':>5}")
        for label, sub_df in period_dfs.items():
            np.random.seed(42)
            r = run_strategy(sub_df, vol_high=vh, vol_low=vl, vol_window=w,
                              high_vol_action=action, min_trend_hours=mth)
            print(f"  {label:<12} ${r['alpha']:>+9,.0f} ${r['bot_mtm']:>+9,.0f} "
                  f"{r['sharpe']:>+7.2f} {r['max_dd']:>7.1%} "
                  f"{r['grid_time']:>5.0f}% {r['trend_time']:>6.0f}% {r['flat_time']:>5.0f}% "
                  f"{r['grid_trades']:>6} {r['trend_trades']:>6} {r['grid_restarts']:>5} {r['vol_switches']:>5}")


if __name__ == "__main__":
    main()
