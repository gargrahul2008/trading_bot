"""
Hybrid Grid + Trend backtest v1
- Tapered grid: sell less as price rises, buy less as price falls
- Natural runway exhaustion = regime signal (no indicators needed)
- Range detection: EMA crosses within lookback window
- Multiple grid restart modes after trend
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
TOTAL_EQUITY   = 100_000.0
GRID_PCT       = 0.40          # 0.4% per step
GRID_RANGE_PCT = 4.0           # 4% each side = 10 steps
BUY_QUOTE      = 5_000.0       # base quote per grid trade (5% of equity)
SELL_QUOTE     = 5_000.0
QTY_STEP       = 0.001
TAPER_FACTOR   = 0.10          # each step reduces qty by 10% (step1=100%, step2=90%, ...)

# Trend settings
MOMENTUM_EMA   = 50
TREND_STOP_PCT = 0.03          # 3% trailing stop

# Range detection
EMA_CROSS_LOOKBACK = 120       # hours to look back for EMA crosses
EMA_CROSS_MIN      = 2         # min crosses to declare "range"

# Grid restart limited mode
LIMITED_GRID_STEPS  = 3        # steps each side in limited mode

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


def _round_qty(q):
    return round(q / QTY_STEP) * QTY_STEP


def _qty(price, quote):
    return _round_qty(quote / price)


def count_ema_crosses(close_arr, ema_arr, end_idx, lookback):
    """Count how many times price crossed EMA in [end_idx-lookback, end_idx]."""
    start = max(1, end_idx - lookback)
    crosses = 0
    for i in range(start + 1, end_idx + 1):
        if np.isnan(ema_arr[i]) or np.isnan(ema_arr[i - 1]):
            continue
        above_now = close_arr[i] > ema_arr[i]
        above_prev = close_arr[i - 1] > ema_arr[i - 1]
        if above_now != above_prev:
            crosses += 1
    return crosses


def run_hybrid(df, mode="full_rebal", trend_active=True, taper=True, label=""):
    """
    mode:
      'full_rebal'  — restore 50/50 when restarting grid
      'limited'     — restart grid with only N steps each side
      'as_is'       — restart grid with whatever portfolio you have
    trend_active:
      True  — actively trade momentum in trend mode
      False — just hold current imbalance
    taper:
      True  — reduce qty at each successive grid step
      False — flat qty (original grid)
    """
    close = df["close"].values
    high  = df["high"].values
    low   = df["low"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0

    ema50 = compute_ema(close, MOMENTUM_EMA)

    # Portfolio
    cash = TOTAL_EQUITY / 2.0
    eth  = (TOTAL_EQUITY / 2.0) / close[0]

    # Grid state
    ref = close[0]
    grid_active = True
    grid_start_ref = ref          # ref when grid was started
    steps_up = 0                  # how many consecutive up steps from grid start
    steps_down = 0                # how many consecutive down steps from grid start
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)  # 10 steps default
    current_max_steps = max_steps

    # Trend state
    trend_mode_on = False
    trend_pos = 0.0
    trend_entry_px = 0.0
    trend_stop_px = 0.0

    # Tracking
    grid_pnl_usdc = 0.0; grid_pnl_eth = 0.0
    trend_pnl_total = 0.0
    rebal_cost = 0.0          # cost of 50/50 rebalances on grid restart
    grid_trades = 0; trend_trades = 0; grid_restarts = 0
    regime_log = []           # track regime for each candle

    snapshots = []

    for i in range(1, n):
        price = close[i]
        regime = "grid" if grid_active else ("trend" if trend_mode_on else "wait")
        regime_log.append(regime)

        # ─── GRID MODE ───
        if grid_active:
            sell_lvl = ref * (1.0 + pct_frac)
            buy_lvl  = ref * (1.0 - pct_frac)

            if price >= sell_lvl:
                # Taper: reduce qty based on steps from start
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_up) if taper else 1.0
                qty = _qty(sell_lvl, SELL_QUOTE * taper_mult)
                qty = min(qty, eth)
                if qty > 0:
                    cash += qty * sell_lvl
                    eth  -= qty
                    grid_pnl_usdc += qty * sell_lvl
                    grid_pnl_eth  -= qty
                    grid_trades += 1
                    ref = sell_lvl
                    steps_up += 1
                    steps_down = max(0, steps_down - 1)

                # Check if we hit range limit
                move_from_start = (ref - grid_start_ref) / grid_start_ref
                if steps_up >= current_max_steps or move_from_start >= range_frac:
                    grid_active = False
                    trend_mode_on = True

            elif price <= buy_lvl:
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_down) if taper else 1.0
                qty = _qty(buy_lvl, BUY_QUOTE * taper_mult)
                cost = qty * buy_lvl
                if qty > 0 and cost <= cash:
                    cash -= cost
                    eth  += qty
                    grid_pnl_usdc -= cost
                    grid_pnl_eth  += qty
                    grid_trades += 1
                    ref = buy_lvl
                    steps_down += 1
                    steps_up = max(0, steps_up - 1)

                move_from_start = (grid_start_ref - ref) / grid_start_ref
                if steps_down >= current_max_steps or move_from_start >= range_frac:
                    grid_active = False
                    trend_mode_on = True

        # ─── TREND MODE ───
        elif trend_mode_on:
            if trend_active:
                # Momentum: long when price > EMA50
                want_long = not np.isnan(ema50[i]) and price > ema50[i]

                if want_long and trend_pos == 0:
                    # Enter long with available cash (up to 50% of portfolio)
                    pv = cash + eth * price
                    alloc = min(cash * 0.9, pv * 0.5)
                    qty = _round_qty(alloc / price)
                    if qty > 0:
                        cash -= qty * price
                        trend_pos = qty
                        trend_entry_px = price
                        trend_stop_px = price * (1 - TREND_STOP_PCT)
                        trend_trades += 1

                elif not want_long and trend_pos > 0:
                    # Exit long
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1
                    trend_pos = 0.0

                elif trend_pos > 0:
                    # Update trailing stop
                    trend_stop_px = max(trend_stop_px, price * (1 - TREND_STOP_PCT))
                    if price <= trend_stop_px:
                        cash += trend_pos * price
                        trend_pnl_total += trend_pos * (price - trend_entry_px)
                        trend_trades += 1
                        trend_pos = 0.0

            # Check if we should restart grid (range detected)
            crosses = count_ema_crosses(close, ema50, i, EMA_CROSS_LOOKBACK)
            if crosses >= EMA_CROSS_MIN:
                # Close any trend position first
                if trend_pos > 0:
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1
                    trend_pos = 0.0

                # Restart grid based on mode
                grid_restarts += 1
                trend_mode_on = False
                grid_active = True
                ref = price
                grid_start_ref = price
                steps_up = 0
                steps_down = 0

                if mode == "full_rebal":
                    # Rebalance to 50/50
                    pv = cash + eth * price
                    target_cash = pv / 2.0
                    target_eth  = (pv / 2.0) / price
                    delta_cash = target_cash - cash
                    delta_eth  = target_eth - eth
                    # Track rebalance cost (mark-to-market impact)
                    rebal_cost += abs(delta_cash) * 0  # zero fees on MEXC
                    cash = target_cash
                    eth  = target_eth
                    current_max_steps = max_steps

                elif mode == "limited":
                    # Don't rebalance, but limit grid to fewer steps
                    current_max_steps = LIMITED_GRID_STEPS

                elif mode == "as_is":
                    # Trade with whatever you have
                    current_max_steps = max_steps

        # Snapshot every 24h
        if i % 24 == 0 or i == n - 1:
            total_eth = eth + trend_pos
            snapshots.append({
                "idx": i, "price": price, "cash": cash,
                "eth": total_eth,
                "pv": cash + total_eth * price,
                "regime": regime,
            })

    # Final accounting
    if trend_pos > 0:
        trend_pnl_total += trend_pos * (close[-1] - trend_entry_px)
    total_eth_end = eth + trend_pos
    final_pv = cash + total_eth_end * close[-1]
    bot_mtm = final_pv - TOTAL_EQUITY

    first_px = close[0]; last_px = close[-1]
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY
    alpha = bot_mtm - hodl_mtm

    grid_mtm = grid_pnl_usdc + grid_pnl_eth * last_px

    snap = pd.DataFrame(snapshots)
    snap["daily_ret"] = snap["pv"].pct_change()
    sharpe = (snap["daily_ret"].mean() / snap["daily_ret"].std() * np.sqrt(365)
              if len(snap) > 1 and snap["daily_ret"].std() > 0 else 0)
    max_dd = 0.0
    if len(snap) > 0:
        peak = snap["pv"].cummax()
        dd = (snap["pv"] - peak) / peak
        max_dd = dd.min()

    # Regime time breakdown
    grid_time = regime_log.count("grid") / len(regime_log) * 100 if regime_log else 0
    trend_time = regime_log.count("trend") / len(regime_log) * 100 if regime_log else 0
    wait_time = regime_log.count("wait") / len(regime_log) * 100 if regime_log else 0

    return {
        "label": label, "mode": mode,
        "grid_mtm": grid_mtm, "trend_pnl": trend_pnl_total,
        "bot_mtm": bot_mtm, "alpha": alpha, "hodl_mtm": hodl_mtm,
        "sharpe": sharpe, "max_dd": max_dd,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts,
        "grid_time": grid_time, "trend_time": trend_time, "wait_time": wait_time,
        "final_cash": cash, "final_eth": total_eth_end,
        "final_pv": final_pv,
        "snapshots": snap,
    }


def run_baselines(df):
    """Run simple baselines for comparison."""
    close = df["close"].values
    n = len(close)
    first_px = close[0]; last_px = close[-1]
    ema50 = compute_ema(close, MOMENTUM_EMA)

    # HODL
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY

    # Pure momentum (for comparison)
    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / first_px
    trend_pos = 0.0
    trend_entry = 0.0
    trend_stop = 0.0
    trend_pnl = 0.0
    snaps = []

    for i in range(1, n):
        price = close[i]
        want_long = not np.isnan(ema50[i]) and price > ema50[i]

        if want_long and trend_pos == 0:
            pv = cash + eth * price
            alloc = min(cash * 0.9, pv * 0.5)
            qty = _round_qty(alloc / price)
            if qty > 0:
                cash -= qty * price
                trend_pos = qty
                trend_entry = price
                trend_stop = price * (1 - TREND_STOP_PCT)
        elif not want_long and trend_pos > 0:
            cash += trend_pos * price
            trend_pnl += trend_pos * (price - trend_entry)
            trend_pos = 0.0
        elif trend_pos > 0:
            trend_stop = max(trend_stop, price * (1 - TREND_STOP_PCT))
            if price <= trend_stop:
                cash += trend_pos * price
                trend_pnl += trend_pos * (price - trend_entry)
                trend_pos = 0.0

        if i % 24 == 0 or i == n - 1:
            te = eth + trend_pos
            snaps.append({"idx": i, "pv": cash + te * price})

    if trend_pos > 0:
        trend_pnl += trend_pos * (last_px - trend_entry)
    final_pv = cash + (eth + trend_pos) * last_px

    snap = pd.DataFrame(snaps)
    snap["daily_ret"] = snap["pv"].pct_change()
    sharpe = (snap["daily_ret"].mean() / snap["daily_ret"].std() * np.sqrt(365)
              if len(snap) > 1 and snap["daily_ret"].std() > 0 else 0)
    peak = snap["pv"].cummax()
    max_dd = ((snap["pv"] - peak) / peak).min() if len(snap) > 0 else 0

    # Blind grid no rebal
    cash2 = TOTAL_EQUITY / 2.0
    eth2 = (TOTAL_EQUITY / 2.0) / first_px
    ref2 = first_px
    pct_frac = GRID_PCT / 100.0
    grid_usdc = 0.0; grid_eth_net = 0.0; gtrades = 0
    snaps2 = []

    for i in range(1, n):
        price = close[i]
        sell_lvl = ref2 * (1 + pct_frac)
        buy_lvl = ref2 * (1 - pct_frac)
        if price >= sell_lvl:
            qty = _qty(sell_lvl, SELL_QUOTE)
            qty = min(qty, eth2)
            if qty > 0:
                cash2 += qty * sell_lvl; eth2 -= qty
                grid_usdc += qty * sell_lvl; grid_eth_net -= qty
                ref2 = sell_lvl; gtrades += 1
        elif price <= buy_lvl:
            qty = _qty(buy_lvl, BUY_QUOTE)
            cost = qty * buy_lvl
            if qty > 0 and cost <= cash2:
                cash2 -= cost; eth2 += qty
                grid_usdc -= cost; grid_eth_net += qty
                ref2 = buy_lvl; gtrades += 1
        if i % 24 == 0 or i == n - 1:
            snaps2.append({"idx": i, "pv": cash2 + eth2 * price})

    grid_final_pv = cash2 + eth2 * last_px
    snap2 = pd.DataFrame(snaps2)
    snap2["daily_ret"] = snap2["pv"].pct_change()
    sharpe2 = (snap2["daily_ret"].mean() / snap2["daily_ret"].std() * np.sqrt(365)
               if len(snap2) > 1 and snap2["daily_ret"].std() > 0 else 0)
    peak2 = snap2["pv"].cummax()
    max_dd2 = ((snap2["pv"] - peak2) / peak2).min() if len(snap2) > 0 else 0

    return {
        "hodl_mtm": hodl_mtm,
        "momentum": {
            "bot_mtm": final_pv - TOTAL_EQUITY,
            "alpha": (final_pv - TOTAL_EQUITY) - hodl_mtm,
            "sharpe": sharpe, "max_dd": max_dd,
        },
        "blind_grid": {
            "bot_mtm": grid_final_pv - TOTAL_EQUITY,
            "alpha": (grid_final_pv - TOTAL_EQUITY) - hodl_mtm,
            "sharpe": sharpe2, "max_dd": max_dd2,
            "trades": gtrades,
        },
    }


def run_all_periods():
    print("Loading data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    df["dt"] = pd.to_datetime(df["ts"])
    print(f"Data: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}, {len(df):,} candles\n")

    periods = [
        ("2024-01-01", "2026-05-19", "FULL (2024-2026)"),
        ("2024-01-01", "2024-07-01", "2024 H1 (bull)"),
        ("2024-07-01", "2025-01-01", "2024 H2 (volatile)"),
        ("2025-01-01", "2025-07-01", "2025 H1 (crash)"),
        ("2025-07-01", "2026-01-01", "2025 H2 (rally)"),
        ("2026-01-01", "2026-05-19", "2026 YTD (bear)"),
    ]

    for start_date, end_date, period_label in periods:
        sub = df[(df["dt"] >= start_date) & (df["dt"] < end_date)].reset_index(drop=True)
        if len(sub) < 200:
            print(f"  Skipping {period_label} — {len(sub)} candles")
            continue

        first_px = sub["close"].iloc[0]
        last_px = sub["close"].iloc[-1]
        pct_chg = (last_px / first_px - 1) * 100

        baselines = run_baselines(sub)

        print(f"\n{'=' * 130}")
        print(f"  {period_label}  |  ETH: ${first_px:,.0f} → ${last_px:,.0f} ({pct_chg:+.1f}%)"
              f"  |  HODL: ${baselines['hodl_mtm']:+,.0f}  |  {len(sub):,} candles")
        print(f"{'=' * 130}")

        # Header
        hdr = (f"{'Strategy':<35} {'Grid%':>5} {'Trnd%':>5} "
               f"{'Grid MTM':>10} {'Trend PnL':>10} {'Bot MTM':>10} "
               f"{'vs HODL':>10} {'Sharpe':>7} {'MaxDD':>7} {'GTrd':>5} {'TTrd':>5} {'Rstr':>4}")
        print(hdr)
        print("-" * len(hdr))

        # Baselines
        b = baselines
        print(f"{'HODL (baseline)':<35} {'---':>5} {'---':>5} "
              f"{'---':>10} {'---':>10} ${b['hodl_mtm']:>+9,.0f} "
              f"${'0':>9} {'---':>7} {'---':>7} {'---':>5} {'---':>5} {'---':>4}")
        print(f"{'Blind Grid (no rebal)':<35} {'100%':>5} {'0%':>5} "
              f"{'---':>10} {'---':>10} ${b['blind_grid']['bot_mtm']:>+9,.0f} "
              f"${b['blind_grid']['alpha']:>+9,.0f} {b['blind_grid']['sharpe']:>+7.2f} "
              f"{b['blind_grid']['max_dd']:>6.1%} {b['blind_grid']['trades']:>5} {'---':>5} {'---':>4}")
        print(f"{'Pure Momentum':<35} {'0%':>5} {'100%':>5} "
              f"{'---':>10} {'---':>10} ${b['momentum']['bot_mtm']:>+9,.0f} "
              f"${b['momentum']['alpha']:>+9,.0f} {b['momentum']['sharpe']:>+7.2f} "
              f"{b['momentum']['max_dd']:>6.1%} {'---':>5} {'---':>5} {'---':>4}")
        print("-" * len(hdr))

        # Hybrid strategies
        configs = [
            # (mode, trend_active, taper, label)
            ("full_rebal", True,  True,  "Hybrid: rebal50 + trend + taper"),
            ("full_rebal", True,  False, "Hybrid: rebal50 + trend"),
            ("full_rebal", False, True,  "Hybrid: rebal50 + hold + taper"),
            ("limited",    True,  True,  "Hybrid: limited + trend + taper"),
            ("limited",    True,  False, "Hybrid: limited + trend"),
            ("limited",    False, True,  "Hybrid: limited + hold + taper"),
            ("as_is",      True,  True,  "Hybrid: as-is + trend + taper"),
            ("as_is",      True,  False, "Hybrid: as-is + trend"),
            ("as_is",      False, True,  "Hybrid: as-is + hold + taper"),
        ]

        results = []
        for mode, trend_active, taper, lbl in configs:
            r = run_hybrid(sub, mode=mode, trend_active=trend_active, taper=taper, label=lbl)
            results.append(r)
            print(f"{r['label']:<35} {r['grid_time']:>4.0f}% {r['trend_time']:>4.0f}% "
                  f"${r['grid_mtm']:>+9,.0f} ${r['trend_pnl']:>+9,.0f} ${r['bot_mtm']:>+9,.0f} "
                  f"${r['alpha']:>+9,.0f} {r['sharpe']:>+7.2f} {r['max_dd']:>6.1%} "
                  f"{r['grid_trades']:>5} {r['trend_trades']:>5} {r['grid_restarts']:>4}")

        best = max(results, key=lambda r: r["alpha"])
        print(f"\n  >>> BEST HYBRID: {best['label']}  |  Alpha: ${best['alpha']:+,.0f}  |  Sharpe: {best['sharpe']:+.2f}")

    print(f"\n{'=' * 80}")
    print("  LEGEND:")
    print("  rebal50 = restore 50/50 on grid restart")
    print("  limited = restart grid with only 3 levels each side")
    print("  as-is   = restart grid with whatever portfolio from trend")
    print("  trend   = active momentum trading in trend mode")
    print("  hold    = just hold imbalance in trend mode (no active trading)")
    print("  taper   = sell/buy less at each successive grid step")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    run_all_periods()
