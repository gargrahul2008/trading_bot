"""
Hybrid Grid (Delta-Neutral) + Trend backtest — v2
Key change: trend mode is stickier — needs stronger range confirmation before
restarting grid. Also tests different range detection approaches.
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
TOTAL_EQUITY   = 100_000.0
GRID_PCT       = 0.40
GRID_RANGE_PCT = 4.0
BUY_QUOTE      = 5_000.0
SELL_QUOTE     = 5_000.0
QTY_STEP       = 0.001
TAPER_FACTOR   = 0.10

# Futures costs
FUTURES_FEE_PCT   = 0.02
FUNDING_RATE_8H   = 0.005
FUNDING_POSITIVE_PCT = 0.60
FUTURES_MARGIN_PCT = 0.10

# Trend settings
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
        above_now = close_arr[i] > ema_arr[i]
        above_prev = close_arr[i - 1] > ema_arr[i - 1]
        if above_now != above_prev:
            crosses += 1
    return crosses


def price_range_pct(close_arr, end_idx, lookback):
    """Max price range as % in the lookback window."""
    start = max(0, end_idx - lookback)
    window = close_arr[start:end_idx + 1]
    if len(window) < 2:
        return 999.0
    return (window.max() - window.min()) / window.mean() * 100


def run_hybrid_dn(df, range_params, taper=True, label=""):
    """
    range_params dict:
      ema_cross_lookback: hours to look back
      ema_cross_min: min crosses needed
      min_trend_hours: minimum hours to stay in trend before allowing grid restart
      max_range_pct: price must be within this % range to confirm consolidation
      range_lookback: hours to check price range
    """
    close = df["close"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)

    ema50 = compute_ema(close, MOMENTUM_EMA)

    # Unpack range params
    ema_cross_lookback = range_params.get("ema_cross_lookback", 120)
    ema_cross_min = range_params.get("ema_cross_min", 2)
    min_trend_hours = range_params.get("min_trend_hours", 0)
    max_range_pct = range_params.get("max_range_pct", 999)
    range_lookback = range_params.get("range_lookback", 120)

    # Portfolio
    cash = TOTAL_EQUITY / 2.0
    eth  = (TOTAL_EQUITY / 2.0) / close[0]

    # Futures state
    futures_short_qty = 0.0
    futures_pnl = 0.0
    futures_fees_paid = 0.0
    funding_received = 0.0
    futures_margin_locked = 0.0
    hedge_entry_price = close[0]

    def open_hedge(qty_to_hedge, price):
        nonlocal futures_short_qty, futures_fees_paid, futures_margin_locked, cash
        if qty_to_hedge <= 0:
            return
        fee = qty_to_hedge * price * FUTURES_FEE_PCT / 100.0
        futures_fees_paid += fee
        cash -= fee
        futures_short_qty += qty_to_hedge
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    def close_hedge(qty_to_close, entry_avg_price, price):
        nonlocal futures_short_qty, futures_pnl, futures_fees_paid, futures_margin_locked, cash
        if qty_to_close <= 0 or futures_short_qty <= 0:
            return
        qty_to_close = min(qty_to_close, futures_short_qty)
        pnl = qty_to_close * (entry_avg_price - price)
        fee = qty_to_close * price * FUTURES_FEE_PCT / 100.0
        futures_pnl += pnl
        futures_fees_paid += fee
        cash += pnl - fee
        futures_short_qty -= qty_to_close
        if futures_short_qty < QTY_STEP:
            futures_short_qty = 0.0
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    # Grid state
    ref = close[0]
    grid_active = True
    grid_start_ref = ref
    steps_up = 0
    steps_down = 0

    # Trend state
    trend_mode_on = False
    trend_entered_at = 0       # candle index when trend mode started
    trend_pos = 0.0
    trend_entry_px = 0.0
    trend_stop_px = 0.0

    # Tracking
    grid_pnl_usdc = 0.0; grid_pnl_eth = 0.0
    trend_pnl_total = 0.0
    grid_trades = 0; trend_trades = 0; grid_restarts = 0
    regime_log = []
    snapshots = []

    # Initial hedge
    open_hedge(eth, close[0])

    for i in range(1, n):
        price = close[i]
        regime = "grid" if grid_active else ("trend" if trend_mode_on else "wait")
        regime_log.append(regime)

        # Funding every 8h
        if futures_short_qty > 0 and i % 8 == 0:
            funding_amount = futures_short_qty * price * FUNDING_RATE_8H / 100.0
            if np.random.random() < FUNDING_POSITIVE_PCT:
                funding_received += funding_amount
                cash += funding_amount
            else:
                funding_received -= funding_amount
                cash -= funding_amount

        # ═══ GRID MODE ═══
        if grid_active:
            sell_lvl = ref * (1.0 + pct_frac)
            buy_lvl  = ref * (1.0 - pct_frac)

            if price >= sell_lvl:
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
                    if futures_short_qty > 0:
                        close_hedge(qty, hedge_entry_price, sell_lvl)

                move_from_start = (ref - grid_start_ref) / grid_start_ref
                if steps_up >= max_steps or move_from_start >= range_frac:
                    if futures_short_qty > 0:
                        close_hedge(futures_short_qty, hedge_entry_price, price)
                    grid_active = False
                    trend_mode_on = True
                    trend_entered_at = i

            elif price <= buy_lvl:
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_down) if taper else 1.0
                qty = _qty(buy_lvl, BUY_QUOTE * taper_mult)
                cost = qty * buy_lvl
                avail_cash = cash - futures_margin_locked
                if qty > 0 and cost <= avail_cash:
                    cash -= cost
                    eth  += qty
                    grid_pnl_usdc -= cost
                    grid_pnl_eth  += qty
                    grid_trades += 1
                    ref = buy_lvl
                    steps_down += 1
                    steps_up = max(0, steps_up - 1)
                    open_hedge(qty, buy_lvl)
                    if futures_short_qty > 0:
                        hedge_entry_price = (
                            (hedge_entry_price * (futures_short_qty - qty) + buy_lvl * qty)
                            / futures_short_qty
                        ) if futures_short_qty > qty else buy_lvl

                move_from_start = (grid_start_ref - ref) / grid_start_ref
                if steps_down >= max_steps or move_from_start >= range_frac:
                    if futures_short_qty > 0:
                        close_hedge(futures_short_qty, hedge_entry_price, price)
                    grid_active = False
                    trend_mode_on = True
                    trend_entered_at = i

        # ═══ TREND MODE ═══
        elif trend_mode_on:
            want_long = not np.isnan(ema50[i]) and price > ema50[i]

            if want_long and trend_pos == 0:
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
                cash += trend_pos * price
                trend_pnl_total += trend_pos * (price - trend_entry_px)
                trend_trades += 1
                trend_pos = 0.0

            elif trend_pos > 0:
                trend_stop_px = max(trend_stop_px, price * (1 - TREND_STOP_PCT))
                if price <= trend_stop_px:
                    cash += trend_pos * price
                    trend_pnl_total += trend_pos * (price - trend_entry_px)
                    trend_trades += 1
                    trend_pos = 0.0

            # ── Range detection: should we restart grid? ──
            hours_in_trend = i - trend_entered_at
            if hours_in_trend < min_trend_hours:
                pass  # too early, stay in trend
            else:
                crosses = count_ema_crosses(close, ema50, i, ema_cross_lookback)
                pr = price_range_pct(close, i, range_lookback)
                if crosses >= ema_cross_min and pr <= max_range_pct:
                    # Close trend position
                    if trend_pos > 0:
                        cash += trend_pos * price
                        trend_pnl_total += trend_pos * (price - trend_entry_px)
                        trend_trades += 1
                        trend_pos = 0.0

                    grid_restarts += 1
                    trend_mode_on = False
                    grid_active = True
                    ref = price
                    grid_start_ref = price
                    steps_up = 0
                    steps_down = 0

                    # Re-establish hedge
                    hedge_entry_price = price
                    open_hedge(eth, price)

        # Snapshot
        if i % 24 == 0 or i == n - 1:
            total_eth = eth + trend_pos
            fut_unreal = futures_short_qty * (hedge_entry_price - price) if futures_short_qty > 0 else 0
            snapshots.append({
                "idx": i, "price": price,
                "pv": cash + total_eth * price + fut_unreal,
                "regime": regime,
            })

    # Final
    if trend_pos > 0:
        trend_pnl_total += trend_pos * (close[-1] - trend_entry_px)
    if futures_short_qty > 0:
        close_hedge(futures_short_qty, hedge_entry_price, close[-1])

    total_eth_end = eth + trend_pos
    final_pv = cash + total_eth_end * close[-1]
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

    return {
        "label": label,
        "grid_mtm": grid_mtm, "trend_pnl": trend_pnl_total,
        "futures_pnl": futures_pnl, "futures_fees": futures_fees_paid,
        "funding_income": funding_received,
        "bot_mtm": bot_mtm, "alpha": alpha, "hodl_mtm": hodl_mtm,
        "sharpe": sharpe, "max_dd": max_dd,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts,
        "grid_time": grid_time, "trend_time": trend_time,
        "final_pv": final_pv,
    }


def run_baselines(df):
    close = df["close"].values
    n = len(close)
    first_px = close[0]; last_px = close[-1]
    ema50 = compute_ema(close, MOMENTUM_EMA)
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY

    # Pure momentum
    cash = TOTAL_EQUITY / 2.0; eth = (TOTAL_EQUITY / 2.0) / first_px
    tp = 0.0; te = 0.0; ts = 0.0
    snaps = []
    for i in range(1, n):
        p = close[i]
        wl = not np.isnan(ema50[i]) and p > ema50[i]
        if wl and tp == 0:
            pv = cash + eth * p; alloc = min(cash * 0.9, pv * 0.5)
            q = _round_qty(alloc / p)
            if q > 0: cash -= q*p; tp = q; te = p; ts = p*(1-TREND_STOP_PCT)
        elif not wl and tp > 0:
            cash += tp*p; tp = 0
        elif tp > 0:
            ts = max(ts, p*(1-TREND_STOP_PCT))
            if p <= ts: cash += tp*p; tp = 0
        if i % 24 == 0 or i == n-1: snaps.append({"pv": cash + (eth+tp)*p})
    mom_pv = cash + (eth+tp)*last_px
    snap = pd.DataFrame(snaps); snap["r"] = snap["pv"].pct_change()
    mom_sharpe = snap["r"].mean()/snap["r"].std()*np.sqrt(365) if snap["r"].std()>0 else 0
    mom_dd = ((snap["pv"]-snap["pv"].cummax())/snap["pv"].cummax()).min()

    return {
        "hodl_mtm": hodl_mtm,
        "momentum": {"bot_mtm": mom_pv-TOTAL_EQUITY, "alpha": (mom_pv-TOTAL_EQUITY)-hodl_mtm,
                      "sharpe": mom_sharpe, "max_dd": mom_dd},
    }


def main():
    print("Loading data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    df["dt"] = pd.to_datetime(df["ts"])
    print(f"Data: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}, {len(df):,} candles\n")

    np.random.seed(42)

    periods = [
        ("2024-01-01", "2026-05-19", "FULL (2024-2026)"),
        ("2024-01-01", "2024-07-01", "2024 H1 (bull)"),
        ("2024-07-01", "2025-01-01", "2024 H2 (volatile)"),
        ("2025-01-01", "2025-07-01", "2025 H1 (crash)"),
        ("2025-07-01", "2026-01-01", "2025 H2 (rally)"),
        ("2026-01-01", "2026-05-19", "2026 YTD (bear)"),
    ]

    # Different stickiness configs to test
    configs = [
        # (range_params, taper, label)
        # v1 baseline (old)
        ({"ema_cross_lookback": 120, "ema_cross_min": 2, "min_trend_hours": 0,
          "max_range_pct": 999, "range_lookback": 120},
         True, "v1 baseline (2 crosses, no wait)"),

        # Min trend stay: 48h, 72h, 168h (1 week)
        ({"ema_cross_lookback": 120, "ema_cross_min": 2, "min_trend_hours": 48,
          "max_range_pct": 999, "range_lookback": 120},
         True, "Min 48h trend"),

        ({"ema_cross_lookback": 120, "ema_cross_min": 2, "min_trend_hours": 168,
          "max_range_pct": 999, "range_lookback": 120},
         True, "Min 1wk trend"),

        ({"ema_cross_lookback": 120, "ema_cross_min": 2, "min_trend_hours": 336,
          "max_range_pct": 999, "range_lookback": 120},
         True, "Min 2wk trend"),

        # More crosses required
        ({"ema_cross_lookback": 168, "ema_cross_min": 3, "min_trend_hours": 168,
          "max_range_pct": 999, "range_lookback": 168},
         True, "3 crosses + 1wk wait"),

        ({"ema_cross_lookback": 240, "ema_cross_min": 4, "min_trend_hours": 168,
          "max_range_pct": 999, "range_lookback": 240},
         True, "4 crosses + 1wk wait"),

        # Price range filter: only restart if price consolidated within X%
        ({"ema_cross_lookback": 168, "ema_cross_min": 3, "min_trend_hours": 168,
          "max_range_pct": 8.0, "range_lookback": 168},
         True, "3 cross + 1wk + 8% range"),

        ({"ema_cross_lookback": 168, "ema_cross_min": 3, "min_trend_hours": 168,
          "max_range_pct": 6.0, "range_lookback": 168},
         True, "3 cross + 1wk + 6% range"),

        ({"ema_cross_lookback": 240, "ema_cross_min": 4, "min_trend_hours": 336,
          "max_range_pct": 6.0, "range_lookback": 240},
         True, "4 cross + 2wk + 6% range"),

        # Very sticky: long minimum stay + tight range
        ({"ema_cross_lookback": 336, "ema_cross_min": 4, "min_trend_hours": 504,
          "max_range_pct": 6.0, "range_lookback": 336},
         True, "4 cross + 3wk + 6% range"),

        # Nuclear: stay in trend at least a month
        ({"ema_cross_lookback": 336, "ema_cross_min": 5, "min_trend_hours": 720,
          "max_range_pct": 5.0, "range_lookback": 336},
         True, "5 cross + 30d + 5% range"),
    ]

    for start_date, end_date, period_label in periods:
        sub = df[(df["dt"] >= start_date) & (df["dt"] < end_date)].reset_index(drop=True)
        if len(sub) < 200:
            continue

        first_px = sub["close"].iloc[0]; last_px = sub["close"].iloc[-1]
        pct_chg = (last_px / first_px - 1) * 100
        b = run_baselines(sub)

        print(f"\n{'='*140}")
        print(f"  {period_label}  |  ETH: ${first_px:,.0f} → ${last_px:,.0f} ({pct_chg:+.1f}%)"
              f"  |  HODL: ${b['hodl_mtm']:+,.0f}  |  {len(sub):,} candles")
        print(f"{'='*140}")

        hdr = (f"{'Strategy':<35} {'Grid%':>5} {'Trnd%':>5} "
               f"{'Grid MTM':>10} {'Fut PnL':>10} {'FutFee':>8} {'TrndPnL':>10} "
               f"{'Bot MTM':>10} {'vs HODL':>10} {'Sharpe':>7} {'MaxDD':>7} {'Rstr':>5}")
        print(hdr)
        print("-" * len(hdr))

        # Baselines
        print(f"{'HODL':<35} {'--':>5} {'--':>5} "
              f"{'--':>10} {'--':>10} {'--':>8} {'--':>10} "
              f"${b['hodl_mtm']:>+9,.0f} ${'0':>9} {'--':>7} {'--':>7} {'--':>5}")
        print(f"{'Pure Momentum':<35} {'0':>4}% {'100':>4}% "
              f"{'--':>10} {'--':>10} {'--':>8} {'--':>10} "
              f"${b['momentum']['bot_mtm']:>+9,.0f} ${b['momentum']['alpha']:>+9,.0f} "
              f"{b['momentum']['sharpe']:>+7.2f} {b['momentum']['max_dd']:>6.1%} {'--':>5}")
        print("-" * len(hdr))

        results = []
        for rp, taper, lbl in configs:
            np.random.seed(42)
            r = run_hybrid_dn(sub, range_params=rp, taper=taper, label=lbl)
            results.append(r)
            print(f"{r['label']:<35} {r['grid_time']:>4.0f}% {r['trend_time']:>4.0f}% "
                  f"${r['grid_mtm']:>+9,.0f} ${r['futures_pnl']:>+9,.0f} "
                  f"${r['futures_fees']:>+7,.0f} ${r['trend_pnl']:>+9,.0f} "
                  f"${r['bot_mtm']:>+9,.0f} ${r['alpha']:>+9,.0f} "
                  f"{r['sharpe']:>+7.2f} {r['max_dd']:>6.1%} {r['grid_restarts']:>5}")

        best = max(results, key=lambda r: r["alpha"])
        print(f"\n  >>> BEST: {best['label']}  |  Alpha: ${best['alpha']:+,.0f}  "
              f"|  Sharpe: {best['sharpe']:+.2f}  |  MaxDD: {best['max_dd']:.1%}  "
              f"|  Grid restarts: {best['grid_restarts']}")


if __name__ == "__main__":
    main()
