"""
Hybrid Grid (Delta-Neutral) + Trend backtest — v3
Adds volatility filter: high vol → prefer hedged grid, low vol trend → momentum
Three regimes: grid (hedged), trend (unhedged momentum), wait (flat/hold)
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
    """Annualized realized volatility from hourly log returns."""
    out = np.full_like(close, np.nan, dtype=float)
    log_ret = np.diff(np.log(close))
    for i in range(window, len(close)):
        out[i] = np.std(log_ret[i - window:i]) * np.sqrt(8760)  # annualize hourly
    return out


def compute_atr_pct(high, low, close, period):
    """ATR as percentage of price."""
    n = len(close)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i] - low[i],
                     abs(high[i] - close[i-1]),
                     abs(low[i] - close[i-1]))
    atr = np.full(n, np.nan)
    atr[period] = np.mean(tr[1:period+1])
    k = 2.0 / (period + 1)
    for i in range(period + 1, n):
        atr[i] = tr[i] * k + atr[i-1] * (1 - k)
    return atr / close * 100  # as percentage


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


def run_strategy(df, params, label=""):
    """
    params dict:
      # Range detection (trend → grid)
      ema_cross_lookback, ema_cross_min, min_trend_hours,
      max_range_pct, range_lookback
      # Volatility filter
      vol_window: lookback for realized vol (hours)
      vol_high_thresh: above this → "high vol" → prefer grid
      vol_low_thresh: below this → "low vol" → prefer trend
      vol_mode: 'realized' or 'atr'
      # What to do in high-vol trend
      high_vol_action: 'grid' = switch to hedged grid
                       'flat' = go flat (close trend, no grid)
                       'reduce' = reduce trend position to 25%
    """
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)

    ema50 = compute_ema(close, MOMENTUM_EMA)

    # Volatility
    vol_window = params.get("vol_window", 168)
    vol_high = params.get("vol_high_thresh", 999)
    vol_low = params.get("vol_low_thresh", 0)
    vol_mode = params.get("vol_mode", "realized")
    high_vol_action = params.get("high_vol_action", "grid")

    if vol_mode == "atr":
        vol = compute_atr_pct(high, low, close, vol_window)
    else:
        vol = compute_realized_vol(close, vol_window)

    # Range params
    ema_cross_lookback = params.get("ema_cross_lookback", 168)
    ema_cross_min = params.get("ema_cross_min", 3)
    min_trend_hours = params.get("min_trend_hours", 168)
    max_range_pct_val = params.get("max_range_pct", 6.0)
    range_lookback = params.get("range_lookback", 168)

    # Portfolio
    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / close[0]

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

    def close_hedge_fn(qty_to_close, entry_avg, price):
        nonlocal futures_short_qty, futures_pnl, futures_fees_paid, futures_margin_locked, cash
        if qty_to_close <= 0 or futures_short_qty <= 0:
            return
        qty_to_close = min(qty_to_close, futures_short_qty)
        pnl = qty_to_close * (entry_avg - price)
        fee = qty_to_close * price * FUTURES_FEE_PCT / 100.0
        futures_pnl += pnl
        futures_fees_paid += fee
        cash += pnl - fee
        futures_short_qty -= qty_to_close
        if futures_short_qty < QTY_STEP:
            futures_short_qty = 0.0
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    # State
    mode = "grid"  # 'grid', 'trend', 'flat'
    ref = close[0]
    grid_start_ref = ref
    steps_up = 0; steps_down = 0
    trend_entered_at = 0
    trend_pos = 0.0; trend_entry_px = 0.0; trend_stop_px = 0.0

    grid_pnl_usdc = 0.0; grid_pnl_eth = 0.0
    trend_pnl_total = 0.0
    grid_trades = 0; trend_trades = 0; grid_restarts = 0
    vol_switches = 0
    regime_log = []
    snapshots = []

    # Initial hedge
    open_hedge(eth, close[0])

    for i in range(1, n):
        price = close[i]
        regime_log.append(mode)
        cur_vol = vol[i] if not np.isnan(vol[i]) else 0

        # Funding
        if futures_short_qty > 0 and i % 8 == 0:
            fa = futures_short_qty * price * FUNDING_RATE_8H / 100.0
            if np.random.random() < FUNDING_POSITIVE_PCT:
                funding_received += fa; cash += fa
            else:
                funding_received -= fa; cash -= fa

        # ═══ VOLATILITY CHECK (only in trend mode) ═══
        if mode == "trend" and cur_vol > 0:
            if cur_vol > vol_high:
                # High vol detected — act based on config
                if high_vol_action == "grid":
                    # Close trend, switch to hedged grid
                    if trend_pos > 0:
                        cash += trend_pos * price
                        trend_pnl_total += trend_pos * (price - trend_entry_px)
                        trend_trades += 1
                        trend_pos = 0.0
                    mode = "grid"
                    ref = price; grid_start_ref = price
                    steps_up = 0; steps_down = 0
                    hedge_entry_price = price
                    open_hedge(eth, price)
                    vol_switches += 1

                elif high_vol_action == "flat":
                    # Go flat — close everything
                    if trend_pos > 0:
                        cash += trend_pos * price
                        trend_pnl_total += trend_pos * (price - trend_entry_px)
                        trend_trades += 1
                        trend_pos = 0.0
                    mode = "flat"
                    vol_switches += 1

                elif high_vol_action == "reduce":
                    # Reduce trend position to 25%
                    if trend_pos > 0:
                        reduce_qty = _round_qty(trend_pos * 0.75)
                        if reduce_qty > 0:
                            cash += reduce_qty * price
                            trend_pnl_total += reduce_qty * (price - trend_entry_px)
                            trend_pos -= reduce_qty
                            trend_trades += 1

        # ═══ FLAT MODE — wait for vol to drop ═══
        if mode == "flat":
            if cur_vol > 0 and cur_vol < vol_low:
                # Vol dropped, go back to trend
                mode = "trend"
                trend_entered_at = i
            elif cur_vol > 0 and cur_vol < vol_high:
                # Vol normalizing, check if range for grid
                crosses = count_ema_crosses(close, ema50, i, ema_cross_lookback)
                pr = price_range_pct(close, i, range_lookback)
                if crosses >= ema_cross_min and pr <= max_range_pct_val:
                    mode = "grid"
                    ref = price; grid_start_ref = price
                    steps_up = 0; steps_down = 0
                    hedge_entry_price = price
                    open_hedge(eth, price)
                    grid_restarts += 1

        # ═══ GRID MODE ═══
        elif mode == "grid":
            sell_lvl = ref * (1.0 + pct_frac)
            buy_lvl  = ref * (1.0 - pct_frac)

            # Vol drops below low threshold → switch to trend
            if cur_vol > 0 and cur_vol < vol_low:
                if futures_short_qty > 0:
                    close_hedge_fn(futures_short_qty, hedge_entry_price, price)
                mode = "trend"
                trend_entered_at = i
                vol_switches += 1
            elif price >= sell_lvl:
                taper_mult = max(0.1, 1.0 - TAPER_FACTOR * steps_up)
                qty = _qty(sell_lvl, SELL_QUOTE * taper_mult)
                qty = min(qty, eth)
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

        # ═══ TREND MODE ═══
        elif mode == "trend":
            want_long = not np.isnan(ema50[i]) and price > ema50[i]

            if want_long and trend_pos == 0:
                pv = cash + eth * price
                alloc = min(cash * 0.9, pv * 0.5)
                qty = _round_qty(alloc / price)
                if qty > 0:
                    cash -= qty * price; trend_pos = qty
                    trend_entry_px = price
                    trend_stop_px = price * (1 - TREND_STOP_PCT)
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

            # Range detection → restart grid
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
                    hedge_entry_price = price
                    open_hedge(eth, price)
                    grid_restarts += 1

        # Snapshot
        if i % 24 == 0 or i == n - 1:
            te = eth + trend_pos
            fu = futures_short_qty * (hedge_entry_price - price) if futures_short_qty > 0 else 0
            snapshots.append({"idx": i, "price": price, "pv": cash + te * price + fu, "mode": mode})

    # Final
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
        "label": label,
        "grid_mtm": grid_mtm, "trend_pnl": trend_pnl_total,
        "futures_pnl": futures_pnl, "futures_fees": futures_fees_paid,
        "funding": funding_received,
        "bot_mtm": bot_mtm, "alpha": alpha, "hodl_mtm": hodl_mtm,
        "sharpe": sharpe, "max_dd": max_dd,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts, "vol_switches": vol_switches,
        "grid_time": grid_time, "trend_time": trend_time, "flat_time": flat_time,
        "final_pv": final_pv,
    }


def run_baselines(df):
    close = df["close"].values; n = len(close)
    first_px = close[0]; last_px = close[-1]
    ema50 = compute_ema(close, MOMENTUM_EMA)
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY

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
    ms = snap["r"].mean()/snap["r"].std()*np.sqrt(365) if snap["r"].std()>0 else 0
    md = ((snap["pv"]-snap["pv"].cummax())/snap["pv"].cummax()).min()
    return {"hodl_mtm": hodl_mtm,
            "momentum": {"bot_mtm": mom_pv-TOTAL_EQUITY, "alpha": (mom_pv-TOTAL_EQUITY)-hodl_mtm,
                          "sharpe": ms, "max_dd": md}}


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

    # Base range params (from v2 winner)
    base_range = {
        "ema_cross_lookback": 168, "ema_cross_min": 3,
        "min_trend_hours": 168, "max_range_pct": 6.0, "range_lookback": 168,
    }

    configs = [
        # v2 winner — no vol filter (baseline)
        ({**base_range, "vol_high_thresh": 999, "vol_low_thresh": 0,
          "vol_window": 168, "vol_mode": "realized", "high_vol_action": "grid"},
         "v2 best (no vol filter)"),

        # Realized vol: switch to grid when vol > X
        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "grid"},
         "RVol>80%→grid, <50%→trend"),

        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.60, "vol_low_thresh": 0.40, "high_vol_action": "grid"},
         "RVol>60%→grid, <40%→trend"),

        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 1.00, "vol_low_thresh": 0.60, "high_vol_action": "grid"},
         "RVol>100%→grid, <60%→trend"),

        # Go flat instead of grid during high vol
        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "flat"},
         "RVol>80%→flat, <50%→trend"),

        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.60, "vol_low_thresh": 0.40, "high_vol_action": "flat"},
         "RVol>60%→flat, <40%→trend"),

        # Reduce position instead
        ({**base_range, "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "reduce"},
         "RVol>80%→reduce, <50%→trend"),

        # ATR-based vol
        ({**base_range, "vol_window": 168, "vol_mode": "atr",
          "vol_high_thresh": 1.5, "vol_low_thresh": 0.8, "high_vol_action": "grid"},
         "ATR>1.5%→grid, <0.8%→trend"),

        ({**base_range, "vol_window": 168, "vol_mode": "atr",
          "vol_high_thresh": 2.0, "vol_low_thresh": 1.0, "high_vol_action": "grid"},
         "ATR>2.0%→grid, <1.0%→trend"),

        ({**base_range, "vol_window": 168, "vol_mode": "atr",
          "vol_high_thresh": 1.5, "vol_low_thresh": 0.8, "high_vol_action": "flat"},
         "ATR>1.5%→flat, <0.8%→trend"),

        # Shorter vol lookback (more responsive)
        ({**base_range, "vol_window": 72, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "grid"},
         "RVol72h>80%→grid, <50%→trnd"),

        ({**base_range, "vol_window": 72, "vol_mode": "realized",
          "vol_high_thresh": 0.60, "vol_low_thresh": 0.40, "high_vol_action": "grid"},
         "RVol72h>60%→grid, <40%→trnd"),

        # Sticky trend + vol: longer min trend, tighter vol
        ({**base_range, "min_trend_hours": 336,
          "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "grid"},
         "2wk+RVol>80%→grid"),

        ({**base_range, "min_trend_hours": 336,
          "vol_window": 168, "vol_mode": "realized",
          "vol_high_thresh": 0.80, "vol_low_thresh": 0.50, "high_vol_action": "flat"},
         "2wk+RVol>80%→flat"),
    ]

    for start_date, end_date, period_label in periods:
        sub = df[(df["dt"] >= start_date) & (df["dt"] < end_date)].reset_index(drop=True)
        if len(sub) < 200:
            continue

        first_px = sub["close"].iloc[0]; last_px = sub["close"].iloc[-1]
        pct_chg = (last_px / first_px - 1) * 100
        b = run_baselines(sub)

        print(f"\n{'='*150}")
        print(f"  {period_label}  |  ETH: ${first_px:,.0f} → ${last_px:,.0f} ({pct_chg:+.1f}%)"
              f"  |  HODL: ${b['hodl_mtm']:+,.0f}  |  {len(sub):,} candles")
        print(f"{'='*150}")

        hdr = (f"{'Strategy':<35} {'Grid%':>5} {'Trnd%':>5} {'Flat%':>5} "
               f"{'GridMTM':>9} {'FutPnL':>9} {'TrndPnL':>9} "
               f"{'BotMTM':>10} {'vsHODL':>10} {'Sharpe':>7} {'MaxDD':>7} "
               f"{'Rstr':>4} {'VSw':>4}")
        print(hdr)
        print("-" * len(hdr))

        print(f"{'HODL':<35} {'--':>5} {'--':>5} {'--':>5} "
              f"{'--':>9} {'--':>9} {'--':>9} "
              f"${b['hodl_mtm']:>+8,.0f} ${'0':>9} {'--':>7} {'--':>7} {'--':>4} {'--':>4}")
        print(f"{'Pure Momentum':<35} {'0':>4}% {'100':>4}% {'0':>4}% "
              f"{'--':>9} {'--':>9} {'--':>9} "
              f"${b['momentum']['bot_mtm']:>+8,.0f} ${b['momentum']['alpha']:>+9,.0f} "
              f"{b['momentum']['sharpe']:>+7.2f} {b['momentum']['max_dd']:>6.1%} {'--':>4} {'--':>4}")
        print("-" * len(hdr))

        results = []
        for p, lbl in configs:
            np.random.seed(42)
            r = run_strategy(sub, p, lbl)
            results.append(r)
            print(f"{r['label']:<35} {r['grid_time']:>4.0f}% {r['trend_time']:>4.0f}% {r['flat_time']:>4.0f}% "
                  f"${r['grid_mtm']:>+8,.0f} ${r['futures_pnl']:>+8,.0f} ${r['trend_pnl']:>+8,.0f} "
                  f"${r['bot_mtm']:>+8,.0f} ${r['alpha']:>+9,.0f} "
                  f"{r['sharpe']:>+7.2f} {r['max_dd']:>6.1%} {r['grid_restarts']:>4} {r['vol_switches']:>4}")

        best = max(results, key=lambda r: r["alpha"])
        print(f"\n  >>> BEST: {best['label']}  |  Alpha: ${best['alpha']:+,.0f}  "
              f"|  Sharpe: {best['sharpe']:+.2f}  |  MaxDD: {best['max_dd']:.1%}")


if __name__ == "__main__":
    main()
