"""
Hybrid Grid (Delta-Neutral) + Trend backtest
- Grid mode: hedge ETH with short futures → capture pure spread
- Trend mode: remove hedge, follow momentum
- Range detection: EMA crosses to switch back to grid
- Tapered grid: sell less going up, buy less going down
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
TOTAL_EQUITY   = 100_000.0
GRID_PCT       = 0.40          # 0.4% per step
GRID_RANGE_PCT = 4.0           # 4% each side = 10 steps max
BUY_QUOTE      = 5_000.0       # base quote per grid trade
SELL_QUOTE     = 5_000.0
QTY_STEP       = 0.001
TAPER_FACTOR   = 0.10          # each step reduces qty by 10%

# Futures costs
FUTURES_FEE_PCT   = 0.02       # 0.02% maker fee on futures (each side)
FUNDING_RATE_8H   = 0.005      # 0.005% per 8h average funding (shorts receive in contango)
FUNDING_POSITIVE_PCT = 0.60    # 60% of time funding is positive (shorts receive)
FUTURES_MARGIN_PCT = 0.10      # 10x leverage → 10% margin required

# Trend settings
MOMENTUM_EMA   = 50
TREND_STOP_PCT = 0.03          # 3% trailing stop

# Range detection
EMA_CROSS_LOOKBACK = 120       # hours to look back
EMA_CROSS_MIN      = 2         # min crosses to declare range

# Grid restart
LIMITED_GRID_STEPS = 3

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


def run_hybrid_dn(df, mode="as_is", trend_active=True, taper=True,
                   delta_neutral=True, label=""):
    """
    Delta-neutral hybrid: grid with futures hedge + trend following.

    mode: 'full_rebal', 'limited', 'as_is' — grid restart mode
    trend_active: actively trade momentum vs just hold
    taper: reduce qty at successive grid steps
    delta_neutral: hedge grid ETH with short futures
    """
    close = df["close"].values
    n = len(close)
    pct_frac = GRID_PCT / 100.0
    range_frac = GRID_RANGE_PCT / 100.0
    max_steps = int(GRID_RANGE_PCT / GRID_PCT)

    ema50 = compute_ema(close, MOMENTUM_EMA)

    # ── Portfolio ──
    cash = TOTAL_EQUITY / 2.0
    eth  = (TOTAL_EQUITY / 2.0) / close[0]

    # ── Futures hedge state ──
    # futures_short_qty: ETH shorted via futures (positive = short)
    # futures_entry_prices: list of (qty, entry_price) for PnL tracking
    futures_short_qty = 0.0
    futures_pnl = 0.0          # realized futures PnL
    futures_unrealized = 0.0   # unrealized futures PnL
    futures_fees_paid = 0.0    # total futures fees
    funding_received = 0.0     # total funding income
    futures_margin_locked = 0.0  # cash locked as margin
    last_futures_price = close[0]

    # ── Grid state ──
    ref = close[0]
    grid_active = True
    grid_start_ref = ref
    steps_up = 0
    steps_down = 0
    current_max_steps = max_steps

    # ── Trend state ──
    trend_mode_on = False
    trend_pos = 0.0
    trend_entry_px = 0.0
    trend_stop_px = 0.0

    # ── Tracking ──
    grid_pnl_usdc = 0.0; grid_pnl_eth = 0.0
    trend_pnl_total = 0.0
    grid_trades = 0; trend_trades = 0; grid_restarts = 0
    regime_log = []
    snapshots = []

    # ── Helper: open/adjust hedge ──
    def open_hedge(qty_to_hedge, price):
        nonlocal futures_short_qty, futures_fees_paid, futures_margin_locked, cash
        if qty_to_hedge <= 0:
            return
        fee = qty_to_hedge * price * FUTURES_FEE_PCT / 100.0
        futures_fees_paid += fee
        cash -= fee  # pay fee from cash
        futures_short_qty += qty_to_hedge
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    def close_hedge(qty_to_close, entry_avg_price, price):
        nonlocal futures_short_qty, futures_pnl, futures_fees_paid, futures_margin_locked, cash
        if qty_to_close <= 0 or futures_short_qty <= 0:
            return
        qty_to_close = min(qty_to_close, futures_short_qty)
        # Short PnL: profit when price goes down
        pnl = qty_to_close * (entry_avg_price - price)
        fee = qty_to_close * price * FUTURES_FEE_PCT / 100.0
        futures_pnl += pnl
        futures_fees_paid += fee
        cash += pnl - fee  # receive PnL minus fee
        futures_short_qty -= qty_to_close
        if futures_short_qty < QTY_STEP:
            futures_short_qty = 0.0
        futures_margin_locked = futures_short_qty * price * FUTURES_MARGIN_PCT

    # Initial hedge if delta-neutral
    hedge_entry_price = close[0]
    if delta_neutral:
        open_hedge(eth, close[0])
        hedge_entry_price = close[0]

    for i in range(1, n):
        price = close[i]
        regime = "grid" if grid_active else ("trend" if trend_mode_on else "wait")
        regime_log.append(regime)

        # ── Funding payment every 8 hours ──
        if delta_neutral and futures_short_qty > 0 and i % 8 == 0:
            # Funding: positive rate = longs pay shorts
            # We model average behavior: shorts receive FUNDING_POSITIVE_PCT of time
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

                    # Adjust hedge: sold ETH, reduce short
                    if delta_neutral and futures_short_qty > 0:
                        close_hedge(qty, hedge_entry_price, sell_lvl)

                move_from_start = (ref - grid_start_ref) / grid_start_ref
                if steps_up >= current_max_steps or move_from_start >= range_frac:
                    # Exit grid → trend mode
                    if delta_neutral and futures_short_qty > 0:
                        close_hedge(futures_short_qty, hedge_entry_price, price)
                    grid_active = False
                    trend_mode_on = True

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

                    # Adjust hedge: bought ETH, increase short
                    if delta_neutral:
                        open_hedge(qty, buy_lvl)
                        # Update weighted average entry
                        if futures_short_qty > 0:
                            hedge_entry_price = (
                                (hedge_entry_price * (futures_short_qty - qty) + buy_lvl * qty)
                                / futures_short_qty
                            ) if futures_short_qty > qty else buy_lvl

                move_from_start = (grid_start_ref - ref) / grid_start_ref
                if steps_down >= current_max_steps or move_from_start >= range_frac:
                    if delta_neutral and futures_short_qty > 0:
                        close_hedge(futures_short_qty, hedge_entry_price, price)
                    grid_active = False
                    trend_mode_on = True

        # ═══ TREND MODE ═══
        elif trend_mode_on:
            if trend_active:
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

            # Check for range → restart grid
            crosses = count_ema_crosses(close, ema50, i, EMA_CROSS_LOOKBACK)
            if crosses >= EMA_CROSS_MIN:
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

                if mode == "full_rebal":
                    pv = cash + eth * price
                    target_cash = pv / 2.0
                    target_eth  = (pv / 2.0) / price
                    cash = target_cash
                    eth  = target_eth
                    current_max_steps = max_steps
                elif mode == "limited":
                    current_max_steps = LIMITED_GRID_STEPS
                elif mode == "as_is":
                    current_max_steps = max_steps

                # Re-establish hedge
                if delta_neutral:
                    hedge_entry_price = price
                    open_hedge(eth, price)

        # Snapshot
        if i % 24 == 0 or i == n - 1:
            total_eth = eth + trend_pos
            # Futures unrealized PnL
            fut_unreal = futures_short_qty * (hedge_entry_price - price) if futures_short_qty > 0 else 0
            spot_pv = cash + total_eth * price
            total_pv = spot_pv + fut_unreal
            snapshots.append({
                "idx": i, "price": price,
                "cash": cash, "eth": total_eth,
                "futures_short": futures_short_qty,
                "spot_pv": spot_pv,
                "pv": total_pv,
                "regime": regime,
            })

    # ── Final accounting ──
    if trend_pos > 0:
        trend_pnl_total += trend_pos * (close[-1] - trend_entry_px)
    # Close any remaining hedge
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
        dd = (snap["pv"] - peak) / peak
        max_dd = dd.min()

    grid_time = regime_log.count("grid") / len(regime_log) * 100 if regime_log else 0
    trend_time = regime_log.count("trend") / len(regime_log) * 100 if regime_log else 0

    return {
        "label": label,
        "grid_mtm": grid_mtm, "trend_pnl": trend_pnl_total,
        "futures_pnl": futures_pnl, "futures_fees": futures_fees_paid,
        "funding_income": funding_received,
        "bot_mtm": bot_mtm, "alpha": alpha,
        "sharpe": sharpe, "max_dd": max_dd,
        "grid_trades": grid_trades, "trend_trades": trend_trades,
        "grid_restarts": grid_restarts,
        "grid_time": grid_time, "trend_time": trend_time,
        "final_pv": final_pv, "final_cash": cash, "final_eth": total_eth_end,
    }


def run_baselines(df):
    """HODL, blind grid (no rebal), pure momentum."""
    close = df["close"].values
    n = len(close)
    first_px = close[0]; last_px = close[-1]
    ema50 = compute_ema(close, MOMENTUM_EMA)
    hodl_pv = TOTAL_EQUITY / 2 + (TOTAL_EQUITY / 2 / first_px) * last_px
    hodl_mtm = hodl_pv - TOTAL_EQUITY

    # Pure momentum
    cash = TOTAL_EQUITY / 2.0
    eth = (TOTAL_EQUITY / 2.0) / first_px
    tp = 0.0; te = 0.0; ts = 0.0; tpnl = 0.0
    snaps = []
    for i in range(1, n):
        p = close[i]
        wl = not np.isnan(ema50[i]) and p > ema50[i]
        if wl and tp == 0:
            pv = cash + eth * p
            alloc = min(cash * 0.9, pv * 0.5)
            q = _round_qty(alloc / p)
            if q > 0: cash -= q*p; tp = q; te = p; ts = p*(1-TREND_STOP_PCT)
        elif not wl and tp > 0:
            cash += tp*p; tpnl += tp*(p-te); tp = 0
        elif tp > 0:
            ts = max(ts, p*(1-TREND_STOP_PCT))
            if p <= ts: cash += tp*p; tpnl += tp*(p-te); tp = 0
        if i % 24 == 0 or i == n-1:
            snaps.append({"pv": cash + (eth+tp)*p})
    if tp > 0: tpnl += tp*(last_px-te)
    mom_pv = cash + (eth+tp)*last_px
    snap = pd.DataFrame(snaps); snap["r"] = snap["pv"].pct_change()
    mom_sharpe = snap["r"].mean()/snap["r"].std()*np.sqrt(365) if snap["r"].std()>0 else 0
    mom_dd = ((snap["pv"]-snap["pv"].cummax())/snap["pv"].cummax()).min()

    # Blind grid no rebal
    pct_frac = GRID_PCT / 100.0
    cash2 = TOTAL_EQUITY/2; eth2 = (TOTAL_EQUITY/2)/first_px; ref2 = first_px; gt = 0
    snaps2 = []
    for i in range(1, n):
        p = close[i]
        sl = ref2*(1+pct_frac); bl = ref2*(1-pct_frac)
        if p >= sl:
            q = min(_qty(sl, SELL_QUOTE), eth2)
            if q>0: cash2+=q*sl; eth2-=q; ref2=sl; gt+=1
        elif p <= bl:
            q=_qty(bl,BUY_QUOTE); c=q*bl
            if q>0 and c<=cash2: cash2-=c; eth2+=q; ref2=bl; gt+=1
        if i%24==0 or i==n-1: snaps2.append({"pv":cash2+eth2*p})
    grid_pv = cash2+eth2*last_px
    snap2 = pd.DataFrame(snaps2); snap2["r"]=snap2["pv"].pct_change()
    g_sharpe = snap2["r"].mean()/snap2["r"].std()*np.sqrt(365) if snap2["r"].std()>0 else 0
    g_dd = ((snap2["pv"]-snap2["pv"].cummax())/snap2["pv"].cummax()).min()

    return {
        "hodl_mtm": hodl_mtm,
        "momentum": {"bot_mtm": mom_pv-TOTAL_EQUITY, "alpha": (mom_pv-TOTAL_EQUITY)-hodl_mtm,
                      "sharpe": mom_sharpe, "max_dd": mom_dd},
        "blind_grid": {"bot_mtm": grid_pv-TOTAL_EQUITY, "alpha": (grid_pv-TOTAL_EQUITY)-hodl_mtm,
                        "sharpe": g_sharpe, "max_dd": g_dd, "trades": gt},
    }


def main():
    print("Loading data...", flush=True)
    df = pd.read_parquet(DATA_FILE)
    df["dt"] = pd.to_datetime(df["ts"])
    print(f"Data: {df['dt'].iloc[0].date()} to {df['dt'].iloc[-1].date()}, {len(df):,} candles")
    print(f"Config: {GRID_PCT}% grid, ${BUY_QUOTE:,.0f} step, {GRID_RANGE_PCT}% range, "
          f"futures fee {FUTURES_FEE_PCT}%, funding {FUNDING_RATE_8H}%/8h\n")

    # Use fixed seed for reproducible funding
    np.random.seed(42)

    periods = [
        ("2024-01-01", "2026-05-19", "FULL (2024-2026)"),
        ("2024-01-01", "2024-07-01", "2024 H1 (bull)"),
        ("2024-07-01", "2025-01-01", "2024 H2 (volatile)"),
        ("2025-01-01", "2025-07-01", "2025 H1 (crash)"),
        ("2025-07-01", "2026-01-01", "2025 H2 (rally)"),
        ("2026-01-01", "2026-05-19", "2026 YTD (bear)"),
    ]

    for start_date, end_date, period_label in periods:
        np.random.seed(42)  # reset seed per period for consistency
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

        hdr = (f"{'Strategy':<38} {'Grid%':>5} {'Trnd%':>5} "
               f"{'Grid MTM':>10} {'Fut PnL':>10} {'FutFee':>8} {'Funding':>8} {'TrndPnL':>10} "
               f"{'Bot MTM':>10} {'vs HODL':>10} {'Sharpe':>7} {'MaxDD':>7}")
        print(hdr)
        print("-" * len(hdr))

        # Baselines
        print(f"{'HODL':<38} {'--':>5} {'--':>5} "
              f"{'--':>10} {'--':>10} {'--':>8} {'--':>8} {'--':>10} "
              f"${b['hodl_mtm']:>+9,.0f} ${'0':>9} {'--':>7} {'--':>7}")
        print(f"{'Blind Grid (no rebal, no hedge)':<38} {'100':>4}% {'0':>4}% "
              f"{'--':>10} {'--':>10} {'--':>8} {'--':>8} {'--':>10} "
              f"${b['blind_grid']['bot_mtm']:>+9,.0f} ${b['blind_grid']['alpha']:>+9,.0f} "
              f"{b['blind_grid']['sharpe']:>+7.2f} {b['blind_grid']['max_dd']:>6.1%}")
        print(f"{'Pure Momentum (no grid)':<38} {'0':>4}% {'100':>4}% "
              f"{'--':>10} {'--':>10} {'--':>8} {'--':>8} {'--':>10} "
              f"${b['momentum']['bot_mtm']:>+9,.0f} ${b['momentum']['alpha']:>+9,.0f} "
              f"{b['momentum']['sharpe']:>+7.2f} {b['momentum']['max_dd']:>6.1%}")
        print("-" * len(hdr))

        # Hybrid DN strategies
        configs = [
            # (mode, trend_active, taper, delta_neutral, label)
            ("as_is", True,  True,  True,  "DN Grid + Trend + Taper"),
            ("as_is", True,  False, True,  "DN Grid + Trend (no taper)"),
            ("as_is", False, True,  True,  "DN Grid + Hold + Taper"),
            ("limited", True,  True,  True,  "DN Limited + Trend + Taper"),
            ("limited", True,  False, True,  "DN Limited + Trend"),
            ("limited", False, True,  True,  "DN Limited + Hold + Taper"),
            ("full_rebal", True,  True,  True,  "DN Rebal50 + Trend + Taper"),
            ("full_rebal", True,  False, True,  "DN Rebal50 + Trend"),
            # Compare: same hybrid WITHOUT delta-neutral
            ("as_is", True,  True,  False, "Grid + Trend + Taper (NO hedge)"),
            ("limited", True,  True,  False, "Limited + Trend + Taper (NO hedge)"),
        ]

        for m, ta, tp, dn, lbl in configs:
            np.random.seed(42)
            r = run_hybrid_dn(sub, mode=m, trend_active=ta, taper=tp,
                               delta_neutral=dn, label=lbl)
            fut_pnl_str = f"${r['futures_pnl']:>+9,.0f}" if dn else f"{'--':>10}"
            fut_fee_str = f"${r['futures_fees']:>+7,.0f}" if dn else f"{'--':>8}"
            fund_str = f"${r['funding_income']:>+7,.0f}" if dn else f"{'--':>8}"
            print(f"{r['label']:<38} {r['grid_time']:>4.0f}% {r['trend_time']:>4.0f}% "
                  f"${r['grid_mtm']:>+9,.0f} {fut_pnl_str} {fut_fee_str} {fund_str} "
                  f"${r['trend_pnl']:>+9,.0f} "
                  f"${r['bot_mtm']:>+9,.0f} ${r['alpha']:>+9,.0f} "
                  f"{r['sharpe']:>+7.2f} {r['max_dd']:>6.1%}")

        print()


if __name__ == "__main__":
    main()
