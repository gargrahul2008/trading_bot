#!/usr/bin/env python3
"""
Daily performance comparison: live trading vs backtest expectations.

Detects the start of the current config period automatically (first proactive fill),
reconstructs the initial state at that point, runs the backtest for the same window,
and reports key efficiency metrics.

Usage:
    python scripts/mexc_backtest_compare.py --config strategies/pct_ladder/config.mexc.json
    python scripts/mexc_backtest_compare.py --config ... --since 2026-04-29
    python scripts/mexc_backtest_compare.py --config ... --telegram
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest.fetch_data import fetch_klines, dt_to_ms
from backtest.proactive_engine import ProactiveBacktestEngine


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_fills(trades_path: str) -> list:
    fills = []
    try:
        with open(trades_path) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("event") == "FILL" and float(r.get("qty", 0)) > 0:
                        fills.append(r)
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    return sorted(fills, key=lambda x: x.get("ts", ""))


def detect_config_start(fills: list) -> str | None:
    """Return date (YYYY-MM-DD) of the first proactive fill, else first fill date."""
    for f in fills:
        if re.match(r"pro_(buy|sell)", f.get("reason", "")):
            return f["ts"][:10]
    return fills[0]["ts"][:10] if fills else None


def reconstruct_initial_state(all_fills: list, since_ts: str, cur_eth: float, cur_cash: float):
    """
    Backward-reconstruct (cash, eth) at `since_ts` by undoing all fills
    that occurred at or after that timestamp.

    ETH reconstruction is exact (0-fee MEXC spot).
    Cash at since_ts is derived from the first fill in the period.
    """
    after = [f for f in all_fills if f.get("ts", "") >= since_ts]

    # ETH: undo each fill after since_ts
    eth = cur_eth
    for f in reversed(after):
        qty = float(f["qty"])
        if f["side"] == "BUY":
            eth -= qty
        else:
            eth += qty

    # Cash: back out the first fill after since_ts
    if after:
        ff = after[0]
        if ff["side"] == "BUY":
            cash = float(ff["cash_after"]) + float(ff["qty"]) * float(ff["price"])
        else:
            cash = float(ff["cash_after"]) - float(ff["qty"]) * float(ff["price"])
    else:
        cash = cur_cash

    return cash, eth


def estimate_avg_cost(all_fills: list, since_ts: str, fallback_price: float) -> float:
    """
    Estimate the avg cost of ETH held at since_ts by averaging all buy fills
    before that date (LIFO lots are complex to reconstruct; weighted avg is a
    reasonable approximation for the initial_eth_cost parameter).
    """
    buys = [f for f in all_fills if f.get("ts", "") < since_ts and f["side"] == "BUY"]
    if not buys:
        return fallback_price
    total_qty  = sum(float(f["qty"]) for f in buys)
    total_cost = sum(float(f["qty"]) * float(f["price"]) for f in buys)
    return total_cost / total_qty if total_qty > 0 else fallback_price


def compute_live_metrics(all_fills: list, window_ts: str | None = None) -> dict:
    """
    LIFO cycles and PnL.

    If window_ts is given (ISO timestamp), runs LIFO on ALL fills but only
    counts cycles where the SELL happened at or after window_ts.
    This correctly handles cross-boundary cycles (buy before window, sell inside).

    Triggers (buys/sells) are counted only within the window when window_ts is set.
    """
    is_rebal = lambda f: bool(re.search(r"rebalance", f.get("reason", ""), re.I))

    lifo_stack: list = []
    cycles   = 0
    lifo_pnl = 0.0

    for f in all_fills:
        qty    = float(f["qty"])
        price  = float(f["price"])
        in_win = window_ts is None or f.get("ts", "") >= window_ts
        f_reb  = is_rebal(f)

        if f["side"] == "BUY":
            lifo_stack.append([qty, price, f_reb])
        else:
            i = len(lifo_stack) - 1
            while i >= 0:
                if price > lifo_stack[i][1]:
                    take = min(qty, lifo_stack[i][0])
                    # Only count the cycle if the SELL is inside the window
                    if in_win and not (f_reb or lifo_stack[i][2]):
                        lifo_pnl += take * (price - lifo_stack[i][1])
                        cycles   += 1
                    lifo_stack[i][0] -= take
                    if lifo_stack[i][0] <= 0:
                        lifo_stack.pop(i)
                    break
                i -= 1

    # Triggers = fills within the window only
    window_fills = all_fills if window_ts is None else [f for f in all_fills if f.get("ts","") >= window_ts]
    grid_fills   = [f for f in window_fills if not is_rebal(f)]
    rebal_fills  = [f for f in window_fills if is_rebal(f)]

    return {
        "cycles":      cycles,
        "lifo_pnl":    lifo_pnl,
        "grid_buys":   sum(1 for f in grid_fills if f["side"] == "BUY"),
        "grid_sells":  sum(1 for f in grid_fills if f["side"] == "SELL"),
        "rebal_fills": len(rebal_fills),
        "total_fills": len(window_fills),
    }


def analyse_missed_cycles(fills: list, df, pct: float) -> dict:
    """
    Walk through gaps between consecutive live fills and count genuinely missed cycles.

    A "missed cycle" = a complete extra buy+sell (or sell+buy) round trip that MEXC price
    data shows should have happened inside a gap, BEYOND the first expected fill.
    The first simulated fill in each gap simply represents the expected next actual fill
    (not a miss), so it is excluded from the missed count.

    Returns dict with missed_cycles count and a list of gap details.
    """
    import pandas as pd

    missed_total = 0
    gap_details  = []

    for i in range(len(fills) - 1):
        f_cur   = fills[i]
        f_next  = fills[i + 1]
        gap_start = f_cur["ts"]
        gap_end   = f_next["ts"]
        ref_price = float(f_cur["price"])
        gap_mins  = (pd.Timestamp(gap_end) - pd.Timestamp(gap_start)).total_seconds() / 60

        gap_candles = df[
            (df["ts"] > pd.Timestamp(gap_start)) & (df["ts"] < pd.Timestamp(gap_end))
        ]
        if gap_candles.empty or gap_mins < 5:
            continue

        sim_side = "SELL" if f_cur["side"] == "BUY" else "BUY"
        sim_ref  = ref_price
        sim_fills: list = []

        for _, c in gap_candles.iterrows():
            lo, hi = float(c["low"]), float(c["high"])
            b1 = sim_ref * (1 - pct)
            s1 = sim_ref * (1 + pct)
            if sim_side == "BUY" and lo <= b1:
                sim_fills.append(("BUY", b1, str(c["ts"])[:16]))
                sim_ref  = b1
                sim_side = "SELL"
            elif sim_side == "SELL" and hi >= s1:
                sim_fills.append(("SELL", s1, str(c["ts"])[:16]))
                sim_ref  = s1
                sim_side = "BUY"

        # First simulated fill = the expected actual fill (not a miss).
        # Each subsequent complete pair (buy+sell or sell+buy) is one genuinely missed cycle.
        extra   = sim_fills[1:]
        missed  = len(extra) // 2
        if missed > 0:
            missed_total += missed
            gap_details.append({
                "gap_start":    gap_start[:19],
                "gap_mins":     round(gap_mins),
                "missed":       missed,
                "extra_fills":  extra,
            })

    return {"missed_cycles": missed_total, "gaps": gap_details}


def detect_effective_params(period_fills: list, strat: dict, ex: dict) -> dict:
    """
    Detect the grid parameters actually used during the period by inspecting
    fill reasons (e.g. 'pro_buy|ref-0.1%'). Falls back to config values.
    Returns dict with eff_pct, eff_buy_quote, eff_pro_levels.
    """
    # Extract pct from reason strings: 'pro_buy|ref-0.1%' or 'pro_sell|ref+0.2%'
    pct_vals = set()
    for f in period_fills:
        m = re.search(r'ref[+\-]([\d.]+)%', f.get("reason", ""))
        if m:
            pct_vals.add(float(m.group(1)))

    cfg_pct = float(strat["lower_pct"])
    eff_pct = min(pct_vals) if pct_vals else cfg_pct

    # If pct differs from config → weekend mode; pro_levels is 4
    if abs(eff_pct - cfg_pct) > 0.001:
        eff_pro_levels = 4
    else:
        eff_pro_levels = int(ex.get("pro_levels", 2))

    # Estimate buy_quote from actual fill sizes (banded_qty: qty ≈ buy_quote / band_mid(price))
    band_width = float(strat.get("band_width", 100))
    grid_buys = [f for f in period_fills
                 if f["side"] == "BUY" and not re.search(r"rebalance", f.get("reason",""), re.I)]
    if grid_buys:
        sample = grid_buys[:20]
        vals = []
        for f in sample:
            p = float(f["price"])
            bm = (p // band_width) * band_width + band_width / 2
            vals.append(float(f["qty"]) * bm)
        eff_buy_quote = round(sum(vals) / len(vals), 2)
    else:
        eff_buy_quote = float(strat["buy_quote"])

    return {"eff_pct": eff_pct, "eff_buy_quote": eff_buy_quote, "eff_pro_levels": eff_pro_levels}


def run_backtest(strat: dict, ex: dict, init_cash: float, init_eth: float,
                 init_eth_cost: float, df, mode: str = "realistic", seed: int = 42,
                 eff_params: dict | None = None, init_ref_price: float | None = None) -> dict:
    p = eff_params or {}
    eff_pct       = p.get("eff_pct",       float(strat["lower_pct"]))
    eff_buy_quote = p.get("eff_buy_quote",  float(strat["buy_quote"]))
    eff_pro_levels= p.get("eff_pro_levels", int(ex.get("pro_levels", 2)))

    from decimal import Decimal
    engine = ProactiveBacktestEngine(
        symbol            = strat["symbols"][0],
        initial_cash      = init_cash,
        initial_eth       = init_eth,
        initial_eth_cost  = init_eth_cost,
        lower_pct         = eff_pct,
        upper_pct         = eff_pct,
        buy_quote         = eff_buy_quote,
        sell_quote        = eff_buy_quote,
        band_width        = float(strat.get("band_width", 100)),
        qty_step          = float(strat.get("qty_step", 1e-6)),
        min_qty           = float(strat.get("min_qty",  1e-6)),
        pro_levels        = eff_pro_levels,
        quote_reserve     = float(ex.get("quote_reserve_usdt", 500)),
        rebalance_threshold_steps = int(strat.get("rebalance_threshold_steps", 1)),
        price_path_mode   = mode,
        seed              = seed,
    )
    # Seed the reference price from the last live fill before the window,
    # so BT grid levels start aligned with the live bot — not at candle open.
    if init_ref_price is not None:
        engine.reference_price = Decimal(str(init_ref_price))
    summary = engine.run(df)
    return summary, engine.trades


def eff(live_val: float, bt_val: float) -> str:
    """Format efficiency as percentage string."""
    if bt_val <= 0:
        return "  —  "
    pct = live_val / bt_val * 100
    return f"{pct:4.0f}%"


def format_report(since_date: str, end_date: str, days: float,
                  live: dict, bt: dict, strat: dict, ex: dict,
                  cur_cash: float, cur_eth: float, cur_price: float,
                  missed: dict | None = None) -> str:
    cycle_eff_pct  = live["cycles"] / bt["cycles"] * 100 if bt["cycles"] > 0 else 0
    missed_cycles  = (missed or {}).get("missed_cycles", 0)
    live_triggers  = live["grid_buys"] + live["grid_sells"]
    bt_triggers    = bt["ladder_buys"] + bt["ladder_sells"]

    eff_icon = "🟢" if cycle_eff_pct >= 90 else ("🟡" if cycle_eff_pct >= 60 else "🔴")
    missed_str = f"⚠️ {missed_cycles}" if missed_cycles > 0 else "✅ 0"

    lines = [
        f"📊 *24h Bot Report* ({since_date} → {end_date} UTC)",
        f"",
        f"{'Metric':<12} {'Live':>8} {'BT':>8}",
        f"{'─'*30}",
        f"{'LIFO PnL':<12} ${live['lifo_pnl']:>7,.0f} ${bt['lifo_cycle_pnl']:>7,.0f}",
        f"{'Triggers':<12} {live_triggers:>8,} {bt_triggers:>8,}",
        f"{'Cycles':<12} {live['cycles']:>8,} {bt['cycles']:>8,}",
        f"{'Misses':<12} {missed_str:>8}",
        f"{'─'*30}",
        f"Eff: {eff_icon} *{cycle_eff_pct:.0f}%*",
    ]

    return "\n".join(lines)


def send_telegram(text: str, secrets_path: str) -> None:
    try:
        import requests
        with open(secrets_path) as f:
            sec = json.load(f)
        token    = sec.get("token") or sec.get("bot_token")
        chat_ids = sec.get("chat_id") or sec.get("telegram_chat_id")
        if not token or not chat_ids:
            print("Telegram secrets missing token/chat_id")
            return
        # Support both single chat_id and list of chat_ids
        if not isinstance(chat_ids, list):
            chat_ids = [chat_ids]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        for chat_id in chat_ids:
            resp = requests.post(url, json={
                "chat_id":    chat_id,
                "text":       text,
                "parse_mode": "Markdown",
            }, timeout=10)
            if not resp.ok:
                print(f"Telegram error (chat {chat_id}): {resp.status_code} {resp.text[:200]}")
            else:
                msg_id = resp.json().get("result", {}).get("message_id", "?")
                print(f"Telegram message sent to {chat_id} (message_id={msg_id}).")
    except Exception as e:
        print(f"Failed to send Telegram: {e}")


def print_trade_comparison(period_fills: list, bt_trades: list) -> None:
    """
    Print a side-by-side timeline of live vs BT grid fills.

    Matching logic: a live fill and BT fill are paired if they are on the
    same side (BUY/SELL) and within the same 1-minute candle window.
    Unmatched fills show as LIVE-ONLY or BT-ONLY rows.
    """
    is_rebal = lambda r: bool(re.search(r"rebalance", r, re.I))
    IST_OFFSET = timedelta(hours=5, minutes=30)

    def to_ist(ts_str: str) -> str:
        """Convert any ISO/datetime string to IST minute-precision string."""
        ts = ts_str[:16].replace("T", " ")          # "2026-05-09 05:46"
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            dt_ist = dt + IST_OFFSET
            return dt_ist.strftime("%m-%d %H:%M")   # "05-09 11:16"
        except Exception:
            return ts[:11]

    # Strip to minute precision for grouping (keep UTC for key matching)
    def minute_bucket(ts_str: str) -> str:
        return ts_str[:16].replace("T", " ")

    # Collect live grid fills keyed by (minute_bucket, side)
    live_by_min: dict = {}
    for f in period_fills:
        if is_rebal(f.get("reason", "")):
            continue
        key = (minute_bucket(f["ts"]), f["side"])
        live_by_min.setdefault(key, []).append(f)

    # Collect BT grid fills keyed by (minute_bucket, side)
    bt_by_min: dict = {}
    for t in bt_trades:
        if is_rebal(t.get("reason", "")):
            continue
        key = (minute_bucket(t["ts"]), t["side"])
        bt_by_min.setdefault(key, []).append(t)

    all_keys = sorted(set(live_by_min) | set(bt_by_min))

    HDR  = f"{'Time (IST)':<14} {'LIVE':^30} {'BT':^30}  Status"
    SEP  = "─" * len(HDR)
    print()
    print("Trade-by-Trade Comparison (grid fills only, rebalances excluded)")
    print(SEP)
    print(HDR)
    print(SEP)

    live_only = bt_only = matched = 0

    for key in all_keys:
        minute, side = key
        live_fills = live_by_min.get(key, [])
        bt_fills   = bt_by_min.get(key, [])

        # Pair up fills in order; excess go to unmatched
        pairs = max(len(live_fills), len(bt_fills))
        for i in range(pairs):
            lf = live_fills[i] if i < len(live_fills) else None
            bf = bt_fills[i]   if i < len(bt_fills)   else None

            if lf:
                l_str = f"{lf['side']:4} {float(lf['qty']):.4f} @ {float(lf['price']):,.2f}"
            else:
                l_str = "—"

            if bf:
                b_str = f"{bf['side']:4} {float(bf['qty']):.4f} @ {float(bf['price']):,.2f}"
            else:
                b_str = "—"

            if lf and bf:
                status = "✅ match"
                matched += 1
            elif lf:
                status = "🔵 live only"
                live_only += 1
            else:
                status = "⬜ BT only"
                bt_only += 1

            ts_label = to_ist(minute) if i == 0 else ""
            print(f"{ts_label:<14} {l_str:<30} {b_str:<30}  {status}")

    print(SEP)
    print(f"Matched: {matched}  |  Live-only: {live_only}  |  BT-only: {bt_only}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Live vs backtest performance comparison")
    parser.add_argument("--config",   required=True,  help="Path to config.mexc.json")
    parser.add_argument("--telegram", action="store_true", help="Send report to Telegram")
    parser.add_argument("--secrets",  default="secrets/telegram.json", help="Telegram secrets file")
    parser.add_argument("--trades",   action="store_true", help="Print trade-by-trade live vs BT comparison")
    args = parser.parse_args()

    # ── Load config ──────────────────────────────────────────────────────────
    with open(args.config) as f:
        cfg = json.load(f)

    strat = cfg["strategy"]
    ex    = cfg["execution"]
    paths = cfg["paths"]

    base_dir    = os.path.dirname(os.path.abspath(args.config))
    state_path  = os.path.join(base_dir, paths["state_path"])
    trades_path = os.path.join(base_dir, paths["trades_path"])
    symbol      = strat["symbols"][0]

    # ── Load current state ───────────────────────────────────────────────────
    with open(state_path) as f:
        state = json.load(f)
    cur_eth   = float(state["extras"].get(f"broker_base_qty_{symbol}", 0))
    cur_cash  = float(state.get("cash", 0))
    cur_price = float((state.get("last_prices") or {}).get(symbol, 0))

    # ── Load all fills ───────────────────────────────────────────────────────
    all_fills = load_fills(trades_path)
    if not all_fills:
        print("No fills found.")
        return

    # ── 24h window ──────────────────────────────────────────────────────────
    now_utc      = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=24)
    window_ts    = window_start.isoformat()          # e.g. "2026-04-30T08:35:00+00:00"
    since_date   = window_start.strftime("%Y-%m-%d %H:%M")
    end_date     = now_utc.strftime("%Y-%m-%d %H:%M")
    days         = 1.0

    period_fills = [f for f in all_fills if f.get("ts", "") >= window_ts]
    if not period_fills:
        print("No fills in the last 24 hours.")
        return

    # ── Reconstruct initial state at start of 24h window ────────────────────
    init_cash, init_eth = reconstruct_initial_state(all_fills, window_ts, cur_eth, cur_cash)
    first_fill_price    = float(period_fills[0]["price"])
    init_eth_cost       = estimate_avg_cost(all_fills, window_ts,
                                            fallback_price=first_fill_price)

    # Reference price at window start = price of last fill BEFORE the window.
    # This aligns BT grid levels with where the live bot's grid was actually placed.
    prev_fills     = [f for f in all_fills if f.get("ts", "") < window_ts]
    init_ref_price = float(prev_fills[-1]["price"]) if prev_fills else first_fill_price

    print(f"Comparison period : last 24h ({since_date} UTC → {end_date} UTC)")
    print(f"Initial state     : cash=${init_cash:,.2f}  ETH={init_eth:.5f} @ ${init_eth_cost:.2f}")
    print(f"Period fills      : {len(period_fills)}")
    print()

    # ── Fetch price data for the last 24h ────────────────────────────────────
    cache_dir = os.path.join(os.path.dirname(__file__), "..", "backtest", "cache")
    start_ms  = int(window_start.timestamp() * 1000)
    end_ms    = int(now_utc.timestamp() * 1000)

    print(f"Fetching MEXC 1m data for last 24h …")
    df_mexc = fetch_klines(
        symbol    = symbol,
        interval  = "1m",
        start_ms  = start_ms,
        end_ms    = end_ms,
        cache_dir = cache_dir,
        source    = "mexc",
    )

    print(f"Fetching Binance 1m data for last 24h …")
    df_binance = fetch_klines(
        symbol    = symbol,
        interval  = "1m",
        start_ms  = start_ms,
        end_ms    = end_ms,
        cache_dir = cache_dir,
        source    = "binance",
    )

    import pandas as pd
    if not df_mexc.empty:
        df_mexc["ts"] = pd.to_datetime(df_mexc["ts"], utc=True)
        df_mexc = df_mexc.sort_values("ts").reset_index(drop=True)

    if df_mexc.empty and df_binance.empty:
        print("Failed to fetch price data from both MEXC and Binance.")
        return

    # Use last candle close as current price if state doesn't have it
    if cur_price <= 0:
        ref_df = df_mexc if not df_mexc.empty else df_binance
        cur_price = float(ref_df["close"].iloc[-1])

    # ── Run backtest (MEXC data + realistic mode = best live match) ──────────
    # ── Detect effective params from actual fills (handles weekend mode) ────────
    eff_params = detect_effective_params(period_fills, strat, ex)
    print(f"Effective params  : pct={eff_params['eff_pct']}%  "
          f"buy_quote=${eff_params['eff_buy_quote']:.2f}  "
          f"pro_levels={eff_params['eff_pro_levels']}"
          + (" [WEEKEND MODE]" if abs(eff_params['eff_pct'] - float(strat['lower_pct'])) > 0.001 else ""))

    df_bt = df_mexc if not df_mexc.empty else df_binance
    print(f"Running backtest (realistic mode, {'MEXC' if not df_mexc.empty else 'Binance'} data) …")
    print(f"Init ref price    : ${init_ref_price:.2f} (last fill before window)")
    bt, bt_trades = run_backtest(strat, ex, init_cash, init_eth, init_eth_cost, df_bt,
                                 mode="realistic", eff_params=eff_params, init_ref_price=init_ref_price)

    # ── Live metrics (LIFO runs on ALL fills; cycles counted only in window) ──
    live = compute_live_metrics(all_fills, window_ts=window_ts)

    # ── Missed cycle analysis (24h fills on MEXC data) ───────────────────────
    missed = None
    if not df_mexc.empty:
        print("Analysing missed cycles on MEXC data …")
        missed = analyse_missed_cycles(
            fills = period_fills,
            df    = df_mexc,
            pct   = eff_params["eff_pct"] / 100,
        )
        print(f"Missed cycles (genuine bot gaps): {missed['missed_cycles']}")

    # ── Format and output report ─────────────────────────────────────────────
    report = format_report(since_date, end_date, days, live, bt, strat, ex,
                           cur_cash, cur_eth, cur_price, missed=missed)
    print(report)

    if args.trades:
        print_trade_comparison(period_fills, bt_trades)

    if args.telegram:
        send_telegram(report, args.secrets)


if __name__ == "__main__":
    main()
