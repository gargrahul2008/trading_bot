#!/usr/bin/env python3
"""
Dynamic bias-free basket selection for permanent grid backtest.

Usage:
    env/bin/python permanent_grid/select_basket_2023.py --start 2023-01-01
    env/bin/python permanent_grid/select_basket_2023.py --start 2025-01-01
    env/bin/python permanent_grid/select_basket_2023.py --start 2024-06-01 --lookback 2

For any start date:
1. Computes Nifty50 composition at that date (reverses index changes)
2. Fetches pre-start data for correlation/volatility analysis
3. Selects 10-stock diversified basket
4. Fetches full data for ALL universe stocks (for dynamic replacement in backtest)
"""

import sys, os, json, pickle, time, argparse
sys.path.insert(0, "/root/trading_bot")

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from common.broker.fyers_client import FyersClient
from permanent_grid.universe import (
    MASTER_STOCKS as _UNIVERSE_MASTER,
    INDEX_CHANGES as _UNIVERSE_CHANGES,
    CURRENT_NIFTY50 as _UNIVERSE_CURRENT,
    get_nifty50_at_date as _universe_get_nifty50,
)

# ── Master stock list: every stock that was in Nifty50 during 2020-2025 ──
# NOTE: canonical source is now permanent_grid/universe.py.
# This local copy is kept for backward compatibility.

MASTER_STOCKS = {
    "ADANIENT":   {"fyers": "NSE:ADANIENT-EQ",   "sector": "Metals & Mining"},
    "ADANIPORTS": {"fyers": "NSE:ADANIPORTS-EQ",  "sector": "Services"},
    "APOLLOHOSP": {"fyers": "NSE:APOLLOHOSP-EQ",  "sector": "Healthcare"},
    "ASIANPAINT": {"fyers": "NSE:ASIANPAINT-EQ",  "sector": "Consumer Durables"},
    "AXISBANK":   {"fyers": "NSE:AXISBANK-EQ",    "sector": "Financial Services"},
    "BAJAJ-AUTO": {"fyers": "NSE:BAJAJ-AUTO-EQ",  "sector": "Automobile"},
    "BAJAJFINSV": {"fyers": "NSE:BAJAJFINSV-EQ",  "sector": "Financial Services"},
    "BAJFINANCE": {"fyers": "NSE:BAJFINANCE-EQ",  "sector": "Financial Services"},
    "BEL":        {"fyers": "NSE:BEL-EQ",         "sector": "Capital Goods"},
    "BHARTIARTL": {"fyers": "NSE:BHARTIARTL-EQ",  "sector": "Telecommunication"},
    "BPCL":       {"fyers": "NSE:BPCL-EQ",        "sector": "Oil Gas & Fuels"},
    "BRITANNIA":  {"fyers": "NSE:BRITANNIA-EQ",   "sector": "FMCG"},
    "CIPLA":      {"fyers": "NSE:CIPLA-EQ",       "sector": "Healthcare"},
    "COALINDIA":  {"fyers": "NSE:COALINDIA-EQ",   "sector": "Oil Gas & Fuels"},
    "DIVISLAB":   {"fyers": "NSE:DIVISLAB-EQ",    "sector": "Healthcare"},
    "DRREDDY":    {"fyers": "NSE:DRREDDY-EQ",     "sector": "Healthcare"},
    "EICHERMOT":  {"fyers": "NSE:EICHERMOT-EQ",   "sector": "Automobile"},
    "ETERNAL":    {"fyers": "NSE:ETERNAL-EQ",     "sector": "Consumer Services"},
    "GRASIM":     {"fyers": "NSE:GRASIM-EQ",      "sector": "Construction Materials"},
    "HCLTECH":    {"fyers": "NSE:HCLTECH-EQ",     "sector": "Information Technology"},
    "HDFCBANK":   {"fyers": "NSE:HDFCBANK-EQ",    "sector": "Financial Services"},
    "HDFCLIFE":   {"fyers": "NSE:HDFCLIFE-EQ",    "sector": "Financial Services"},
    "HEROMOTOCO": {"fyers": "NSE:HEROMOTOCO-EQ",  "sector": "Automobile"},
    "HINDALCO":   {"fyers": "NSE:HINDALCO-EQ",    "sector": "Metals & Mining"},
    "HINDUNILVR": {"fyers": "NSE:HINDUNILVR-EQ",  "sector": "FMCG"},
    "ICICIBANK":  {"fyers": "NSE:ICICIBANK-EQ",   "sector": "Financial Services"},
    "INDIGO":     {"fyers": "NSE:INDIGO-EQ",      "sector": "Services"},
    "INDUSINDBK": {"fyers": "NSE:INDUSINDBK-EQ",  "sector": "Financial Services"},
    "INFY":       {"fyers": "NSE:INFY-EQ",        "sector": "Information Technology"},
    "ITC":        {"fyers": "NSE:ITC-EQ",          "sector": "FMCG"},
    "JIOFIN":     {"fyers": "NSE:JIOFIN-EQ",      "sector": "Financial Services"},
    "JSWSTEEL":   {"fyers": "NSE:JSWSTEEL-EQ",    "sector": "Metals & Mining"},
    "KOTAKBANK":  {"fyers": "NSE:KOTAKBANK-EQ",   "sector": "Financial Services"},
    "LT":         {"fyers": "NSE:LT-EQ",          "sector": "Construction"},
    "LTIM":       {"fyers": "NSE:LTIM-EQ",        "sector": "Information Technology"},
    "M&M":        {"fyers": "NSE:M&M-EQ",         "sector": "Automobile"},
    "MARUTI":     {"fyers": "NSE:MARUTI-EQ",      "sector": "Automobile"},
    "MAXHEALTH":  {"fyers": "NSE:MAXHEALTH-EQ",   "sector": "Healthcare"},
    "NESTLEIND":  {"fyers": "NSE:NESTLEIND-EQ",   "sector": "FMCG"},
    "NTPC":       {"fyers": "NSE:NTPC-EQ",        "sector": "Power"},
    "ONGC":       {"fyers": "NSE:ONGC-EQ",        "sector": "Oil Gas & Fuels"},
    "POWERGRID":  {"fyers": "NSE:POWERGRID-EQ",   "sector": "Power"},
    "RELIANCE":   {"fyers": "NSE:RELIANCE-EQ",    "sector": "Oil Gas & Fuels"},
    "SBILIFE":    {"fyers": "NSE:SBILIFE-EQ",     "sector": "Financial Services"},
    "SBIN":       {"fyers": "NSE:SBIN-EQ",        "sector": "Financial Services"},
    "SHRIRAMFIN": {"fyers": "NSE:SHRIRAMFIN-EQ",  "sector": "Financial Services"},
    "SUNPHARMA":  {"fyers": "NSE:SUNPHARMA-EQ",   "sector": "Healthcare"},
    "TATACONSUM": {"fyers": "NSE:TATACONSUM-EQ",  "sector": "FMCG"},
    "TATAMOTORS": {"fyers": "NSE:TMPV-EQ",        "sector": "Automobile"},
    "TATASTEEL":  {"fyers": "NSE:TATASTEEL-EQ",   "sector": "Metals & Mining"},
    "TCS":        {"fyers": "NSE:TCS-EQ",         "sector": "Information Technology"},
    "TECHM":      {"fyers": "NSE:TECHM-EQ",       "sector": "Information Technology"},
    "TITAN":      {"fyers": "NSE:TITAN-EQ",       "sector": "Consumer Durables"},
    "TRENT":      {"fyers": "NSE:TRENT-EQ",       "sector": "Consumer Services"},
    "ULTRACEMCO": {"fyers": "NSE:ULTRACEMCO-EQ",  "sector": "Construction Materials"},
    "UPL":        {"fyers": "NSE:UPL-EQ",         "sector": "Chemicals"},
    "WIPRO":      {"fyers": "NSE:WIPRO-EQ",       "sector": "Information Technology"},
}

# ── Nifty50 current composition (Dec 2025) ──

CURRENT_NIFTY50 = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "INDIGO", "INFY", "ITC",
    "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT", "M&M",
    "MARUTI", "MAXHEALTH", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
}

# ── Index change history (effective_date, added, removed) ──
# Sorted chronologically. Used to compute composition at any past date.

INDEX_CHANGES = [
    ("2023-07-13", ["LTIM"],              ["HDFC"]),
    ("2024-03-28", ["SHRIRAMFIN"],        ["UPL"]),
    ("2024-09-30", ["BEL", "TRENT"],      ["LTIM", "DIVISLAB"]),
    ("2025-03-28", ["JIOFIN", "ETERNAL"], ["BPCL", "BRITANNIA"]),
    ("2025-09-30", ["INDIGO", "MAXHEALTH"], ["HEROMOTOCO", "INDUSINDBK"]),
]

# Stocks to exclude: HDFC merged Jul 2023 (announced Apr 2022)
EXCLUDE_ALWAYS = {"HDFC"}

NIFTY_SYMBOL = "NSE:NIFTY50-INDEX"
FYERS_DAILY_MAX_DAYS = 365
OUTPUT_DIR = "/root/trading_bot/permanent_grid/data"


def get_nifty50_at_date(date_str: str) -> set:
    """Compute Nifty50 composition at a given date by reversing future index changes."""
    target = datetime.strptime(date_str, "%Y-%m-%d")
    members = set(CURRENT_NIFTY50)

    for change_date_str, added, removed in reversed(INDEX_CHANGES):
        change_date = datetime.strptime(change_date_str, "%Y-%m-%d")
        if change_date > target:
            for sym in added:
                members.discard(sym)
            for sym in removed:
                members.add(sym)

    members -= EXCLUDE_ALWAYS
    return members


def get_broker():
    with open("/root/trading_bot/fyers_auth.json") as f:
        auth = json.load(f)
    user = auth["users"]["user1"]
    return FyersClient(client_id=user["client_id"], access_token=user["access_token"])


def fetch_daily(broker, symbol: str, start_str: str, end_str: str) -> pd.DataFrame:
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")

    all_candles = []
    chunk_end = end_dt
    chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_DAILY_MAX_DAYS))

    while chunk_start < chunk_end:
        data = {
            "symbol": symbol,
            "resolution": "D",
            "date_format": "1",
            "range_from": chunk_start.strftime("%Y-%m-%d"),
            "range_to": chunk_end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        }
        resp = broker.history(data)
        candles = resp.get("candles") or []
        if candles:
            all_candles.extend(candles)

        chunk_end = chunk_start - timedelta(days=1)
        chunk_start = max(start_dt, chunk_end - timedelta(days=FYERS_DAILY_MAX_DAYS))
        if chunk_start < chunk_end:
            time.sleep(0.3)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.normalize()
    return df


def run_selection(daily_data: dict, universe: list) -> list:
    """Run correlation + volatility analysis and select 10-stock diversified basket."""

    sym_info = {}
    for s in universe:
        fsym = s["fyers_symbol"]
        sym_info[fsym] = {"name": s["symbol"], "sector": s["sector"]}

    returns = {}
    for fsym, df in daily_data.items():
        if fsym not in sym_info:
            continue
        df = df.sort_values("date")
        if len(df) < 200:
            print(f"  SKIP {sym_info[fsym]['name']}: only {len(df)} days (need 200+)")
            continue
        ret = df["close"].pct_change().dropna()
        ret.index = df["date"].iloc[1:].values
        returns[sym_info[fsym]["name"]] = ret

    ret_df = pd.DataFrame(returns).dropna()
    print(f"\nReturns matrix: {ret_df.shape[0]} days x {ret_df.shape[1]} stocks")

    corr = ret_df.corr()
    avg_corr = corr.mean().sort_values()
    vol = ret_df.std() * np.sqrt(252) * 100

    print(f"\n{'='*70}")
    print(f"  GRID CANDIDATE SCORES")
    print(f"{'='*70}")

    vol_norm = (vol - vol.min()) / (vol.max() - vol.min())
    corr_norm = (avg_corr - avg_corr.min()) / (avg_corr.max() - avg_corr.min())
    grid_score = (vol_norm * 0.6 - corr_norm * 0.4).sort_values(ascending=False)

    print(f"\n  {'Stock':15s} {'Score':>7s} {'Vol%':>7s} {'AvgCorr':>8s} {'Sector'}")
    print(f"  {'-'*60}")
    for name, gs in grid_score.head(20).items():
        fsym = [k for k, v in sym_info.items() if v["name"] == name][0]
        sector = sym_info[fsym]["sector"]
        print(f"  {name:15s} {gs:>7.3f} {vol[name]:>6.1f}% {avg_corr[name]:>8.3f} {sector}")

    # Greedy diversified basket: max 10, unique sectors, pairwise corr < 0.50
    print(f"\n{'='*70}")
    print(f"  SELECTED BASKET (greedy: high score, unique sectors, corr < 0.50)")
    print(f"{'='*70}")

    candidates = grid_score.index.tolist()
    basket = []
    basket_sectors = set()

    for name in candidates:
        if len(basket) >= 10:
            break
        fsym = [k for k, v in sym_info.items() if v["name"] == name][0]
        sector = sym_info[fsym]["sector"]

        if sector in basket_sectors:
            continue

        too_correlated = False
        for existing in basket:
            if corr.loc[name, existing] > 0.50:
                too_correlated = True
                break
        if too_correlated:
            continue

        basket.append(name)
        basket_sectors.add(sector)
        print(f"  {len(basket):2d}. {name:15s}  vol={vol[name]:.1f}%  avg_corr={avg_corr[name]:.3f}  sector={sector}")

    basket_corr = corr.loc[basket, basket]
    pair_corrs = []
    for i, a in enumerate(basket):
        for b in basket[i + 1:]:
            pair_corrs.append(corr.loc[a, b])

    print(f"\n  Avg pairwise correlation: {np.mean(pair_corrs):.3f}")
    print(f"  Max pairwise correlation: {np.max(pair_corrs):.3f}")

    selected = []
    for name in basket:
        fsym = [k for k, v in sym_info.items() if v["name"] == name][0]
        sector = sym_info[fsym]["sector"]
        selected.append({
            "symbol": name,
            "fyers_symbol": fsym,
            "sector": sector,
            "grid_score": round(float(grid_score[name]), 4),
            "volatility": round(float(vol[name]), 2),
            "avg_correlation": round(float(avg_corr[name]), 3),
        })

    return selected


def main():
    parser = argparse.ArgumentParser(description="Dynamic Basket Selection for Permanent Grid")
    parser.add_argument("--start", type=str, required=True,
                        help="Backtest start date YYYY-MM-DD (selects Nifty50 composition at this date)")
    parser.add_argument("--end", type=str, default="2026-06-03",
                        help="Backtest end date YYYY-MM-DD")
    parser.add_argument("--lookback", type=int, default=3,
                        help="Years of pre-start data for scoring (default: 3)")
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end, "%Y-%m-%d")
    sel_end = start_dt - timedelta(days=1)
    sel_start = sel_end - timedelta(days=args.lookback * 365 + 30)
    sel_start_str = sel_start.strftime("%Y-%m-%d")
    sel_end_str = sel_end.strftime("%Y-%m-%d")

    # Full data start: lookback + backtest (for support analysis at start)
    data_start_str = sel_start_str

    broker = get_broker()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── PHASE 1: Determine universe at start date ──
    universe_syms = get_nifty50_at_date(args.start)
    universe = []
    for sym in sorted(universe_syms):
        info = MASTER_STOCKS.get(sym)
        if info:
            universe.append({"symbol": sym, "fyers_symbol": info["fyers"], "sector": info["sector"]})

    print(f"{'='*70}")
    print(f"  Nifty50 composition at {args.start}: {len(universe)} stocks")
    print(f"  Selection data: {sel_start_str} to {sel_end_str} ({args.lookback} yr lookback)")
    print(f"  Backtest data:  {args.start} to {args.end}")
    print(f"{'='*70}")

    # ── PHASE 2: Fetch selection-period data ──
    cache_path = os.path.join(OUTPUT_DIR, f"universe_{sel_start_str}_{sel_end_str}.pkl")
    if os.path.exists(cache_path):
        print(f"\n  Loading cached selection data from {cache_path}...")
        with open(cache_path, "rb") as f:
            cached = pickle.load(f)
        selection_data = cached["daily_data"]
        cached_fsyms = set(selection_data.keys())
        needed_fsyms = {s["fyers_symbol"] for s in universe}
        missing = needed_fsyms - cached_fsyms
    else:
        selection_data = {}
        missing = {s["fyers_symbol"] for s in universe}

    if missing:
        print(f"\n  Fetching selection data for {len(missing)} stocks...")
        for item in universe:
            if item["fyers_symbol"] not in missing:
                continue
            fsym = item["fyers_symbol"]
            print(f"    {item['symbol']:15s}...", end="", flush=True)
            try:
                df = fetch_daily(broker, fsym, sel_start_str, sel_end_str)
                if not df.empty:
                    selection_data[fsym] = df
                    print(f"  {len(df)} days")
                else:
                    print(f"  NO DATA")
            except Exception as e:
                print(f"  ERROR: {e}")
            time.sleep(0.4)

        with open(cache_path, "wb") as f:
            pickle.dump({"universe": universe, "daily_data": selection_data}, f)
        print(f"  Cached to {cache_path}")
    else:
        print(f"  All {len(cached_fsyms)} stocks cached")

    # ── PHASE 3: Run basket selection ──
    print(f"\n{'='*70}")
    print(f"  PHASE 3: Basket selection using {sel_start_str} to {sel_end_str} data")
    print(f"{'='*70}")

    selected_basket = run_selection(selection_data, universe)

    if len(selected_basket) < 10:
        print(f"\n  WARNING: Only {len(selected_basket)} stocks selected (target: 10)")

    # ── PHASE 4: Fetch full data for ALL universe stocks ──
    # Backtest needs all 50 for dynamic stock replacement
    print(f"\n{'='*70}")
    print(f"  PHASE 4: Fetching full data ({data_start_str} to {args.end})")
    print(f"  All {len(universe)} universe stocks (for dynamic replacement)")
    print(f"{'='*70}")

    # Nifty50 index
    print(f"  Nifty50 index...", end="", flush=True)
    nifty_df = fetch_daily(broker, NIFTY_SYMBOL, args.start, args.end)
    print(f"  {len(nifty_df)} days" if not nifty_df.empty else "  FAILED")
    time.sleep(0.5)

    all_daily_data = {}
    for item in universe:
        fsym = item["fyers_symbol"]
        print(f"  {item['symbol']:15s}...", end="", flush=True)
        try:
            df = fetch_daily(broker, fsym, data_start_str, args.end)
            if not df.empty:
                all_daily_data[fsym] = df
                n_bt = len(df[df["date"] >= args.start])
                print(f"  {len(df)} total, {n_bt} backtest days")
            else:
                print(f"  NO DATA")
        except Exception as e:
            print(f"  ERROR: {e}")
        time.sleep(0.4)

    # Build initial basket daily_data (backward compat)
    basket_data = {}
    for item in selected_basket:
        fsym = item["fyers_symbol"]
        if fsym in all_daily_data:
            basket_data[fsym] = all_daily_data[fsym]

    # ── Save ──
    result = {
        "basket": selected_basket,
        "daily_data": basket_data,
        "universe": universe,
        "universe_data": all_daily_data,
        "nifty_data": nifty_df,
        "start_date": args.start,
        "end_date": args.end,
        "selection_period": f"{sel_start_str} to {sel_end_str}",
        "selection_method": "greedy diversified (high vol + low corr, unique sectors, pairwise < 0.50)",
    }

    out_path = os.path.join(OUTPUT_DIR, "basket_3y.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(result, f)

    print(f"\n{'='*70}")
    print(f"  DONE — saved to {out_path}")
    print(f"{'='*70}")
    print(f"  Universe:         {len(universe)} stocks (Nifty50 at {args.start})")
    print(f"  Selection period: {sel_start_str} to {sel_end_str}")
    print(f"  Backtest period:  {args.start} to {args.end}")
    print(f"  Initial basket:   {len(selected_basket)} stocks")
    print(f"  Universe data:    {len(all_daily_data)} stocks (for dynamic replacement)")
    for i, s in enumerate(selected_basket):
        print(f"    {i + 1:2d}. {s['symbol']:15s}  {s['sector']:30s}  vol={s['volatility']:.1f}%  corr={s['avg_correlation']:.3f}")


if __name__ == "__main__":
    main()
