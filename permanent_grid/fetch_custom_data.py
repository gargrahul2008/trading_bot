#!/usr/bin/env python3
"""
fetch_custom_data.py — fetch daily + 5-min history for ANY list of stocks
(not limited to the Nifty50 master). Saves a self-contained pickle that
run_custom_basket.py / the notebook can backtest directly.

The pickle is keyed by FYERS symbol (e.g. "NSE:KPITTECH-EQ"), so no name
mangling is needed and non-Nifty50 names work out of the box. The Nifty50
index 5-min series (needed for market-state) is reused from master_data.pkl
if present, otherwise fetched.

Usage:
    # comma-separated fyers symbols
    env/bin/python permanent_grid/fetch_custom_data.py \
        --symbols NSE:KPITTECH-EQ,NSE:PERSISTENT-EQ,NSE:CDSL-EQ

    # or a file with one symbol per line (blank lines / # comments ignored)
    env/bin/python permanent_grid/fetch_custom_data.py --file my_stocks.txt

    # options
    --out   permanent_grid/data/custom_data.pkl   (default)
    --daily-start 2019-01-01   --min5-start 2021-01-01   --end 2026-06-08
    --resume   (skip symbols already in the output pickle)
"""
import sys, os, json, pickle, time, argparse
sys.path.insert(0, "/root/trading_bot")

from datetime import datetime
from permanent_grid.fetch_master_data import (
    get_broker, fetch_candles, DAILY_CHUNK_DAYS, MIN5_CHUNK_DAYS,
)
from permanent_grid.universe import NIFTY_SYMBOL

DEFAULT_OUT = "/root/trading_bot/permanent_grid/data/custom_data.pkl"
MASTER_PKL  = "/root/trading_bot/permanent_grid/data/master_data.pkl"


def parse_symbols(args) -> list[str]:
    syms: list[str] = []
    if args.symbols:
        syms += [s.strip() for s in args.symbols.split(",") if s.strip()]
    if args.file:
        with open(args.file) as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if line:
                    syms.append(line)
    # normalise: bare names -> NSE:NAME-EQ
    out = []
    for s in syms:
        s = s.upper()
        if ":" not in s:
            s = f"NSE:{s}-EQ"
        out.append(s)
    # de-dup, preserve order
    seen, uniq = set(), []
    for s in out:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq


def load_nifty_5min():
    """Reuse the Nifty50 5-min series from master_data.pkl if available."""
    if os.path.exists(MASTER_PKL):
        try:
            m = pickle.load(open(MASTER_PKL, "rb"))
            if m.get("nifty_5min") is not None:
                return m["nifty_5min"]
        except Exception as e:
            print(f"  (could not reuse nifty_5min from master: {e})")
    return None


def main():
    ap = argparse.ArgumentParser(description="Fetch data for an arbitrary stock list")
    ap.add_argument("--symbols", help="comma-separated fyers symbols (or bare names)")
    ap.add_argument("--file", help="file with one symbol per line")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--daily-start", default="2019-01-01")
    ap.add_argument("--min5-start", default="2021-01-01")
    ap.add_argument("--end", default="2026-06-08")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    symbols = parse_symbols(args)
    if not symbols:
        ap.error("provide --symbols or --file")

    data = {"daily": {}, "min5": {}, "nifty_5min": None,
            "sectors": {}, "symbols": [],
            "daily_range": (args.daily_start, args.end),
            "min5_range": (args.min5_start, args.end)}

    if args.resume and os.path.exists(args.out):
        data = pickle.load(open(args.out, "rb"))
        print(f"Resuming: {len(data.get('daily', {}))} daily already present")

    broker = get_broker()

    # Nifty index 5-min (market-state input)
    if data.get("nifty_5min") is None:
        data["nifty_5min"] = load_nifty_5min()
        if data["nifty_5min"] is None:
            print("Fetching Nifty50 index (5-min)...")
            data["nifty_5min"] = fetch_candles(broker, NIFTY_SYMBOL, "5",
                                               args.min5_start, args.end, MIN5_CHUNK_DAYS)

    print(f"\n{'='*64}\n  Custom fetch: {len(symbols)} symbols\n"
          f"  daily {args.daily_start}→{args.end} | 5-min {args.min5_start}→{args.end}\n{'='*64}\n")

    for i, fsym in enumerate(symbols, 1):
        name = fsym.split(":", 1)[-1].replace("-EQ", "")
        data["sectors"][fsym] = data["sectors"].get(fsym, "Custom")
        if fsym not in data["symbols"]:
            data["symbols"].append(fsym)

        need_d = fsym not in data["daily"]
        need_5 = fsym not in data["min5"]
        if not need_d and not need_5:
            continue
        print(f"[{i}/{len(symbols)}] {name} ({fsym})")

        if need_d:
            df = fetch_candles(broker, fsym, "D", args.daily_start, args.end, DAILY_CHUNK_DAYS)
            if not df.empty:
                data["daily"][fsym] = df
                print(f"    daily: {len(df)} bars ({df['date'].min().date()} → {df['date'].max().date()})")
            else:
                print("    daily: NO DATA — check the symbol spelling")
            time.sleep(0.4)

        if need_5:
            df = fetch_candles(broker, fsym, "5", args.min5_start, args.end, MIN5_CHUNK_DAYS)
            if not df.empty:
                data["min5"][fsym] = df
                print(f"    5-min: {len(df)} candles, {df['date'].nunique()} days")
            else:
                print("    5-min: NO DATA")
            time.sleep(0.4)

        if i % 5 == 0:
            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            pickle.dump(data, open(args.out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
            print(f"    --- checkpoint: {len(data['daily'])} daily, {len(data['min5'])} min5 ---")

    data["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    pickle.dump(data, open(args.out, "wb"), protocol=pickle.HIGHEST_PROTOCOL)

    no_daily = [s for s in symbols if s not in data["daily"]]
    print(f"\n{'='*64}\n  DONE → {args.out}")
    print(f"  daily: {len(data['daily'])} | 5-min: {len(data['min5'])}")
    if no_daily:
        print(f"  NO DATA for: {', '.join(no_daily)}  (bad symbol or not on Fyers)")
    print(f"  size: {os.path.getsize(args.out)/1024/1024:.1f} MB\n{'='*64}")


if __name__ == "__main__":
    main()
