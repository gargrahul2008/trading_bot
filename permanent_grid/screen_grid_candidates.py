#!/usr/bin/env python3
"""
screen_grid_candidates.py — find grid-friendly stocks across the whole NSE.

Screens the entire liquid NSE universe (from daily bhavcopies — no Fyers token
needed) for the profile that suits a permanent grid: HIGH volatility + CHOPPY /
mean-reverting (not trending) + mild positive drift + liquid. Outputs a ranked
table and an uncorrelated shortlist of finalists to backtest.

Pipeline:
  1. download last N NSE bhavcopies  -> per-symbol daily OHLC + turnover
  2. liquidity gate: median daily turnover > --min-turnover-cr (proxies mcap)
  3. score: ann.vol, choppiness (1-EfficiencyRatio), Hurst, ATR%, drift, maxDD
  4. GridScore = weighted z-scores; rank
  5. greedily pick an uncorrelated shortlist (return-corr < --max-corr)

Usage:
  env/bin/python permanent_grid/screen_grid_candidates.py --days 250 \
      --min-turnover-cr 5 --top 30 --picks 4 --out permanent_grid/data/grid_candidates.csv
"""
import sys, os, io, time, argparse
sys.path.insert(0, "/root/trading_bot")
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
           "Accept": "text/csv,*/*"}
BHAV_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"
ETF_LIST_URL = "https://archives.nseindia.com/content/equities/eq_etfseclist.csv"
KEEP_SERIES = {"EQ"}        # normal-traded equities + ETFs (ETFs trade in EQ)


def fetch_etf_symbols() -> set:
    """NSE ETF master -> set of ETF symbols (so we can classify stock vs ETF)."""
    try:
        r = requests.get(ETF_LIST_URL, headers=HEADERS, timeout=20)
        df = pd.read_csv(io.StringIO(r.text))
        col = "Symbol" if "Symbol" in df.columns else df.columns[0]
        return set(df[col].astype(str).str.strip())
    except Exception as e:
        print(f"  (ETF list fetch failed: {e}; treating all as stocks)")
        return set()


def download_bhavcopies(n_days: int, cache_dir: str) -> pd.DataFrame:
    """Walk back calendar days, pull each available bhavcopy, return concatenated frame."""
    os.makedirs(cache_dir, exist_ok=True)
    frames, got, day = [], 0, datetime.now()
    sess = requests.Session(); sess.headers.update(HEADERS)
    tries = 0
    while got < n_days and tries < n_days * 2 + 40:
        tries += 1
        if day.weekday() < 5:                      # skip weekends
            dd = day.strftime("%d%m%Y")
            cache = os.path.join(cache_dir, f"bhav_{dd}.csv")
            text = None
            if os.path.exists(cache):
                text = open(cache).read()
            else:
                try:
                    r = sess.get(BHAV_URL.format(ddmmyyyy=dd), timeout=20)
                    if r.status_code == 200 and len(r.text) > 500:
                        text = r.text
                        open(cache, "w").write(text)
                    time.sleep(0.25)
                except Exception:
                    pass
            if text:
                df = pd.read_csv(io.StringIO(text))
                df.columns = [c.strip() for c in df.columns]
                df = df[["SYMBOL", "SERIES", "DATE1", "OPEN_PRICE", "HIGH_PRICE",
                         "LOW_PRICE", "CLOSE_PRICE", "TURNOVER_LACS"]].copy()
                df["SYMBOL"] = df["SYMBOL"].str.strip()
                df["SERIES"] = df["SERIES"].str.strip()
                frames.append(df)
                got += 1
                if got % 25 == 0:
                    print(f"  ...{got}/{n_days} bhavcopies")
        day -= timedelta(days=1)
    if not frames:
        raise RuntimeError("No bhavcopies downloaded — check NSE connectivity.")
    print(f"  downloaded {got} trading days")
    return pd.concat(frames, ignore_index=True)


def hurst(prices: np.ndarray) -> float:
    """Lag-variance Hurst proxy. <0.5 mean-reverting, >0.5 trending."""
    p = np.log(prices[prices > 0])
    if len(p) < 40:
        return np.nan
    lags = range(2, 20)
    tau = []
    for lag in lags:
        d = p[lag:] - p[:-lag]
        tau.append(np.std(d) if len(d) else np.nan)
    tau = np.array(tau)
    ok = tau > 0
    if ok.sum() < 5:
        return np.nan
    return float(np.polyfit(np.log(np.array(list(lags))[ok]), np.log(tau[ok]), 1)[0])


def adjust_corporate_actions(c, h, l, o=None):
    """Back-adjust raw prices for splits/bonuses. A single-day move beyond circuit
    limits (< -35% or > +60%) is not a market move — it's a corporate action.
    Splice the series by multiplying all PRIOR bars by the gap ratio so the line
    is continuous. Returns adjusted (close, high, low) and the #events found."""
    c = c.astype(float).copy(); h = h.astype(float).copy(); l = l.astype(float).copy()
    events = 0
    for i in range(1, len(c)):
        if c[i - 1] <= 0:
            continue
        ratio = c[i] / c[i - 1]
        if ratio < 0.65 or ratio > 1.60:        # ~split / bonus / reverse-split
            c[:i] *= ratio
            h[:i] *= ratio
            l[:i] *= ratio
            events += 1
    return c, h, l, events


def score_symbol(g: pd.DataFrame) -> dict:
    g = g.sort_values("DATE")
    c = g["CLOSE_PRICE"].values.astype(float)
    h = g["HIGH_PRICE"].values.astype(float)
    l = g["LOW_PRICE"].values.astype(float)
    if len(c) < 60 or c[0] <= 0:
        return None
    c, h, l, ca_events = adjust_corporate_actions(c, h, l)
    rets = np.diff(c) / c[:-1]
    ann_vol = float(np.std(rets) * np.sqrt(252))
    path = float(np.sum(np.abs(np.diff(c))))
    er = abs(c[-1] - c[0]) / path if path > 0 else 1.0           # efficiency ratio
    atr_pct = float(np.mean((h - l) / np.where(c == 0, np.nan, c)))
    total_ret = float(c[-1] / c[0] - 1)
    peak = np.maximum.accumulate(c)
    max_dd = float(np.min(c / peak - 1))
    return {
        "days": len(c),
        "corp_actions": ca_events,
        "med_turnover_cr": float(np.median(g["TURNOVER_LACS"].values) / 100.0),
        "last_close": float(c[-1]),
        "ann_vol": ann_vol,
        "efficiency_ratio": er,
        "choppiness": 1 - er,
        "hurst": hurst(c),
        "atr_pct": atr_pct,
        "total_ret": total_ret,
        "max_dd": max_dd,
    }


def zscore(s: pd.Series) -> pd.Series:
    sd = s.std(ddof=0)
    return (s - s.mean()) / sd if sd > 0 else s * 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=250, help="lookback trading days")
    ap.add_argument("--mode", choices=["stocks", "etf", "all"], default="stocks",
                    help="screen individual stocks, ETFs only, or both")
    ap.add_argument("--min-turnover-cr", type=float, default=5.0)
    ap.add_argument("--min-days", type=int, default=200, help="min trading-day history (kills fresh IPOs)")
    ap.add_argument("--min-price", type=float, default=50.0, help="price floor (no penny stocks)")
    ap.add_argument("--max-vol", type=float, default=1.20, help="cap annualized vol (avoid blow-up names)")
    ap.add_argument("--top", type=int, default=30, help="rows to consider for diversification")
    ap.add_argument("--picks", type=int, default=4, help="uncorrelated finalists")
    ap.add_argument("--max-corr", type=float, default=0.35)
    ap.add_argument("--min-drift", type=float, default=0.0, help="min total return over window")
    ap.add_argument("--out", default="/root/trading_bot/permanent_grid/data/grid_candidates.csv")
    ap.add_argument("--cache", default="/root/trading_bot/permanent_grid/data/bhav_cache")
    args = ap.parse_args()

    print(f"Downloading ~{args.days} bhavcopies...")
    raw = download_bhavcopies(args.days, args.cache)
    raw = raw[raw["SERIES"].isin(KEEP_SERIES)].copy()
    # parse the bhavcopy date (e.g. "16-Jun-2026") for correct chronological order,
    # then drop duplicate (symbol, date) rows.
    raw["DATE"] = pd.to_datetime(raw["DATE1"].astype(str).str.strip(),
                                 format="%d-%b-%Y", errors="coerce")
    raw = raw.dropna(subset=["DATE"]).drop_duplicates(["SYMBOL", "DATE"])

    # classify stock vs ETF and apply the mode filter
    etfs = fetch_etf_symbols()
    print(f"ETF master: {len(etfs)} ETFs")
    if args.mode == "stocks":
        raw = raw[~raw["SYMBOL"].isin(etfs)]
    elif args.mode == "etf":
        raw = raw[raw["SYMBOL"].isin(etfs)]
    print(f"Universe ({args.mode}): {raw['SYMBOL'].nunique()} symbols")

    # close-price panel (for correlation later)
    closes = {}
    rows = []
    for sym, g in raw.groupby("SYMBOL"):
        sc = score_symbol(g)
        if not sc:
            continue
        sc["symbol"] = sym
        rows.append(sc)
        gs = g.sort_values("DATE")
        cadj, _, _, _ = adjust_corporate_actions(gs["CLOSE_PRICE"].values,
                                                 gs["HIGH_PRICE"].values, gs["LOW_PRICE"].values)
        closes[sym] = pd.Series(cadj, index=gs["DATE"].values)   # split-adjusted for correlation
    if not rows:
        raise SystemExit("No symbols scored — need at least ~60 trading days (--days 80+).")
    df = pd.DataFrame(rows).set_index("symbol")
    print(f"Scored: {len(df)} symbols")

    # liquidity gate (proxies mcap>500cr) + quality filters + mild positive drift
    liq = df[(df["med_turnover_cr"] >= args.min_turnover_cr) &
             (df["days"] >= args.min_days) &
             (df["last_close"] >= args.min_price) &
             (df["ann_vol"] <= args.max_vol) &
             (df["total_ret"] >= args.min_drift)].copy()
    print(f"After gates (turnover>={args.min_turnover_cr}cr, >={args.min_days}d, "
          f">=Rs{args.min_price:g}, vol<={args.max_vol:g}, drift>0): {len(liq)}")

    # GridScore: want high vol, high chop, high ATR%, mild+capped drift, shallow DD
    liq["drift_reward"] = np.tanh(liq["total_ret"].clip(lower=0) * 2.0)   # rewards up, saturates
    liq["GridScore"] = (
        1.2 * zscore(liq["ann_vol"]) +
        1.5 * zscore(liq["choppiness"]) +
        0.8 * zscore(liq["atr_pct"]) +
        0.7 * zscore(liq["drift_reward"]) +
        0.5 * zscore(liq["max_dd"])           # less-negative DD scores higher
    )
    liq = liq.sort_values("GridScore", ascending=False)

    cols = ["GridScore", "med_turnover_cr", "ann_vol", "choppiness", "efficiency_ratio",
            "hurst", "atr_pct", "total_ret", "max_dd", "last_close", "days", "corp_actions"]
    liq[cols].round(3).to_csv(args.out)
    print(f"\nRanked table -> {args.out}  ({len(liq)} names)")
    print("\nTOP 15 grid candidates:")
    print(liq[cols].head(15).round(3).to_string())

    # greedy uncorrelated shortlist from the top
    top = liq.head(args.top)
    ret_panel = pd.DataFrame({s: closes[s] for s in top.index}).sort_index().pct_change().dropna(how="all")
    picks = []
    for sym in top.index:
        if not picks:
            picks.append(sym); continue
        if sym not in ret_panel:
            continue
        corrs = [abs(ret_panel[sym].corr(ret_panel[p])) for p in picks if p in ret_panel]
        if all((not np.isnan(c)) and c < args.max_corr for c in corrs):
            picks.append(sym)
        if len(picks) >= args.picks:
            break
    print(f"\nUNCORRELATED SHORTLIST ({len(picks)}, corr<{args.max_corr}):")
    print(liq.loc[picks, cols].round(3).to_string())
    print("\nFyers symbols for backtest:", [f"NSE:{s}-EQ" for s in picks])


if __name__ == "__main__":
    main()
