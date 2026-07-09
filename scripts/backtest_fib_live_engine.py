#!/usr/bin/env python3
"""
backtest_fib_live_engine.py — backtest that IS the live bot.

Instead of a separate backtest engine, this replays the live bot's own
`FibLiveBot.on_bar` over historical MEXC bars. Same signal path, same fill
model, same exit resolver, same daily-frozen lookback scaling → the backtest
matches live dot-to-dot by construction (no "mirror" drift).

Usage:
    python scripts/backtest_fib_live_engine.py --start 2026-06-24 --end 2026-06-30
    python scripts/backtest_fib_live_engine.py --start 2026-06-30 --end 2026-06-30 \
        --mode window --compare-live      # reproduce the legacy live run, diff it
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from live_fib_bot import FibLiveBot

WARMUP_BARS = 350  # trailing window the live bot feeds on_bar each minute


def _fetch_range(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    rows = FibLiveBot._fetch_klines_range(
        symbol, int(start.timestamp() * 1000), int(end.timestamp() * 1000))
    if not rows:
        return pd.DataFrame()
    seen, uniq = set(), []
    for b in rows:
        if b[0] not in seen:
            seen.add(b[0]); uniq.append(b)
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp(int(b[0]), unit="ms", tz="UTC") for b in uniq],
        "symbol":    f"MEXC:{symbol}",
        "open":  [float(b[1]) for b in uniq], "high": [float(b[2]) for b in uniq],
        "low":   [float(b[3]) for b in uniq], "close": [float(b[4]) for b in uniq],
        "volume": [float(b[5]) for b in uniq],
    })
    df["trade_date"] = df["timestamp"].dt.date.astype(str)
    return df.sort_values("timestamp").reset_index(drop=True)


def run(cfg: dict, start: datetime, end: datetime, out_dir: str) -> FibLiveBot:
    cfg = dict(cfg)
    cfg["output_dir"] = out_dir
    cfg["telegram"] = {"enabled": False}   # never alert from a backtest
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    # fresh scratch state
    for f in ("bot_state.json", "paper_trades.jsonl", "bot.log"):
        p = Path(out_dir) / f
        if p.exists():
            p.unlink()

    bot = FibLiveBot(cfg, paper=True)
    end_excl = end + timedelta(days=1)  # inclusive end date

    for sym in bot.symbols:
        df = _fetch_range(sym, start - timedelta(minutes=WARMUP_BARS + 5), end_excl)
        if df.empty:
            print(f"  {sym}: no bars"); continue
        # first index at/after `start`
        first = df["timestamp"].searchsorted(pd.Timestamp(start), side="left")
        first = max(first, WARMUP_BARS)
        print(f"  {sym}: {len(df)} bars fetched; replaying "
              f"{df['timestamp'].iloc[first]} → {df['timestamp'].iloc[-1]}")
        for i in range(first, len(df)):
            window = df.iloc[i - WARMUP_BARS + 1:i + 1].reset_index(drop=True)
            bot.on_bar(sym, window)
    bot._save_state()
    return bot


def summarize(bot: FibLiveBot) -> None:
    print("\n── Backtest trades (live engine) ──")
    total = 0.0
    for sym in bot.symbols:
        trades = bot.paper_trades[sym]
        n = len(trades)
        pnl = bot.paper_pnl[sym]
        total += pnl
        wins = sum(1 for t in trades if t["gross_pnl"] > 0)
        print(f"\n  {sym}:  {n} trades  win={wins}/{n}  cumPnL=${pnl:+,.2f}")
        for t in trades:
            print(f"     {str(t['entry_bar'])[:16]}  {t['side']:5s} "
                  f"{t['entry']:.4f}→{t['exit']:.4f}  {t['reason']:12s} "
                  f"${t['gross_pnl']:+.2f}")
    print(f"\n  TOTAL cumPnL = ${total:+,.2f}")


def compare_live(bot: FibLiveBot, date_str: str) -> None:
    """Diff backtest trades vs the actual live paper_trades.jsonl for `date_str`."""
    live_path = Path("artifacts/fib_live/paper_trades.jsonl")
    live = []
    if live_path.exists():
        for line in live_path.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if str(r.get("entry_bar", ""))[:10] == date_str:
                live.append(r)
    print(f"\n── Compare vs LIVE journal ({date_str}) ──")
    bt = [t for sym in bot.symbols for t in bot.paper_trades[sym]
          if str(t.get("entry_bar", ""))[:10] == date_str]

    def key(t):
        return (t["symbol"], str(t["entry_bar"])[:16], t["side"])
    bmap = {key(t): t for t in bt}
    lmap = {key(t): t for t in live}
    allk = sorted(set(bmap) | set(lmap))
    match = diff = only_bt = only_live = 0
    for k in allk:
        b, l = bmap.get(k), lmap.get(k)
        if b and l:
            same = (abs(b["entry"] - l["entry"]) < 5e-4 and
                    abs(b["exit"] - l["exit"]) < 5e-4 and b["reason"] == l["reason"])
            if same:
                match += 1
            else:
                diff += 1
                print(f"   ≠ {k}: bt {b['entry']:.4f}→{b['exit']:.4f}/{b['reason']} | "
                      f"live {l['entry']:.4f}→{l['exit']:.4f}/{l['reason']}")
        elif b:
            only_bt += 1
            print(f"   + only in BACKTEST: {k}  {b['entry']:.4f}→{b['exit']:.4f}/{b['reason']}")
        else:
            only_live += 1
            print(f"   - only in LIVE: {k}  {l['entry']:.4f}→{l['exit']:.4f}/{l['reason']}")
    print(f"\n   matched={match}  diff={diff}  only_bt={only_bt}  only_live={only_live}")
    print("   ✅ dot-to-dot" if diff == only_bt == only_live == 0
          else "   ⚠️  differences above")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/fib_live.json")
    ap.add_argument("--start", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--mode", choices=["window", "lookback"],
                    help="override scaling.mode")
    ap.add_argument("--lookback-days", type=int, help="override scaling.lookback_days")
    ap.add_argument("--out", default="artifacts/fib_backtest")
    ap.add_argument("--compare-live", action="store_true",
                    help="diff vs artifacts/fib_live/paper_trades.jsonl (single-day)")
    args = ap.parse_args()

    cfg = json.load(open(args.config))
    if args.mode:
        cfg.setdefault("scaling", {})["mode"] = args.mode
    if args.lookback_days:
        cfg.setdefault("scaling", {})["lookback_days"] = args.lookback_days
    sc = cfg.get("scaling", {})
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    print(f"\n=== Live-engine backtest {args.start} → {args.end} ===")
    print(f"scaling: mode={sc.get('mode')}  lookback_days={sc.get('lookback_days')}\n")
    bot = run(cfg, start, end, args.out)
    summarize(bot)
    if args.compare_live:
        compare_live(bot, args.start)


if __name__ == "__main__":
    main()
