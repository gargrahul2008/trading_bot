#!/usr/bin/env python3
"""
compare_backtest_vs_live.py — did the live paper bot match a backtest for a given day?

For a target UTC date:
  1. Fetch that day's 1-min bars (plus lookback) from MEXC — the same source the
     live bot uses.
  2. "Backtest" = replay the live bot's OWN signal function
     (`get_signal_for_last_bar`, EMA filter + target_ext) over a trailing 350-bar
     window, exactly as the live loop does each minute.
  3. Parse the live bot's actually-logged SIGNAL lines from bot.log for that day.
  4. Diff the two, restricted to the window the live bot was actually running.

This surfaces:
  • signals the backtest produced but the live bot MISSED (e.g. downtime/restarts)
  • signals the live bot logged that the backtest does NOT reproduce (phantom)
  • signals in both but with different entry/stop/target (data or logic drift)

No strategy/bot logic is modified — only existing functions are imported.
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from live_fib_bot import get_signal_for_last_bar  # exact live signal path

MEXC_KLINES = "https://api.mexc.com/api/v3/klines"


def fetch_mexc_range(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch [start, end) 1-min bars from MEXC in <=1000-bar chunks."""
    rows = []
    cur = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    for _ in range(200):  # safety cap; MEXC returns <=500 bars per call
        if cur >= end_ms:
            break
        r = requests.get(MEXC_KLINES, params={
            "symbol": symbol, "interval": "1m",
            "startTime": cur, "endTime": end_ms, "limit": 500}, timeout=20)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        last_open = int(batch[-1][0])
        nxt = last_open + 60_000
        if nxt <= cur:   # no forward progress → done
            break
        cur = nxt
        time.sleep(0.2)
    if not rows:
        return pd.DataFrame()
    # dedupe by open time
    seen, uniq = set(), []
    for b in rows:
        if b[0] not in seen:
            seen.add(b[0]); uniq.append(b)
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp(int(b[0]), unit="ms", tz="UTC") for b in uniq],
        "symbol":    f"MEXC:{symbol}",
        "open":  [float(b[1]) for b in uniq],
        "high":  [float(b[2]) for b in uniq],
        "low":   [float(b[3]) for b in uniq],
        "close": [float(b[4]) for b in uniq],
        "volume":[float(b[5]) for b in uniq],
    })
    df["trade_date"] = df["timestamp"].dt.date.astype(str)
    return df.sort_values("timestamp").reset_index(drop=True)


def backtest_signals(symbol: str, strat_cfg: dict, day: datetime,
                     lookback: int = 350) -> dict:
    """Replay live signal logic across `day`. Returns {bar_ts: signal}."""
    # fetch from lookback minutes before midnight through end of day
    start = day - timedelta(minutes=lookback + 5)
    end = day + timedelta(days=1)
    df = fetch_mexc_range(symbol, start, end)
    if df.empty:
        return {}
    out = {}
    for i in range(lookback, len(df) + 1):
        window = df.iloc[i - lookback:i].reset_index(drop=True)
        sig = get_signal_for_last_bar(window, strat_cfg)
        if sig is not None:
            bar_ts = pd.Timestamp(sig["timestamp"])
            out[bar_ts] = sig
    return out


_SIG_RE = re.compile(
    r"^(?P<log>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ .*\[PAPER SIGNAL\] "
    r"(?P<sym>\w+) (?P<side>LONG|SHORT) @ limit=(?P<limit>[\d.]+)\s+"
    r"stop=(?P<stop>[\d.]+)\s+target=(?P<target>[\d.]+)")


def live_signals_from_log(log_path: Path, symbol: str, day_str: str) -> dict:
    """Parse live SIGNAL lines for `symbol` on `day_str`. Returns {bar_ts: dict}."""
    out = {}
    if not log_path.exists():
        return out
    for line in log_path.read_text().splitlines():
        m = _SIG_RE.match(line)
        if not m or m["sym"] != symbol:
            continue
        log_t = pd.Timestamp(m["log"], tz="UTC")
        # signal fires ~:02 after close of the PREVIOUS minute's bar
        bar_ts = log_t.floor("min") - pd.Timedelta(minutes=1)
        if str(bar_ts.date()) != day_str:
            continue
        out[bar_ts] = {
            "entry_price": float(m["limit"]),
            "stop_loss": float(m["stop"]),
            "target": float(m["target"]),
            "direction": m["side"],
        }
    return out


_EVT_RE = re.compile(
    r"^(?P<log>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ .*\[PAPER "
    r"(?P<evt>SIGNAL|OPEN|CLOSE|CANCEL)\] (?P<sym>\w+) ")


def live_busy_intervals(log_path: Path, symbol: str) -> list:
    """
    Reconstruct spans where the live bot could NOT scan for a new signal
    (a pending entry exists, or it is in a position). Returns [(start, end)]
    of processing datetimes. While busy, the bot skips signal detection by
    design (one setup per symbol at a time).
    """
    if not log_path.exists():
        return []
    intervals, busy_start = [], None
    for line in log_path.read_text().splitlines():
        m = _EVT_RE.match(line)
        if not m or m["sym"] != symbol:
            continue
        t = pd.Timestamp(m["log"], tz="UTC")
        evt = m["evt"]
        if evt == "SIGNAL" and busy_start is None:
            busy_start = t                       # pending entry created
        elif evt in ("CLOSE", "CANCEL") and busy_start is not None:
            intervals.append((busy_start, t))    # setup resolved (full close / expiry)
            busy_start = None
        # OPEN and PARTIAL keep the span open
    if busy_start is not None:
        intervals.append((busy_start, pd.Timestamp.max.tz_localize("UTC")))
    return intervals


def _in_busy(proc_time: pd.Timestamp, intervals: list) -> bool:
    return any(s <= proc_time <= e for s, e in intervals)


def live_run_window(log_path: Path, day_str: str):
    """Return (first_bar_seen, last_bar_seen) the live bot processed that day."""
    bars = []
    rex = re.compile(rf"^{day_str} (\d{{2}}:\d{{2}}):\d{{2}},\d+ .*── Bar close (\d{{2}}:\d{{2}}) UTC ──")
    for line in log_path.read_text().splitlines():
        m = rex.match(line)
        if m:
            bars.append(m.group(2))
    if not bars:
        return None, None
    return bars[0], bars[-1]


def approx(a: float, b: float, tol: float = 5e-4) -> bool:
    return abs(a - b) <= tol * max(1.0, abs(b))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/fib_live.json")
    ap.add_argument("--date", required=True, help="UTC date YYYY-MM-DD to compare")
    args = ap.parse_args()

    import json
    cfg = json.load(open(args.config))
    strat_cfg = cfg["strategy"]
    symbols = cfg["symbols"]
    log_path = Path(cfg.get("output_dir", "artifacts/fib_live")) / "bot.log"
    day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_str = args.date

    first_bar, last_bar = live_run_window(log_path, day_str)
    print(f"\n=== Backtest vs Live signal comparison — {day_str} (UTC) ===")
    print(f"Live bot ran that day: bars {first_bar} → {last_bar} UTC")
    print(f"Strategy: {strat_cfg.get('variant')}  imp={strat_cfg.get('min_impulse_pct')}%  "
          f"ext={strat_cfg.get('target_ext')}\n")

    # only compare within the window the live bot was actually up
    if first_bar:
        lo = pd.Timestamp(f"{day_str} {first_bar}", tz="UTC")
    else:
        lo = day
    hi = day + timedelta(days=1)

    grand = dict(match=0, missed=0, phantom=0, drift=0)

    for sym in symbols:
        print(f"── {sym} " + "─" * 50)
        bt = backtest_signals(sym, strat_cfg, day)
        lv = live_signals_from_log(log_path, sym, day_str)
        bt = {t: s for t, s in bt.items() if lo <= t < hi}
        lv = {t: s for t, s in lv.items() if lo <= t < hi}
        all_ts = sorted(set(bt) | set(lv))
        print(f"   backtest signals: {len(bt)}   live-logged signals: {len(lv)}")

        for t in all_ts:
            b, l = bt.get(t), lv.get(t)
            tstr = t.strftime("%H:%M")
            if b and l:
                same = (b["direction"] == l["direction"]
                        and approx(b["entry_price"], l["entry_price"])
                        and approx(b["stop_loss"], l["stop_loss"])
                        and approx(b["target"], l["target"]))
                if same:
                    grand["match"] += 1
                    print(f"   ✓ {tstr}  {l['direction']:5s} @ {l['entry_price']:.4f}  MATCH")
                else:
                    grand["drift"] += 1
                    print(f"   ≠ {tstr}  DRIFT  bt={b['direction']} "
                          f"{b['entry_price']:.4f}/{b['stop_loss']:.4f}/{b['target']:.4f}  "
                          f"live={l['direction']} "
                          f"{l['entry_price']:.4f}/{l['stop_loss']:.4f}/{l['target']:.4f}")
            elif b and not l:
                grand["missed"] += 1
                print(f"   ✗ {tstr}  MISSED by live  (backtest: {b['direction']} "
                      f"@ {b['entry_price']:.4f})  ← live bot not running / skipped this bar")
            else:
                grand["phantom"] += 1
                print(f"   ! {tstr}  PHANTOM in live  (live: {l['direction']} "
                      f"@ {l['entry_price']:.4f})  ← backtest did not reproduce")
        print()

    print("=" * 60)
    print(f"TOTAL  matched={grand['match']}  missed={grand['missed']}  "
          f"phantom={grand['phantom']}  drift={grand['drift']}")
    if grand["missed"] == grand["phantom"] == grand["drift"] == 0:
        print("✅ Live paper signals are IDENTICAL to the backtest — no differences.")
    else:
        print("⚠️  Differences found — see rows above.")
    print("=" * 60)


if __name__ == "__main__":
    main()
