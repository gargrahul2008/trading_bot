#!/usr/bin/env python3
"""
replay_live_signals.py — sanity check for the live fib bot.

Fetches recent 1-min bars from MEXC (same endpoint the live bot uses) and
slides the live bot's OWN signal function (`get_signal_for_last_bar`, which calls
`_make_strategy` with the EMA filter + target_ext) bar-by-bar across history.

This replays the EXACT code path the live loop runs each minute, so if the live
bot is silently missing setups due to a bug, this will reveal it (and vice-versa:
if this finds nothing, "flat for an hour" is genuine, not a bug).

No strategy/bot logic is modified — this only imports and calls existing functions.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Import the live bot's actual functions — same code path as live.
from live_fib_bot import _fetch_bars_mexc, get_signal_for_last_bar  # noqa: E402


def replay(symbol: str, strat_cfg: dict, total: int, lookback: int) -> list[dict]:
    """Fetch `total` recent bars; replay live signal logic over a sliding window."""
    df = _fetch_bars_mexc(symbol, n=total)
    if df.empty:
        print(f"  {symbol}: no bars returned")
        return []

    print(f"  {symbol}: fetched {len(df)} bars  "
          f"{df['timestamp'].iloc[0]} → {df['timestamp'].iloc[-1]}")

    signals = []
    # Mirror live: at each bar close, the bot has the trailing `lookback` window.
    for i in range(lookback, len(df) + 1):
        window = df.iloc[i - lookback:i].reset_index(drop=True)
        sig = get_signal_for_last_bar(window, strat_cfg)
        if sig is not None:
            signals.append(sig)
    return signals


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay live fib signal path over recent MEXC bars")
    ap.add_argument("--config", default="configs/fib_live.json")
    ap.add_argument("--total", type=int, default=1000,
                    help="Total recent bars to fetch (MEXC max ~1000 ≈ 16h)")
    ap.add_argument("--lookback", type=int, default=350,
                    help="Trailing window size the live bot uses each minute (default 350)")
    args = ap.parse_args()

    cfg = json.load(open(args.config))
    strat_cfg = cfg["strategy"]
    symbols = cfg["symbols"]

    print(f"\nLive-path signal replay  variant={strat_cfg.get('variant')}  "
          f"imp={strat_cfg.get('min_impulse_pct')}%  ext={strat_cfg.get('target_ext')}  "
          f"lookback={args.lookback}")
    print("─" * 80)

    for sym in symbols:
        sigs = replay(sym, strat_cfg, args.total, args.lookback)
        hrs = (args.total - args.lookback) / 60.0
        print(f"  {sym}: {len(sigs)} signal(s) over ~{hrs:.1f}h of replay")
        for s in sigs:
            print(f"      {s['timestamp']}  {s['direction']:5s}  "
                  f"entry={s['entry_price']:.4f}  stop={s['stop_loss']:.4f}  "
                  f"target={s['target']:.4f}")
    print("─" * 80)


if __name__ == "__main__":
    main()
