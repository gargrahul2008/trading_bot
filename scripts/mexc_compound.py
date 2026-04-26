#!/usr/bin/env python3
"""
mexc_compound.py — Daily compounding for pct_ladder (actual LIFO cycle PnL basis).

Runs at UTC 00:00 via cron.  Reads all trade files, computes cumulative
actual LIFO-matched cycle PnL (buy low / sell high pairs only), derives the
new step size, and writes compound_buy_quote / compound_sell_quote into the
state JSON extras.

The running bot's strategy.on_prices() reads these values from state.extras
and uses them instead of the static config buy_quote/sell_quote.

Formula:
    ratio            = initial_buy_quote / initial_equity
    compound_equity  = initial_equity + cum_actual_cycle_pnl
    new_step         = round(compound_equity × ratio, 2)

Usage:
    python3 scripts/mexc_compound.py \\
        --config  strategies/pct_ladder/config.mexc.json \\
        --trades  strategies/pct_ladder/state/mexc_trades_2026_04_13_v1.jsonl \\
        --initial-equity  53233 \\
        --initial-buy-quote 2662
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def load_trades(paths: list[str]) -> list[dict]:
    seen: set[str] = set()
    events: list[dict] = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                    except Exception:
                        continue
                    if r.get("event") != "FILL":
                        continue
                    oid = str(r.get("order_id") or "")
                    if oid and oid in seen:
                        continue
                    if oid:
                        seen.add(oid)
                    events.append(r)
        except FileNotFoundError:
            pass
    return events


def compute_actual_cycle_pnl(fills: list[dict]) -> Decimal:
    """
    LIFO-matched actual cycle PnL.
    Matches each non-rebalance sell (ltp>=ref pattern) against the most
    recent non-rebalance buy whose price is lower than the sell price.
    Only profitable buy→sell pairs are counted.
    """
    open_buys: list[list] = []  # [remaining_qty, buy_price]
    total = D0
    for r in fills:
        side   = str(r.get("side") or "").upper()
        reason = str(r.get("reason") or "")
        qty    = _dec(r.get("qty") or "0")
        price  = _dec(r.get("price") or "0")
        is_reb = bool(re.search(r'rebalance|rebal', reason, re.IGNORECASE))
        if qty <= D0 or price <= D0:
            continue
        if side == "BUY" and not is_reb:
            open_buys.append([qty, price])
        elif side == "SELL" and not is_reb:
            if not re.search(r'ltp[<>]=ref[+\-]', reason):
                continue
            remaining = qty
            for i in range(len(open_buys) - 1, -1, -1):
                if price > open_buys[i][1] and remaining > D0:
                    take = min(remaining, open_buys[i][0])
                    total += take * (price - open_buys[i][1])
                    open_buys[i][0] -= take
                    remaining -= take
                    if open_buys[i][0] <= D0:
                        open_buys.pop(i)
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--trades", required=True, nargs="+")
    ap.add_argument("--initial-equity", type=float, default=None,
                    help="Portfolio value when compounding started (stored in state on first run)")
    ap.add_argument("--initial-buy-quote", type=float, default=None,
                    help="buy_quote when compounding started (stored in state on first run)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Load config
    with open(args.config, encoding="utf-8") as f:
        cfg = json.load(f)
    strategy_cfg = cfg.get("strategy", {})
    config_buy_quote = _dec(strategy_cfg.get("buy_quote", 2662))

    # Resolve state path
    base_dir   = os.path.dirname(os.path.abspath(args.config))
    state_rel  = cfg.get("paths", {}).get("state_path", "")
    state_path = os.path.join(base_dir, state_rel) if state_rel else None

    if not state_path or not os.path.exists(state_path):
        print(f"ERROR: state file not found: {state_path}")
        raise SystemExit(1)

    # Load state
    with open(state_path, encoding="utf-8") as f:
        state = json.load(f)
    extras = state.get("extras") or {}

    # Resolve initial values: CLI args > state extras > config defaults
    initial_equity = (
        args.initial_equity
        or (float(extras["compound_initial_equity"]) if "compound_initial_equity" in extras else None)
    )
    initial_buy_quote = (
        args.initial_buy_quote
        or (float(extras["compound_initial_buy_quote"]) if "compound_initial_buy_quote" in extras else None)
    )

    if initial_equity is None or initial_buy_quote is None:
        print("ERROR: --initial-equity and --initial-buy-quote are required on first run")
        print("       (they will be stored in state for subsequent runs)")
        raise SystemExit(1)

    initial_equity    = _dec(initial_equity)
    initial_buy_quote = _dec(initial_buy_quote)
    ratio = initial_buy_quote / initial_equity

    # Compute actual LIFO cycle PnL
    fills = load_trades(args.trades)
    actual_cycle_pnl = compute_actual_cycle_pnl(fills)

    # New step size
    compound_equity = initial_equity + actual_cycle_pnl
    new_step = (compound_equity * ratio).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if new_step < config_buy_quote:
        new_step = config_buy_quote  # never shrink below original

    old_step = _dec(extras.get("compound_buy_quote", str(config_buy_quote)))
    now = datetime.datetime.now(tz=UTC)

    print(f"═══ Daily Compound ({now.astimezone(IST).strftime('%Y-%m-%d %H:%M IST')}) ═══")
    print(f"  Initial equity       : ${initial_equity}")
    print(f"  Initial buy_quote    : ${initial_buy_quote}")
    print(f"  Ratio                : {float(ratio):.6f}")
    print(f"  Actual LIFO cycle PnL: ${actual_cycle_pnl:.2f}")
    print(f"  Compound equity      : ${compound_equity:.2f}")
    print(f"  Old step             : ${old_step}")
    print(f"  New step             : ${new_step}")
    print(f"  Change               : ${new_step - old_step:+.2f}")

    if args.dry_run:
        print("\n[dry-run] State not updated.")
        return

    # Write to state extras
    extras["compound_buy_quote"]         = str(new_step)
    extras["compound_sell_quote"]        = str(new_step)
    extras["compound_initial_equity"]    = str(initial_equity)
    extras["compound_initial_buy_quote"] = str(initial_buy_quote)
    extras["compound_last_ts"]           = now.isoformat()
    extras["compound_last_actual_pnl"]   = str(actual_cycle_pnl)
    state["extras"] = extras

    tmp = state_path + ".compound.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, state_path)

    print(f"\n  Written to {state_path}")


if __name__ == "__main__":
    main()
