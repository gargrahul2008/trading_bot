#!/usr/bin/env python3
"""
mexc_cycle_verify.py  —  Verify bot cycle value-add vs hold-only.

Core idea:
    The bot trades ~600 buy/sells, accumulating N extra ETH.
    Net cash spent / N = effective cost per ETH (WITH cycles).
    Without cycles, the gross cost would be higher.
    The difference = cycle savings = formula cycles_pnl.

Output CSV (one row appended per run):
    ts, Z (current price), net_eth, net_cash_spent,
    effective_cost, gross_cost, cycles_saving,
    pnl_with_cycles, pnl_without_cycles,
    breakeven_with, breakeven_without

Usage:
    python3 scripts/mexc_cycle_verify.py \\
        --state   strategies/pct_ladder/state/mexc_state_2026_03_05_v1.json \\
        --trades  strategies/pct_ladder/state/mexc_trades.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \\
                  ... \\
        --out     strategies/pct_ladder/state/mexc_cycle_verify.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

HEADERS = [
    "ts",
    "Z_current",
    # Trading totals
    "total_buys",
    "total_sells",
    "total_buy_qty",
    "total_sell_qty",
    "total_buy_value",
    "total_sell_value",
    # Net accumulation
    "net_eth",
    "net_cash_spent",
    # Cost basis
    "effective_cost",       # with cycles
    "gross_cost",           # without cycles
    "saving_per_eth",
    "total_cycle_saving",   # = formula cycles_pnl
    # PnL at current price
    "pnl_with_cycles",
    "pnl_without_cycles",
    # Breakeven
    "breakeven_with",
    "breakeven_without",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _r(x, places: int = 2) -> str:
    try:
        q = Decimal("0." + "0" * places)
        return str(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    except Exception:
        return str(x)


def _is_cycle(reason: str) -> bool:
    return bool(re.search(r'ltp[<>]=ref[+\-]\d', reason))


def _parse_pct(reason: str) -> Decimal:
    m = re.search(r'[+\-](\d+\.?\d*)%', reason)
    if m:
        return Decimal(m.group(1)) / Decimal("100")
    return D0


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S%z")


# ── data loading ──────────────────────────────────────────────────────────────

def load_trades(trades_paths: list[str]) -> list[dict]:
    """Load and deduplicate all FILL events, sorted by timestamp."""
    seen: set[str] = set()
    events: list[dict] = []
    for path in trades_paths:
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
                    qty = _dec(r.get("qty") or "0")
                    if qty <= D0:
                        continue
                    events.append(r)
        except FileNotFoundError:
            pass
    events.sort(key=lambda r: r.get("ts", ""))
    return events


# ── core ──────────────────────────────────────────────────────────────────────

def generate(state_path: str, trades_paths: list[str], out_path: str) -> None:

    # ── current price from state ──
    try:
        with open(state_path, encoding="utf-8") as f:
            state = json.load(f)
        Z = _dec(state.get("last_prices", {}).get("ETHUSDC") or "0")
    except Exception:
        print("ERROR: cannot read state file for current price")
        return

    if Z <= D0:
        print("ERROR: current price is 0")
        return

    # ── load all trades ──
    fills = load_trades(trades_paths)
    if not fills:
        print("No trades found.")
        return

    # ── compute totals ──
    total_buy_qty  = D0
    total_sell_qty = D0
    total_buy_val  = D0
    total_sell_val = D0
    n_buys  = 0
    n_sells = 0

    # For formula cycles_pnl: sum(cqq × pct) for all cycle sells
    formula_cycles_pnl = D0

    for r in fills:
        qty   = _dec(r.get("qty") or "0")
        price = _dec(r.get("price") or "0")
        side  = str(r.get("side") or "").upper()
        reason = str(r.get("reason") or "")
        cqq   = _dec(r.get("cum_quote_qty") or "0")

        if side == "BUY":
            n_buys += 1
            total_buy_qty += qty
            total_buy_val += qty * price
        elif side == "SELL":
            n_sells += 1
            total_sell_qty += qty
            total_sell_val += qty * price
            if _is_cycle(reason):
                pct = _parse_pct(reason)
                formula_cycles_pnl += cqq * pct

    net_eth  = total_buy_qty - total_sell_qty
    net_cash = total_sell_val - total_buy_val   # negative = cash spent

    if net_eth <= D0:
        print(f"Net ETH is {net_eth} (no accumulation). Skipping.")
        return

    net_cash_spent = -net_cash  # positive number = cash consumed

    # ── cost basis ──
    effective_cost = net_cash_spent / net_eth                         # with cycles
    gross_cost     = (net_cash_spent + formula_cycles_pnl) / net_eth  # without cycles
    saving_per_eth = gross_cost - effective_cost

    # ── PnL at current price ──
    pnl_with    = net_eth * (Z - effective_cost)
    pnl_without = net_eth * (Z - gross_cost)

    # ── breakeven prices ──
    breakeven_with    = effective_cost
    breakeven_without = gross_cost

    now = datetime.datetime.now(tz=UTC)

    row = {
        "ts":                 _fmt_ist(now),
        "Z_current":          _r(Z, 2),
        "total_buys":         str(n_buys),
        "total_sells":        str(n_sells),
        "total_buy_qty":      _r(total_buy_qty, 4),
        "total_sell_qty":     _r(total_sell_qty, 4),
        "total_buy_value":    _r(total_buy_val, 2),
        "total_sell_value":   _r(total_sell_val, 2),
        "net_eth":            _r(net_eth, 4),
        "net_cash_spent":     _r(net_cash_spent, 2),
        "effective_cost":     _r(effective_cost, 2),
        "gross_cost":         _r(gross_cost, 2),
        "saving_per_eth":     _r(saving_per_eth, 2),
        "total_cycle_saving": _r(formula_cycles_pnl, 2),
        "pnl_with_cycles":    _r(pnl_with, 2),
        "pnl_without_cycles": _r(pnl_without, 2),
        "breakeven_with":     _r(breakeven_with, 2),
        "breakeven_without":  _r(breakeven_without, 2),
    }

    # ── append to CSV ──
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    write_header = not os.path.exists(out_path) or os.path.getsize(out_path) == 0
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    # ── print summary ──
    print(f"═══ Cycle Value-Add Verification ({_fmt_ist(now)}) ═══")
    print()
    print(f"ETH price now : ${_r(Z, 2)}")
    print(f"Trades        : {n_buys} buys + {n_sells} sells = {n_buys + n_sells} total")
    print(f"Net ETH       : +{_r(net_eth, 4)} accumulated")
    print(f"Net cash spent: ${_r(net_cash_spent, 2)}")
    print()
    print(f"  Cost/ETH with cycles    : ${_r(effective_cost, 2)}")
    print(f"  Cost/ETH without cycles : ${_r(gross_cost, 2)}")
    print(f"  Saving per ETH          : ${_r(saving_per_eth, 2)}")
    print(f"  Total cycle saving      : ${_r(formula_cycles_pnl, 2)}")
    print()
    print(f"  PnL at ${_r(Z,2)} (with cycles)    : ${_r(pnl_with, 2)}")
    print(f"  PnL at ${_r(Z,2)} (without cycles) : ${_r(pnl_without, 2)}")
    print(f"  Cycles reduced loss by             : ${_r(formula_cycles_pnl, 2)}")
    print()
    print(f"  Breakeven with cycles    : ETH = ${_r(breakeven_with, 2)}")
    print(f"  Breakeven without cycles : ETH = ${_r(breakeven_without, 2)}")
    print()
    print(f"Appended to {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state",  required=True, help="State JSON for current price")
    ap.add_argument("--trades", required=True, nargs="+", help="Trade .jsonl files")
    ap.add_argument("--out",    required=True, help="Output CSV path (appended)")
    args = ap.parse_args()
    generate(
        state_path=args.state,
        trades_paths=args.trades,
        out_path=args.out,
    )


if __name__ == "__main__":
    main()
