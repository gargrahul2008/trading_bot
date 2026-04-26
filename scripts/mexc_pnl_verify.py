#!/usr/bin/env python3
"""
mexc_pnl_verify.py — Append a P&L verification row to a CSV file.

Runs alongside the Telegram cron (every 8 hours) to track:
  - Cycle PnL (LIFO)
  - True realized (avg buy vs avg sell method)
  - Hidden losses (sells below cost, LIFO ignores)
  - Unrealized PnL on open ETH position
  - Portfolio value vs invested capital

Usage:
    python3 scripts/mexc_pnl_verify.py \\
        --config  strategies/pct_ladder/config.mexc.json \\
        --trades  strategies/pct_ladder/state/mexc_trades.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \\
        --capital strategies/pct_ladder/state/capital_flows_2026_03_05_v1.json \\
        --manual  strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json \\
        --out     strategies/pct_ladder/state/pnl_verify.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
from decimal import Decimal
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

CSV_FIELDS = [
    "ts_ist", "cmp",
    "avg_buy", "avg_sell", "spread_per_eth",
    "cycle_pnl_lifo",
    "true_realized", "hidden_losses",
    "gross_gains", "gross_losses",
    "open_eth_qty", "open_eth_avg_cost", "unrealized_pnl",
    "net_pnl",
    "cash", "broker_eth", "pv",
    "invested", "pv_gain",
    "breakeven_eth",
]


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _is_rebalance(reason: str) -> bool:
    return bool(re.search(r'rebalance|rebal', reason, re.IGNORECASE))


def load_trades(paths: list[str]) -> list[dict]:
    seen: set[str] = set()
    fills: list[dict] = []
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
                    if _dec(r.get("qty") or "0") <= D0:
                        continue
                    fills.append(r)
        except FileNotFoundError:
            pass
    fills.sort(key=lambda r: r.get("ts", ""))
    return fills


def compute_verify(fills: list[dict], manual_positions: list[dict],
                   cash: Decimal, broker_eth: Decimal,
                   cmp: Decimal, invested: Decimal) -> dict:
    """
    FIFO matching (manual ETH seeded as initial lots) to compute:
      - avg buy / avg sell prices
      - true realized PnL  = qty_sold × (avg_sell - avg_buy)
      - gross gains / losses
      - unrealized on remaining open lots
      - cycle PnL (LIFO, profitable-only matches — same logic as telegram report)
    """
    # ---- FIFO lots (true accounting) ----
    fifo_lots: list[list] = []
    for mp in manual_positions:
        q = _dec(mp.get("qty", 0))
        p = _dec(mp.get("buy_price", 0))
        if q > D0 and p > D0:
            fifo_lots.append([q, p])

    total_buy_qty  = sum(_dec(mp.get("qty", 0)) for mp in manual_positions)
    total_buy_cost = sum(_dec(mp.get("qty", 0)) * _dec(mp.get("buy_price", 0))
                         for mp in manual_positions)
    total_sell_qty  = D0
    total_sell_cost = D0
    gross_gains     = D0
    gross_losses    = D0

    # ---- LIFO stack (cycle PnL — same as telegram report) ----
    lifo_stack: list[list] = []
    for mp in manual_positions:
        q = _dec(mp.get("qty", 0))
        p = _dec(mp.get("buy_price", 0))
        if q > D0 and p > D0:
            lifo_stack.append([q, p])
    lifo_pnl = D0

    for r in fills:
        side  = str(r.get("side") or "").upper()
        qty   = _dec(r.get("qty") or "0")
        price = _dec(r.get("price") or "0")
        cqq   = _dec(r.get("cum_quote_qty") or "0")
        if qty <= D0 or price <= D0:
            continue

        if side == "BUY":
            notional = cqq if cqq > D0 else qty * price
            # FIFO
            fifo_lots.append([qty, price])
            total_buy_qty  += qty
            total_buy_cost += notional
            # LIFO
            lifo_stack.append([qty, price])

        elif side == "SELL":
            notional = cqq if cqq > D0 else qty * price
            total_sell_qty  += qty
            total_sell_cost += notional

            # FIFO: consume oldest lots
            rem = qty
            while rem > D0 and fifo_lots:
                take  = min(rem, fifo_lots[0][0])
                pnl   = take * (price - fifo_lots[0][1])
                if pnl >= D0:
                    gross_gains  += pnl
                else:
                    gross_losses += pnl
                rem            -= take
                fifo_lots[0][0] -= take
                if fifo_lots[0][0] <= D0:
                    fifo_lots.pop(0)

            # LIFO: match most recent buy BELOW sell price (one lot)
            i = len(lifo_stack) - 1
            while i >= 0:
                if price > lifo_stack[i][1]:
                    take = min(qty, lifo_stack[i][0])
                    lifo_pnl        += take * (price - lifo_stack[i][1])
                    lifo_stack[i][0] -= take
                    if lifo_stack[i][0] <= D0:
                        lifo_stack.pop(i)
                    break
                i -= 1

    avg_buy  = total_buy_cost  / total_buy_qty  if total_buy_qty  > D0 else D0
    avg_sell = total_sell_cost / total_sell_qty if total_sell_qty > D0 else D0
    spread   = avg_sell - avg_buy

    true_realized  = total_sell_qty * spread
    hidden_losses  = lifo_pnl - true_realized

    open_qty  = sum(l[0] for l in fifo_lots)
    open_cost = sum(l[0] * l[1] for l in fifo_lots)
    open_avg  = open_cost / open_qty if open_qty > D0 else D0
    unrealized = open_qty * (cmp - open_avg)

    net_pnl   = true_realized + unrealized
    pv        = cash + broker_eth * cmp
    pv_gain   = pv - invested
    breakeven = (invested - cash) / broker_eth if broker_eth > D0 else D0

    return {
        "cmp":              cmp,
        "avg_buy":          avg_buy,
        "avg_sell":         avg_sell,
        "spread_per_eth":   spread,
        "cycle_pnl_lifo":   lifo_pnl,
        "true_realized":    true_realized,
        "hidden_losses":    hidden_losses,
        "gross_gains":      gross_gains,
        "gross_losses":     gross_losses,
        "open_eth_qty":     open_qty,
        "open_eth_avg_cost":open_avg,
        "unrealized_pnl":   unrealized,
        "net_pnl":          net_pnl,
        "cash":             cash,
        "broker_eth":       broker_eth,
        "pv":               pv,
        "invested":         invested,
        "pv_gain":          pv_gain,
        "breakeven_eth":    breakeven,
    }


_QTY_FIELDS = {"open_eth_qty", "broker_eth"}   # 4 decimals
_STR_FIELDS = {"ts_ist"}                        # keep as-is

def _fmt(key: str, val) -> str:
    if key in _STR_FIELDS:
        return str(val)
    if key in _QTY_FIELDS:
        return f"{float(val):.4f}"
    return f"{float(val):.2f}"


def append_row(out_path: str, row: dict) -> None:
    write_header = not os.path.exists(out_path)
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow({k: _fmt(k, row[k]) for k in CSV_FIELDS})


def print_report(row: dict) -> None:
    f2 = lambda x: f"{float(x):,.2f}"
    print(f"=== Portfolio Verify  {row['ts_ist']} ===")
    print(f"CMP: {f2(row['cmp'])}  |  Invested: {f2(row['invested'])}  |  PV: {f2(row['pv'])}  (gain: {f2(row['pv_gain'])})")
    print()
    print(f"Avg buy:  {f2(row['avg_buy'])}  |  Avg sell: {f2(row['avg_sell'])}  |  Spread: {f2(row['spread_per_eth'])}/ETH")
    print()
    print(f"Cycle PnL (LIFO):   {f2(row['cycle_pnl_lifo'])}")
    print(f"True realized:      {f2(row['true_realized'])}  ({f2(row['open_eth_qty'])} ETH × {f2(row['spread_per_eth'])})")
    print(f"Hidden losses:      {f2(row['hidden_losses'])}  (LIFO skips sells below cost)")
    print(f"  Gross gains:      {f2(row['gross_gains'])}")
    print(f"  Gross losses:     {f2(row['gross_losses'])}")
    print()
    print(f"Open ETH:           {float(row['open_eth_qty']):.4f} @ avg {f2(row['open_eth_avg_cost'])}")
    print(f"Unrealized PnL:     {f2(row['unrealized_pnl'])}  (CMP {f2(row['cmp'])} vs avg {f2(row['open_eth_avg_cost'])})")
    print()
    print(f"Net PnL:            {f2(row['net_pnl'])}")
    print(f"Breakeven ETH:      {f2(row['breakeven_eth'])}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config",   required=True)
    ap.add_argument("--trades",   required=True, nargs="+")
    ap.add_argument("--capital",  default=None,  help="capital_flows JSON")
    ap.add_argument("--manual",   default=None,  help="manual_positions JSON")
    ap.add_argument("--out",      default=None,  help="Output CSV path (default: state/pnl_verify.csv)")
    ap.add_argument("--dry-run",  action="store_true")
    args = ap.parse_args()

    cfg      = json.load(open(args.config, encoding="utf-8"))
    strategy = cfg.get("strategy", {})
    symbol   = (strategy.get("symbols") or ["ETHUSDC"])[0]
    base     = os.path.dirname(os.path.abspath(args.config))

    # Resolve paths
    state_path = None
    paths = cfg.get("paths", {})
    if paths.get("state_path"):
        state_path = os.path.join(base, paths["state_path"])

    capital_path = args.capital or os.path.join(base, "state", "capital_flows_2026_03_05_v1.json")
    manual_path  = args.manual  or os.path.join(base, "state", "manual_positions_2026_03_05_v1.json")
    out_path     = args.out     or os.path.join(base, "state", "pnl_verify.csv")

    # Load capital flows → invested total
    invested = D0
    try:
        for flow in json.load(open(capital_path, encoding="utf-8")):
            invested += _dec(flow.get("delta", 0))
    except FileNotFoundError:
        pass

    # Load manual positions
    manual_positions: list[dict] = []
    try:
        manual_positions = json.load(open(manual_path, encoding="utf-8"))
    except FileNotFoundError:
        pass

    # Load broker state
    cash = broker_eth = cmp = D0
    if state_path:
        try:
            st           = json.load(open(state_path, encoding="utf-8"))
            cmp          = _dec(st.get("last_prices", {}).get(symbol) or "0")
            cash         = _dec(st.get("cash") or "0")
            broker_eth   = _dec(st.get("extras", {}).get(f"broker_base_qty_{symbol}") or "0")
            if broker_eth <= D0:
                ss         = (st.get("symbol_states") or {}).get(symbol) or {}
                broker_eth = _dec(ss.get("traded_qty") or "0")
        except Exception:
            pass

    fills  = load_trades(args.trades)
    result = compute_verify(fills, manual_positions, cash, broker_eth, cmp, invested)

    now_ist = datetime.datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M IST")
    row = {"ts_ist": now_ist}
    row.update({k: result[k] for k in CSV_FIELDS if k != "ts_ist"})

    print_report(row)
    print()

    if args.dry_run:
        print("[dry-run] CSV not updated.")
        return

    append_row(out_path, row)
    print(f"Row appended → {out_path}")


if __name__ == "__main__":
    main()
