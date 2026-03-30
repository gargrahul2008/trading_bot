#!/usr/bin/env python3
"""
mexc_style_pnl.py  —  Period PnL breakdown (user style):

    actual_delta = cycles_pnl + rebal_pnl + stock_pnl

cycles_pnl:
    Only cycle trades (reason matches ltp>=ref+X% or ltp<=ref-X%):
      completed_cycles = min(cycle_sell_count, cycle_buy_count) in period
      cycles_pnl = sum(cum_quote_qty * pct) for first `completed` sells in period
    Approximates N x ladder_size x pct using actual trade sizes.

rebal_pnl (FIFO matching across the full history):
    rebal_sell -> enqueued; matched by the next BUY  (any reason)
                  pnl = (sell_price - buy_price) * matched_qty   [profit if price fell]
    rebal_buy  -> enqueued; matched by the next SELL (any reason)
                  pnl = (sell_price - buy_price) * matched_qty   [profit if price rose]
    PnL is credited to the period of the closing (matching) trade.
    Cycle buys/sells consumed by rebal matching are excluded from cycle counting.

stock_pnl = actual_delta - cycles_pnl - rebal_pnl
            (pure ETH price-movement effect; realised + unrealised combined)
check     = cycles_pnl + rebal_pnl + stock_pnl   (= actual_delta always)

Usage:
    python3 scripts/mexc_style_pnl.py \\
        --snapshots  strategies/pct_ladder/state/mexc_snapshots.csv \\
        --trades     strategies/pct_ladder/state/mexc_trades.jsonl \\
                     strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \\
                     strategies/pct_ladder/state/mexc_trades_2026_03_03.jsonl \\
                     strategies/pct_ladder/state/mexc_trades_2026_03_03_v1.jsonl \\
                     strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --out        strategies/pct_ladder/state/mexc_style_pnl.csv
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
    "period_start_ts",
    "period_end_ts",
    "open_portfolio",
    "close_portfolio",
    "actual_delta",
    # Cycles
    "cycle_sells",
    "cycle_buys",
    "completed_cycles",
    "cycles_pnl",
    # Rebal
    "rebal_pnl",
    # Stock (residual)
    "stock_pnl",
    # Reconciliation
    "check",
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


def _parse_ts(ts_str: str) -> datetime.datetime:
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return datetime.datetime.fromtimestamp(0, tz=UTC)


def _parse_pct(reason: str) -> Decimal:
    """'ltp>=ref+0.4%' -> Decimal('0.004')"""
    m = re.search(r'[+\-](\d+\.?\d*)%', reason)
    if m:
        return Decimal(m.group(1)) / Decimal("100")
    return D0


def _is_cycle(reason: str) -> bool:
    return bool(re.search(r'ltp[<>]=ref[+\-]\d', reason))


def _is_rebal_sell(reason: str) -> bool:
    return 'rebalance_sell' in reason


def _is_rebal_buy(reason: str) -> bool:
    return 'rebalance_buy' in reason


# ── data loading ──────────────────────────────────────────────────────────────

def load_bot_trades(trades_paths: list[str], symbol: str) -> list[tuple]:
    """
    Returns deduplicated (ts, side, qty, price, reason, cum_quote_qty)
    sorted by timestamp.
    """
    seen: set[str] = set()
    events: list[tuple] = []
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
                    side    = str(r.get("side") or "").upper()
                    price   = _dec(r.get("price") or "0")
                    ts      = _parse_ts(r.get("ts") or "")
                    reason  = str(r.get("reason") or "")
                    cqq     = _dec(r.get("cum_quote_qty") or "0")
                    events.append((ts, side, qty, price, reason, cqq))
        except FileNotFoundError:
            pass
    return sorted(events, key=lambda e: e[0])


# ── core ──────────────────────────────────────────────────────────────────────

def generate(snapshots_path: str, trades_paths: list[str],
             symbol: str, out_path: str) -> None:

    if not os.path.exists(snapshots_path):
        print(f"ERROR: {snapshots_path} not found")
        return

    with open(snapshots_path, encoding="utf-8") as f:
        snap_rows = list(csv.DictReader(f))

    if len(snap_rows) < 2:
        print("Need at least 2 snapshot rows.")
        return

    # Build periods from consecutive snapshot rows
    # period i covers [curr.period_start_ts, curr.period_end_ts)
    # open_portfolio = prev row's portfolio_value
    # close_portfolio = curr row's portfolio_value
    periods: list[dict] = []
    for i in range(1, len(snap_rows)):
        prev = snap_rows[i - 1]
        curr = snap_rows[i]
        periods.append({
            "start_ts":    _parse_ts(curr.get("period_start_ts") or ""),
            "end_ts":      _parse_ts(curr.get("period_end_ts") or ""),
            "start_label": curr.get("period_start_ts") or "",
            "end_label":   curr.get("period_end_ts") or "",
            "open_port":   _dec(prev.get("portfolio_value") or "0"),
            "close_port":  _dec(curr.get("portfolio_value") or "0"),
        })

    n = len(periods)

    # Per-period accumulators
    # cycle_sells[i] = list of (cqq, pct) for cycle sells in period i
    # cycle_buys[i]  = list of (cqq, pct) for cycle buys in period i
    # rebal_pnl[i]   = Decimal total rebal pnl matched in period i
    period_csells: list[list] = [[] for _ in range(n)]
    period_cbuys:  list[list] = [[] for _ in range(n)]
    period_rpnl:   list[Decimal] = [D0] * n

    # FIFO rebal queues: [remaining_qty, price]
    pending_rsells: list[list] = []   # rebal sells waiting for matching buys
    pending_rbuys:  list[list] = []   # rebal buys waiting for matching sells

    def find_pidx(ts: datetime.datetime) -> int | None:
        for i, p in enumerate(periods):
            if p["start_ts"] <= ts < p["end_ts"]:
                return i
        return None

    # ── main trade processing loop ────────────────────────────────────────────

    for ts, side, qty, price, reason, cqq in load_bot_trades(trades_paths, symbol):
        pidx = find_pidx(ts)

        if side == "BUY":
            remaining = qty

            # Match against pending rebal sells (FIFO)
            # rebal sold ETH at higher price; now buying back at lower price → profit
            while remaining > D0 and pending_rsells:
                rs = pending_rsells[0]
                matched = min(remaining, rs[0])
                pnl = (rs[1] - price) * matched
                if pidx is not None:
                    period_rpnl[pidx] += pnl
                remaining -= matched
                rs[0] -= matched
                if rs[0] <= D0:
                    pending_rsells.pop(0)

            if remaining > D0:
                if _is_rebal_buy(reason):
                    # Queue this rebal buy waiting for a future sell
                    pending_rbuys.append([remaining, price])
                elif _is_cycle(reason) and pidx is not None:
                    # Pure cycle buy (not consumed by rebal matching)
                    pct = _parse_pct(reason)
                    ratio = remaining / qty
                    period_cbuys[pidx].append((cqq * ratio, pct))

        elif side == "SELL":
            remaining = qty

            # Match against pending rebal buys (FIFO)
            # rebal bought ETH at lower price; now selling at higher price → profit
            while remaining > D0 and pending_rbuys:
                rb = pending_rbuys[0]
                matched = min(remaining, rb[0])
                pnl = (price - rb[1]) * matched
                if pidx is not None:
                    period_rpnl[pidx] += pnl
                remaining -= matched
                rb[0] -= matched
                if rb[0] <= D0:
                    pending_rbuys.pop(0)

            if remaining > D0:
                if _is_rebal_sell(reason):
                    # Queue this rebal sell waiting for a future buy
                    pending_rsells.append([remaining, price])
                elif _is_cycle(reason) and pidx is not None:
                    # Pure cycle sell (not consumed by rebal matching)
                    pct = _parse_pct(reason)
                    ratio = remaining / qty
                    period_csells[pidx].append((cqq * ratio, pct))

    # ── build output rows ─────────────────────────────────────────────────────

    breakdown: list[dict] = []
    for i, p in enumerate(periods):
        sells     = period_csells[i]
        buys      = period_cbuys[i]
        completed = len(sells)   # every sell will eventually close — credit when sell fires

        # cycles_pnl = sum of (cqq * pct) for ALL cycle sells in period
        # Each sell locks in expected profit = ladder_size * pct at that moment.
        # Using min(sells,buys) undercounts because cross-period imbalances never catch up.
        cycles_pnl = sum((cqq * pct for cqq, pct in sells), D0)

        rebal_pnl    = period_rpnl[i]
        actual_delta = p["close_port"] - p["open_port"]
        stock_pnl    = actual_delta - cycles_pnl - rebal_pnl
        check        = cycles_pnl + rebal_pnl + stock_pnl

        breakdown.append({
            "period_start_ts":  p["start_label"],
            "period_end_ts":    p["end_label"],
            "open_portfolio":   _r(p["open_port"], 2),
            "close_portfolio":  _r(p["close_port"], 2),
            "actual_delta":     _r(actual_delta, 2),
            "cycle_sells":      str(len(sells)),
            "cycle_buys":       str(len(buys)),
            "completed_cycles": str(min(len(sells), len(buys))),  # for reference only
            "cycles_pnl":       _r(cycles_pnl, 2),
            "rebal_pnl":        _r(rebal_pnl, 2),
            "stock_pnl":        _r(stock_pnl, 2),
            "check":            _r(check, 2),
        })

    # ── write CSV ─────────────────────────────────────────────────────────────

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(breakdown)

    print(f"Written {len(breakdown)} rows to {out_path}")

    # Print recent summary
    recent = breakdown[-10:]
    print()
    hdr = f"{'Period End':<26} {'Open':>10} {'Close':>10} {'Delta':>8} {'Cyc':>4} {'CycPnl':>8} {'RebalPnl':>9} {'StockPnl':>10}"
    print(hdr)
    print("-" * len(hdr))
    for r in recent:
        print(
            f"{r['period_end_ts']:<26} "
            f"{r['open_portfolio']:>10} "
            f"{r['close_portfolio']:>10} "
            f"{r['actual_delta']:>8} "
            f"{r['completed_cycles']:>4} "
            f"{r['cycles_pnl']:>8} "
            f"{r['rebal_pnl']:>9} "
            f"{r['stock_pnl']:>10}"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots", required=True, help="Path to mexc_snapshots.csv")
    ap.add_argument("--trades",    required=True, nargs="+",
                    help="Trade .jsonl files (all, in any order — deduplicated by order_id)")
    ap.add_argument("--symbol",    default="ETHUSDC")
    ap.add_argument("--out",       required=True, help="Output CSV path")
    args = ap.parse_args()
    generate(
        snapshots_path=args.snapshots,
        trades_paths=args.trades,
        symbol=args.symbol,
        out_path=args.out,
    )


if __name__ == "__main__":
    main()
