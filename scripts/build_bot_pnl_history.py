#!/usr/bin/env python3
"""
Build the bot's realized-P&L history from its own trade logs (accounts/<user>/<run>/state/
trades.jsonl). Unlike the broker tradebook (today-only), these hold the full history, so this
backfills the bot's realized P&L all the way back — per day, per symbol, per run.

Gross realized only (the bot doesn't track charges; those come from the broker reconciliation).
Writes accounts/<user>/reports/bot_pnl_history.json per user + prints a summary. Read-only.

    python scripts/build_bot_pnl_history.py            # all accounts
    python scripts/build_bot_pnl_history.py --account rahul
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from common.reporting.charges import compute_charges, mtf_interest

D0 = Decimal("0")


def _D(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _base_symbol(sym: str) -> str:
    """Merge same-stock, different-group/series symbols into one. Fyers symbols are
    EXCHANGE:NAME-SERIES (BSE groups B/X/T/XT…, NSE series EQ/BE/ST…). The same shares can move
    group over time (BSE:SHISHIND-B → BSE:SHISHIND-X), so we key P&L by EXCHANGE:NAME and drop
    the trailing -SERIES. Kept within an exchange (NSE vs BSE listings stay distinct)."""
    s = str(sym or "")
    return s.rsplit("-", 1)[0] if "-" in s else s


def _grid_replay(trades) -> float:
    """Flat-start LIFO + sell-first replay of one symbol's (side, qty, price) fills → the grid's
    OWN round-trip realized P&L, as if there were NO pre-existing base holding. This isolates the
    grid's buy↔sell cycles (each clears ~step) from the liquidation of adopted base inventory,
    which the bot's actual realized (seeded with the base) folds together."""
    lots = []            # [[qty, price], ...] LIFO
    bq = D0; bas = D0; rz = D0
    for side, qty, px in trades:
        q = _D(qty); px = _D(px)
        if q <= 0:
            continue
        if side == "SELL":
            traded = sum((lq for lq, _ in lots), D0)
            sft = min(q, traded)
            rem = sft
            while rem > 0 and lots:
                lq, lp = lots[-1]; take = rem if rem < lq else lq
                rz += take * (px - lp); lq -= take; rem -= take
                if lq <= 0:
                    lots.pop()
                else:
                    lots[-1] = [lq, lp]
            borrow = q - sft
            if borrow > 0:
                ob = bq; nb = ob + borrow
                if nb > 0:
                    bas = ((bas * ob) + px * borrow) / nb
                bq = nb
        else:  # BUY
            cover = min(q, bq)
            if cover > 0:
                rz += cover * (bas - px); bq -= cover
            remb = q - cover
            if remb > 0:
                lots.append([remb, px])
    return float(rz)


def scan_run(trades_file: Path) -> Dict[str, Any]:
    """Aggregate one run's trades.jsonl → per-day, per-symbol realized + fill counts."""
    by_day: Dict[str, Decimal] = defaultdict(lambda: D0)
    by_symbol: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"realized": D0, "n_fills": 0, "buy_qty": D0, "sell_qty": D0,
                 "first": None, "last": None})
    total = D0
    n = 0
    charge_trades: list = []
    eod_by_sym_day: Dict[str, Dict[str, float]] = defaultdict(dict)
    grid_trades: Dict[str, list] = defaultdict(list)
    for line in trades_file.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        if str(r.get("event")) != "FILL" or str(r.get("status")) not in ("FILLED", "PARTIAL", ""):
            # keep FILLED/PARTIAL fills; skip non-fill events
            if str(r.get("event")) != "FILL":
                continue
        ts = str(r.get("ts") or "")
        day = ts.split("T")[0]
        sym = _base_symbol(str(r.get("symbol") or ""))  # merge B/X/… group variants
        rd = _D(r.get("realized_delta"))
        qty = _D(r.get("qty"))
        side = str(r.get("side") or "")
        charge_trades.append({"symbol": sym, "side": side, "qty": float(qty),
                              "price": float(_D(r.get("price"))),
                              "value": float(_D(r.get("cum_quote_qty"))),
                              "date": day, "order_id": r.get("order_id")})
        by_day[day] += rd
        s = by_symbol[sym]
        s["realized"] += rd
        s["n_fills"] += 1
        s["buy_qty" if side == "BUY" else "sell_qty"] += qty
        s["first"] = ts if s["first"] is None else min(s["first"], ts)
        s["last"] = ts if s["last"] is None else max(s["last"], ts)
        # EOD carried-position value per symbol (last fill of the day wins; 0 when closed)
        # — for per-symbol MTF interest accrual.
        eod_by_sym_day[sym][day] = float(_D(r.get("traded_qty_after")) * _D(r.get("traded_avg_after")))
        grid_trades[sym].append((side, qty, _D(r.get("price"))))
        total += rd
        n += 1
    charges = compute_charges(charge_trades)
    return {"by_day": {k: float(v) for k, v in by_day.items()},
            "by_symbol": {k: {**{kk: (float(vv) if isinstance(vv, Decimal) else vv)
                                 for kk, vv in v.items()}} for k, v in by_symbol.items()},
            "total_realized": float(total), "n_fills": n,
            "charges": round(charges["totals"].get("total_charges", 0.0), 2),
            "charges_by_symbol": {s: round(v.get("total_charges", 0.0), 2)
                                  for s, v in charges["by_symbol"].items()},
            "grid_realized_by_symbol": {s: round(_grid_replay(t), 2) for s, t in grid_trades.items()},
            "grid_realized": round(sum(_grid_replay(t) for t in grid_trades.values()), 2),
            "eod_by_sym_day": {s: dict(v) for s, v in eod_by_sym_day.items()}}


def build_account(account_dir: Path) -> Dict[str, Any]:
    runs: Dict[str, Any] = {}
    acc_by_day: Dict[str, float] = defaultdict(float)
    acc_by_symbol: Dict[str, float] = defaultdict(float)
    acc_total = 0.0
    acc_charges = 0.0
    acc_mtf = 0.0
    acc_grid = 0.0
    today = dt.date.today().isoformat()
    for tf in sorted(account_dir.glob("*/state/trades.jsonl")):
        run = tf.parent.parent.name
        res = scan_run(tf)
        # MTF funding interest (only for leveraged runs). Read leverage from the run config.
        lev = 0.0
        try:
            cfg = json.loads((tf.parent.parent / "config.json").read_text())
            lev = float(cfg.get("execution", {}).get("mtf_leverage") or 0)
        except Exception:
            pass
        eod = res.pop("eod_by_sym_day", {})
        mtf_by_sym = {s: round(mtf_interest(days, lev, today), 2) for s, days in eod.items()}
        res["mtf_interest_by_symbol"] = {s: v for s, v in mtf_by_sym.items() if v}
        res["mtf_interest"] = round(sum(mtf_by_sym.values()), 2)
        res["net"] = round(res["total_realized"] - res["charges"] - res["mtf_interest"], 2)
        runs[run] = res
        for d, v in res["by_day"].items():
            acc_by_day[d] += v
        for sym, sv in res["by_symbol"].items():
            acc_by_symbol[sym] += sv["realized"]
        acc_total += res["total_realized"]
        acc_charges += res.get("charges", 0.0)
        acc_mtf += res.get("mtf_interest", 0.0)
        acc_grid += res.get("grid_realized", 0.0)
    return {"runs": runs,
            "by_day": dict(sorted(acc_by_day.items())),
            "by_symbol": dict(acc_by_symbol),
            "total_realized": round(acc_total, 2),
            "total_grid_realized": round(acc_grid, 2),
            "total_charges": round(acc_charges, 2),
            "total_mtf_interest": round(acc_mtf, 2),
            "net_realized": round(acc_total - acc_charges - acc_mtf, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--account", help="one account folder; default = all")
    args = ap.parse_args()

    accounts = ([REPO / "accounts" / args.account] if args.account
                else [p for p in (REPO / "accounts").iterdir()
                      if p.is_dir() and not p.name.startswith("_")])

    grand = 0.0
    for acc in sorted(accounts):
        if not acc.exists():
            print(f"  (no such account: {acc.name})")
            continue
        rep = build_account(acc)
        out = acc / "reports" / "bot_pnl_history.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rep, indent=2) + "\n")
        grand += rep["total_realized"]
        print(f"\n=== {acc.name} — realized {rep['total_realized']:,.2f} "
              f"({sum(r['n_fills'] for r in rep['runs'].values())} fills) ===")
        for sym, v in sorted(rep["by_symbol"].items()):
            print(f"    {sym:22s} {v:12,.2f}")
        days = rep["by_day"]
        if days:
            print(f"    span {min(days)} … {max(days)} over {len(days)} trading days")
    print(f"\n=== GRAND bot realized (all accounts): {grand:,.2f} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
