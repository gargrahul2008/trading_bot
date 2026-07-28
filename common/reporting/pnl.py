"""
P&L aggregation across the accounts/ layout — strategy level and user level.

Pure reader: reads each run's local trades.jsonl (what the bot recorded) and, if present,
the cached broker report written by scripts/fetch_broker_pnl.py (what Fyers actually charged).
No broker calls, no network — safe to run anywhere (e.g. the dashboard on the master host).

Two P&L views per strategy:
  - local  : GROSS realized from the bot's own fills (sum of realized_delta).
  - broker : broker-truth realized + charges → NET, and a reconciliation delta vs local.

Because each strategy trades a distinct symbol, broker per-symbol data attributes back to the
strategy by symbol.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[2]
ACCOUNTS = REPO / "accounts"
D0 = Decimal("0")


def _dec(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _load_json(p: Path) -> Optional[dict]:
    try:
        return json.loads(p.read_text()) if p.exists() else None
    except Exception:
        return None


@dataclass
class StrategyPnL:
    user: str
    strategy: str
    symbol: str = ""
    # local (from trades.jsonl)
    local_realized: Decimal = D0        # sum of realized_delta (gross)
    n_fills: int = 0
    buy_qty: Decimal = D0
    sell_qty: Decimal = D0
    avg_slippage_bps: Optional[Decimal] = None
    last_trade_ts: Optional[str] = None
    # broker (from cached broker report), if available
    broker_realized: Optional[Decimal] = None
    charges: Optional[Decimal] = None
    net_realized: Optional[Decimal] = None
    reconcile_delta: Optional[Decimal] = None   # broker_realized - local_realized

    def as_row(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in d.items()}


def _strategy_symbol(config: dict) -> str:
    syms = ((config or {}).get("strategy") or {}).get("symbols") or []
    if syms and isinstance(syms[0], dict):       # sell_first style [{symbol:...}]
        return str(syms[0].get("symbol") or "")
    return str(syms[0]) if syms else ""


def _read_local(run_dir: Path, sp: StrategyPnL) -> None:
    trades = run_dir / "state" / "trades.jsonl"
    if not trades.exists():
        return
    slips: List[Decimal] = []
    with trades.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("event") != "FILL":
                continue
            qty = _dec(rec.get("qty"))
            if qty <= 0:
                continue
            sp.n_fills += 1
            sp.local_realized += _dec(rec.get("realized_delta"))
            if (rec.get("side") or "").upper() == "BUY":
                sp.buy_qty += qty
            else:
                sp.sell_qty += qty
            if rec.get("slippage_bps") is not None:
                slips.append(_dec(rec.get("slippage_bps")))
            sp.last_trade_ts = rec.get("ts") or sp.last_trade_ts
    if slips:
        sp.avg_slippage_bps = sum(slips) / Decimal(len(slips))


def _merge_broker(account_report: Optional[dict], sp: StrategyPnL) -> None:
    if not account_report:
        return
    by_symbol = ((account_report.get("normalized") or {}).get("by_symbol")) or {}
    row = by_symbol.get(sp.symbol)
    if not row:
        return
    if "broker_realized" in row:
        sp.broker_realized = _dec(row["broker_realized"])
        sp.reconcile_delta = sp.broker_realized - sp.local_realized
    if "charges_apportioned" in row:
        sp.charges = _dec(row["charges_apportioned"])
    if "net_realized" in row:
        sp.net_realized = _dec(row["net_realized"])


def build_report(accounts_dir: Path = ACCOUNTS) -> List[StrategyPnL]:
    """One StrategyPnL per run, across all accounts. Skips `_template` and non-account dirs."""
    out: List[StrategyPnL] = []
    if not accounts_dir.exists():
        return out
    for user_dir in sorted(accounts_dir.iterdir()):
        if not user_dir.is_dir() or user_dir.name.startswith("_"):
            continue
        user = user_dir.name
        broker_report = _load_json(user_dir / "reports" / "broker_pnl.json")
        for run_dir in sorted(user_dir.iterdir()):
            if not run_dir.is_dir() or not (run_dir / "config.json").exists():
                continue
            cfg = _load_json(run_dir / "config.json") or {}
            sp = StrategyPnL(user=user, strategy=run_dir.name, symbol=_strategy_symbol(cfg))
            _read_local(run_dir, sp)
            _merge_broker(broker_report, sp)
            out.append(sp)
    return out


def user_totals(rows: List[StrategyPnL]) -> Dict[str, Dict[str, Decimal]]:
    """Roll strategy rows up to per-user subtotals (+ a grand total under key '')."""
    agg: Dict[str, Dict[str, Decimal]] = {}
    for sp in rows:
        for key in (sp.user, ""):  # '' = grand total
            t = agg.setdefault(key, {"local_realized": D0, "broker_realized": D0,
                                     "charges": D0, "net_realized": D0, "n_fills": D0})
            t["local_realized"] += sp.local_realized
            t["broker_realized"] += sp.broker_realized or D0
            t["charges"] += sp.charges or D0
            t["net_realized"] += sp.net_realized if sp.net_realized is not None else sp.local_realized
            t["n_fills"] += Decimal(sp.n_fills)
    return agg


if __name__ == "__main__":  # quick CLI table
    rows = build_report()
    for sp in rows:
        print(f"{sp.user:10} {sp.strategy:10} {sp.symbol:16} "
              f"local={sp.local_realized} net={sp.net_realized} "
              f"charges={sp.charges} fills={sp.n_fills} recon={sp.reconcile_delta}")
    print("--- user totals ---")
    for user, t in user_totals(rows).items():
        print(f"{user or 'TOTAL':10} local={t['local_realized']} net={t['net_realized']} charges={t['charges']}")
