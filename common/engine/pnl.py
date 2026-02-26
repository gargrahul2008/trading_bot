from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from common.broker.interfaces import Broker, to_decimal
from common.utils.logger import setup_logger

LOG = setup_logger("pnl")

D0 = Decimal("0")

def _dec(x: Any) -> Decimal:
    return to_decimal(x)

@dataclass
class PnLPoint:
    ts: str
    broker: str
    quote_asset: str
    portfolio_value: Decimal
    portfolio_pnl: Decimal
    portfolio_pnl_pct: Decimal
    strategy_equity: Decimal
    strategy_realized: Decimal
    strategy_unrealized: Decimal
    strategy_total: Decimal
    drawdown_pct: Decimal
    exposure: Decimal
    exposure_pct: Decimal

class PnLWriter:
    """Stage-1 persistence:
    - Append pnl_points.csv
    - Write positions_snapshot.json
    - Write pnl_summary.json (latest)
    """
    def __init__(self, *, csv_path: str, snapshot_path: str, summary_path: Optional[str] = None):
        self.csv_path = csv_path
        self.snapshot_path = snapshot_path
        self.summary_path = summary_path

        os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(snapshot_path) or ".", exist_ok=True)
        if summary_path:
            os.makedirs(os.path.dirname(summary_path) or ".", exist_ok=True)

        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", encoding="utf-8") as f:
                f.write(",".join([
                    "ts","broker","quote_asset",
                    "portfolio_value","portfolio_pnl","portfolio_pnl_pct",
                    "strategy_equity","strategy_realized","strategy_unrealized","strategy_total",
                    "drawdown_pct","exposure","exposure_pct"
                ]) + "\n")

    def append(self, pt: PnLPoint) -> None:
        with open(self.csv_path, "a", encoding="utf-8") as f:
            f.write(",".join([
                pt.ts, pt.broker, pt.quote_asset,
                str(pt.portfolio_value), str(pt.portfolio_pnl), str(pt.portfolio_pnl_pct),
                str(pt.strategy_equity), str(pt.strategy_realized), str(pt.strategy_unrealized), str(pt.strategy_total),
                str(pt.drawdown_pct), str(pt.exposure), str(pt.exposure_pct),
            ]) + "\n")

    def write_snapshot(self, snapshot: Dict[str, Any]) -> None:
        tmp = self.snapshot_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, default=str)
        os.replace(tmp, self.snapshot_path)

    def write_summary(self, summary: Dict[str, Any]) -> None:
        if not self.summary_path:
            return
        tmp = self.summary_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        os.replace(tmp, self.summary_path)

def infer_broker_name(broker: Broker) -> str:
    n = broker.__class__.__name__.lower()
    if "mexc" in n:
        return "mexc_spot"
    if "fyers" in n:
        return "fyers"
    return n

def compute_portfolio_value_for_symbols(
    broker: Broker,
    symbols: List[str],
    prices: Dict[str, Decimal],
    state,
) -> Tuple[Decimal, str, Dict[str, Any]]:
    """Account/portfolio value scoped to strategy symbols.
    Crypto: quote_total + Σ(base_total*px)
    Equities: cash + Σ(total_qty(symbol)*px), where total_qty from holdings/positions.
    """
    details: Dict[str, Any] = {}

    # Crypto path: balances exists and is non-empty
    bals = {}
    try:
        bals = broker.balances() or {}
    except Exception:
        bals = {}

    if bals:
        quote_assets = set()
        base_by_sym: Dict[str, str] = {}
        for sym in symbols:
            base = None
            quote = None
            try:
                info = getattr(broker, "symbol_info")(sym)
                base = info.base_asset
                quote = info.quote_asset
            except Exception:
                if sym.endswith("USDC"):
                    base, quote = sym[:-4], "USDC"
                elif sym.endswith("USDT"):
                    base, quote = sym[:-4], "USDT"
            if quote:
                quote_assets.add(quote)
            if base:
                base_by_sym[sym] = base

        quote_asset = None
        if len(quote_assets) == 1:
            quote_asset = next(iter(quote_assets))
        else:
            q = state.extras.get("quote_asset")
            quote_asset = str(q) if q else "USDT"

        qfree = _dec((bals.get(quote_asset) or {}).get("free"))
        qlock = _dec((bals.get(quote_asset) or {}).get("locked"))
        quote_total = qfree + qlock

        total = quote_total
        sym_details = {}
        for sym in symbols:
            px = _dec(prices.get(sym) or 0)
            base = base_by_sym.get(sym)
            if not base:
                continue
            bfree = _dec((bals.get(base) or {}).get("free"))
            block = _dec((bals.get(base) or {}).get("locked"))
            btotal = bfree + block
            total += btotal * px
            sym_details[sym] = {"base": base, "base_total": str(btotal), "px": str(px)}

        details["quote_total"] = str(quote_total)
        details["per_symbol"] = sym_details
        return total, quote_asset, details

    # Equities path
    quote_asset = "INR"
    cash = D0
    try:
        cash = _dec(broker.funds_cash())
    except Exception:
        cash = _dec(getattr(state, "cash", D0))
    total = cash

    holdings_qty: Dict[str, Decimal] = {}
    try:
        for lot in broker.holdings():
            if lot.symbol in symbols:
                holdings_qty[lot.symbol] = holdings_qty.get(lot.symbol, D0) + _dec(lot.remaining_qty)
    except Exception:
        pass

    pos_qty: Dict[str, Decimal] = {}
    try:
        for pos in broker.positions():
            if pos.symbol in symbols:
                pos_qty[pos.symbol] = _dec(pos.net_qty)
    except Exception:
        pass

    sym_details = {}
    for sym in symbols:
        px = _dec(prices.get(sym) or 0)
        hq = holdings_qty.get(sym, D0)
        pq = pos_qty.get(sym, D0)
        tq = pq if pq > hq else hq  # avoid double count
        total += tq * px
        sym_details[sym] = {"qty_holdings": str(hq), "qty_positions": str(pq), "qty_used": str(tq), "px": str(px)}

    details["cash"] = str(cash)
    details["per_symbol"] = sym_details
    return total, quote_asset, details

def compute_strategy_pnl(state) -> Tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
    se = _dec(state.strategy_equity())
    realized = _dec(state.total_realized()) if hasattr(state, "total_realized") else D0
    unreal = _dec(state.total_unrealized()) if hasattr(state, "total_unrealized") else D0
    total = realized + unreal
    exposure = _dec(state.exposure()) if hasattr(state, "exposure") else D0
    exp_pct = (exposure / se) if se > 0 else D0
    return se, realized, unreal, total, exposure, exp_pct

def update_drawdown(state, portfolio_value: Decimal) -> Decimal:
    peak = _dec(state.extras.get("pnl_peak_portfolio") or "0")
    max_dd = _dec(state.extras.get("pnl_max_dd") or "0")
    if peak <= 0:
        peak = portfolio_value
    if portfolio_value > peak:
        peak = portfolio_value
    dd = (peak - portfolio_value) / peak if peak > 0 else D0
    if dd > max_dd:
        max_dd = dd
    state.extras["pnl_peak_portfolio"] = str(peak)
    state.extras["pnl_max_dd"] = str(max_dd)
    return dd

def ensure_portfolio_start(state, portfolio_value: Decimal) -> Decimal:
    start = state.extras.get("portfolio_start_value")
    if start is None:
        state.extras["portfolio_start_value"] = str(portfolio_value)
        return portfolio_value
    try:
        return Decimal(str(start))
    except Exception:
        state.extras["portfolio_start_value"] = str(portfolio_value)
        return portfolio_value
