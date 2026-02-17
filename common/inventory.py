from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class HoldingLot:
    holding_type: str
    qty: int
    remaining_qty: int
    cost_price: float

@dataclass
class SymbolInventory:
    symbol: str
    lots: List[HoldingLot]
    total_qty: int
    total_remaining_qty: int
    weighted_cost: float

def is_nse_eq_or_bse_a(symbol: str) -> bool:
    s = (symbol or "").upper()
    if s.startswith("NSE:") and s.endswith("-EQ"):
        return True
    if s.startswith("BSE:") and (s.endswith("-A") or s.endswith("-EQ")):
        return True
    return False

def summarize_holdings(holdings_resp: Dict[str, Any]) -> Dict[str, SymbolInventory]:
    """Parse FYERS holdings response (defensive)."""
    holdings = holdings_resp.get("holdings") or holdings_resp.get("data") or []
    if isinstance(holdings, dict):
        holdings = list(holdings.values())

    by_symbol: Dict[str, List[HoldingLot]] = {}
    for h in holdings or []:
        if not isinstance(h, dict):
            continue
        sym = str(h.get("symbol") or "").strip()
        if not sym:
            continue
        holding_type = str(h.get("holdingType") or h.get("type") or "HLD").strip().upper()
        qty = int(float(h.get("quantity") or 0) or 0)
        remaining = int(float(h.get("remainingQuantity") or qty) or 0)
        cost = float(h.get("costPrice") or h.get("avgPrice") or 0.0)
        by_symbol.setdefault(sym, []).append(HoldingLot(holding_type=holding_type, qty=qty, remaining_qty=remaining, cost_price=cost))

    out: Dict[str, SymbolInventory] = {}
    for sym, lots in by_symbol.items():
        total_qty = sum(max(l.qty, 0) for l in lots)
        total_remaining = sum(max(l.remaining_qty, 0) for l in lots)
        # weighted by remaining (more relevant to sellable)
        denom = sum(max(l.remaining_qty, 0) for l in lots) or sum(max(l.qty, 0) for l in lots) or 0
        if denom <= 0:
            wcost = 0.0
        else:
            num = 0.0
            for l in lots:
                w = max(l.remaining_qty, 0) if sum(max(x.remaining_qty, 0) for x in lots) > 0 else max(l.qty, 0)
                num += float(w) * float(l.cost_price)
            wcost = num / denom
        out[sym] = SymbolInventory(
            symbol=sym,
            lots=lots,
            total_qty=int(total_qty),
            total_remaining_qty=int(total_remaining),
            weighted_cost=float(wcost),
        )
    return out

def compute_sellable_qty(inv: SymbolInventory, *, include_t_settled: bool) -> int:
    """Sellable quantity heuristic.

    If include_t_settled=True, count T0/T1/T2 as sellable (BTST).
    Else, count only HLD.
    Always uses remaining_qty (broker-provided).
    """
    sellable = 0
    for lot in inv.lots:
        t = lot.holding_type.upper()
        if t == "HLD":
            sellable += max(lot.remaining_qty, 0)
        elif include_t_settled and t in {"T0", "T1", "T2"}:
            sellable += max(lot.remaining_qty, 0)
        elif include_t_settled and t not in {"HLD"}:
            # Some brokers use other labels; if user opted in, include it
            sellable += max(lot.remaining_qty, 0)
    return int(max(sellable, 0))
