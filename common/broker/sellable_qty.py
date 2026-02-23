from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Tuple
from common.broker.interfaces import HoldingLot, Position

D0 = Decimal("0")

@dataclass
class SellableBreakdown:
    pos_long: Decimal
    settled_holdings: Decimal
    t1_holdings: Decimal
    pending_sell: Decimal
    btst_eligible: bool

    @property
    def total_sellable_conservative(self) -> Decimal:
        return max(self.pos_long + self.settled_holdings - self.pending_sell, D0)

    @property
    def total_sellable_btst(self) -> Decimal:
        return max(self.pos_long + self.settled_holdings + self.t1_holdings - self.pending_sell, D0)

def is_btst_eligible(symbol: str) -> bool:
    s = symbol.upper().strip()
    if s.startswith("NSE:") and s.endswith("-EQ"):
        return True
    if s.startswith("BSE:") and s.endswith("-A"):
        return True
    return False

def _group_holdings(holdings: List[HoldingLot]) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    by_sym: Dict[str, List[HoldingLot]] = {}
    for h in holdings:
        by_sym.setdefault(h.symbol, []).append(h)

    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    for sym, lots in by_sym.items():
        has_explicit_t1 = any((l.holding_type or "").upper() == "T1" for l in lots)
        settled = D0
        t1 = D0
        cost_num = D0
        cost_den = D0

        if has_explicit_t1:
            for l in lots:
                ht = (l.holding_type or "").upper()
                rq = Decimal(l.remaining_qty)
                if rq <= 0:
                    continue
                if ht == "T1":
                    t1 += rq
                else:
                    settled += rq
                if l.cost_price > 0:
                    cost_num += l.cost_price * rq
                    cost_den += rq
        else:
            for l in lots:
                rq = Decimal(l.remaining_qty)
                if rq <= 0:
                    continue
                raw = l.raw or {}
                qt1 = raw.get("qty_t1") or raw.get("t1_quantity") or 0
                try:
                    qt1 = Decimal(str(qt1))
                except Exception:
                    qt1 = D0
                qt1 = max(D0, min(qt1, rq))
                t1 += qt1
                settled += (rq - qt1)
                if l.cost_price > 0:
                    cost_num += l.cost_price * rq
                    cost_den += rq

        w_cost = (cost_num / cost_den) if cost_den > 0 else D0
        out[sym] = (settled, t1, w_cost)
    return out

def compute_sellable_qty(
    symbol: str,
    *,
    positions: List[Position],
    holdings: List[HoldingLot],
    pending_sell_qty: Decimal,
    allow_btst_auto: bool,
) -> Tuple[Decimal, SellableBreakdown, Decimal]:
    sym = symbol
    pos_long = D0
    for p in positions:
        if p.symbol == sym:
            pos_long = max(Decimal(p.net_qty), D0)
            break

    grouped = _group_holdings(holdings)
    settled, t1, w_cost = grouped.get(sym, (D0, D0, D0))
    btst_ok = is_btst_eligible(sym) if allow_btst_auto else False
    bd = SellableBreakdown(
        pos_long=pos_long,
        settled_holdings=settled,
        t1_holdings=(t1 if btst_ok else D0),
        pending_sell=Decimal(pending_sell_qty),
        btst_eligible=btst_ok,
    )
    total = bd.total_sellable_btst if btst_ok else bd.total_sellable_conservative
    return total, bd, w_cost
