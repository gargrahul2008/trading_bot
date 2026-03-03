from __future__ import annotations
import dataclasses, os, json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

D0 = Decimal("0")

def to_decimal(x: Any, default: Decimal = D0) -> Decimal:
    if x is None:
        return default
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return default


def _dec(x: Any) -> Decimal:
    return to_decimal(x)

@dataclass
class SymbolState:
    initialized: bool = False
    reference_price: Optional[Decimal] = None
    core_qty: Decimal = D0
    traded_qty: Decimal = D0
    traded_avg_price: Decimal = D0
    realized_pnl: Decimal = D0
    pending_order_id: Optional[str] = None
    pending_reason: Optional[str] = None
    pending_since: Optional[str] = None  # UTC ISO
    last_mark_price: Optional[Decimal] = None
    borrowed_qty: Decimal = D0
    borrowed_avg_sell: Decimal = D0

@dataclass
class GlobalState:
    cash: Decimal = D0  # quote currency cash (INR for equities; USDT for USDT-spot crypto)
    symbol_states: Dict[str, SymbolState] = field(default_factory=dict)
    last_prices: Dict[str, Decimal] = field(default_factory=dict)
    last_update_ts: Optional[str] = None
    trades: List[Dict[str, Any]] = field(default_factory=list)

    # reject tracking / session fields (kept for compatibility)
    reject_events: List[str] = field(default_factory=list)
    cooldown_until: Optional[str] = None
    halted_until: Optional[str] = None
    halt_reason: Optional[str] = None
    last_eod_cancel_date: Optional[str] = None
    session_date: Optional[str] = None

    # free-form runtime metrics (portfolio_value, balances snapshot, etc.)
    extras: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str) -> "GlobalState":
        if not os.path.exists(path):
            return cls()
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        sym_states: Dict[str, SymbolState] = {}
        for sym, ss in (raw.get("symbol_states") or {}).items():
            if not isinstance(ss, dict):
                continue
            sym_states[str(sym)] = SymbolState(
                initialized=bool(ss.get("initialized", False)),
                reference_price=to_decimal(ss.get("reference_price")) if ss.get("reference_price") is not None else None,
                core_qty=to_decimal(ss.get("core_qty", "0")),
                traded_qty=to_decimal(ss.get("traded_qty", "0")),
                traded_avg_price=to_decimal(ss.get("traded_avg_price", "0")),
                realized_pnl=to_decimal(ss.get("realized_pnl", "0")),
                pending_order_id=ss.get("pending_order_id"),
                pending_reason=ss.get("pending_reason"),
                pending_since=ss.get("pending_since"),
                last_mark_price=to_decimal(ss.get("last_mark_price")) if ss.get("last_mark_price") is not None else None,
            )

        last_prices = {k: to_decimal(v) for k, v in (raw.get("last_prices") or {}).items()}
        cash = to_decimal(raw.get("cash", "0"))

        return cls(
            cash=cash,
            symbol_states=sym_states,
            last_prices=last_prices,
            last_update_ts=raw.get("last_update_ts"),
            trades=list(raw.get("trades", [])),
            reject_events=list(raw.get("reject_events", [])),
            cooldown_until=raw.get("cooldown_until"),
            halted_until=raw.get("halted_until"),
            halt_reason=raw.get("halt_reason"),
            last_eod_cancel_date=raw.get("last_eod_cancel_date"),
            session_date=raw.get("session_date"),
            extras=dict(raw.get("extras") or {}),
        )

    def dump(self, path: str) -> None:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dataclasses.asdict(self), f, indent=2, default=str)
        os.replace(tmp, path)

    def ensure_symbols(self, symbols: list[str]) -> None:
        for s in symbols:
            self.symbol_states.setdefault(s, SymbolState())

    def exposure(self) -> Decimal:
        exp = D0
        for sym, ss in self.symbol_states.items():
            px = self.last_prices.get(sym) or ss.last_mark_price
            if px is None:
                continue
            px = _dec(px)
            net_qty = _dec(ss.traded_qty) - _dec(getattr(ss, "borrowed_qty", D0))
            exp += abs(net_qty) * px
        return _dec(exp)

    def strategy_equity(self) -> Decimal:
        eq = _dec(self.cash)
        for sym, ss in self.symbol_states.items():
            px = self.last_prices.get(sym) or ss.last_mark_price
            if px is None:
                continue
            px = _dec(px)
            net_qty = _dec(ss.traded_qty) - _dec(getattr(ss, "borrowed_qty", D0))
            eq += net_qty * px
        return _dec(eq)

    def total_realized(self) -> Decimal:
        return sum((ss.realized_pnl for ss in self.symbol_states.values()), D0)

    def total_unrealized(self) -> Decimal:
        total = D0
        for sym, ss in self.symbol_states.items():
            px = self.last_prices.get(sym) or ss.last_mark_price
            if px is None:
                continue
            px = _dec(px)

            traded_qty = _dec(ss.traded_qty)
            borrowed_qty = _dec(getattr(ss, "borrowed_qty", D0))

            # long unrealized
            if traded_qty > 0:
                total += traded_qty * (px - _dec(ss.traded_avg_price))

            # short/buffer unrealized (sold-first)
            if borrowed_qty > 0:
                total += borrowed_qty * (_dec(getattr(ss, "borrowed_avg_sell", D0)) - px)

        return _dec(total)
