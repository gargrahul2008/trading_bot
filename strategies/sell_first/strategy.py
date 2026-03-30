from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

from common.broker.interfaces import to_decimal

D0 = Decimal("0")


def _dec(x: Any) -> Decimal:
    return to_decimal(x)


def _round_tick(px: Decimal, tick: Decimal) -> Decimal:
    if tick <= D0:
        return px
    return (px / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def _calc_disclosed_qty(qty: int, lot_size: int) -> int:
    """Min disclosed qty allowed by NSE = 10% of total, rounded UP to nearest lot_size."""
    if lot_size < 1:
        lot_size = 1
    raw = math.ceil(qty * 0.1)
    return max(lot_size, math.ceil(raw / lot_size) * lot_size)


@dataclass
class SymbolConfig:
    symbol: str
    range_type: str       # "pct" or "abs"
    range_value: Decimal
    qty: int
    lot_size: int         # 1 for regular NSE, >1 for SME
    price_tick: Decimal

    @property
    def disclosed_qty(self) -> int:
        return _calc_disclosed_qty(self.qty, self.lot_size)

    def compute_levels(self, prev_close: Decimal) -> Tuple[Decimal, Decimal]:
        """Returns (sell_level, buy_level) from prev_close."""
        if self.range_type == "pct":
            sell = _round_tick(prev_close * (Decimal("1") + self.range_value / Decimal("100")), self.price_tick)
            buy  = _round_tick(prev_close * (Decimal("1") - self.range_value / Decimal("100")), self.price_tick)
        else:  # abs
            sell = _round_tick(prev_close + self.range_value, self.price_tick)
            buy  = _round_tick(prev_close - self.range_value, self.price_tick)
        return sell, buy


class SellFirstStrategy:
    """
    Proactive sell-first strategy based on prev-close levels.
    - All symbols assumed to be in holdings.
    - Each day: SELL at prev_close + range. After fill: BUY at prev_close - range.
    - After BUY fills: place SELL again. One order per symbol at a time.
    - Levels reset each morning using fresh prev_close.
    """

    def __init__(self, cfg: Dict[str, Any]):
        raw = cfg.get("symbols") or []
        self.sym_cfgs: Dict[str, SymbolConfig] = {}
        self.symbols: List[str] = []
        self.lookback_days: int = int(cfg.get("anchor_lookback_days") or 10)

        for item in raw:
            if isinstance(item, str):
                raise ValueError(
                    f"sell_first strategy requires symbols as list of dicts, got string: {item!r}. "
                    "Use [{\"symbol\": \"NSE:FOO-EQ\", \"range_type\": \"pct\", ...}]"
                )
            sym = str(item.get("symbol") or "")
            if not sym:
                continue
            rtype    = str(item.get("range_type") or "pct").lower()
            rval     = _dec(item.get("range_value") or "1")
            qty      = int(item.get("qty") or 0)
            lot_size = max(1, int(item.get("lot_size") or 1))
            tick     = _dec(item.get("price_tick") or "0.05")
            self.sym_cfgs[sym] = SymbolConfig(
                symbol=sym,
                range_type=rtype,
                range_value=rval,
                qty=qty,
                lot_size=lot_size,
                price_tick=tick,
            )
            self.symbols.append(sym)

    def get(self, symbol: str) -> Optional[SymbolConfig]:
        return self.sym_cfgs.get(symbol)


def create_strategy(strategy_cfg: dict) -> SellFirstStrategy:
    return SellFirstStrategy(strategy_cfg)
