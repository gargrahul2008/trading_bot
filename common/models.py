from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

Side = Literal["BUY", "SELL"]

@dataclass
class OrderAction:
    symbol: str
    side: Side
    qty: int
    reason: str
    product_type: str = "CNC"

@dataclass
class FillEvent:
    order_id: str
    symbol: str
    side: Side
    filled_qty: int
    avg_price: float
    status: str
    message: str
    ts_utc: str
    raw: Dict[str, Any] = field(default_factory=dict)

@dataclass
class StrategyContext:
    ts_utc: str
    prices: Dict[str, float]
    sellable_qty: Dict[str, int]
    holdings_cost: Dict[str, float]
    cash: float
