from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Protocol, runtime_checkable
from common.broker.interfaces import Side, PlaceOrderRequest

D0 = Decimal("0")

@dataclass
class OrderIntent:
    symbol: str
    side: Side
    qty: Decimal
    reason: str
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price: Decimal = D0

@dataclass
class OrderAction:
    kind: Literal["PLACE", "CANCEL"]
    request: Optional[PlaceOrderRequest] = None
    order_id: Optional[str] = None
    reason: str = ""
    meta: Optional[Dict[str, Any]] = None

@runtime_checkable
class ReactiveStrategy(Protocol):
    def on_prices(self, prices: Dict[str, Decimal], state, now_ts: str) -> List[OrderIntent]: ...

@runtime_checkable
class ManagedOrderStrategy(Protocol):
    def desired_actions(self, prices: Dict[str, Decimal], open_orders: List[dict], state, now_ts: str) -> List[OrderAction]: ...
