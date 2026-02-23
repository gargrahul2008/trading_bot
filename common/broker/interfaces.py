from __future__ import annotations
import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Protocol, runtime_checkable

Side = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT"]
ProductType = Literal["CNC", "INTRADAY", "MARGIN"]

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

@dataclass
class PlaceOrderRequest:
    """Cross-broker order request.
    - qty is base-asset quantity (Decimal). For equities it must be an integer value.
    - quote_qty is optional (Decimal) and may be supported for MARKET buys on some crypto venues.
    """
    symbol: str
    side: Side
    qty: Decimal

    # Equity-only (FYERS) fields
    product_type: str = "CNC"

    order_type: OrderType = "MARKET"
    limit_price: Decimal = D0
    time_in_force: str = "GTC"   # crypto typical; FYERS ignores
    validity: str = "DAY"        # FYERS typical; crypto ignores
    disclosed_qty: int = 0
    offline_order: bool = False

    # Optional quote-asset spend/receive (crypto MARKET buys often support this)
    quote_qty: Optional[Decimal] = None

    raw: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OrderTerminal:
    order_id: str
    symbol: str
    side: Side
    status: str  # FILLED/REJECTED/CANCELLED (or broker-specific mapped)
    filled_qty: Decimal = D0           # base executed qty
    avg_price: Decimal = D0            # avg execution price (quote/base)
    cum_quote_qty: Decimal = D0        # total quote spent/received
    message: str = ""
    ts: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    raw: Dict[str, Any] = field(default_factory=dict)

@dataclass
class HoldingLot:
    symbol: str
    holding_type: str  # HLD/T1 for equities, or "" for crypto
    remaining_qty: Decimal
    cost_price: Decimal = D0
    raw: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Position:
    symbol: str
    net_qty: Decimal
    avg_price: Decimal = D0
    raw: Dict[str, Any] = field(default_factory=dict)

class BrokerError(RuntimeError):
    def __init__(self, message: str, *, resp: Any = None):
        super().__init__(message)
        self.resp = resp

class RetryableError(BrokerError):
    pass

@runtime_checkable
class Broker(Protocol):
    def get_ltps(self, symbols: List[str]) -> Dict[str, Decimal]: ...
    def place_order(self, req: PlaceOrderRequest) -> str: ...
    def orderbook(self) -> Dict[str, Any]: ...
    def cancel_order(self, order_id: str) -> Dict[str, Any]: ...
    def get_order_terminal(self, order_id: str) -> Optional[OrderTerminal]: ...

    # Equities-style inventory (FYERS)
    def positions(self) -> List[Position]: ...
    def holdings(self) -> List[HoldingLot]: ...
    def funds_cash(self) -> Decimal: ...

    # Crypto-style inventory (optional)
    def balances(self) -> Dict[str, Dict[str, Decimal]]: ...

    def history(self, data: Dict[str, Any]) -> Dict[str, Any]: ...
