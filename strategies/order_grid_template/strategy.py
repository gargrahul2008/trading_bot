from __future__ import annotations
from decimal import Decimal
from typing import Dict, List
from common.engine.strategy_base import OrderAction
from common.broker.interfaces import PlaceOrderRequest

# This is a TEMPLATE for proactive (managed) strategies.
#
# The managed runner calls desired_actions() every tick, and gives you:
# - latest prices
# - current open orders (raw FYERS orderbook records)
# - state object
#
# You return a list of actions:
# - PLACE: provide a PlaceOrderRequest
# - CANCEL: provide order_id
#
# NOTE: Modify order isn't implemented in this base; you can cancel+place.

class OrderGridTemplate:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.symbols = cfg.get("symbols") or []

    def desired_actions(self, prices: Dict[str, float], open_orders: List[dict], state, now_ts: str) -> List[OrderAction]:
        actions: List[OrderAction] = []
        # Example: Ensure exactly one limit BUY order exists below market for each symbol
        for sym in self.symbols:
            px = float(prices[sym])
            target = px * 0.99
            # check if an open BUY exists
            has_buy = False
            for o in open_orders:
                if str(o.get("symbol") or o.get("tradingSymbol") or "") != sym:
                    continue
                status = str(o.get("status") or o.get("orderStatus") or "").upper()
                if status in {"TRADED","FILLED","COMPLETE","REJECTED","CANCELLED","CANCELED"}:
                    continue
                side = o.get("side")
                if side in (1, "1", "BUY", "B"):
                    has_buy = True
                    break
            if not has_buy:
                actions.append(OrderAction(
                    kind="PLACE",
                    request=PlaceOrderRequest(
                        symbol=sym,
                        side="BUY",
                        qty=Decimal(str(self.cfg.get("qty", 1))),
                        product_type=str(self.cfg.get("product_type","CNC")),
                        order_type="LIMIT",
                        limit_price=float(target),
                    ),
                    reason="seed_grid_buy"
                ))
        return actions

def create_strategy(strategy_cfg: dict) -> OrderGridTemplate:
    return OrderGridTemplate(strategy_cfg)
