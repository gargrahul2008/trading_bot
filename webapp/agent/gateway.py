"""The agent's broker surface: raw payloads in, normalised rows out.

Kept apart from the poller so the polling schedule can be tested without a
broker, and so every place that reshapes a Fyers response lives in one file.

Normalisation is deliberately additive — each row keeps the broker's original
dict under `raw`. Fyers changes field names between products (a position uses
`netQty`, a holding `remainingQuantity`), and a dashboard that silently drops a
field it did not recognise is worse than one that shows it unparsed.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from common.broker.interfaces import PlaceOrderRequest, to_decimal

# Fyers order status codes, as used in generic_runner.py:2460.
STATUS_CANCELLED = 1
STATUS_FILLED = 2
STATUS_REJECTED = 5
STATUS_PENDING = 6
TERMINAL_STATUSES = (STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED)

STATUS_NAMES = {
    STATUS_CANCELLED: "CANCELLED",
    STATUS_FILLED: "FILLED",
    3: "TRANSIT",
    4: "OPEN",
    STATUS_REJECTED: "REJECTED",
    STATUS_PENDING: "PENDING",
}

# Fyers productType strings, mapped to the distinction that actually matters on
# the dashboard: does this position survive the close?
POSITIONAL_PRODUCTS = ("CNC", "MARGIN", "MTF")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _first(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def product_kind(product_type: str) -> str:
    """intraday | positional. Fyers stamps productType on every order and
    position, so this is the broker's own classification, not a guess."""
    return "positional" if str(product_type or "").upper() in POSITIONAL_PRODUCTS else "intraday"


def normalise_order(row: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    status = _first(row, "status")
    try:
        status_code = int(status)
    except (TypeError, ValueError):
        status_code = None

    product = str(_first(row, "productType", "product_type") or "")
    side_raw = _first(row, "side")
    try:
        side = "BUY" if int(side_raw) > 0 else "SELL"
    except (TypeError, ValueError):
        side = str(side_raw or "").upper()

    qty = _f(_first(row, "qty", "quantity"))
    filled = _f(_first(row, "filledQty", "filled_qty", "tradedQty"))

    return {
        "order_id": str(_first(row, "id", "orderNumber", "order_id") or ""),
        "symbol": str(_first(row, "symbol", "tradingSymbol") or ""),
        "side": side,
        "qty": qty,
        "filled_qty": filled,
        "remaining_qty": max(qty - filled, 0.0),
        "limit_price": _f(_first(row, "limitPrice", "limit_price")),
        "stop_price": _f(_first(row, "stopPrice", "stop_price")),
        "traded_price": _f(_first(row, "tradedPrice", "traded_price")),
        "product_type": product,
        "kind": product_kind(product),
        "status_code": status_code,
        "status": STATUS_NAMES.get(status_code, str(status)),
        "is_open": status_code not in TERMINAL_STATUSES if status_code is not None else False,
        "placed_at": _first(row, "orderDateTime", "order_date_time"),
        "epoch": _f(_first(row, "orderNumStatus", "epoch"), default=0.0) or None,
        "message": str(_first(row, "message") or ""),
        "raw": row,
    }


def normalise_position(row: Dict[str, Any]) -> Dict[str, Any]:
    """Positions carry their own LTP and unrealised P&L, which is why the agent
    never polls quotes separately — see webapp/agent/README.md."""
    net = _f(_first(row, "netQty", "net_qty", "qty", "quantity"))
    product = str(_first(row, "productType", "product_type") or "")
    return {
        "position_id": str(_first(row, "id") or ""),
        "symbol": str(_first(row, "symbol", "tradingSymbol") or ""),
        "net_qty": net,
        "direction": "LONG" if net > 0 else ("SHORT" if net < 0 else "FLAT"),
        "avg_price": _f(_first(row, "avgPrice", "netAvg", "averagePrice", "buyAvg")),
        "ltp": _f(_first(row, "ltp", "lastPrice")),
        "unrealised": _f(_first(row, "unrealized_profit", "unrealizedProfit", "pl")),
        "realised": _f(_first(row, "realized_profit", "realizedProfit")),
        "buy_qty": _f(_first(row, "buyQty", "buy_qty")),
        "sell_qty": _f(_first(row, "sellQty", "sell_qty")),
        "product_type": product,
        "kind": product_kind(product),
        "raw": row,
    }


def normalise_holding(row: Dict[str, Any]) -> Dict[str, Any]:
    qty = _f(_first(row, "remainingQuantity", "quantity", "qty"))
    cost = _f(_first(row, "costPrice", "buyPrice"))
    ltp = _f(_first(row, "ltp", "lastPrice"))
    return {
        "symbol": str(_first(row, "symbol") or ""),
        "qty": qty,
        "cost_price": cost,
        "ltp": ltp,
        "market_value": _f(_first(row, "marketVal", "market_value")) or (qty * ltp),
        "invested": qty * cost,
        "unrealised": _f(_first(row, "pl")) or (qty * (ltp - cost) if ltp else 0.0),
        "holding_type": str(_first(row, "holdingType", "type") or ""),
        "raw": row,
    }


def normalise_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    qty = _f(_first(row, "tradedQty", "qty", "quantity"))
    price = _f(_first(row, "tradePrice", "tradedPrice", "price"))
    side_raw = _first(row, "side")
    try:
        side = "BUY" if int(side_raw) > 0 else "SELL"
    except (TypeError, ValueError):
        side = str(side_raw or "").upper()
    product = str(_first(row, "productType", "product_type") or "")
    return {
        "trade_id": str(_first(row, "id", "tradeNumber", "orderNumber") or ""),
        "order_id": str(_first(row, "orderNumber", "order_id", "id") or ""),
        "symbol": str(_first(row, "symbol", "tradingSymbol") or ""),
        "side": side,
        "qty": qty,
        "price": price,
        "value": _f(_first(row, "tradeValue", "orderValue")) or (qty * price),
        "product_type": product,
        "kind": product_kind(product),
        "traded_at": _first(row, "orderDateTime", "tradeDateTime"),
        "raw": row,
    }


def summarise_funds(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten Fyers' `fund_limit` list into the handful of figures the
    dashboard shows, keeping the whole list alongside."""
    out = {"available": 0.0, "total": 0.0, "utilised": 0.0, "realised_pnl": 0.0, "raw": payload}
    rows = payload.get("fund_limit")
    if not isinstance(rows, list):
        return out
    wanted = {
        "AVAILABLE BALANCE": "available",
        "TOTAL BALANCE": "total",
        "UTILIZED AMOUNT": "utilised",
        "REALIZED PROFIT AND LOSS": "realised_pnl",
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(_first(row, "title", "name") or "").strip().upper()
        key = wanted.get(title)
        if key:
            out[key] = _f(_first(row, "equityAmount", "amount", "value"))
    return out


class FyersGateway:
    """Thin adapter over FyersClient. One instance per agent process, so one
    account, so one whitelisted IP."""

    def __init__(self, client: Any) -> None:
        self._client = client

    # ── reads ───────────────────────────────────────────────────────────────
    def positions(self) -> List[Dict[str, Any]]:
        return [normalise_position(p.raw or {}) for p in self._client.positions()]

    def holdings(self) -> List[Dict[str, Any]]:
        return [normalise_holding(h.raw or {}) for h in self._client.holdings()]

    def orders(self) -> List[Dict[str, Any]]:
        payload = self._client.orderbook()
        rows = payload.get("orderBook") or payload.get("orders") or payload.get("data") or []
        now = time.time()
        return [normalise_order(r, now=now) for r in rows if isinstance(r, dict)]

    def trades(self) -> List[Dict[str, Any]]:
        payload = self._client.tradebook()
        rows = payload.get("tradeBook") or payload.get("trades") or payload.get("data") or []
        return [normalise_trade(r) for r in rows if isinstance(r, dict)]

    def funds(self) -> Dict[str, Any]:
        return summarise_funds(self._client.funds_raw())

    # ── writes ──────────────────────────────────────────────────────────────
    def place_order(self, req: PlaceOrderRequest) -> str:
        return self._client.place_order(req)

    def modify_order(self, order_id: str, **kwargs: Any) -> Dict[str, Any]:
        return self._client.modify_order(order_id, **kwargs)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return self._client.cancel_order(order_id)

    def exit_position(self, position_id: str) -> Dict[str, Any]:
        return self._client.exit_position(position_id)


def build_order_request(payload: Dict[str, Any]) -> PlaceOrderRequest:
    """Turn a validated JSON body into a broker request. Raises ValueError on
    anything the broker would reject, so a bad request fails here rather than at
    Fyers with an opaque code."""
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        raise ValueError("symbol is required")

    side = str(payload.get("side") or "").upper()
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")

    qty = to_decimal(payload.get("qty"))
    if qty <= 0 or qty != qty.to_integral_value():
        raise ValueError("qty must be a positive whole number")

    order_type = str(payload.get("order_type") or "LIMIT").upper()
    if order_type not in ("MARKET", "LIMIT"):
        raise ValueError("order_type must be MARKET or LIMIT")

    limit_price = to_decimal(payload.get("limit_price"))
    if order_type == "LIMIT" and limit_price <= 0:
        raise ValueError("limit_price is required for a LIMIT order")

    product_type = str(payload.get("product_type") or "CNC").upper()

    return PlaceOrderRequest(
        symbol=symbol,
        side=side,
        qty=qty,
        product_type=product_type,
        order_type=order_type,
        limit_price=limit_price,
        validity=str(payload.get("validity") or "DAY").upper(),
        disclosed_qty=int(payload.get("disclosed_qty") or 0),
        offline_order=bool(payload.get("offline_order") or False),
    )
