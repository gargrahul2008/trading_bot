from __future__ import annotations
import hashlib
import hmac
import time
import urllib.parse
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests

from common.broker.interfaces import Broker, BrokerError, PlaceOrderRequest, OrderTerminal, HoldingLot, Position, to_decimal

# MEXC Spot API base: https://api.mexc.com
# Docs: https://www.mexc.com/api-docs/spot-v3/

D0 = Decimal("0")

@dataclass
class MexcSymbolInfo:
    symbol: str
    base_asset: str
    quote_asset: str
    # base_size_precision: int
    base_step: Decimal
    quote_precision: int
    # quote_amount_precision: int

def _to_step(v) -> Decimal:
    try:
        d = Decimal(str(v))
        return d if d > 0 else Decimal("0")
    except Exception:
        return Decimal("0")

class MexcSpotClient(Broker):
    def __init__(self, api_key: str, api_secret: str, *, base_url: str = "https://api.mexc.com", recv_window_ms: int = 5000, timeout_s: int = 10):
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip().encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = int(recv_window_ms)
        self.timeout_s = int(timeout_s)
        self._session = requests.Session()
        self._order_symbol: Dict[str, str] = {}
        self._time_offset_ms = 0
        self._exchange_cache: Dict[str, MexcSymbolInfo] = {}
        self._exchange_cache_ts = 0.0
        self._warmup_time_offset()

    def _warmup_time_offset(self) -> None:
        try:
            server = self._public_get("/api/v3/time")
            server_ms = int(server.get("serverTime"))
            local_ms = int(time.time() * 1000)
            self._time_offset_ms = server_ms - local_ms
        except Exception:
            self._time_offset_ms = 0

    def _ts_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _sign(self, params: Dict[str, Any]) -> str:
        # MEXC signing: HMAC SHA256 of query string
        qs = urllib.parse.urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()
        return sig

    def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url + path
        r = self._session.get(url, params=params or {}, timeout=self.timeout_s)
        if r.status_code != 200:
            raise BrokerError(f"MEXC public GET {path} failed: {r.status_code} {r.text}")
        data = r.json()
        if isinstance(data, dict) and data.get("code") not in (None, 0, 200):
            # sometimes {code,msg}
            raise BrokerError(f"MEXC public error: {data}", resp=data)
        return data

    def _private_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url + path
        params = dict(params or {})
        params["timestamp"] = self._ts_ms()
        params["recvWindow"] = self.recv_window_ms
        params["signature"] = self._sign(params)

        headers = {"X-MEXC-APIKEY": self.api_key, "Content-Type": "application/json"}

        if method.upper() == "GET":
            r = self._session.get(url, params=params, headers=headers, timeout=self.timeout_s)
        elif method.upper() == "POST":
            # MEXC expects params as query/body; requests will send as form by default
            r = self._session.post(url, data=params, headers=headers, timeout=self.timeout_s)
        elif method.upper() == "DELETE":
            r = self._session.delete(url, params=params, headers=headers, timeout=self.timeout_s)
        else:
            raise ValueError("Unsupported method")

        if r.status_code != 200:
            raise BrokerError(f"MEXC private {method} {path} failed: {r.status_code} {r.text}", resp=r.text)

        data = r.json()
        # Some private endpoints return {code,msg} on error
        if isinstance(data, dict) and data.get("code") not in (None, 0, 200):
            raise BrokerError(f"MEXC private error: {data}", resp=data)
        return data

    def _ensure_exchange_info(self, symbols: Optional[List[str]] = None) -> None:
        now = time.time()

        # Cache for 10 minutes; refresh if any requested symbol missing
        if self._exchange_cache and (now - self._exchange_cache_ts) < 600 and symbols:
            missing = [s for s in symbols if s not in self._exchange_cache]
            if not missing:
                return
            # fetch only missing
            data = self._public_get("/api/v3/exchangeInfo", params={"symbols": ",".join(missing)})
        elif self._exchange_cache and (now - self._exchange_cache_ts) < 600 and symbols is None:
            return
        else:
            data = self._public_get("/api/v3/exchangeInfo", params=None)

        syms = data.get("symbols") or []
        cache = dict(self._exchange_cache)  # keep existing

        for s in syms:
            if not isinstance(s, dict):
                continue
            sym = str(s.get("symbol") or "")
            if not sym:
                continue

            base_step = _to_step(s.get("baseSizePrecision") or "0")
            quote_precision = int(s.get("quotePrecision") or s.get("quoteAssetPrecision") or 8)

            cache[sym] = MexcSymbolInfo(
                symbol=sym,
                base_asset=str(s.get("baseAsset") or ""),
                quote_asset=str(s.get("quoteAsset") or ""),
                base_step=base_step,
                quote_precision=quote_precision,
            )

        self._exchange_cache = cache
        self._exchange_cache_ts = now

    def symbol_info(self, symbol: str) -> MexcSymbolInfo:
        self._ensure_exchange_info([symbol])
        if symbol not in self._exchange_cache:
            raise BrokerError(f"MEXC exchangeInfo missing symbol: {symbol}")
        return self._exchange_cache[symbol]

    def _round_qty(self, symbol: str, qty: Decimal) -> Decimal:
        info = self.symbol_info(symbol)
        step = info.base_step
        if step <= 0:
            return qty
        return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _round_price(self, symbol: str, price: Decimal) -> Decimal:
        info = self.symbol_info(symbol)
        p = max(int(info.quote_precision), 0)
        tick = Decimal("1e-%d" % p)
        return price.quantize(tick, rounding=ROUND_DOWN)

    # ----- Broker interface -----

    def get_ltps(self, symbols: List[str]) -> Dict[str, Decimal]:
        # For a few symbols, calling /ticker/price per symbol is fine.
        out: Dict[str, Decimal] = {}
        for sym in symbols:
            data = self._public_get("/api/v3/ticker/price", params={"symbol": sym})
            price = data.get("price")
            if price is None:
                raise BrokerError(f"MEXC missing price for {sym}: {data}")
            out[sym] = to_decimal(price)
        return out

    def balances(self) -> Dict[str, Dict[str, Decimal]]:
        data = self._private_request("GET", "/api/v3/account", params={})
        bals = data.get("balances") or []
        out: Dict[str, Dict[str, Decimal]] = {}
        for b in bals:
            if not isinstance(b, dict):
                continue
            asset = str(b.get("asset") or "")
            if not asset:
                continue
            out[asset] = {
                "free": to_decimal(b.get("free") or "0"),
                "locked": to_decimal(b.get("locked") or "0"),
            }
        return out

    def funds_cash(self) -> Decimal:
        bals = self.balances()
        usdt = bals.get("USDT") or {"free": D0}
        return to_decimal(usdt.get("free") or "0")

    def orderbook(self) -> Dict[str, Any]:
        # Normalize openOrders into runner-friendly structure
        data = self._private_request("GET", "/api/v3/openOrders", params={})
        if not isinstance(data, list):
            # some responses might be dict
            orders = data.get("data") or data.get("orders") or []
        else:
            orders = data
        out = []
        for o in orders or []:
            if not isinstance(o, dict):
                continue
            sym = str(o.get("symbol") or "")
            oid = str(o.get("orderId") or o.get("order_id") or o.get("id") or "")
            side = str(o.get("side") or "").upper()
            typ = str(o.get("type") or "").upper()
            status = str(o.get("status") or "NEW").upper()
            orig = to_decimal(o.get("origQty") or o.get("origQuantity") or o.get("quantity") or "0")
            execq = to_decimal(o.get("executedQty") or o.get("executedQuantity") or "0")
            price = to_decimal(o.get("price") or "0")
            out.append({
                "id": oid,
                "symbol": sym,
                "side": 1 if side == "BUY" else -1,
                "type": typ,
                "status": status,
                "qty": str(orig),
                "filledQty": str(execq),
                "price": str(price),
            })
        return {"s": "ok", "orderBook": out}

    def place_order(self, req: PlaceOrderRequest) -> str:
        sym = req.symbol
        side = req.side
        order_type = req.order_type.upper()

        params: Dict[str, Any] = {
            "symbol": sym,
            "side": side,
            "type": order_type,
        }

        if order_type == "LIMIT":
            price = self._round_price(sym, to_decimal(req.limit_price))
            qty = self._round_qty(sym, to_decimal(req.qty))
            if qty <= 0:
                raise BrokerError(f"MEXC qty rounded to 0 for {sym}")
            params["price"] = str(price)
            params["quantity"] = str(qty)
            params["timeInForce"] = req.time_in_force or "GTC"
        else:
            # MARKET
            if req.quote_qty is not None and side == "BUY":
                params["quoteOrderQty"] = str(to_decimal(req.quote_qty))
            else:
                qty = self._round_qty(sym, to_decimal(req.qty))
                if qty <= 0:
                    raise BrokerError(f"MEXC qty rounded to 0 for {sym}")
                params["quantity"] = str(qty)

        data = self._private_request("POST", "/api/v3/order", params=params)
        oid = data.get("orderId") or data.get("order_id") or data.get("id")
        if not oid:
            raise BrokerError(f"MEXC place_order missing orderId: {data}", resp=data)
        oid = str(oid)
        self._order_symbol[oid] = sym
        return oid
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        oid = str(order_id)
        sym = self._order_symbol.get(oid)
        if not sym:
            # Fallback: search open orders to find symbol
            try:
                ob = self.orderbook()
                for o in ob.get("orderBook") or []:
                    if str(o.get("id") or "") == oid:
                        sym = str(o.get("symbol") or "")
                        break
            except Exception:
                sym = None
        if not sym:
            raise BrokerError(f"MEXC cancel needs symbol for order_id={oid}.")
        data = self._private_request("DELETE", "/api/v3/order", params={"symbol": sym, "orderId": oid})
        return data

    def get_order_terminal(self, order_id: str) -> Optional[OrderTerminal]:
        # For MEXC, we prefer get_order_snapshot() for partial/TTL handling.
        snap = self.get_order_snapshot(order_id)
        if snap is None:
            return None
        status = snap["status"]
        if status in {"FILLED", "REJECTED", "CANCELLED"}:
            return OrderTerminal(
                order_id=str(order_id),
                symbol=snap["symbol"],
                side=snap["side"],
                status=status,
                filled_qty=snap["executed_qty"],
                avg_price=snap["avg_price"],
                cum_quote_qty=snap["cum_quote_qty"],
                message=snap.get("message",""),
                raw=snap.get("raw",{}),
            )
        return None

    def get_order_snapshot(self, order_id: str) -> Optional[Dict[str, Any]]:
        oid = str(order_id)
        sym = self._order_symbol.get(oid)
        if not sym:
            return None
        data = self._private_request("GET", "/api/v3/order", params={"symbol": sym, "orderId": oid})
        status_raw = str(data.get("status") or "").upper()
        side = str(data.get("side") or "").upper()
        executed = to_decimal(data.get("executedQty") or "0")
        cum_quote = to_decimal(data.get("cummulativeQuoteQty") or data.get("cumulativeQuoteQty") or "0")
        orig = to_decimal(data.get("origQty") or data.get("origQuantity") or "0")

        # Map MEXC statuses
        if status_raw in {"FILLED"}:
            status = "FILLED"
        elif status_raw in {"CANCELED", "CANCELLED"}:
            status = "CANCELLED"
        elif status_raw in {"REJECTED", "EXPIRED"}:
            status = "REJECTED"
        else:
            status = "OPEN"

        avg_price = (cum_quote / executed) if executed > 0 else to_decimal(data.get("price") or "0")
        return {
            "order_id": oid,
            "symbol": sym,
            "side": "BUY" if side == "BUY" else "SELL",
            "status": status,
            "orig_qty": orig,
            "executed_qty": executed,
            "cum_quote_qty": cum_quote,
            "avg_price": avg_price,
            "message": str(data.get("msg") or data.get("message") or ""),
            "raw": data,
        }

    # Equities-style methods unused for crypto
    def positions(self) -> List[Position]:
        return []

    def holdings(self) -> List[HoldingLot]:
        return []

    def history(self, data: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("MEXC spot history not implemented in this codebase")

    def self_symbols(self) -> list[str]:
        data = self._private_request("GET", "/api/v3/selfSymbols", params={})
        # docs show dict with "data": [...] :contentReference[oaicite:2]{index=2}
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return list(data["data"])
        if isinstance(data, list):
            return list(data)
        return []
