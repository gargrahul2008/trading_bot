from __future__ import annotations
import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.parse
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests
import websocket

from common.broker.interfaces import Broker, BrokerError, PlaceOrderRequest, OrderTerminal, HoldingLot, Position, to_decimal

_LOG = logging.getLogger("mexc_price_feed")

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
    def cancel_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        oid = str(order_id)
        sym = self._order_symbol.get(oid) or symbol
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
        self._order_symbol[oid] = sym  # register for future lookups
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

    def get_order_snapshot(self, order_id: str, symbol: str = None) -> Optional[Dict[str, Any]]:
        oid = str(order_id)
        sym = self._order_symbol.get(oid) or symbol
        if not sym:
            return None
        self._order_symbol[oid] = sym  # register for future lookups
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


# ---------------------------------------------------------------------------
# MexcPriceFeed — fast REST polling price feed
# ---------------------------------------------------------------------------
#
# MEXC blocks public WebSocket streams (miniTicker, deals, bookTicker) from
# datacenter/VPS IPs.  We achieve the same event-driven benefit by running
# REST polling at 200 ms in a dedicated background thread and waking the
# main business loop via a threading.Event on every price change.
#
# With ~1 ms ping to MEXC from Singapore DO, each poll costs ~80–100 ms,
# so the effective price-refresh rate is ~100–120 ms — vs 2 000 ms before.
#
# Interface is the same as a WebSocket feed: start / stop / get_prices /
# wait_for_tick.  The main loop in generic_runner.py is unchanged.

class MexcPriceFeed:
    """
    Fast REST-polling price feed for MEXC.

    Polls /api/v3/ticker/price at `poll_ms` ms intervals in a daemon thread.
    Wakes the main loop via threading.Event on every new price tick — so the
    business loop reacts as soon as the price is refreshed, without sleeping.

    Usage:
        feed = MexcPriceFeed(["ETHUSDC"], base_url="https://api.mexc.com")
        feed.start()
        prices = feed.get_prices()          # latest prices (or None on startup)
        feed.wait_for_tick(timeout=2.0)     # block until next tick or timeout
        feed.stop()
    """

    def __init__(
        self,
        symbols: List[str],
        base_url: str = "https://api.mexc.com",
        poll_ms: int = 200,
    ) -> None:
        self._symbols    = [s.upper() for s in symbols]
        self._base_url   = base_url.rstrip("/")
        self._poll_s     = poll_ms / 1000.0
        self._prices: Dict[str, Decimal] = {}
        self._lock       = threading.Lock()
        self._tick_event = threading.Event()
        self._stop_flag  = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._connected  = False
        self._session    = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="MexcPriceFeed"
        )
        self._thread.start()
        _LOG.info("MexcPriceFeed started — polling %s every %d ms", self._symbols, int(self._poll_s * 1000))

    def stop(self) -> None:
        self._stop_flag.set()

    def get_prices(self) -> Optional[Dict[str, Decimal]]:
        """Return latest cached prices, or None if not yet ready."""
        with self._lock:
            if not self._connected:
                return None
            if not all(s in self._prices for s in self._symbols):
                return None
            return dict(self._prices)

    def wait_for_tick(self, timeout: float = 2.0) -> bool:
        """Block until a new price arrives (or timeout). Returns True on tick."""
        fired = self._tick_event.wait(timeout=timeout)
        self._tick_event.clear()
        return fired

    @property
    def connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_prices(self) -> tuple[Optional[Dict[str, Decimal]], bool]:
        """
        Returns (prices_dict, rate_limited).
        prices_dict is None on any error; rate_limited is True on HTTP 429.
        """
        out: Dict[str, Decimal] = {}
        try:
            for sym in self._symbols:
                r = self._session.get(
                    f"{self._base_url}/api/v3/ticker/price",
                    params={"symbol": sym},
                    timeout=5,
                )
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    _LOG.warning("MexcPriceFeed: rate-limited (429), backing off %ds", retry_after)
                    return None, True
                if r.status_code != 200:
                    _LOG.debug("MexcPriceFeed: HTTP %s for %s", r.status_code, sym)
                    return None, False
                data = r.json()
                price_str = data.get("price")
                if price_str is None:
                    return None, False
                out[sym] = Decimal(str(price_str))
        except Exception as e:
            _LOG.debug("MexcPriceFeed fetch error: %s", e)
            return None, False
        return out, False

    def _run_loop(self) -> None:
        errors   = 0
        backoff  = 0.0   # extra sleep on rate-limit / repeated errors
        MAX_BACK = 60.0

        while not self._stop_flag.is_set():
            # Apply any backoff before fetching
            if backoff > 0:
                _LOG.info("MexcPriceFeed: sleeping %.0fs (backoff)", backoff)
                self._stop_flag.wait(timeout=backoff)
                backoff = 0.0
                if self._stop_flag.is_set():
                    break

            t0 = time.monotonic()
            prices, rate_limited = self._fetch_prices()

            if rate_limited:
                errors  += 1
                backoff  = min(10.0 * errors, MAX_BACK)
                with self._lock:
                    self._connected = False
                continue

            if prices:
                errors = 0
                with self._lock:
                    self._prices.update(prices)
                    self._connected = True
                self._tick_event.set()
            else:
                errors += 1
                if errors >= 5:
                    with self._lock:
                        self._connected = False
                    _LOG.warning("MexcPriceFeed: %d consecutive fetch errors", errors)
                    backoff = min(2.0 ** min(errors - 5, 5), MAX_BACK)

            # Sleep for remainder of poll interval
            elapsed = time.monotonic() - t0
            sleep   = max(0.0, self._poll_s - elapsed)
            self._stop_flag.wait(timeout=sleep)
