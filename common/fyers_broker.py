from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from .logger import LOG
from .reject_parser import QtySuggestion, parse_qty_suggestion
from .inventory import summarize_holdings, compute_sellable_qty, is_nse_eq_or_bse_a

Side = Literal["BUY", "SELL"]

try:
    from fyers_apiv3 import fyersModel  # type: ignore
except Exception:  # pragma: no cover
    fyersModel = None  # type: ignore

class BrokerError(RuntimeError):
    pass

@dataclass
class PlaceOrderResult:
    ok: bool
    order_id: Optional[str]
    message: str
    raw: Dict[str, Any]
    qty_suggestion: Optional[int] = None

@dataclass
class OrderTerminal:
    found: bool
    terminal: bool
    status: str
    message: str
    symbol: str
    side: Side
    qty: int
    filled_qty: int
    avg_price: float
    raw: Dict[str, Any]

def _sleep_backoff(attempt: int, base: float, cap: float) -> float:
    # jittery exponential
    import random
    return min(cap, base * (2 ** attempt)) * (0.7 + random.random() * 0.6)

def _with_retries(fn, *, max_retries: int = 4, base_sleep: float = 0.5, max_sleep: float = 5.0):
    last = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            last = e
            s = _sleep_backoff(attempt, base_sleep, max_sleep)
            LOG.warning("Retryable error: %s attempt=%d sleep=%.2fs", type(e).__name__, attempt + 1, s)
            time.sleep(s)
    raise BrokerError(f"Failed after {max_retries} retries: {last!r}")

class FyersBroker:
    """Thin wrapper around FYERS SDK with defensive parsing."""

    def __init__(self, client_id: str, access_token: str, log_path: str = ""):
        if fyersModel is None:
            raise ImportError("Missing dependency fyers-apiv3. Install: pip install fyers-apiv3")
        self._fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            is_async=False,
            log_path=log_path or "",
        )

    # ---------- market data ----------
    def quotes(self, symbols: List[str]) -> Dict[str, float]:
        sym_str = ",".join(symbols)

        def _call():
            resp = self._fyers.quotes({"symbols": sym_str})
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Quotes error: {resp!r}")
            out: Dict[str, float] = {}
            for item in (resp.get("d") or []):
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("n") or item.get("symbol") or "")
                v = item.get("v") or {}
                lp = v.get("lp")
                if sym and lp is not None:
                    out[sym] = float(lp)
            # fallback for single symbol responses missing 'n'
            if len(symbols) == 1 and symbols[0] not in out:
                d0 = (resp.get("d") or [{}])[0] or {}
                v0 = d0.get("v") or {}
                lp0 = v0.get("lp")
                if lp0 is not None:
                    out[symbols[0]] = float(lp0)
            missing = [s for s in symbols if s not in out]
            if missing:
                raise BrokerError(f"Missing LTP for {missing}. resp={resp!r}")
            return out

        return _with_retries(_call, max_retries=4, base_sleep=0.4, max_sleep=3.0)

    # ---------- account ----------
    def funds(self) -> Dict[str, Any]:
        def _call():
            resp = self._fyers.funds()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Funds error: {resp!r}")
            return resp
        return _with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0)

    def positions(self) -> Dict[str, Any]:
        def _call():
            resp = self._fyers.positions()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Positions error: {resp!r}")
            return resp
        return _with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0)

    def holdings(self) -> Dict[str, Any]:
        # FYERS sometimes uses .holdings() and sometimes .holdings(data=..). Here no args.
        def _call():
            try:
                resp = self._fyers.holdings()
            except Exception as e:
                raise BrokerError(f"Holdings call failed: {e!r}")
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Holdings error: {resp!r}")
            return resp
        return _with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0)

    # ---------- orders ----------
    def orderbook(self) -> Dict[str, Any]:
        def _call():
            resp = self._fyers.orderbook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Orderbook error: {resp!r}")
            return resp
        return _with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0)

    def place_market_order(self, *, symbol: str, qty: int, side: Side, product_type: str) -> PlaceOrderResult:
        fy_side = 1 if side == "BUY" else -1
        payload = {
            "symbol": symbol,
            "qty": int(qty),
            "type": 2,  # MARKET
            "side": fy_side,
            "productType": product_type,
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "stopLoss": 0,
            "takeProfit": 0,
            "isSliceOrder": False,
        }

        def _call():
            resp = self._fyers.place_order(payload)
            if not isinstance(resp, dict):
                raise BrokerError(f"place_order unexpected resp: {resp!r}")
            if resp.get("s") != "ok":
                msg = str(resp.get("message") or resp.get("msg") or resp.get("error") or "")
                sug = None
                if msg:
                    sug = parse_qty_suggestion(msg, int(qty)).suggested_qty
                return PlaceOrderResult(ok=False, order_id=None, message=msg or "place_order_failed", raw=resp, qty_suggestion=sug)
            oid = resp.get("id") or resp.get("order_id") or (resp.get("data") or {}).get("id")
            if not oid:
                return PlaceOrderResult(ok=False, order_id=None, message="place_order_ok_but_id_missing", raw=resp)
            return PlaceOrderResult(ok=True, order_id=str(oid), message="ok", raw=resp)

        # Do NOT retry too aggressively for place_order; retries can create duplicate orders if request was accepted but response lost.
        try:
            return _call()
        except Exception as e:
            return PlaceOrderResult(ok=False, order_id=None, message=str(e), raw={"exception": repr(e)})

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        def _call():
            payload = {"id": str(order_id)}
            try:
                resp = self._fyers.cancel_order(data=payload)
            except TypeError:
                resp = self._fyers.cancel_order(payload)
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Cancel order failed: {resp!r}")
            return resp
        return _with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0)

    # ---------- order parsing ----------
    def _iter_orders(self, orderbook_resp: Dict[str, Any]):
        orders = orderbook_resp.get("orderBook") or orderbook_resp.get("orders") or orderbook_resp.get("data") or []
        if isinstance(orders, dict):
            orders = list(orders.values())
        for o in orders or []:
            if isinstance(o, dict):
                yield o

    def get_order_terminal(self, order_id: str) -> OrderTerminal:
        ob = self.orderbook()
        found = None
        for o in self._iter_orders(ob):
            oid = str(o.get("id") or o.get("order_id") or "")
            if oid == str(order_id):
                found = o
                break

        if not found:
            return OrderTerminal(
                found=False, terminal=False, status="", message="", symbol="", side="BUY",
                qty=0, filled_qty=0, avg_price=0.0, raw={}
            )

        status = str(found.get("status") or found.get("orderStatus") or found.get("order_status") or "").upper()
        msg = str(found.get("message") or found.get("statusMessage") or found.get("reason") or "")
        sym = str(found.get("symbol") or found.get("tradingSymbol") or "")
        side_val = found.get("side")
        if side_val in (1, "1", "BUY", "B"):
            side: Side = "BUY"
        elif side_val in (-1, "-1", "SELL", "S"):
            side = "SELL"
        else:
            side = "BUY"

        qty = int(found.get("qty") or found.get("quantity") or 0)
        filled = int(found.get("filledQty") or found.get("tradedQty") or found.get("filled_qty") or 0)
        avg_price = float(found.get("avgPrice") or found.get("averagePrice") or found.get("avg_price") or found.get("tradedPrice") or 0.0)

        terminal_statuses = {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}
        terminal = status in terminal_statuses or (qty > 0 and filled >= qty)

        return OrderTerminal(
            found=True,
            terminal=terminal,
            status=status,
            message=msg,
            symbol=sym,
            side=side,
            qty=qty,
            filled_qty=filled,
            avg_price=avg_price,
            raw=found,
        )

    # ---------- holdings helpers ----------
    def get_inventory(self, symbols: List[str]) -> Tuple[Dict[str, int], Dict[str, float], Dict[str, int]]:
        """Returns (sellable_qty, weighted_cost, total_remaining_qty) by symbol based on holdings()."""
        hresp = self.holdings()
        inv = summarize_holdings(hresp)

        sellable: Dict[str, int] = {}
        wcost: Dict[str, float] = {}
        remaining: Dict[str, int] = {}

        for sym in symbols:
            si = inv.get(sym)
            if not si:
                sellable[sym] = 0
                wcost[sym] = 0.0
                remaining[sym] = 0
                continue
            include_t = is_nse_eq_or_bse_a(sym)
            sellable[sym] = compute_sellable_qty(si, include_t_settled=include_t)
            wcost[sym] = float(si.weighted_cost)
            remaining[sym] = int(si.total_remaining_qty)

        return sellable, wcost, remaining
