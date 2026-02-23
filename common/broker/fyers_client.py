from __future__ import annotations
import datetime as dt
from decimal import Decimal
from typing import Any, Dict, List, Optional
from common.broker.interfaces import (
    Broker, BrokerError, PlaceOrderRequest, OrderTerminal, HoldingLot, Position, to_decimal
)
from common.broker.retry import with_retries
from common.utils.logger import setup_logger

LOG = setup_logger("fyers")

try:
    from fyers_apiv3 import fyersModel  # type: ignore
except Exception:
    fyersModel = None  # type: ignore

class FyersClient(Broker):
    def __init__(self, client_id: str, access_token: str, *, log_path: str = ""):
        if fyersModel is None:
            raise ImportError("Missing dependency fyers-apiv3. Install: pip install fyers-apiv3")
        self._fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            is_async=False,
            log_path=log_path or "",
        )

    def get_ltps(self, symbols: List[str]) -> Dict[str, Decimal]:
        sym_str = ",".join(symbols)

        def _call():
            resp = self._fyers.quotes({"symbols": sym_str})
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Quotes error: {resp!r}", resp=resp)
            out: Dict[str, Decimal] = {}
            for item in (resp.get("d") or []):
                if not isinstance(item, dict):
                    continue
                sym = str(item.get("n") or item.get("symbol") or "")
                v = item.get("v") or {}
                lp = v.get("lp")
                if sym and lp is not None:
                    out[sym] = to_decimal(lp)
            if len(symbols) == 1 and not out:
                d0 = (resp.get("d") or [{}])[0] or {}
                v0 = d0.get("v") or {}
                lp0 = v0.get("lp")
                if lp0 is not None:
                    out[symbols[0]] = to_decimal(lp0)
            missing = [s for s in symbols if s not in out]
            if missing:
                raise BrokerError(f"Missing LTP for {missing}. resp={resp!r}", resp=resp)
            return out

        return with_retries(_call, max_retries=4, base_sleep=0.4, max_sleep=3.0, logger=LOG)

    def place_order(self, req: PlaceOrderRequest) -> str:
        # FYERS requires integer qty
        qty_int = int(req.qty)
        fy_side = 1 if req.side == "BUY" else -1
        order_type = 2 if req.order_type == "MARKET" else 1  # 2=MARKET, 1=LIMIT in FYERS
        data = {
            "symbol": req.symbol,
            "qty": int(qty_int),
            "type": order_type,
            "side": fy_side,
            "productType": str(req.product_type),
            "limitPrice": float(req.limit_price) if order_type == 1 else 0,
            "stopPrice": 0,
            "validity": req.validity,
            "disclosedQty": int(req.disclosed_qty),
            "offlineOrder": bool(req.offline_order),
            "stopLoss": 0,
            "takeProfit": 0,
            "isSliceOrder": False,
        }
        data.update(req.raw or {})

        def _call():
            resp = self._fyers.place_order(data)
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Order placement failed: {resp!r}", resp=resp)
            oid = resp.get("id") or resp.get("order_id") or (resp.get("data") or {}).get("id")
            if not oid:
                raise BrokerError(f"place_order ok but id missing: {resp!r}", resp=resp)
            return str(oid)

        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=5.0, logger=LOG)

    def orderbook(self) -> Dict[str, Any]:
        def _call():
            resp = self._fyers.orderbook()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Orderbook error: {resp!r}", resp=resp)
            return resp
        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0, logger=LOG)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        def _call():
            payload = {"id": str(order_id)}
            try:
                resp = self._fyers.cancel_order(data=payload)
            except TypeError:
                resp = self._fyers.cancel_order(payload)
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Cancel order failed: {resp!r}", resp=resp)
            return resp
        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0, logger=LOG)

    def _iter_orders(self, ob: Dict[str, Any]) -> List[dict]:
        orders = ob.get("orderBook") or ob.get("data") or ob.get("orders") or []
        if isinstance(orders, dict):
            return list(orders.values())
        if isinstance(orders, list):
            return orders
        return []

    def get_order_terminal(self, order_id: str) -> Optional[OrderTerminal]:
        ob = self.orderbook()
        found = None
        for o in self._iter_orders(ob):
            if not isinstance(o, dict):
                continue
            oid = str(o.get("id") or o.get("order_id") or "")
            if oid == str(order_id):
                found = o
                break
        if not found:
            return None

        status_raw = str(found.get("status") or found.get("orderStatus") or found.get("order_status") or "").upper()
        qty = int(found.get("qty") or found.get("quantity") or 0)
        filled = int(found.get("filledQty") or found.get("tradedQty") or found.get("filled_qty") or 0)
        avg_price = to_decimal(found.get("avgPrice") or found.get("averagePrice") or found.get("avg_price") or found.get("tradedPrice") or 0)
        sym = str(found.get("symbol") or found.get("tradingSymbol") or "")
        side_val = found.get("side")

        if side_val in (1, "1", "BUY", "B"):
            side = "BUY"
        elif side_val in (-1, "-1", "SELL", "S"):
            side = "SELL"
        else:
            side = "BUY"

        terminal = {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}
        is_filled = (status_raw in {"TRADED", "FILLED", "COMPLETE"}) or (qty > 0 and filled >= qty)

        if status_raw in {"REJECTED", "CANCELLED", "CANCELED"}:
            return OrderTerminal(
                order_id=str(order_id),
                symbol=sym,
                side=side,  # type: ignore
                status="REJECTED" if status_raw == "REJECTED" else "CANCELLED",
                filled_qty=to_decimal(0),
                avg_price=to_decimal(0),
                cum_quote_qty=to_decimal(0),
                message=str(found.get("message") or found.get("msg") or ""),
                ts=dt.datetime.now(dt.timezone.utc),
                raw=found,
            )

        if not is_filled:
            return None

        if filled <= 0:
            filled = qty
        cum_quote = avg_price * to_decimal(filled) if avg_price > 0 else to_decimal(0)
        return OrderTerminal(
            order_id=str(order_id),
            symbol=sym,
            side=side,  # type: ignore
            status="FILLED",
            filled_qty=to_decimal(filled),
            avg_price=avg_price,
            cum_quote_qty=cum_quote,
            message="",
            ts=dt.datetime.now(dt.timezone.utc),
            raw=found,
        )

    def positions(self) -> List[Position]:
        def _call():
            resp = self._fyers.positions()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Positions error: {resp!r}", resp=resp)
            rows = None
            for k in ("netPositions", "positions", "net", "data"):
                v = resp.get(k)
                if isinstance(v, list):
                    rows = v
                    break
            rows = rows or []
            out: List[Position] = []
            for p in rows:
                if not isinstance(p, dict):
                    continue
                sym = str(p.get("symbol") or p.get("tradingSymbol") or "")
                if not sym:
                    continue
                net = p.get("netQty") or p.get("net_qty") or p.get("qty") or p.get("quantity") or 0
                avg = p.get("avgPrice") or p.get("averagePrice") or p.get("buyAvg") or p.get("avg") or 0
                try:
                    out.append(Position(symbol=sym, net_qty=to_decimal(net), avg_price=to_decimal(avg or 0), raw=p))
                except Exception:
                    continue
            return out
        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0, logger=LOG)

    def holdings(self) -> List[HoldingLot]:
        def _call():
            resp = self._fyers.holdings()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Holdings error: {resp!r}", resp=resp)
            rows = resp.get("holdings") or resp.get("data") or []
            if not isinstance(rows, list):
                rows = []
            out: List[HoldingLot] = []
            for h in rows:
                if not isinstance(h, dict):
                    continue
                sym = str(h.get("symbol") or "")
                if not sym:
                    continue
                rem = h.get("remainingQuantity") if h.get("remainingQuantity") is not None else h.get("quantity") or 0
                ht = str(h.get("holdingType") or h.get("type") or "HLD")
                cp = to_decimal(h.get("costPrice") or h.get("buyPrice") or 0)
                try:
                    out.append(HoldingLot(symbol=sym, holding_type=ht, remaining_qty=to_decimal(rem), cost_price=cp, raw=h))
                except Exception:
                    continue
            return out
        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0, logger=LOG)

    def funds_cash(self) -> Decimal:
        def _call():
            resp = self._fyers.funds()
            if not isinstance(resp, dict) or resp.get("s") != "ok":
                raise BrokerError(f"Funds error: {resp!r}", resp=resp)
            cash = None
            fund_limit = resp.get("fund_limit")
            if isinstance(fund_limit, list):
                priority = {"AVAILABLE BALANCE": 0, "CLEAR BALANCE": 1, "TOTAL BALANCE": 2}
                best = None
                for it in fund_limit:
                    if not isinstance(it, dict):
                        continue
                    title = str(it.get("title") or it.get("name") or "").strip().upper()
                    amt = it.get("equityAmount") or it.get("amount") or it.get("value")
                    if amt is None:
                        continue
                    rank = priority.get(title, 99)
                    cand = (rank, to_decimal(amt))
                    if best is None or cand[0] < best[0]:
                        best = cand
                if best is not None:
                    cash = best[1]
            if cash is None:
                cash = to_decimal(resp.get("cash") or resp.get("availableCash") or resp.get("available_balance") or 0)
            return to_decimal(cash or 0)
        return with_retries(_call, max_retries=3, base_sleep=0.5, max_sleep=4.0, logger=LOG)

    def balances(self) -> Dict[str, Dict[str, Decimal]]:
        # Not applicable to FYERS equities; return empty map
        return {}

    def history(self, data: Dict[str, Any]) -> Dict[str, Any]:
        def _call():
            try:
                resp = self._fyers.history(data=data)
            except TypeError:
                resp = self._fyers.history(data)
            if not isinstance(resp, dict):
                raise BrokerError(f"History unexpected response: {resp!r}", resp=resp)
            if resp.get("s") not in {"ok", "no_data"}:
                raise BrokerError(f"History error: {resp!r}", resp=resp)
            return resp
        return with_retries(_call, max_retries=4, base_sleep=0.6, max_sleep=6.0, logger=LOG)
