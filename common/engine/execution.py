from __future__ import annotations
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from common.broker.interfaces import Broker, PlaceOrderRequest, BrokerError
from common.broker.reject_parser import parse_reject
from common.broker.sellable_qty import compute_sellable_qty
from common.utils.logger import setup_logger
from common.utils.timeutils import utcnow

LOG = setup_logger("exec")

D0 = Decimal("0")

@dataclass
class ExecutionConfig:
    product_type: str = "CNC"
    allow_btst_auto: bool = True

    order_mode: str = "market"          # market | marketable_limit | limit
    slippage_bps: int = 10
    limit_ttl_seconds: int = 15

    max_place_retries: int = 3
    quote_reserve: Decimal = D0         # keep some quote cash unused (USDT/INR etc)
    use_inventory_buffer: bool = False
    price_tick: Decimal = D0            # round limit prices to this tick (e.g. 0.05 for NSE); 0 = no rounding

class OrderExecutor:
    def __init__(self, broker: Broker, state, cfg: ExecutionConfig, *, rejects_path: str):
        self.broker = broker
        self.state = state
        self.cfg = cfg
        self.rejects_path = rejects_path

    def _append_jsonl(self, path: str, rec: Dict[str, Any]) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as e:
            LOG.warning("Failed writing %s: %s", path, e)

    def _note_reject(self, *, symbol: str, order_id: str, resp: Any, reason: str) -> None:
        now = utcnow()
        rec = {"ts": now.isoformat(), "symbol": symbol, "order_id": order_id, "reason": reason, "resp": resp}
        self._append_jsonl(self.rejects_path, rec)

    def _pending_sell_qty_equity(self, symbol: str) -> Decimal:
        try:
            ob = self.broker.orderbook()
        except Exception:
            return D0
        orders = ob.get("orderBook") or ob.get("data") or ob.get("orders") or []
        if isinstance(orders, dict):
            orders = list(orders.values())
        pending = D0
        for o in orders or []:
            if not isinstance(o, dict):
                continue
            sym = str(o.get("symbol") or o.get("tradingSymbol") or "")
            if sym != symbol:
                continue
            status = str(o.get("status") or o.get("orderStatus") or o.get("order_status") or "").upper()
            if status in {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}:
                continue
            side = o.get("side")
            if side not in (-1, "-1", "SELL", "S"):
                continue
            qty = Decimal(str(o.get("qty") or o.get("quantity") or 0))
            filled = Decimal(str(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0))
            rem = max(qty - filled, D0)
            pending += rem
        return pending

    def compute_broker_sellable(self, symbol: str) -> Decimal:
        # If broker exposes crypto balances, compute sellable from balances + open sell orders
        is_crypto_symbol = (":" not in symbol)
        if is_crypto_symbol  and hasattr(self.broker, "balances"):
            try:
                # MEXC-specific helper: sellable is base_free - base_locked_in_open_sells
                # We'll compute locked in open sells by scanning open orders.
                ob = self.broker.orderbook()
                orders = ob.get("orderBook") or []
                base_asset = None
                try:
                    info = getattr(self.broker, "symbol_info")(symbol)  # MexcSpotClient
                    base_asset = info.base_asset
                except Exception:
                    base_asset = None

                bals = self.broker.balances() or {}
                free = D0
                if base_asset and base_asset in bals:
                    free = Decimal(str(bals[base_asset].get("free") or "0"))
                else:
                    # fallback: try infer base by stripping quote USDT
                    if symbol.endswith("USDT"):
                        base_asset = symbol[:-4]
                        free = Decimal(str((bals.get(base_asset) or {}).get("free") or "0"))

                locked_in_sells = D0
                for o in orders:
                    if not isinstance(o, dict):
                        continue
                    sym = str(o.get("symbol") or "")
                    if sym != symbol:
                        continue
                    side = o.get("side")
                    if side not in (-1, "SELL", "-1"):
                        continue
                    qty = Decimal(str(o.get("qty") or "0"))
                    filled = Decimal(str(o.get("filledQty") or "0"))
                    locked_in_sells += max(qty - filled, D0)

                return max(free - locked_in_sells, D0)
            except Exception:
                pass

        # Default equities sellable calc
        pos = self.broker.positions()
        hld = self.broker.holdings()
        pending_sell = self._pending_sell_qty_equity(symbol)
        sellable, _, _ = compute_sellable_qty(
            symbol,
            positions=pos,
            holdings=hld,
            pending_sell_qty=pending_sell,
            allow_btst_auto=bool(self.cfg.allow_btst_auto),
        )
        return sellable

    def place_with_adaptive_qty(self, req: PlaceOrderRequest, *, reason: str) -> Optional[str]:
        symbol = req.symbol
        side = req.side
        qty = Decimal(req.qty)

        if qty <= 0:
            return None

        if side == "SELL":
            sellable = self.compute_broker_sellable(symbol)
            allow_buffer = bool(self.state.extras.get("use_inventory_buffer"))

            if allow_buffer:
                qty = min(qty, sellable)
            else:
                ss = self.state.symbol_states.get(symbol)
                state_cap = Decimal(ss.traded_qty) if ss else sellable
                qty = min(qty, sellable, state_cap) if state_cap > 0 else min(qty, sellable)
            if qty <= 0:
                LOG.warning("SELL qty capped to 0 for %s. Skipping.", symbol)
                return None
            req.qty = qty

        last_err: Any = None
        for attempt in range(int(self.cfg.max_place_retries)):
            try:
                oid = self.broker.place_order(req)
                return oid
            except BrokerError as e:
                last_err = e.resp or str(e)
                act = parse_reject(last_err)
                self._note_reject(symbol=symbol, order_id="PLACE_FAIL", resp=last_err, reason=reason)

                if act.kind == "AUTH_REQUIRED":
                    LOG.error("AUTH_REQUIRED for %s: %s", symbol, act.raw_message)
                    return None

                if side == "SELL" and act.kind == "REDUCE_QTY":
                    sellable = self.compute_broker_sellable(symbol)
                    caps = [sellable]
                    if act.max_qty is not None:
                        caps.append(Decimal(str(act.max_qty)))
                    new_cap = min(caps) if caps else sellable
                    new_qty = min(qty, new_cap)
                    if new_qty == qty:
                        new_qty = max(new_qty - Decimal("0.00000001"), D0)  # tiny step down
                    LOG.warning("SELL rejected; retry with reduced qty. old=%s new=%s msg=%s", qty, new_qty, act.raw_message)
                    qty = new_qty
                    if qty <= 0:
                        return None
                    req.qty = qty
                    continue

                LOG.error("Order rejected (non-retryable or BUY): %s", act.raw_message or last_err)
                return None
            except Exception as e:
                last_err = str(e)
                self._note_reject(symbol=symbol, order_id="PLACE_FAIL", resp=last_err, reason=reason)
                LOG.error("Order placement error: %s", e)
                return None

        LOG.error("Order placement failed after retries: %s", last_err)
        return None

    def poll_terminal(self, order_id: str):
        return self.broker.get_order_terminal(order_id)
