from __future__ import annotations

import datetime as dt
import time
from typing import Any, Dict, List

from zoneinfo import ZoneInfo

from common.fyers_broker import FyersBroker, PlaceOrderResult
from common.logger import LOG
from common.models import FillEvent, OrderAction, StrategyContext
from common.timeutils import iso_utc, now_utc, parse_hhmm, parse_hhmmss

def _cancel_open_orders(broker: FyersBroker, symbols: List[str], *, cancel_all: bool) -> int:
    try:
        ob = broker.orderbook()
    except Exception as e:
        LOG.warning("EOD cancel: orderbook fetch failed: %s", e)
        return 0

    symset = set(symbols)
    open_ids: List[str] = []
    orders = ob.get("orderBook") or ob.get("orders") or ob.get("data") or []
    if isinstance(orders, dict):
        orders = list(orders.values())

    terminal = {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}
    for o in orders or []:
        if not isinstance(o, dict):
            continue
        oid = str(o.get("id") or o.get("order_id") or "")
        if not oid:
            continue
        sym = str(o.get("symbol") or o.get("tradingSymbol") or "")
        if (not cancel_all) and sym and (sym not in symset):
            continue
        st = str(o.get("status") or o.get("orderStatus") or o.get("order_status") or "").upper()
        qty = int(o.get("qty") or o.get("quantity") or 0)
        filled = int(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0)
        if st in terminal:
            continue
        if qty > 0 and filled >= qty:
            continue
        open_ids.append(oid)

    n = 0
    for oid in open_ids:
        try:
            broker.cancel_order(oid)
            n += 1
        except Exception:
            pass
    return n

def run_live_reactive(strategy, broker: FyersBroker, cfg: Dict[str, Any]) -> None:
    exec_cfg = cfg.get("execution", {})
    market_tz = exec_cfg.get("market_tz", "Asia/Kolkata")
    market_open = exec_cfg.get("market_open", "09:15")
    market_close = exec_cfg.get("market_close", "15:30")
    eod_cancel = exec_cfg.get("eod_cancel", "15:29:30")
    poll_seconds = int(exec_cfg.get("poll_seconds", 5))
    closed_poll_seconds = int(exec_cfg.get("closed_poll_seconds", 30))
    cancel_all_open = bool(exec_cfg.get("cancel_all_open", False))
    sync_on_start = bool(exec_cfg.get("sync_on_start", True))

    beh_cfg = cfg.get("behaviour", {})
    include_t = bool(beh_cfg.get("include_t_settled_for_eq", True))

    tz = ZoneInfo(market_tz)
    open_t = parse_hhmm(market_open)
    close_t = parse_hhmm(market_close)
    eod_t = parse_hhmmss(eod_cancel)

    last_eod_cancel_date: str | None = None

    LOG.info(
        "LIVE started strategy=%s symbols=%s market=%s %s-%s eod_cancel=%s poll=%ss",
        type(strategy).__name__,
        ",".join(strategy.symbols),
        market_tz, market_open, market_close, eod_cancel, poll_seconds
    )

    if sync_on_start and hasattr(strategy, "sync_from_broker"):
        try:
            strategy.sync_from_broker(broker)  # type: ignore[attr-defined]
            LOG.info("Synced cash/positions/holdings from broker.")
        except Exception as e:
            LOG.warning("Sync-on-start failed: %s", e)

    while True:
        try:
            now_local = dt.datetime.now(tz)
            today = now_local.date().isoformat()

            open_dt = dt.datetime(now_local.year, now_local.month, now_local.day, open_t.hour, open_t.minute, open_t.second, tzinfo=tz)
            close_dt = dt.datetime(now_local.year, now_local.month, now_local.day, close_t.hour, close_t.minute, close_t.second, tzinfo=tz)
            eod_dt = dt.datetime(now_local.year, now_local.month, now_local.day, eod_t.hour, eod_t.minute, eod_t.second, tzinfo=tz)

            # EOD cancel once per day
            if now_local >= eod_dt and last_eod_cancel_date != today:
                n = _cancel_open_orders(broker, strategy.symbols, cancel_all=cancel_all_open)
                last_eod_cancel_date = today
                LOG.warning("EOD cancel: cancelled=%d (cancel_all=%s)", n, cancel_all_open)

            in_trading = (now_local >= open_dt) and (now_local < close_dt)
            allow_new_orders = (now_local >= open_dt) and (now_local < eod_dt)

            # 1) fetch prices
            prices = broker.quotes(strategy.symbols)

            # 2) poll pending orders -> fills
            pending = strategy.pending_orders()
            for oid, action in list(pending.items()):
                term = broker.get_order_terminal(oid)
                if not term.found:
                    continue
                if not term.terminal:
                    continue
                fill = FillEvent(
                    order_id=oid,
                    symbol=term.symbol or action.symbol,
                    side=term.side,
                    filled_qty=int(term.filled_qty or 0),
                    avg_price=float(term.avg_price or 0.0),
                    status=term.status or "TERMINAL",
                    message=term.message or "",
                    ts_utc=iso_utc(now_utc()),
                    raw=term.raw,
                )
                strategy.on_fill(fill)
                strategy.clear_order(oid)

            # 3) compute sellable inventory + cost (holdings)
            sellable, cost, _remaining = broker.get_inventory(strategy.symbols)#, include_t_settled=include_t)

            # 4) strategy decisions
            ctx = StrategyContext(
                ts_utc=iso_utc(now_utc()),
                prices=prices,
                sellable_qty=sellable,
                holdings_cost=cost,
                cash=float(strategy.get_cash()),
            )

            if allow_new_orders and (not strategy.is_paused()):
                actions = strategy.on_tick(ctx)
            else:
                actions = []

            # 5) execute actions (one-by-one)
            for act in actions:
                if act.qty <= 0:
                    continue
                # clamp sells to sellable
                eff_qty = int(act.qty)
                if act.side == "SELL":
                    eff_qty = min(eff_qty, int(sellable.get(act.symbol, 0) or 0))
                if eff_qty <= 0:
                    continue

                res = broker.place_market_order(symbol=act.symbol, qty=eff_qty, side=act.side, product_type=act.product_type)
                if res.ok and res.order_id:
                    strategy.register_order(res.order_id, act)
                    LOG.info("Placed %s %s qty=%d oid=%s reason=%s", act.symbol, act.side, eff_qty, res.order_id, act.reason)
                else:
                    # synthesize terminal reject
                    fill = FillEvent(
                        order_id="PLACE_FAIL",
                        symbol=act.symbol,
                        side=act.side,
                        filled_qty=0,
                        avg_price=0.0,
                        status="REJECTED",
                        message=res.message or "place_failed",
                        ts_utc=iso_utc(now_utc()),
                        raw=res.raw,
                    )
                    strategy.on_fill(fill)
                    LOG.error("Place failed %s %s qty=%d: %s", act.symbol, act.side, eff_qty, res.message)

                    # optional: immediate retry for SELL if broker suggests qty
                    if act.side == "SELL" and res.qty_suggestion:
                        sug_qty = min(int(res.qty_suggestion), int(sellable.get(act.symbol, 0) or 0))
                        if sug_qty > 0 and sug_qty != eff_qty:
                            res2 = broker.place_market_order(symbol=act.symbol, qty=sug_qty, side=act.side, product_type=act.product_type)
                            if res2.ok and res2.order_id:
                                strategy.register_order(res2.order_id, OrderAction(act.symbol, act.side, sug_qty, act.reason, act.product_type))
                                LOG.info("Retry placed %s %s qty=%d oid=%s", act.symbol, act.side, sug_qty, res2.order_id)

            # 6) logging + persist
            LOG.info(strategy.snapshot_line(prices))
            strategy.persist()

            sleep_s = poll_seconds if in_trading else closed_poll_seconds
            time.sleep(max(int(sleep_s), 1))

        except KeyboardInterrupt:
            LOG.info("Stopped by user.")
            break
        except Exception as e:
            LOG.exception("Loop error: %s", e)
            time.sleep(2)
