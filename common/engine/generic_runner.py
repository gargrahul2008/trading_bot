from __future__ import annotations
import datetime as dt
import os
import json
import time
from decimal import Decimal
from typing import Any, Dict, List

from common.broker.interfaces import Broker, PlaceOrderRequest, to_decimal
from common.engine.state import GlobalState
from common.engine.strategy_base import ReactiveStrategy, ManagedOrderStrategy, OrderIntent
from common.engine.execution import OrderExecutor, ExecutionConfig
from common.utils.logger import setup_logger
from common.utils.timeutils import parse_hhmm, parse_hhmmss, now_local, utcnow
from common.engine.pnl import PnLWriter, PnLPoint, infer_broker_name, compute_portfolio_value_for_symbols, compute_strategy_pnl, update_drawdown, ensure_portfolio_start

LOG = setup_logger("runner")

TERMINAL_STATUSES = {"FILLED", "REJECTED", "CANCELLED"}

D0 = Decimal("0")

def _dec(x: Any) -> Decimal:
    return to_decimal(x)

class GenericRunner:
    def __init__(self, *, broker: Broker, state: GlobalState, symbols: List[str], exec_cfg: ExecutionConfig,
                 trades_path: str, rejects_path: str, market_tz: str, market_open: str, market_close: str,
                 eod_cancel_time: str, poll_seconds: int, closed_poll_seconds: int, cancel_all_open_orders: bool,
                 sync_on_start: bool, adopt_broker_inventory: bool):
        self.broker = broker
        self.state = state
        self.symbols = symbols
        self.exec = OrderExecutor(broker, state, exec_cfg, rejects_path=rejects_path)
        self.exec_cfg = exec_cfg
        self.trades_path = trades_path
        self.market_tz = market_tz
        self.open_t = parse_hhmm(market_open)
        self.close_t = parse_hhmm(market_close)
        self.eod_cancel_t = parse_hhmmss(eod_cancel_time)
        self.poll_seconds = int(poll_seconds)
        self.closed_poll_seconds = int(closed_poll_seconds)
        self.cancel_all_open_orders = bool(cancel_all_open_orders)
        self.sync_on_start = bool(sync_on_start)
        self.adopt_broker_inventory = bool(adopt_broker_inventory)
        base_dir = os.path.dirname(os.path.abspath(trades_path)) or "."
        self._pnl_writer = PnLWriter(
            csv_path=os.path.join(base_dir, "pnl_points.csv"),
            snapshot_path=os.path.join(base_dir, "positions_snapshot.json"),
            summary_path=os.path.join(base_dir, "pnl_summary.json"),
        )

    def _append_jsonl(self, path: str, rec: Dict[str, Any]) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as e:
            LOG.warning("Failed writing %s: %s", path, e)

    def reconcile_from_broker(self) -> None:
        """Sync cash and adopt broker inventory into traded_qty (best-effort).
        - For equities: keeps old behavior (cash from funds, traded qty from sellable)
        - For crypto spot: traded_qty per symbol = base free+locked from balances
        """
        quote_assets = set()
        base_by_sym = {}

        for sym in self.symbols:
            info = getattr(self.broker, "symbol_info")(sym)  # MexcSpotClient
            quote_assets.add(info.quote_asset)
            base_by_sym[sym] = info.base_asset

        if len(quote_assets) != 1:
            raise RuntimeError(f"All crypto symbols must share same quote asset. got={sorted(quote_assets)}")

        quote_asset = next(iter(quote_assets))
        self.state.extras["quote_asset"] = quote_asset
        bals = self.broker.balances()
        q = bals.get(quote_asset) or {}
        self.state.cash = _dec(q.get("free") or "0")

        if not self.adopt_broker_inventory:
            return

        if bals:
            # require MexcSpotClient-like symbol_info if possible
            for sym in self.symbols:
                ss = self.state.symbol_states[sym]
                ss.core_qty = D0
                base = None
                try:
                    info = getattr(self.broker, "symbol_info")(sym)
                    base = info.base_asset
                except Exception:
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                total = D0
                if base and base in bals:
                    total = _dec(bals[base].get("free")) + _dec(bals[base].get("locked"))
                ss.traded_qty = total
                # avg price unknown; keep existing or 0
            LOG.info("Adopted crypto balances into traded_qty.")
            return

        # Equities fallback
        from common.broker.sellable_qty import compute_sellable_qty
        pos = self.broker.positions()
        hld = self.broker.holdings()
        for sym in self.symbols:
            total_sellable, _, w_cost = compute_sellable_qty(
                sym,
                positions=pos,
                holdings=hld,
                pending_sell_qty=D0,
                allow_btst_auto=True,
            )
            ss = self.state.symbol_states[sym]
            ss.core_qty = D0
            ss.traded_qty = total_sellable
            if ss.traded_qty > 0 and ss.traded_avg_price <= 0:
                ss.traded_avg_price = w_cost

        LOG.info("Reconciled cash and adopted broker inventory into traded_qty.")

    def cancel_open_orders(self, *, cancel_all: bool) -> int:
        try:
            ob = self.broker.orderbook()
        except Exception as e:
            LOG.warning("EOD cancel: orderbook fetch failed: %s", e)
            return 0

        orders = ob.get("orderBook") or ob.get("orders") or ob.get("data") or []
        if isinstance(orders, dict):
            orders = list(orders.values())

        open_ids: List[str] = []
        symset = set(self.symbols)
        for o in (orders or []):
            if not isinstance(o, dict):
                continue
            oid = str(o.get("id") or o.get("order_id") or "")
            if not oid:
                continue
            sym = str(o.get("symbol") or o.get("tradingSymbol") or "")
            if (not cancel_all) and sym and (sym not in symset):
                continue
            status = str(o.get("status") or o.get("orderStatus") or o.get("order_status") or "").upper()
            qty = _dec(o.get("qty") or o.get("quantity") or 0)
            filled_qty = _dec(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0)
            if status in {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}:
                continue
            if qty > 0 and filled_qty >= qty:
                continue
            open_ids.append(oid)

        n = 0
        for oid in open_ids:
            try:
                self.broker.cancel_order(oid)
                n += 1
            except Exception:
                pass
        return n

    def _apply_fill(self, symbol: str, side: str, qty: Decimal, price: Decimal, cum_quote: Decimal, *, reason: str, order_id: str, status: str) -> None:
        ss = self.state.symbol_states[symbol]
        realized_delta = D0

        if qty <= 0:
            ss.pending_order_id = None
            ss.pending_reason = None
            ss.pending_since = None
        else:
            if side == "BUY":
                # Prefer cum_quote if provided (crypto). Else compute.
                cost = cum_quote if cum_quote > 0 else (price * qty)
                self.state.cash -= cost
                old_qty = ss.traded_qty
                new_qty = old_qty + qty
                if new_qty > 0:
                    ss.traded_avg_price = ((ss.traded_avg_price * old_qty) + (price * qty)) / new_qty
                ss.traded_qty = new_qty
            else:
                proceeds = cum_quote if cum_quote > 0 else (price * qty)
                self.state.cash += proceeds
                sell_qty = min(qty, ss.traded_qty) if ss.traded_qty > 0 else qty
                realized_delta = sell_qty * (price - ss.traded_avg_price)
                ss.realized_pnl += realized_delta
                ss.traded_qty = max(ss.traded_qty - sell_qty, D0)
                if ss.traded_qty == 0:
                    ss.traded_avg_price = D0

            ss.reference_price = price
            ss.pending_order_id = None
            ss.pending_reason = None
            ss.pending_since = None

        rec = {
            "ts": utcnow().isoformat(),
            "event": "FILL" if qty > 0 else "ORDER_TERMINAL",
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "price": str(price),
            "cum_quote_qty": str(cum_quote),
            "status": status,
            "reason": reason,
            "realized_delta": str(realized_delta),
            "cash_after": str(self.state.cash),
            "traded_qty_after": str(ss.traded_qty),
            "traded_avg_after": str(ss.traded_avg_price),
            "realized_pnl_after": str(ss.realized_pnl),
            "reference_after": str(ss.reference_price) if ss.reference_price is not None else None,
        }
        self.state.trades.append(rec)
        self._append_jsonl(self.trades_path, rec)

    def _poll_pending(self, symbol: str, current_price: Decimal) -> None:
        ss = self.state.symbol_states[symbol]
        if not ss.pending_order_id:
            return

        oid = ss.pending_order_id
        now = utcnow()
        ttl = int(self.exec_cfg.limit_ttl_seconds or 0)

        # If broker supports snapshots (crypto), use them for TTL and partials
        if hasattr(self.broker, "get_order_snapshot"):
            snap = getattr(self.broker, "get_order_snapshot")(oid)
            if snap is None:
                return

            status = snap["status"]
            executed = _dec(snap["executed_qty"])
            cum_quote = _dec(snap.get("cum_quote_qty") or 0)
            avg_price = _dec(snap.get("avg_price") or 0)
            orig = _dec(snap.get("orig_qty") or 0)
            side = snap["side"]

            # Terminal filled
            if status in TERMINAL_STATUSES:
                if executed > 0:
                    self._apply_fill(symbol, side, executed, avg_price, cum_quote, reason=ss.pending_reason or "fill",
                                     order_id=oid, status=status)
                else:
                    self._apply_fill(symbol, side, D0, D0, D0, reason=ss.pending_reason or "terminal",
                                     order_id=oid, status=status)
                return

            # TTL handling for LIMIT orders
            if ttl > 0 and ss.pending_since:
                try:
                    placed = dt.datetime.fromisoformat(ss.pending_since)
                    if placed.tzinfo is None:
                        placed = placed.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    placed = now
                age = (now - placed).total_seconds()
                if age >= ttl:
                    # cancel and replace for remaining
                    try:
                        self.broker.cancel_order(oid)
                    except Exception:
                        return
                    # refresh snapshot after cancel
                    snap2 = getattr(self.broker, "get_order_snapshot")(oid)
                    if snap2:
                        executed2 = _dec(snap2.get("executed_qty") or 0)
                        cum_quote2 = _dec(snap2.get("cum_quote_qty") or 0)
                        avg2 = _dec(snap2.get("avg_price") or 0)
                        side2 = snap2.get("side") or side
                        # apply executed portion (if any)
                        if executed2 > 0:
                            self._apply_fill(symbol, side2, executed2, avg2, cum_quote2, reason=(ss.pending_reason or "") + "|ttl_cancel",
                                             order_id=oid, status="CANCELLED")
                        else:
                            # just clear pending
                            ss.pending_order_id = None
                            ss.pending_reason = None
                            ss.pending_since = None

                        remaining = max(orig - executed2, D0)
                    else:
                        remaining = max(orig - executed, D0)

                    if remaining <= 0:
                        return

                    # place replacement marketable limit with remaining qty
                    # compute new limit price from current_price
                    sl = Decimal(str(self.exec_cfg.slippage_bps)) / Decimal("10000")
                    if side == "BUY":
                        limit_px = current_price * (Decimal("1") + sl)
                    else:
                        limit_px = current_price * (Decimal("1") - sl)

                    req = PlaceOrderRequest(
                        symbol=symbol,
                        side=side,
                        qty=remaining,
                        order_type="LIMIT",
                        limit_price=limit_px,
                        time_in_force="GTC",
                    )
                    new_oid = self.exec.place_with_adaptive_qty(req, reason="ttl_replace")
                    if new_oid:
                        ss.pending_order_id = new_oid
                        ss.pending_reason = "ttl_replace"
                        ss.pending_since = utcnow().isoformat()
                    return

            return

        # Fyers path (poll terminal only)
        term = self.exec.poll_terminal(oid)
        if term is None:
            return
        if term.status in TERMINAL_STATUSES:
            if term.status == "FILLED":
                px = term.avg_price if term.avg_price > 0 else (ss.last_mark_price or D0)
                cum = term.cum_quote_qty if term.cum_quote_qty > 0 else (px * term.filled_qty)
                self._apply_fill(symbol, term.side, term.filled_qty, px, cum, reason=ss.pending_reason or "fill",
                                 order_id=term.order_id, status=term.status)
            else:
                self._apply_fill(symbol, term.side, D0, D0, D0, reason=ss.pending_reason or "terminal",
                                 order_id=term.order_id, status=term.status)

    def _init_reference(self, symbol: str, price: Decimal) -> None:
        ss = self.state.symbol_states[symbol]
        if ss.reference_price is None:
            ss.reference_price = price
            ss.initialized = True

    def _update_extras_crypto(self, prices: Dict[str, Decimal]) -> None:
        # Compute portfolio value if broker provides balances
        try:
            bals = self.broker.balances()
        except Exception:
            bals = {}
        if not bals:
            return
        quote = self.state.extras.get("quote_asset") or "USDC"  # fallback
        qfree = _dec((bals.get(quote) or {}).get("free"))
        qlock = _dec((bals.get(quote) or {}).get("locked"))
        quote_total = qfree + qlock

        total = quote_total
        for sym, px in prices.items():
            info = getattr(self.broker, "symbol_info")(sym)
            base = info.base_asset
            base_total = _dec((bals.get(base) or {}).get("free")) + _dec((bals.get(base) or {}).get("locked"))
            total += base_total * _dec(px)
        self.state.extras["portfolio_value"] = str(total)
        self.state.extras["quote_asset"] = str(quote)

    def _place_intent(self, intent: OrderIntent, *, ltp: Decimal) -> None:
        ss = self.state.symbol_states[intent.symbol]
        if ss.pending_order_id:
            return

        qty = _dec(intent.qty)
        if qty <= 0:
            return

        # BUY cash limit (quote currency)
        if intent.side == "BUY":
            # keep quote reserve
            available = max(self.state.cash - _dec(self.exec_cfg.quote_reserve), D0)
            if ltp > 0:
                max_afford = available / ltp
                if max_afford <= 0:
                    return
                qty = min(qty, max_afford)
                if qty <= 0:
                    return

        # Determine order type and limit px based on execution config
        mode = (self.exec_cfg.order_mode or "market").lower()
        if mode == "marketable_limit":
            sl = Decimal(str(self.exec_cfg.slippage_bps)) / Decimal("10000")
            if intent.side == "BUY":
                limit_px = ltp * (Decimal("1") + sl)
            else:
                limit_px = ltp * (Decimal("1") - sl)
            req = PlaceOrderRequest(
                symbol=intent.symbol,
                side=intent.side,
                qty=qty,
                product_type=self.exec_cfg.product_type,
                order_type="LIMIT",
                limit_price=limit_px,
                time_in_force="GTC",
            )
        elif mode == "limit":
            req = PlaceOrderRequest(
                symbol=intent.symbol,
                side=intent.side,
                qty=qty,
                product_type=self.exec_cfg.product_type,
                order_type="LIMIT",
                limit_price=_dec(intent.limit_price),
                time_in_force="GTC",
            )
        else:
            req = PlaceOrderRequest(
                symbol=intent.symbol,
                side=intent.side,
                qty=qty,
                product_type=self.exec_cfg.product_type,
                order_type="MARKET",
            )

        oid = self.exec.place_with_adaptive_qty(req, reason=intent.reason)
        if oid:
            ss.pending_order_id = oid
            ss.pending_reason = intent.reason
            ss.pending_since = utcnow().isoformat()
            LOG.info("Placed %s %s qty=%s oid=%s reason=%s", intent.symbol, intent.side, str(qty), oid, intent.reason)

    def run_reactive(self, strategy: ReactiveStrategy, *, state_path: str) -> None:
        self.state.ensure_symbols(self.symbols)

        if self.sync_on_start:
            try:
                self.reconcile_from_broker()
            except Exception as e:
                LOG.warning("Sync-on-start failed: %s", e)

        LOG.info("LIVE started symbols=%s", ",".join(self.symbols))

        while True:
            now = now_local(self.market_tz)
            today = now.date().isoformat()

            if self.state.session_date != today:
                self.state.session_date = today
                self.state.reject_events = []
                self.state.cooldown_until = None
                self.state.halted_until = None
                self.state.halt_reason = None
                self.state.last_eod_cancel_date = None

            open_dt = now.replace(hour=self.open_t.hour, minute=self.open_t.minute, second=self.open_t.second, microsecond=0)
            close_dt = now.replace(hour=self.close_t.hour, minute=self.close_t.minute, second=self.close_t.second, microsecond=0)
            eod_cancel_dt = now.replace(hour=self.eod_cancel_t.hour, minute=self.eod_cancel_t.minute, second=self.eod_cancel_t.second, microsecond=0)

            # EOD cancel once (equities). For crypto you can set eod_cancel_time far in the future or ignore.
            if now >= eod_cancel_dt and self.state.last_eod_cancel_date != today:
                n = self.cancel_open_orders(cancel_all=self.cancel_all_open_orders)
                self.state.last_eod_cancel_date = today
                self.state.halted_until = close_dt.astimezone(dt.timezone.utc).isoformat()
                self.state.halt_reason = "EOD_CANCEL"
                LOG.warning("EOD cancel done: cancelled=%d", n)

            allow_new = (now >= open_dt) and (now < eod_cancel_dt)

            try:
                prices = self.broker.get_ltps(self.symbols)
                for sym, px in prices.items():
                    self.state.last_prices[sym] = _dec(px)
                    self.state.symbol_states[sym].last_mark_price = _dec(px)

                self._update_extras_crypto(prices)

                # poll pending
                for sym in self.symbols:
                    self._poll_pending(sym, current_price=_dec(prices[sym]))

                # init refs
                for sym in self.symbols:
                    self._init_reference(sym, _dec(prices[sym]))

                intents = strategy.on_prices(prices, self.state, utcnow().isoformat())

                if allow_new:
                    for it in intents:
                        self._place_intent(it, ltp=_dec(prices[it.symbol]))

                # log status
                parts = []
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    ref = ss.reference_price or D0
                    parts.append(f"{s} px={px} ref={ref} traded={ss.traded_qty} avg={ss.traded_avg_price} R={ss.realized_pnl}")
                pv = self.state.extras.get("portfolio_value")
                pv_str = f" pv={pv}" if pv else ""
                # --- Stage-1 PnL persistence (hybrid: account + strategy) ---
                broker_name = infer_broker_name(self.broker)
                port_val, quote_asset, port_details = compute_portfolio_value_for_symbols(self.broker, self.symbols, prices, self.state)
                start_val = ensure_portfolio_start(self.state, port_val)
                port_pnl = port_val - start_val
                port_pnl_pct = (port_pnl / start_val) if start_val > 0 else D0
                dd = update_drawdown(self.state, port_val)

                se, realized, unreal, st_total, exposure, exp_pct = compute_strategy_pnl(self.state)

                pt = PnLPoint(
                    ts=utcnow().isoformat(),
                    broker=broker_name,
                    quote_asset=str(quote_asset),
                    portfolio_value=port_val,
                    portfolio_pnl=port_pnl,
                    portfolio_pnl_pct=port_pnl_pct,
                    strategy_equity=se,
                    strategy_realized=realized,
                    strategy_unrealized=unreal,
                    strategy_total=st_total,
                    drawdown_pct=dd,
                    exposure=exposure,
                    exposure_pct=exp_pct,
                )
                if self._pnl_writer:
                    self._pnl_writer.append(pt)

                snap = {
                    "ts": pt.ts,
                    "broker": broker_name,
                    "quote_asset": str(quote_asset),
                    "portfolio_value": str(port_val),
                    "portfolio_pnl": str(port_pnl),
                    "portfolio_pnl_pct": str(port_pnl_pct),
                    "drawdown_pct": str(dd),
                    "portfolio_details": port_details,
                    "symbols": {},
                }
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    snap["symbols"][s] = {
                        "px": str(px),
                        "ref": str(ss.reference_price) if ss.reference_price is not None else None,
                        "traded_qty": str(ss.traded_qty),
                        "avg_price": str(ss.traded_avg_price),
                        "realized": str(ss.realized_pnl),
                        "unrealized": str((ss.traded_qty * (px - ss.traded_avg_price)) if (px is not None) else D0),
                        "pending_order_id": ss.pending_order_id,
                        "pending_reason": ss.pending_reason,
                    }
                if self._pnl_writer:
                    self._pnl_writer.write_snapshot(snap)
                    self._pnl_writer.write_summary({
                        "ts": pt.ts,
                        "portfolio_value": str(port_val),
                        "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct),
                        "strategy_total": str(st_total),
                        "strategy_realized": str(realized),
                        "strategy_unrealized": str(unreal),
                        "max_dd": str(self.state.extras.get("pnl_max_dd") or "0"),
                        "quote_asset": str(quote_asset),
                    })
                LOG.info("cash=%s eq=%s%s | %s", str(self.state.cash), str(self.state.strategy_equity()), pv_str, " | ".join(parts))

            except KeyboardInterrupt:
                LOG.info("Stopped by user.")
                break
            except Exception as e:
                LOG.exception("Loop error: %s", e)

            self.state.last_update_ts = utcnow().isoformat()
            self.state.dump(state_path)

            sleep_s = self.poll_seconds if (now >= open_dt and now < close_dt) else self.closed_poll_seconds
            time.sleep(max(int(sleep_s), 1))

    def run_managed(self, strategy: ManagedOrderStrategy, *, state_path: str) -> None:
        self.state.ensure_symbols(self.symbols)

        if self.sync_on_start:
            try:
                self.reconcile_from_broker()
            except Exception as e:
                LOG.warning("Sync-on-start failed: %s", e)

        LOG.info("MANAGED LIVE started symbols=%s", ",".join(self.symbols))

        # Track orders placed by managed strategies so we can detect fills and notify strategy
        self.state.extras.setdefault("managed_order_meta", {})  # order_id -> meta dict

        while True:
            now = now_local(self.market_tz)
            today = now.date().isoformat()

            # session reset by market date
            if self.state.session_date != today:
                self.state.session_date = today
                self.state.reject_events = []
                self.state.cooldown_until = None
                self.state.halted_until = None
                self.state.halt_reason = None
                self.state.last_eod_cancel_date = None

            open_dt = now.replace(hour=self.open_t.hour, minute=self.open_t.minute, second=self.open_t.second, microsecond=0)
            close_dt = now.replace(hour=self.close_t.hour, minute=self.close_t.minute, second=self.close_t.second, microsecond=0)
            eod_cancel_dt = now.replace(hour=self.eod_cancel_t.hour, minute=self.eod_cancel_t.minute, second=self.eod_cancel_t.second, microsecond=0)

            # EOD cancel at/after time once per day (equities). For crypto you typically set eod_cancel_time=23:59:59.
            if now >= eod_cancel_dt and self.state.last_eod_cancel_date != today:
                try:
                    n = self.cancel_open_orders(cancel_all=self.cancel_all_open_orders)
                    self.state.last_eod_cancel_date = today
                    self.state.halted_until = close_dt.astimezone(dt.timezone.utc).isoformat()
                    self.state.halt_reason = "EOD_CANCEL"
                    LOG.warning("EOD cancel done: cancelled=%d", n)
                except Exception as e:
                    LOG.warning("EOD cancel failed: %s", e)

            allow_place = (now >= open_dt) and (now < eod_cancel_dt)

            try:
                prices = self.broker.get_ltps(self.symbols)

                # update marks
                for sym, px in prices.items():
                    self.state.last_prices[sym] = _dec(px)
                    self.state.symbol_states[sym].last_mark_price = _dec(px)

                # fetch open orders once
                ob = self.broker.orderbook()
                open_orders = ob.get("orderBook") or ob.get("data") or ob.get("orders") or []
                if isinstance(open_orders, dict):
                    open_orders = list(open_orders.values())

                open_ids = set()
                for o in (open_orders or []):
                    if not isinstance(o, dict):
                        continue
                    oid = str(o.get("id") or o.get("order_id") or "")
                    if oid:
                        open_ids.add(oid)

                now_ts = utcnow().isoformat()

                # Optional hook for strategies that need broker access (e.g., anchor prev close)
                if hasattr(strategy, "ensure_anchor"):
                    try:
                        strategy.ensure_anchor(self.broker, self.state, now_ts, prices)  # type: ignore[attr-defined]
                    except Exception as e:
                        LOG.warning("strategy.ensure_anchor failed: %s", e)

                # 1) detect terminals for tracked orders that disappeared from open orders
                meta_map: Dict[str, Any] = self.state.extras.get("managed_order_meta", {})  # type: ignore[assignment]
                for oid, meta in list(meta_map.items()):
                    if str(oid) in open_ids:
                        continue

                    try:
                        term = self.exec.poll_terminal(str(oid))
                    except Exception:
                        term = None

                    if term is None:
                        continue

                    if term.status in TERMINAL_STATUSES:
                        if term.status == "FILLED" and term.filled_qty > 0:
                            px = term.avg_price if term.avg_price > 0 else (_dec(self.state.last_prices.get(term.symbol)) if term.symbol else D0)
                            cum = term.cum_quote_qty if term.cum_quote_qty > 0 else (px * term.filled_qty)
                            self._apply_fill(term.symbol, term.side, term.filled_qty, px, cum,
                                             reason=str(meta.get("reason") or "managed_fill"),
                                             order_id=term.order_id, status=term.status)
                        else:
                            self._apply_fill(term.symbol, term.side, D0, D0, D0,
                                             reason=str(meta.get("reason") or "managed_terminal"),
                                             order_id=term.order_id, status=term.status)

                        if hasattr(strategy, "on_order_terminal"):
                            try:
                                strategy.on_order_terminal(term, meta, self.state)  # type: ignore[attr-defined]
                            except Exception as e:
                                LOG.warning("strategy.on_order_terminal failed: %s", e)

                        meta_map.pop(str(oid), None)

                self.state.extras["managed_order_meta"] = meta_map

                # 2) get desired actions from strategy
                actions = strategy.desired_actions(prices, list(open_orders or []), self.state, now_ts)

                # 3) execute actions
                for act in actions:
                    if act.kind == "CANCEL" and act.order_id:
                        try:
                            self.broker.cancel_order(act.order_id)
                            LOG.info("Cancelled order %s reason=%s", act.order_id, act.reason)
                        except Exception as e:
                            LOG.warning("Cancel failed %s: %s", act.order_id, e)
                        self.state.extras.get("managed_order_meta", {}).pop(str(act.order_id), None)
                        if hasattr(strategy, "on_order_cancelled"):
                            try:
                                strategy.on_order_cancelled(str(act.order_id), act.meta or {}, self.state)  # type: ignore[attr-defined]
                            except Exception:
                                pass

                    elif act.kind == "PLACE" and act.request:
                        if not allow_place:
                            continue
                        oid = self.exec.place_with_adaptive_qty(act.request, reason=act.reason)
                        if oid:
                            meta = dict(act.meta or {})
                            meta.setdefault("symbol", act.request.symbol)
                            meta.setdefault("side", act.request.side)
                            meta.setdefault("reason", act.reason)
                            meta.setdefault("ts", now_ts)
                            self.state.extras.setdefault("managed_order_meta", {})[str(oid)] = meta
                            LOG.info("Placed order oid=%s %s %s qty=%s", oid, act.request.symbol, act.request.side, str(act.request.qty))

                            if hasattr(strategy, "on_order_placed"):
                                try:
                                    strategy.on_order_placed(str(oid), meta, self.state)  # type: ignore[attr-defined]
                                except Exception as e:
                                    LOG.warning("strategy.on_order_placed failed: %s", e)

                # status line
                parts = []
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    ref = ss.reference_price or D0
                    parts.append(f"{s} px={px} ref={ref} traded={ss.traded_qty} avg={ss.traded_avg_price} R={ss.realized_pnl}")
                # --- Stage-1 PnL persistence (hybrid: account + strategy) ---
                broker_name = infer_broker_name(self.broker)
                port_val, quote_asset, port_details = compute_portfolio_value_for_symbols(self.broker, self.symbols, prices, self.state)
                start_val = ensure_portfolio_start(self.state, port_val)
                port_pnl = port_val - start_val
                port_pnl_pct = (port_pnl / start_val) if start_val > 0 else D0
                dd = update_drawdown(self.state, port_val)

                se, realized, unreal, st_total, exposure, exp_pct = compute_strategy_pnl(self.state)

                pt = PnLPoint(
                    ts=utcnow().isoformat(),
                    broker=broker_name,
                    quote_asset=str(quote_asset),
                    portfolio_value=port_val,
                    portfolio_pnl=port_pnl,
                    portfolio_pnl_pct=port_pnl_pct,
                    strategy_equity=se,
                    strategy_realized=realized,
                    strategy_unrealized=unreal,
                    strategy_total=st_total,
                    drawdown_pct=dd,
                    exposure=exposure,
                    exposure_pct=exp_pct,
                )
                if self._pnl_writer:
                    self._pnl_writer.append(pt)

                snap = {
                    "ts": pt.ts,
                    "broker": broker_name,
                    "quote_asset": str(quote_asset),
                    "portfolio_value": str(port_val),
                    "portfolio_pnl": str(port_pnl),
                    "portfolio_pnl_pct": str(port_pnl_pct),
                    "drawdown_pct": str(dd),
                    "portfolio_details": port_details,
                    "symbols": {},
                }
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    snap["symbols"][s] = {
                        "px": str(px),
                        "ref": str(ss.reference_price) if ss.reference_price is not None else None,
                        "traded_qty": str(ss.traded_qty),
                        "avg_price": str(ss.traded_avg_price),
                        "realized": str(ss.realized_pnl),
                        "unrealized": str((ss.traded_qty * (px - ss.traded_avg_price)) if (px is not None) else D0),
                        "pending_order_id": ss.pending_order_id,
                        "pending_reason": ss.pending_reason,
                    }
                if self._pnl_writer:
                    self._pnl_writer.write_snapshot(snap)
                    self._pnl_writer.write_summary({
                        "ts": pt.ts,
                        "portfolio_value": str(port_val),
                        "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct),
                        "strategy_total": str(st_total),
                        "strategy_realized": str(realized),
                        "strategy_unrealized": str(unreal),
                        "max_dd": str(self.state.extras.get("pnl_max_dd") or "0"),
                        "quote_asset": str(quote_asset),
                    })
                LOG.info("cash=%s eq=%s | %s", str(self.state.cash), str(self.state.strategy_equity()), " | ".join(parts))

            except KeyboardInterrupt:
                LOG.info("Stopped by user.")
                break
            except Exception as e:
                LOG.exception("Managed loop error: %s", e)

            self.state.last_update_ts = utcnow().isoformat()
            self.state.dump(state_path)

            sleep_s = self.poll_seconds if (now >= open_dt and now < close_dt) else self.closed_poll_seconds
            time.sleep(max(int(sleep_s), 1))
