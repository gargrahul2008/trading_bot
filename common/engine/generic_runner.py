from __future__ import annotations
import datetime as dt
import csv
import os
import json
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List

from common.broker.interfaces import Broker, PlaceOrderRequest, to_decimal, OrderTerminal
from common.engine.state import GlobalState
from common.engine.strategy_base import ReactiveStrategy, ManagedOrderStrategy, OrderIntent
from common.engine.execution import OrderExecutor, ExecutionConfig
from common.utils.logger import setup_logger
from common.utils.timeutils import parse_hhmm, parse_hhmmss, now_local, utcnow
from common.engine.pnl import (
    PnLWriter, PnLPoint,
    infer_broker_name, compute_portfolio_value_for_symbols,
    compute_strategy_pnl, update_drawdown, ensure_portfolio_start,
    ensure_today_buckets, update_trade_counters, realized_today,
)

LOG = setup_logger("runner")

TERMINAL_STATUSES = {"FILLED", "REJECTED", "CANCELLED"}

D0 = Decimal("0")

def _dec(x: Any) -> Decimal:
    return to_decimal(x)

class GenericRunner:
    def __init__(self, *, broker: Broker, state: GlobalState, symbols: List[str], exec_cfg: ExecutionConfig,
                 trades_path: str, rejects_path: str, market_tz: str, market_open: str, market_close: str,
                 eod_cancel_time: str, poll_seconds: int, closed_poll_seconds: int, cancel_all_open_orders: bool,
                 sync_on_start: bool, adopt_broker_inventory: bool, manual_adjustments_path: str | None = None):
        self.broker = broker
        self.state = state
        self.symbols = symbols
        self.exec = OrderExecutor(broker, state, exec_cfg, rejects_path=rejects_path)
        self.exec_cfg = exec_cfg
        self.state.extras["use_inventory_buffer"] = bool(exec_cfg.use_inventory_buffer)
        self.trades_path = trades_path
        self.manual_adjustments_path = manual_adjustments_path
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
        self.price_points_path = os.path.join(base_dir, "price_points.jsonl")
        self.pnl_daily_path = os.path.join(base_dir, "pnl_daily.csv")
        self.price_daily_path = os.path.join(base_dir, "price_daily.csv")
        self._last_price_point: Dict[str, str] = {}
        os.makedirs(os.path.dirname(self.price_points_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(self.pnl_daily_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(self.price_daily_path) or ".", exist_ok=True)
        self._maybe_backfill_daily_files()
        if self.manual_adjustments_path:
            os.makedirs(os.path.dirname(self.manual_adjustments_path) or ".", exist_ok=True)

    def _append_jsonl(self, path: str, rec: Dict[str, Any]) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as e:
            LOG.warning("Failed writing %s: %s", path, e)

    def _append_price_point(self, *, ts: str) -> None:
        """Persist symbol price snapshots for exact adjusted-curve reconstruction."""
        prices: Dict[str, str] = {}
        for s in self.symbols:
            px = self.state.last_prices.get(s)
            if px is None:
                continue
            prices[s] = str(_dec(px))
        if not prices:
            return
        # Write only on change to keep file growth under control.
        if prices == self._last_price_point:
            return
        self._last_price_point = dict(prices)
        self._append_jsonl(self.price_points_path, {"ts": ts, "prices": prices})

    def _append_daily_csv_row(self, path: str, fieldnames: List[str], row: Dict[str, Any]) -> None:
        exists = os.path.exists(path)
        try:
            with open(path, "a", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                if not exists:
                    w.writeheader()
                w.writerow({k: row.get(k, "") for k in fieldnames})
        except Exception as e:
            LOG.warning("Failed writing %s: %s", path, e)

    def _maybe_backfill_daily_files(self) -> None:
        # One-time backfill so dashboard can show long history immediately.
        if not os.path.exists(self.pnl_daily_path):
            src = getattr(self._pnl_writer, "csv_path", "")
            if src and os.path.exists(src):
                try:
                    out_rows: List[Dict[str, str]] = []
                    prev_date = ""
                    prev_row: Dict[str, str] | None = None
                    with open(src, "r", encoding="utf-8", newline="") as f:
                        r = csv.DictReader(f)
                        for row in r:
                            ts = str(row.get("ts") or "")
                            if len(ts) < 10:
                                continue
                            d = ts[:10]
                            slim = {
                                "date_utc": d,
                                "ts": ts,
                                "portfolio_value": str(row.get("portfolio_value") or ""),
                                "portfolio_pnl": str(row.get("portfolio_pnl") or ""),
                                "portfolio_pnl_pct": str(row.get("portfolio_pnl_pct") or ""),
                            }
                            if not prev_date:
                                prev_date = d
                                prev_row = slim
                                continue
                            if d != prev_date and prev_row:
                                out_rows.append(prev_row)
                            prev_date = d
                            prev_row = slim
                    if prev_row:
                        out_rows.append(prev_row)
                    if out_rows:
                        with open(self.pnl_daily_path, "w", encoding="utf-8", newline="") as f:
                            w = csv.DictWriter(f, fieldnames=["date_utc", "ts", "portfolio_value", "portfolio_pnl", "portfolio_pnl_pct"])
                            w.writeheader()
                            for rr in out_rows:
                                w.writerow(rr)
                        LOG.info("Backfilled %s daily portfolio points into %s", len(out_rows), self.pnl_daily_path)
                except Exception as e:
                    LOG.warning("Daily pnl backfill failed: %s", e)

        if not os.path.exists(self.price_daily_path) and os.path.exists(self.price_points_path):
            try:
                out_rows2: List[Dict[str, str]] = []
                prev_date = ""
                prev_row: Dict[str, str] | None = None
                with open(self.price_points_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except Exception:
                            continue
                        ts = str(rec.get("ts") or "")
                        prices = rec.get("prices")
                        if len(ts) < 10 or not isinstance(prices, dict):
                            continue
                        d = ts[:10]
                        slim = {"date_utc": d, "ts": ts, "prices": json.dumps(prices)}
                        if not prev_date:
                            prev_date = d
                            prev_row = slim
                            continue
                        if d != prev_date and prev_row:
                            out_rows2.append(prev_row)
                        prev_date = d
                        prev_row = slim
                if prev_row:
                    out_rows2.append(prev_row)
                if out_rows2:
                    with open(self.price_daily_path, "w", encoding="utf-8", newline="") as f:
                        w = csv.DictWriter(f, fieldnames=["date_utc", "ts", "prices"])
                        w.writeheader()
                        for rr in out_rows2:
                            w.writerow(rr)
                    LOG.info("Backfilled %s daily price points into %s", len(out_rows2), self.price_daily_path)
            except Exception as e:
                LOG.warning("Daily price backfill failed: %s", e)

    def _update_daily_points(self, *, pt: PnLPoint) -> None:
        """
        Keep one finalized point per UTC day.
        We flush previous day when a new UTC day starts.
        """
        try:
            dt_utc = dt.datetime.fromisoformat(str(pt.ts))
        except Exception:
            dt_utc = utcnow()
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=dt.timezone.utc)
        date_utc = dt_utc.date().isoformat()

        prices: Dict[str, str] = {}
        for s in self.symbols:
            px = self.state.last_prices.get(s)
            if px is not None:
                prices[s] = str(_dec(px))

        pending_date = str(self.state.extras.get("pnl_daily_pending_date") or "")
        if pending_date and pending_date != date_utc:
            prev_row = {
                "date_utc": pending_date,
                "ts": str(self.state.extras.get("pnl_daily_pending_ts") or ""),
                "portfolio_value": str(self.state.extras.get("pnl_daily_pending_portfolio_value") or ""),
                "portfolio_pnl": str(self.state.extras.get("pnl_daily_pending_portfolio_pnl") or ""),
                "portfolio_pnl_pct": str(self.state.extras.get("pnl_daily_pending_portfolio_pnl_pct") or ""),
            }
            if prev_row["portfolio_value"]:
                self._append_daily_csv_row(
                    self.pnl_daily_path,
                    ["date_utc", "ts", "portfolio_value", "portfolio_pnl", "portfolio_pnl_pct"],
                    prev_row,
                )

            prev_prices_json = str(self.state.extras.get("pnl_daily_pending_prices_json") or "")
            if prev_prices_json:
                self._append_daily_csv_row(
                    self.price_daily_path,
                    ["date_utc", "ts", "prices"],
                    {
                        "date_utc": pending_date,
                        "ts": str(self.state.extras.get("pnl_daily_pending_ts") or ""),
                        "prices": prev_prices_json,
                    },
                )

        self.state.extras["pnl_daily_pending_date"] = date_utc
        self.state.extras["pnl_daily_pending_ts"] = str(pt.ts)
        self.state.extras["pnl_daily_pending_portfolio_value"] = str(pt.portfolio_value)
        self.state.extras["pnl_daily_pending_portfolio_pnl"] = str(pt.portfolio_pnl)
        self.state.extras["pnl_daily_pending_portfolio_pnl_pct"] = str(pt.portfolio_pnl_pct)
        self.state.extras["pnl_daily_pending_prices_json"] = json.dumps(prices)

    def reconcile_from_broker(self) -> None:
        """Sync cash + adopt broker inventory into traded_qty (best-effort).
        - Crypto: uses balances() and symbol_info()
        - Equities: uses funds_cash() + positions/holdings sellable qty
        """
        # Try crypto path first (balances exist and non-empty)
        bals = {}
        try:
            bals = self.broker.balances() or {}
        except Exception:
            bals = {}

        if bals:
            # ---- Crypto path ----
            quote_assets = set()
            base_by_sym = {}

            for sym in self.symbols:
                info = getattr(self.broker, "symbol_info")(sym)
                quote_assets.add(info.quote_asset)
                base_by_sym[sym] = info.base_asset

            if len(quote_assets) != 1:
                raise RuntimeError(f"All crypto symbols must share same quote asset. got={sorted(quote_assets)}")

            quote_asset = next(iter(quote_assets))
            self.state.extras["quote_asset"] = quote_asset

            q = bals.get(quote_asset) or {}
            if not self.state.extras.get("strategy_cash_initialized"):
                self.state.cash = _dec(q.get("free") or "0")
                self.state.extras["strategy_cash_initialized"] = True

            # if not self.adopt_broker_inventory:
            #     return
            #
            # for sym in self.symbols:
            #     ss = self.state.symbol_states[sym]
            #     ss.core_qty = D0
            #     base = base_by_sym.get(sym)
            #     total = D0
            #     if base and base in bals:
            #         total = _dec(bals[base].get("free")) + _dec(bals[base].get("locked"))
            #     ss.traded_qty = total
            # LOG.info("Adopted crypto balances into traded_qty.")
            LOG.info("Crypto reconcile: cash synced from balances; strategy inventory NOT adopted from broker.")
            return

        # ---- Equities fallback ----
        try:
            self.state.cash = _dec(self.broker.funds_cash())
        except Exception:
            pass

        if not self.adopt_broker_inventory:
            return

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

    def _ensure_lots(self, ss) -> None:
        lots = getattr(ss, "lots", None)
        if lots is None:
            ss.lots = []
            lots = ss.lots
        cleaned = []
        for lot in (lots or []):
            if not isinstance(lot, dict):
                continue
            qty = _dec(lot.get("qty") or lot.get("quantity") or 0)
            price = _dec(lot.get("price") or lot.get("avg_price") or 0)
            if qty > 0:
                cleaned.append({"qty": qty, "price": price})
        if not cleaned and ss.traded_qty > 0:
            cleaned = [{"qty": _dec(ss.traded_qty), "price": _dec(ss.traded_avg_price)}]
        ss.lots = cleaned
        self._recalc_from_lots(ss)

    def _recalc_from_lots(self, ss) -> None:
        lots = getattr(ss, "lots", None) or []
        total_qty = D0
        total_cost = D0
        for lot in lots:
            if not isinstance(lot, dict):
                continue
            qty = _dec(lot.get("qty") or 0)
            price = _dec(lot.get("price") or 0)
            if qty <= 0:
                continue
            total_qty += qty
            total_cost += qty * price
        if total_qty <= 0:
            ss.traded_qty = D0
            ss.traded_avg_price = D0
            ss.lots = []
        else:
            ss.traded_qty = total_qty
            ss.traded_avg_price = (total_cost / total_qty) if total_qty > 0 else D0

    def _add_lot(self, ss, qty: Decimal, price: Decimal) -> None:
        if qty <= 0:
            return
        self._ensure_lots(ss)
        ss.lots.append({"qty": _dec(qty), "price": _dec(price)})
        self._recalc_from_lots(ss)

    def _consume_lots_lifo(self, ss, qty: Decimal, sell_price: Decimal) -> Decimal:
        if qty <= 0:
            return D0
        self._ensure_lots(ss)
        remaining = _dec(qty)
        realized = D0
        while remaining > 0 and ss.lots:
            lot = ss.lots[-1]
            lot_qty = _dec(lot.get("qty") or 0)
            lot_price = _dec(lot.get("price") or 0)
            if lot_qty <= 0:
                ss.lots.pop()
                continue
            take = remaining if remaining < lot_qty else lot_qty
            realized += take * (_dec(sell_price) - lot_price)
            lot_qty = lot_qty - take
            remaining = remaining - take
            if lot_qty <= 0:
                ss.lots.pop()
            else:
                lot["qty"] = lot_qty
        self._recalc_from_lots(ss)
        return realized

    def _apply_fill(self, symbol: str, side: str, qty: Decimal, price: Decimal, cum_quote: Decimal, *, reason: str, order_id: str, status: str, skip_ref_update: bool = False) -> None:
        ss = self.state.symbol_states[symbol]
        realized_delta = D0

        if qty <= 0:
            ss.pending_order_id = None
            ss.pending_reason = None
            ss.pending_since = None
        else:
            if side == "SELL":
                proceeds = cum_quote if cum_quote > 0 else (price * qty)
                self.state.cash += proceeds

                # 1) sell from strategy inventory first (LIFO lots)
                self._ensure_lots(ss)
                sell_from_traded = min(qty, ss.traded_qty) if ss.traded_qty > 0 else D0
                if sell_from_traded > 0:
                    realized_delta = self._consume_lots_lifo(ss, sell_from_traded, price)
                    ss.realized_pnl += realized_delta

                # 2) remainder is "borrowed" (sell-first buffer)
                sell_from_borrow = qty - sell_from_traded
                if sell_from_borrow > 0:
                    old_b = ss.borrowed_qty
                    new_b = old_b + sell_from_borrow
                    if new_b > 0:
                        ss.borrowed_avg_sell = ((ss.borrowed_avg_sell * old_b) + (price * sell_from_borrow)) / new_b
                    ss.borrowed_qty = new_b

            else:  # BUY
                cost = cum_quote if cum_quote > 0 else (price * qty)
                self.state.cash -= cost

                remaining = qty

                # 1) cover borrowed first (realized happens here)
                if ss.borrowed_qty > 0:
                    cover = min(remaining, ss.borrowed_qty)
                    if cover > 0:
                        realized_delta = cover * (ss.borrowed_avg_sell - price)
                        ss.realized_pnl += realized_delta

                        ss.borrowed_qty = ss.borrowed_qty - cover
                        remaining = remaining - cover
                        if ss.borrowed_qty <= 0:
                            ss.borrowed_qty = D0
                            ss.borrowed_avg_sell = D0

                # 2) leftover adds to strategy inventory
                if remaining > 0:
                    self._add_lot(ss, remaining, price)

            if not skip_ref_update and not (reason or "").startswith("rebalance_"):
                ss.reference_price = price
            ss.pending_order_id = None
            ss.pending_reason = None
            ss.pending_since = None

        expected = _dec(ss.pending_expected_price) if ss.pending_expected_price is not None else D0
        expected_src = ss.pending_expected_source or None

        # slippage bps: positive = worse
        slip_bps = None
        if expected > 0 and qty > 0 and price > 0:
            if side == "BUY":
                slip_bps = (price - expected) / expected * Decimal("10000")
            else:  # SELL
                slip_bps = (expected - price) / expected * Decimal("10000")
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
            "borrowed_qty_after": str(ss.borrowed_qty),
            "borrowed_avg_sell_after": str(ss.borrowed_avg_sell),
            "net_qty_after": str(ss.traded_qty - ss.borrowed_qty),
            "expected_price": str(expected) if expected and expected > 0 else None,
            "expected_source": expected_src,
            "slippage_bps": str(slip_bps) if slip_bps is not None else None,
        }
        self.state.trades.append(rec)
        self._append_jsonl(self.trades_path, rec)
        # --- cycles/trade totals today(UTC) + all-time (ladder trades only) ---
        try:
            if qty > 0 and not (reason or "").startswith("rebalance_"):
                eff_cum = cum_quote if cum_quote > 0 else (price * qty)
                update_trade_counters(
                    self.state,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    cum_quote_qty=eff_cum,
                )
        except Exception:
            pass
        ss.pending_expected_price = None
        ss.pending_expected_source = None

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
        self.state.cash = qfree
        qlock = _dec((bals.get(quote) or {}).get("locked"))
        quote_total = qfree + qlock

        total = quote_total
        per_symbol_totals: Dict[str, Dict[str, Decimal]] = {}
        for sym, px in prices.items():
            info = getattr(self.broker, "symbol_info")(sym)
            base = info.base_asset
            base_total = _dec((bals.get(base) or {}).get("free")) + _dec((bals.get(base) or {}).get("locked"))
            total += base_total * _dec(px)
            per_symbol_totals[sym] = {"base": str(base), "base_total": base_total, "px": _dec(px)}
            self.state.extras[f"broker_base_qty_{sym}"] = str(base_total)
        self.state.extras["portfolio_value"] = str(total)
        self.state.extras["quote_asset"] = str(quote)
        if self.state.extras.get("reconcile_crypto_balances"):
            self._reconcile_manual_inventory(per_symbol_totals, quote_asset=str(quote))

    def _reconcile_manual_inventory(self, per_symbol_totals: Dict[str, Dict[str, Decimal]], *, quote_asset: str) -> None:
        manual_map = self.state.extras.setdefault("manual_inventory_by_symbol", {})
        ts = utcnow().isoformat()
        for sym, d in per_symbol_totals.items():
            base_total = _dec(d.get("base_total") or 0)
            ss = self.state.symbol_states.get(sym)
            bot_net = D0
            if ss is not None:
                bot_net = _dec(ss.traded_qty) - _dec(getattr(ss, "borrowed_qty", D0))
            manual_qty = base_total - bot_net
            prev = _dec(manual_map.get(sym, "0"))
            if manual_qty != prev:
                manual_map[sym] = str(manual_qty)
                if self.manual_adjustments_path:
                    rec = {
                        "ts": ts,
                        "event": "MANUAL_BALANCE_RECONCILE",
                        "symbol": sym,
                        "base_asset": str(d.get("base") or ""),
                        "quote_asset": quote_asset,
                        "base_total": str(base_total),
                        "bot_net_qty": str(bot_net),
                        "manual_qty": str(manual_qty),
                        "manual_delta": str(manual_qty - prev),
                        "px": str(_dec(d.get("px") or 0)),
                        "reason": "balance_reconcile",
                    }
                    self._append_jsonl(self.manual_adjustments_path, rec)

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

        # SELL cap for crypto (buffer inventory protection)
        if intent.side == "SELL" and hasattr(self.broker, "balances") and hasattr(self.broker, "symbol_info"):
            try:
                bals = self.broker.balances() or {}
                info = self.broker.symbol_info(intent.symbol)
                base = info.base_asset
                base_free = _dec((bals.get(base) or {}).get("free"))
                if base_free <= 0:
                    return  # nothing sellable -> skip
                qty = min(qty, base_free)
                if qty <= 0:
                    return
            except Exception:
                pass
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
        # expected price for slippage tracking
        if getattr(req, "order_type", "").upper() == "LIMIT" and getattr(req, "limit_price", None):
            exp = _dec(req.limit_price)
            src = "limit_price"
        else:
            exp = _dec(ltp)
            src = "ltp"
        ss.pending_expected_price = exp
        ss.pending_expected_source = src

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
                # --- realized today (UTC) baseline + value ---
                ensure_today_buckets(self.state, realized_now=realized)
                realized_td = realized_today(self.state, realized_now=realized)
                non_strategy_value = port_val - se
                non_strategy_pct = (non_strategy_value / port_val) if port_val > 0 else D0
                non_strategy_value = port_val - se
                non_strategy_pct = (non_strategy_value / port_val) if port_val > 0 else D0
                # --- deployed (market now/peak) and deployed cost (strategy bucket) ---
                deployed_market = D0

                # crypto: port_details has quote_total
                if isinstance(port_details, dict) and port_details.get("quote_total") is not None:
                    quote_total = Decimal(str(port_details.get("quote_total") or "0"))
                    deployed_market = port_val - quote_total
                else:
                    # equities: sum qty_used*px from port_details
                    per = (port_details.get("per_symbol") or {}) if isinstance(port_details, dict) else {}
                    for _, d in per.items():
                        try:
                            deployed_market += Decimal(str(d.get("qty_used") or "0")) * Decimal(str(d.get("px") or "0"))
                        except Exception:
                            pass

                peak = Decimal(str(self.state.extras.get("deployed_market_peak") or "0"))
                if deployed_market > peak:
                    self.state.extras["deployed_market_peak"] = str(deployed_market)

                deployed_cost_strategy = D0
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    deployed_cost_strategy += Decimal(str(ss.traded_qty)) * Decimal(str(ss.traded_avg_price))
                unit_map = self.state.extras.get("cycle_unit_quote_by_symbol") or {}

                def _cycles_block(store: dict) -> dict:
                    out = {"per_symbol": {}}
                    per = (store.get("per_symbol") or {})
                    for sym, rec in per.items():
                        buy_q = Decimal(str(rec.get("buy_quote") or "0"))
                        sell_q = Decimal(str(rec.get("sell_quote") or "0"))
                        cycle_q = min(buy_q, sell_q)
                        unit = unit_map.get(sym)
                        cycles_est = (cycle_q / Decimal(str(unit))) if unit else None
                        out["per_symbol"][sym] = {
                            "buy_quote": str(buy_q),
                            "sell_quote": str(sell_q),
                            "buy_qty": str(rec.get("buy_qty") or "0"),
                            "sell_qty": str(rec.get("sell_qty") or "0"),
                            "cycle_quote": str(cycle_q),
                            "cycle_unit_quote": str(unit) if unit else None,
                            "cycles_est": str(cycles_est) if cycles_est is not None else None,
                        }
                    return out

                cycles_today_store = self.state.extras.get("cycles_today") or {"per_symbol": {}}
                cycles_all_store = self.state.extras.get("cycles_all_time") or {"per_symbol": {}}

                cycles_today_out = {
                    "date_utc": self.state.extras.get("cycles_today_utc_date"),
                    **_cycles_block(cycles_today_store),
                }
                cycles_all_out = _cycles_block(cycles_all_store)
                holdings_out = {
                    "quote_total": str(port_details.get("quote_total")) if isinstance(port_details,
                                                                                      dict) and port_details.get(
                        "quote_total") is not None else None,
                    "per_symbol": (port_details.get("per_symbol") if isinstance(port_details, dict) else {}),
                }

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
                self._append_price_point(ts=pt.ts)
                self._update_daily_points(pt=pt)

                manual_map = self.state.extras.get("manual_inventory_by_symbol") or {}
                snap = {"ts": pt.ts, "broker": broker_name, "quote_asset": str(quote_asset),
                        "portfolio_value": str(port_val), "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct), "drawdown_pct": str(dd),
                        "portfolio_details": port_details, "symbols": {}, "created": {
                        "strategy_realized_today": str(realized_td),
                        "strategy_realized_all_time": str(realized),
                        "strategy_unrealized_now": str(unreal),
                        "strategy_total_now": str(st_total),
                    }, "bot": {
                        "equity": str(se),
                        "realized_today": str(realized_td),
                        "realized_all_time": str(realized),
                        "unrealized_now": str(unreal),
                        "total_now": str(st_total),
                    }, "non_strategy": {
                        "value_est": str(non_strategy_value),
                        "value_pct_est": str(non_strategy_pct),
                    }, "manual_inventory_by_symbol": manual_map, "deployed": {
                        "deployed_market_now": str(deployed_market),
                        "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                        "deployed_cost_strategy_now": str(deployed_cost_strategy),
                        "quote_asset": str(quote_asset),
                    }, "cycles_today": cycles_today_out, "cycles_all_time": cycles_all_out, "holdings": holdings_out}
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
                        "borrowed_qty": str(ss.borrowed_qty),
                        "borrowed_avg_sell": str(ss.borrowed_avg_sell),
                        "net_qty": str(ss.traded_qty - ss.borrowed_qty),
                    }
                if self._pnl_writer:
                    self._pnl_writer.write_snapshot(snap)
                    summary_out = {
                        "ts": pt.ts,
                        "portfolio_value": str(port_val),
                        "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct),
                        "max_dd": str(self.state.extras.get("pnl_max_dd") or "0"),
                        "quote_asset": str(quote_asset),

                        "created": {
                            "strategy_realized_today": str(realized_td),
                            "strategy_realized_all_time": str(realized),
                            "strategy_unrealized_now": str(unreal),
                            "strategy_total_now": str(st_total),
                        },
                        "bot": {
                            "equity": str(se),
                            "realized_today": str(realized_td),
                            "realized_all_time": str(realized),
                            "unrealized_now": str(unreal),
                            "total_now": str(st_total),
                        },
                        "non_strategy": {
                            "value_est": str(non_strategy_value),
                            "value_pct_est": str(non_strategy_pct),
                        },
                        "manual_inventory_by_symbol": manual_map,
                        "deployed": {
                            "deployed_market_now": str(deployed_market),
                            "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                            "deployed_cost_strategy_now": str(deployed_cost_strategy),
                            "quote_asset": str(quote_asset),
                        },
                        "cycles_today": cycles_today_out,
                        "cycles_all_time": cycles_all_out,
                        "holdings": holdings_out,
                    }
                    self._pnl_writer.write_summary(summary_out)
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

    # ================================================================
    # Proactive runner helpers
    # ================================================================

    def _get_pro_oids(self, sym: str):
        """Returns (buy_oid, sell_oid) — None if not placed."""
        buy_oid = self.state.extras.get(f"pro_buy_oid_{sym}") or None
        sell_oid = self.state.extras.get(f"pro_sell_oid_{sym}") or None
        return buy_oid, sell_oid

    def _set_pro_oid(self, sym: str, side: str, oid: str) -> None:
        key = f"pro_buy_oid_{sym}" if side == "BUY" else f"pro_sell_oid_{sym}"
        self.state.extras[key] = oid

    def _clear_pro_oid(self, sym: str, side: str) -> None:
        key = f"pro_buy_oid_{sym}" if side == "BUY" else f"pro_sell_oid_{sym}"
        self.state.extras.pop(key, None)

    def _cancel_pro_order(self, sym: str, side: str, partial_reason: str = "") -> None:
        """
        Cancel the stored proactive order for this side, then clear it.
        After cancel, checks for any partial fill and applies it to state.
        Works for both MEXC (instant snapshot) and Fyers (polls until terminal).
        skip_ref_update=True so the reference_price set by the triggering fill is preserved.
        partial_reason: trade reason string to use for the recovered partial fill.
        """
        buy_oid, sell_oid = self._get_pro_oids(sym)
        oid = buy_oid if side == "BUY" else sell_oid
        if oid:
            try:
                self.broker.cancel_order(oid)
                LOG.info("PRO cancelled %s %s oid=%s", sym, side, oid)
            except Exception as e:
                LOG.warning("PRO cancel failed %s %s oid=%s: %s", sym, side, oid, e)
            # Recover any partial fill on the cancelled order
            try:
                if hasattr(self.broker, "get_order_snapshot"):
                    # MEXC: snapshot is immediately available after cancel
                    snap = getattr(self.broker, "get_order_snapshot")(oid)
                    if snap:
                        executed = _dec(snap.get("executed_qty") or 0)
                        avg_px   = _dec(snap.get("avg_price") or 0)
                        cum_q    = _dec(snap.get("cum_quote_qty") or 0)
                        if executed > D0:
                            reason = partial_reason or f"pro_{side.lower()}_partial_cancel"
                            self._apply_fill(sym, side, executed, avg_px, cum_q,
                                             reason=reason, order_id=oid, status="CANCELLED",
                                             skip_ref_update=True)
                            LOG.info("PRO %s %s partial fill recovered: qty=%s @ %s",
                                     sym, side, executed, avg_px)
                else:
                    # Fyers: poll until order reaches terminal state (CANCELLED/FILLED)
                    # Use short timeout — cancel confirmation on NSE is usually <5s
                    filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(
                        sym, oid, timeout_s=10)
                    if terminal and filled_qty > D0:
                        reason = partial_reason or f"pro_{side.lower()}_partial_cancel"
                        self._apply_fill(sym, side, filled_qty, avg_px, cum_q,
                                         reason=reason, order_id=oid, status="CANCELLED",
                                         skip_ref_update=True)
                        LOG.info("PRO %s %s partial fill recovered (Fyers): qty=%s @ %s",
                                 sym, side, filled_qty, avg_px)
            except Exception as e:
                LOG.warning("PRO partial fill check after cancel failed %s %s: %s", sym, side, e)
        self._clear_pro_oid(sym, side)

    def _cancel_all_pro_orders(self, sym: str) -> None:
        self._cancel_pro_order(sym, "BUY")
        self._cancel_pro_order(sym, "SELL")

    def _check_pro_fill(self, sym: str, oid: str):
        """
        Poll broker for order status.
        Returns (filled_qty, avg_price, cum_quote, is_terminal) or None if still open.
        """
        if hasattr(self.broker, "get_order_snapshot"):
            snap = getattr(self.broker, "get_order_snapshot")(oid)
            if snap is None:
                return None
            status = snap["status"]
            executed = _dec(snap.get("executed_qty") or 0)
            cum_quote = _dec(snap.get("cum_quote_qty") or 0)
            avg_price = _dec(snap.get("avg_price") or 0)
            if status in TERMINAL_STATUSES:
                return (executed, avg_price, cum_quote, True)
            return None
        else:
            # Fyers path
            term = self.exec.poll_terminal(oid)
            if term is None:
                return None
            if term.status in TERMINAL_STATUSES:
                px = term.avg_price if term.avg_price > 0 else D0
                cum = term.cum_quote_qty if term.cum_quote_qty > 0 else (px * term.filled_qty)
                return (term.filled_qty, px, cum, True)
            return None

    def _wait_fill_blocking(self, sym: str, oid: str, timeout_s: int = 60) -> tuple:
        """Block until the order reaches a terminal state. Returns same tuple as _check_pro_fill."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            result = self._check_pro_fill(sym, oid)
            if result is not None:
                return result
            time.sleep(1)
        LOG.warning("PRO _wait_fill_blocking: timeout sym=%s oid=%s", sym, oid)
        return (D0, D0, D0, False)

    def _round_price_to_tick(self, price: Decimal) -> Decimal:
        """Round price down to nearest tick size. No-op if price_tick not configured."""
        tick = _dec(self.exec_cfg.price_tick)
        if tick <= D0:
            return price
        return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    def _round_qty_pro(self, qty: Decimal, strategy) -> Decimal:
        """Round qty down to qty_step, respecting min_qty."""
        step = strategy.cfg.qty_step if strategy.cfg.qty_step > 0 else Decimal("0.000001")
        q = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
        if q < strategy.cfg.min_qty:
            return D0
        return q

    def _rebalance_sync(self, sym: str, strategy, direction: str, ref: Decimal) -> bool:
        """
        Synchronous blocking rebalance (Option B: restore full runway).
        direction='buy'  → need more cash, sell ETH.
        direction='sell' → need more ETH, buy with cash.
        Returns True if the rebalance order filled (or no action needed).
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        target_steps = cfg.rebalance_target_steps or 8
        quote_reserve = _dec(self.exec_cfg.quote_reserve)

        if direction == "buy":
            # Target cash = target_steps × cost_per_buy_trade
            # fixed_qty: cost = fixed_qty_buy × ref   |   fixed_quote: cost = buy_quote
            if cfg.sizing_mode == "fixed_qty":
                if ref <= D0:
                    return False
                cost_per_trade = _dec(cfg.fixed_qty_buy) * ref
            else:
                cost_per_trade = _dec(cfg.buy_quote)
            target_cash = _dec(target_steps) * cost_per_trade
            available_cash = max(self.state.cash - quote_reserve, D0)
            deficit = target_cash - available_cash
            if deficit <= D0:
                return True  # already enough
            sell_qty = self._round_qty_pro(deficit / ref, strategy)
            # cap to what we actually hold
            base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            sell_qty = min(sell_qty, base_qty)
            sell_qty = self._round_qty_pro(sell_qty, strategy)
            if sell_qty <= D0:
                return False
            req = PlaceOrderRequest(
                symbol=sym, side="SELL", qty=sell_qty,
                product_type=self.exec_cfg.product_type, order_type="MARKET",
            )
            oid = self.exec.place_with_adaptive_qty(req, reason="rebalance_restore")
            if not oid:
                return False
            LOG.info("PRO rebalance_sync SELL qty=%s oid=%s", sell_qty, oid)
            filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(sym, oid)
            if terminal and filled_qty > 0:
                self._apply_fill(sym, "SELL", filled_qty, avg_px if avg_px > D0 else ref, cum_q,
                                 reason="rebalance_restore", order_id=oid, status="FILLED")
            return terminal

        else:  # direction == "sell" — need more ETH
            if cfg.sizing_mode == "fixed_qty":
                target_eth = _dec(target_steps) * _dec(cfg.fixed_qty_sell)
            else:
                if ref <= D0:
                    return False
                target_eth = _dec(target_steps) * _dec(cfg.sell_quote) / ref
                target_eth = self._round_qty_pro(target_eth, strategy)
            current_eth = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            deficit_eth = target_eth - current_eth
            if deficit_eth <= D0:
                return True  # already enough
            deficit_eth = self._round_qty_pro(deficit_eth, strategy)
            if deficit_eth <= D0:
                return False
            # Reduce buy qty if we can't afford it
            available_cash = max(self.state.cash - quote_reserve, D0)
            if ref > D0 and deficit_eth * ref > available_cash:
                deficit_eth = self._round_qty_pro(available_cash / ref, strategy)
            if deficit_eth <= D0:
                return False
            req = PlaceOrderRequest(
                symbol=sym, side="BUY", qty=deficit_eth,
                product_type=self.exec_cfg.product_type, order_type="MARKET",
            )
            oid = self.exec.place_with_adaptive_qty(req, reason="rebalance_restore")
            if not oid:
                return False
            LOG.info("PRO rebalance_sync BUY qty=%s oid=%s", deficit_eth, oid)
            filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(sym, oid)
            if terminal and filled_qty > 0:
                self._apply_fill(sym, "BUY", filled_qty, avg_px if avg_px > D0 else ref, cum_q,
                                 reason="rebalance_restore", order_id=oid, status="FILLED")
            return terminal

    def _place_proactive_orders(self, sym: str, strategy, price: Decimal) -> None:
        """
        Place resting GTC LIMIT BUY and SELL orders if not already in the book.
        Pre-validates funds; triggers synchronous rebalance (Option B) if needed.
        Works for both fixed_quote (crypto) and fixed_qty (India equity) modes.
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        ref = _dec(ss.reference_price) if ss.reference_price is not None else price
        quote_reserve = _dec(self.exec_cfg.quote_reserve)

        buy_level  = self._round_price_to_tick(ref * (Decimal("1") - cfg.lower_pct / Decimal("100")))
        sell_level = self._round_price_to_tick(ref * (Decimal("1") + cfg.upper_pct / Decimal("100")))

        # ---- BUY side ----
        buy_oid, _ = self._get_pro_oids(sym)
        if not buy_oid and buy_level > D0:
            if cfg.sizing_mode == "fixed_qty":
                buy_qty = _dec(cfg.fixed_qty_buy)
            else:
                buy_qty = self._round_qty_pro(_dec(cfg.buy_quote) / buy_level, strategy)

            buy_cost = buy_qty * buy_level
            available_cash = max(self.state.cash - quote_reserve, D0)
            if buy_qty > D0 and buy_cost > available_cash:
                if cfg.rebalance_threshold_steps > 0:
                    LOG.info("PRO %s BUY: insufficient cash (need=%s have=%s), rebalancing", sym, buy_cost, available_cash)
                    self._rebalance_sync(sym, strategy, "buy", ref)
                    # Refresh balances after market rebalance order
                    self._update_extras_crypto({sym: price})
                    available_cash = max(self.state.cash - quote_reserve, D0)
                if buy_cost > available_cash:
                    LOG.warning("PRO %s BUY: insufficient cash after rebalance, skipping", sym)
                    buy_qty = D0

            if buy_qty > D0:
                req = PlaceOrderRequest(
                    symbol=sym, side="BUY", qty=buy_qty,
                    product_type=self.exec_cfg.product_type,
                    order_type="LIMIT", limit_price=buy_level, time_in_force="GTC",
                )
                oid = self.exec.place_with_adaptive_qty(req, reason=f"pro_buy|ref-{float(cfg.lower_pct)}%")
                if oid:
                    self._set_pro_oid(sym, "BUY", oid)
                    LOG.info("PRO %s BUY placed oid=%s qty=%s @ %s", sym, oid, buy_qty, buy_level)

        # ---- SELL side ----
        _, sell_oid = self._get_pro_oids(sym)
        if not sell_oid and sell_level > D0:
            if cfg.sizing_mode == "fixed_qty":
                sell_qty = _dec(cfg.fixed_qty_sell)
            else:
                sell_qty = self._round_qty_pro(_dec(cfg.sell_quote) / sell_level, strategy)

            base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            if sell_qty > D0 and base_qty < sell_qty:
                if cfg.rebalance_threshold_steps > 0:
                    LOG.info("PRO %s SELL: insufficient ETH (need=%s have=%s), rebalancing", sym, sell_qty, base_qty)
                    self._rebalance_sync(sym, strategy, "sell", ref)
                    self._update_extras_crypto({sym: price})
                    base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
                if base_qty < sell_qty:
                    LOG.warning("PRO %s SELL: insufficient inventory after rebalance, skipping", sym)
                    sell_qty = D0

            if sell_qty > D0:
                req = PlaceOrderRequest(
                    symbol=sym, side="SELL", qty=sell_qty,
                    product_type=self.exec_cfg.product_type,
                    order_type="LIMIT", limit_price=sell_level, time_in_force="GTC",
                )
                oid = self.exec.place_with_adaptive_qty(req, reason=f"pro_sell|ref+{float(cfg.upper_pct)}%")
                if oid:
                    self._set_pro_oid(sym, "SELL", oid)
                    LOG.info("PRO %s SELL placed oid=%s qty=%s @ %s", sym, oid, sell_qty, sell_level)

    def _poll_proactive_symbol(self, sym: str, strategy, price: Decimal, allow_new: bool = True) -> None:
        """
        Check BUY and SELL proactive orders for fills.
        On fill: record trade, cancel other side, guard re-center, place fresh orders immediately.
        Also handles drift: if price drifts > 2*pct from ref, cancel stale orders and re-center.
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        ref = _dec(ss.reference_price) if ss.reference_price is not None else price

        buy_oid, sell_oid = self._get_pro_oids(sym)

        # Check BUY fill
        filled_buy = False
        if buy_oid:
            result = self._check_pro_fill(sym, buy_oid)
            if result is not None:
                filled_qty, avg_px, cum_q, _ = result
                self._clear_pro_oid(sym, "BUY")
                if filled_qty > 0:
                    fill_px = avg_px if avg_px > D0 else price
                    self._apply_fill(sym, "BUY", filled_qty, fill_px, cum_q,
                                     reason=f"pro_buy|ref-{float(cfg.lower_pct)}%",
                                     order_id=buy_oid, status="FILLED")
                    filled_buy = True
                else:
                    LOG.info("PRO %s BUY terminal (no fill) oid=%s", sym, buy_oid)

        # Check SELL fill (re-read sell_oid in case BUY clear above changed extras)
        filled_sell = False
        _, sell_oid = self._get_pro_oids(sym)
        if sell_oid:
            result = self._check_pro_fill(sym, sell_oid)
            if result is not None:
                filled_qty, avg_px, cum_q, _ = result
                self._clear_pro_oid(sym, "SELL")
                if filled_qty > 0:
                    fill_px = avg_px if avg_px > D0 else price
                    self._apply_fill(sym, "SELL", filled_qty, fill_px, cum_q,
                                     reason=f"pro_sell|ref+{float(cfg.upper_pct)}%",
                                     order_id=sell_oid, status="FILLED")
                    filled_sell = True
                else:
                    LOG.info("PRO %s SELL terminal (no fill) oid=%s", sym, sell_oid)

        # Drift check — only if no fills this iteration (fills take priority)
        if not filled_buy and not filled_sell and ref > D0:
            drift_pct = abs(price - ref) / ref * Decimal("100")
            drift_threshold = cfg.lower_pct + cfg.upper_pct
            if drift_pct > drift_threshold:
                LOG.info("PRO %s drift=%.4f%% > threshold=%.4f%%, re-centering ref=%s->%s",
                         sym, float(drift_pct), float(drift_threshold), ref, price)
                self._cancel_all_pro_orders(sym)
                ss.reference_price = price
                return

        # Post-fill: cancel opposite side + guard re-center + immediately place fresh orders
        if filled_buy and not filled_sell:
            self._cancel_pro_order(sym, "SELL",
                                   partial_reason=f"pro_sell|ref+{float(cfg.upper_pct)}%")
            new_ref = _dec(ss.reference_price)
            new_sell_level = new_ref * (Decimal("1") + cfg.upper_pct / Decimal("100"))
            if price >= new_sell_level:
                LOG.info("PRO %s BUY filled, price=%s >= new_sell=%s, re-centering", sym, price, new_sell_level)
                ss.reference_price = price
            if allow_new:
                self._place_proactive_orders(sym, strategy, price)

        elif filled_sell and not filled_buy:
            self._cancel_pro_order(sym, "BUY",
                                   partial_reason=f"pro_buy|ref-{float(cfg.lower_pct)}%")
            new_ref = _dec(ss.reference_price)
            new_buy_level = new_ref * (Decimal("1") - cfg.lower_pct / Decimal("100"))
            if price <= new_buy_level:
                LOG.info("PRO %s SELL filled, price=%s <= new_buy=%s, re-centering", sym, price, new_buy_level)
                ss.reference_price = price
            if allow_new:
                self._place_proactive_orders(sym, strategy, price)

        elif filled_buy and filled_sell:
            # Both filled in same poll (big move) — re-center on current price
            LOG.info("PRO %s both BUY+SELL filled, re-centering ref on price=%s", sym, price)
            ss.reference_price = price
            if allow_new:
                self._place_proactive_orders(sym, strategy, price)

    def run_proactive(self, strategy, *, state_path: str) -> None:
        """
        Proactive runner: keeps resting GTC LIMIT BUY and SELL orders in the book at all times.
        On fill: cancels other side, handles guard re-center, places fresh orders.
        Works for both India (Fyers, fixed_qty) and crypto (MEXC, fixed_quote).
        """
        self.state.ensure_symbols(self.symbols)

        if self.sync_on_start:
            try:
                self.reconcile_from_broker()
            except Exception as e:
                LOG.warning("Sync-on-start failed: %s", e)

        LOG.info("PROACTIVE started symbols=%s", ",".join(self.symbols))

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

            # EOD cancel: pull all proactive orders off the book
            if now >= eod_cancel_dt and self.state.last_eod_cancel_date != today:
                for sym in self.symbols:
                    self._cancel_all_pro_orders(sym)
                n = self.cancel_open_orders(cancel_all=self.cancel_all_open_orders)
                self.state.last_eod_cancel_date = today
                self.state.halted_until = close_dt.astimezone(dt.timezone.utc).isoformat()
                self.state.halt_reason = "EOD_CANCEL"
                LOG.warning("PRO EOD cancel done: cancelled=%d", n)

            allow_new = (now >= open_dt) and (now < eod_cancel_dt)

            try:
                prices = self.broker.get_ltps(self.symbols)
                for sym, px in prices.items():
                    self.state.last_prices[sym] = _dec(px)
                    self.state.symbol_states[sym].last_mark_price = _dec(px)

                self._update_extras_crypto(prices)

                # Init references on first run
                for sym in self.symbols:
                    self._init_reference(sym, _dec(prices[sym]))

                for sym in self.symbols:
                    price = _dec(prices[sym])
                    # Check fills, handle drift, place fresh orders immediately on fill
                    self._poll_proactive_symbol(sym, strategy, price, allow_new=allow_new)
                    # Safety net: place any still-missing orders (drift re-center, startup, etc.)
                    if allow_new:
                        self._place_proactive_orders(sym, strategy, price)

                # --- Status log ---
                parts = []
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    ref = ss.reference_price or D0
                    buy_oid, sell_oid = self._get_pro_oids(s)
                    parts.append(f"{s} px={px} ref={ref} buy_oid={buy_oid} sell_oid={sell_oid} traded={ss.traded_qty} R={ss.realized_pnl}")

                # --- PnL persistence (identical to run_reactive) ---
                broker_name = infer_broker_name(self.broker)
                port_val, quote_asset, port_details = compute_portfolio_value_for_symbols(self.broker, self.symbols, prices, self.state)
                start_val = ensure_portfolio_start(self.state, port_val)
                port_pnl = port_val - start_val
                port_pnl_pct = (port_pnl / start_val) if start_val > 0 else D0
                dd = update_drawdown(self.state, port_val)

                se, realized, unreal, st_total, exposure, exp_pct = compute_strategy_pnl(self.state)
                ensure_today_buckets(self.state, realized_now=realized)
                realized_td = realized_today(self.state, realized_now=realized)
                non_strategy_value = port_val - se
                non_strategy_pct = (non_strategy_value / port_val) if port_val > 0 else D0

                deployed_market = D0
                if isinstance(port_details, dict) and port_details.get("quote_total") is not None:
                    quote_total = Decimal(str(port_details.get("quote_total") or "0"))
                    deployed_market = port_val - quote_total
                else:
                    per = (port_details.get("per_symbol") or {}) if isinstance(port_details, dict) else {}
                    for _, d in per.items():
                        try:
                            deployed_market += Decimal(str(d.get("qty_used") or "0")) * Decimal(str(d.get("px") or "0"))
                        except Exception:
                            pass

                peak = Decimal(str(self.state.extras.get("deployed_market_peak") or "0"))
                if deployed_market > peak:
                    self.state.extras["deployed_market_peak"] = str(deployed_market)

                deployed_cost_strategy = D0
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    deployed_cost_strategy += Decimal(str(ss.traded_qty)) * Decimal(str(ss.traded_avg_price))
                unit_map = self.state.extras.get("cycle_unit_quote_by_symbol") or {}

                def _cycles_block(store: dict) -> dict:
                    out = {"per_symbol": {}}
                    per = (store.get("per_symbol") or {})
                    for sym_k, rec in per.items():
                        buy_q = Decimal(str(rec.get("buy_quote") or "0"))
                        sell_q = Decimal(str(rec.get("sell_quote") or "0"))
                        cycle_q = min(buy_q, sell_q)
                        unit = unit_map.get(sym_k)
                        cycles_est = (cycle_q / Decimal(str(unit))) if unit else None
                        out["per_symbol"][sym_k] = {
                            "buy_quote": str(buy_q), "sell_quote": str(sell_q),
                            "buy_qty": str(rec.get("buy_qty") or "0"),
                            "sell_qty": str(rec.get("sell_qty") or "0"),
                            "cycle_quote": str(cycle_q),
                            "cycle_unit_quote": str(unit) if unit else None,
                            "cycles_est": str(cycles_est) if cycles_est is not None else None,
                        }
                    return out

                cycles_today_store = self.state.extras.get("cycles_today") or {"per_symbol": {}}
                cycles_all_store = self.state.extras.get("cycles_all_time") or {"per_symbol": {}}
                cycles_today_out = {"date_utc": self.state.extras.get("cycles_today_utc_date"), **_cycles_block(cycles_today_store)}
                cycles_all_out = _cycles_block(cycles_all_store)
                holdings_out = {
                    "quote_total": str(port_details.get("quote_total")) if isinstance(port_details, dict) and port_details.get("quote_total") is not None else None,
                    "per_symbol": (port_details.get("per_symbol") if isinstance(port_details, dict) else {}),
                }

                pt = PnLPoint(
                    ts=utcnow().isoformat(), broker=broker_name, quote_asset=str(quote_asset),
                    portfolio_value=port_val, portfolio_pnl=port_pnl, portfolio_pnl_pct=port_pnl_pct,
                    strategy_equity=se, strategy_realized=realized, strategy_unrealized=unreal,
                    strategy_total=st_total, drawdown_pct=dd, exposure=exposure, exposure_pct=exp_pct,
                )
                if self._pnl_writer:
                    self._pnl_writer.append(pt)
                self._append_price_point(ts=pt.ts)
                self._update_daily_points(pt=pt)

                manual_map = self.state.extras.get("manual_inventory_by_symbol") or {}
                snap = {
                    "ts": pt.ts, "broker": broker_name, "quote_asset": str(quote_asset),
                    "portfolio_value": str(port_val), "portfolio_pnl": str(port_pnl),
                    "portfolio_pnl_pct": str(port_pnl_pct), "drawdown_pct": str(dd),
                    "portfolio_details": port_details, "symbols": {},
                    "created": {
                        "strategy_realized_today": str(realized_td),
                        "strategy_realized_all_time": str(realized),
                        "strategy_unrealized_now": str(unreal),
                        "strategy_total_now": str(st_total),
                    },
                    "bot": {
                        "equity": str(se), "realized_today": str(realized_td),
                        "realized_all_time": str(realized), "unrealized_now": str(unreal),
                        "total_now": str(st_total),
                    },
                    "non_strategy": {"value_est": str(non_strategy_value), "value_pct_est": str(non_strategy_pct)},
                    "manual_inventory_by_symbol": manual_map,
                    "deployed": {
                        "deployed_market_now": str(deployed_market),
                        "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                        "deployed_cost_strategy_now": str(deployed_cost_strategy),
                        "quote_asset": str(quote_asset),
                    },
                    "cycles_today": cycles_today_out, "cycles_all_time": cycles_all_out, "holdings": holdings_out,
                }
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    px = self.state.last_prices.get(s, D0)
                    buy_oid_s, sell_oid_s = self._get_pro_oids(s)
                    snap["symbols"][s] = {
                        "px": str(px),
                        "ref": str(ss.reference_price) if ss.reference_price is not None else None,
                        "traded_qty": str(ss.traded_qty), "avg_price": str(ss.traded_avg_price),
                        "realized": str(ss.realized_pnl),
                        "unrealized": str((ss.traded_qty * (px - ss.traded_avg_price)) if px is not None else D0),
                        "pro_buy_oid": buy_oid_s, "pro_sell_oid": sell_oid_s,
                        "borrowed_qty": str(ss.borrowed_qty), "borrowed_avg_sell": str(ss.borrowed_avg_sell),
                        "net_qty": str(ss.traded_qty - ss.borrowed_qty),
                    }
                if self._pnl_writer:
                    self._pnl_writer.write_snapshot(snap)
                    summary_out = {
                        "ts": pt.ts, "portfolio_value": str(port_val),
                        "portfolio_pnl": str(port_pnl), "portfolio_pnl_pct": str(port_pnl_pct),
                        "max_dd": str(self.state.extras.get("pnl_max_dd") or "0"),
                        "quote_asset": str(quote_asset),
                        "created": {
                            "strategy_realized_today": str(realized_td),
                            "strategy_realized_all_time": str(realized),
                            "strategy_unrealized_now": str(unreal),
                            "strategy_total_now": str(st_total),
                        },
                        "bot": {
                            "equity": str(se), "realized_today": str(realized_td),
                            "realized_all_time": str(realized), "unrealized_now": str(unreal),
                            "total_now": str(st_total),
                        },
                        "non_strategy": {"value_est": str(non_strategy_value), "value_pct_est": str(non_strategy_pct)},
                        "manual_inventory_by_symbol": manual_map,
                        "deployed": {
                            "deployed_market_now": str(deployed_market),
                            "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                            "deployed_cost_strategy_now": str(deployed_cost_strategy),
                            "quote_asset": str(quote_asset),
                        },
                        "cycles_today": cycles_today_out, "cycles_all_time": cycles_all_out, "holdings": holdings_out,
                    }
                    self._pnl_writer.write_summary(summary_out)
                LOG.info("PRO cash=%s eq=%s | %s", str(self.state.cash), str(self.state.strategy_equity()), " | ".join(parts))

            except KeyboardInterrupt:
                LOG.info("PRO stopped by user.")
                for sym in self.symbols:
                    self._cancel_all_pro_orders(sym)
                break
            except Exception as e:
                LOG.exception("PRO loop error: %s", e)

            self.state.last_update_ts = utcnow().isoformat()
            self.state.dump(state_path)

            sleep_s = self.poll_seconds if (now >= open_dt and now < close_dt) else self.closed_poll_seconds
            try:
                time.sleep(max(int(sleep_s), 1))
            except KeyboardInterrupt:
                LOG.info("PRO stopped by user (during sleep).")
                for sym in self.symbols:
                    self._cancel_all_pro_orders(sym)
                break

    # =========================================================
    # SELL-FIRST RUNNER HELPERS
    # =========================================================

    def _sf_state(self, symbol: str) -> dict:
        """Get or init per-symbol sell_first state from extras."""
        sf: Dict[str, Any] = self.state.extras.setdefault("sf", {})
        if symbol not in sf or not isinstance(sf.get(symbol), dict):
            sf[symbol] = {
                "mode": "sell_first",   # "sell_first" | "waiting_buy"
                "sell_oid": None,
                "buy_oid": None,
                "prev_close": None,
                "sell_level": None,
                "buy_level": None,
                "level_date": None,
            }
        return sf[symbol]

    def _sf_cancel(self, symbol: str, oid: str) -> None:
        """Best-effort cancel an order."""
        try:
            self.broker.cancel_order(oid)
            LOG.info("SF %s cancelled oid=%s", symbol, oid)
        except Exception as e:
            LOG.warning("SF %s cancel oid=%s failed: %s", symbol, oid, e)

    def _sf_check_fill(self, oid: str):
        """Returns OrderTerminal if terminal, else None."""
        try:
            return self.exec.poll_terminal(oid)
        except Exception:
            return None

    def _sf_place_order(self, symbol: str, side: str, price: Decimal,
                        qty: int, disclosed: int, product_type: str, *, reason: str):
        """Place a DAY LIMIT order with disclosed qty. Returns order_id or None."""
        req = PlaceOrderRequest(
            symbol=symbol,
            side=side,
            qty=Decimal(str(qty)),
            product_type=product_type,
            order_type="LIMIT",
            limit_price=price,
            validity="DAY",
            disclosed_qty=disclosed,
        )
        return self.exec.place_with_adaptive_qty(req, reason=reason)

    def _sf_refresh_levels(self, symbol: str, sym_cfg, today: str, lookback_days: int) -> bool:
        """Fetch prev_close and compute sell/buy levels. Returns True on success."""
        from common.engine.anchors import fetch_prev_close
        try:
            prev_close = fetch_prev_close(
                self.broker, symbol=symbol,
                market_tz=self.market_tz, lookback_days=lookback_days,
            )
        except Exception as e:
            LOG.warning("SF %s fetch_prev_close failed: %s", symbol, e)
            return False
        sell_level, buy_level = sym_cfg.compute_levels(prev_close)
        ss = self._sf_state(symbol)
        ss["prev_close"]  = str(prev_close)
        ss["sell_level"]  = str(sell_level)
        ss["buy_level"]   = str(buy_level)
        ss["level_date"]  = today
        LOG.info("SF %s levels refreshed prev_close=%s sell=%s buy=%s disclosed=%s",
                 symbol, prev_close, sell_level, buy_level, sym_cfg.disclosed_qty)
        return True

    def _sf_poll_symbol(self, symbol: str, sym_cfg, *,
                        allow_place: bool, today: str,
                        product_type: str, lookback_days: int) -> None:
        """Core per-symbol state machine for sell-first strategy."""
        ss = self._sf_state(symbol)

        # --- New trading day: cancel stale orders and refresh levels ---
        if ss.get("level_date") != today:
            for oid_key in ("sell_oid", "buy_oid"):
                oid = ss.get(oid_key)
                if oid:
                    self._sf_cancel(symbol, oid)
                    ss[oid_key] = None
            ok = self._sf_refresh_levels(symbol, sym_cfg, today, lookback_days)
            if not ok:
                return
            ss["mode"] = "sell_first"
            ss["sell_oid"] = None
            ss["buy_oid"]  = None

        sell_level = _dec(ss.get("sell_level") or "0")
        buy_level  = _dec(ss.get("buy_level")  or "0")
        if sell_level <= D0 or buy_level <= D0:
            return

        mode     = ss.get("mode", "sell_first")
        disc     = sym_cfg.disclosed_qty
        qty      = sym_cfg.qty

        # ---- SELL_FIRST: waiting for sell to fill ----
        if mode == "sell_first":
            sell_oid = ss.get("sell_oid")
            if sell_oid:
                term = self._sf_check_fill(sell_oid)
                if term is None:
                    return  # still live in book
                if term.status == "FILLED" and term.filled_qty > D0:
                    px  = term.avg_price if term.avg_price > D0 else sell_level
                    cum = term.cum_quote_qty if term.cum_quote_qty > D0 else (px * term.filled_qty)
                    LOG.info("SF %s SELL filled qty=%s @ %s", symbol, term.filled_qty, px)
                    self._apply_fill(symbol, "SELL", term.filled_qty, px, cum,
                                     reason="sf_sell", order_id=sell_oid, status="FILLED")
                    ss["sell_oid"] = None
                    ss["mode"] = "waiting_buy"
                    if allow_place:
                        oid = self._sf_place_order(symbol, "BUY", buy_level, qty, disc, product_type, reason="sf_buy")
                        if oid:
                            ss["buy_oid"] = oid
                            LOG.info("SF %s BUY placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, buy_level, disc)
                elif term.status in ("CANCELLED", "REJECTED"):
                    LOG.warning("SF %s SELL oid=%s %s — re-placing", symbol, sell_oid, term.status)
                    ss["sell_oid"] = None
                    if allow_place:
                        oid = self._sf_place_order(symbol, "SELL", sell_level, qty, disc, product_type, reason="sf_sell")
                        if oid:
                            ss["sell_oid"] = oid
                            LOG.info("SF %s SELL re-placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, sell_level, disc)
            elif allow_place:
                oid = self._sf_place_order(symbol, "SELL", sell_level, qty, disc, product_type, reason="sf_sell")
                if oid:
                    ss["sell_oid"] = oid
                    LOG.info("SF %s SELL placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, sell_level, disc)

        # ---- WAITING_BUY: sell filled, waiting for buy ----
        elif mode == "waiting_buy":
            buy_oid = ss.get("buy_oid")
            if buy_oid:
                term = self._sf_check_fill(buy_oid)
                if term is None:
                    return  # still live in book
                if term.status == "FILLED" and term.filled_qty > D0:
                    px  = term.avg_price if term.avg_price > D0 else buy_level
                    cum = term.cum_quote_qty if term.cum_quote_qty > D0 else (px * term.filled_qty)
                    LOG.info("SF %s BUY filled qty=%s @ %s", symbol, term.filled_qty, px)
                    self._apply_fill(symbol, "BUY", term.filled_qty, px, cum,
                                     reason="sf_buy", order_id=buy_oid, status="FILLED")
                    ss["buy_oid"] = None
                    ss["mode"] = "sell_first"
                    if allow_place:
                        oid = self._sf_place_order(symbol, "SELL", sell_level, qty, disc, product_type, reason="sf_sell")
                        if oid:
                            ss["sell_oid"] = oid
                            LOG.info("SF %s SELL placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, sell_level, disc)
                elif term.status in ("CANCELLED", "REJECTED"):
                    LOG.warning("SF %s BUY oid=%s %s — re-placing", symbol, buy_oid, term.status)
                    ss["buy_oid"] = None
                    if allow_place:
                        oid = self._sf_place_order(symbol, "BUY", buy_level, qty, disc, product_type, reason="sf_buy")
                        if oid:
                            ss["buy_oid"] = oid
                            LOG.info("SF %s BUY re-placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, buy_level, disc)
            elif allow_place:
                oid = self._sf_place_order(symbol, "BUY", buy_level, qty, disc, product_type, reason="sf_buy")
                if oid:
                    ss["buy_oid"] = oid
                    LOG.info("SF %s BUY placed oid=%s qty=%s @ %s disc=%s", symbol, oid, qty, buy_level, disc)

    def run_sell_first(self, strategy, *, state_path: str) -> None:
        """
        Sell-first proactive runner.
        Keeps one resting DAY LIMIT order per symbol: SELL first, then BUY after fill,
        then SELL again. Levels recalculated from prev_close each morning.
        Uses disclosed quantity (10% of total, rounded to lot_size).
        """
        self.state.ensure_symbols(self.symbols)

        if self.sync_on_start:
            try:
                self.reconcile_from_broker()
            except Exception as e:
                LOG.warning("Sync-on-start failed: %s", e)

        product_type  = self.exec_cfg.product_type
        lookback_days = int(getattr(strategy, "lookback_days", 10))

        LOG.info("SELL_FIRST started symbols=%s", ",".join(self.symbols))

        while True:
            now   = now_local(self.market_tz)
            today = now.date().isoformat()

            if self.state.session_date != today:
                self.state.session_date       = today
                self.state.reject_events      = []
                self.state.cooldown_until     = None
                self.state.halted_until       = None
                self.state.halt_reason        = None
                self.state.last_eod_cancel_date = None

            open_dt       = now.replace(hour=self.open_t.hour,       minute=self.open_t.minute,       second=self.open_t.second,       microsecond=0)
            close_dt      = now.replace(hour=self.close_t.hour,      minute=self.close_t.minute,      second=self.close_t.second,      microsecond=0)
            eod_cancel_dt = now.replace(hour=self.eod_cancel_t.hour, minute=self.eod_cancel_t.minute, second=self.eod_cancel_t.second, microsecond=0)

            # --- EOD cancel ---
            if now >= eod_cancel_dt and self.state.last_eod_cancel_date != today:
                for sym in self.symbols:
                    ss = self._sf_state(sym)
                    for oid_key in ("sell_oid", "buy_oid"):
                        oid = ss.get(oid_key)
                        if oid:
                            self._sf_cancel(sym, oid)
                            ss[oid_key] = None
                n = self.cancel_open_orders(cancel_all=self.cancel_all_open_orders)
                self.state.last_eod_cancel_date = today
                self.state.halted_until = close_dt.astimezone(dt.timezone.utc).isoformat()
                self.state.halt_reason  = "EOD_CANCEL"
                LOG.warning("SF EOD cancel done: cancelled=%d", n)

            allow_place = (now >= open_dt) and (now < eod_cancel_dt)

            try:
                prices = self.broker.get_ltps(self.symbols)
                for sym, px in prices.items():
                    self.state.last_prices[sym]                      = _dec(px)
                    self.state.symbol_states[sym].last_mark_price    = _dec(px)

                for sym in self.symbols:
                    sym_cfg = strategy.get(sym)
                    if sym_cfg is None:
                        continue
                    self._sf_poll_symbol(
                        sym, sym_cfg,
                        allow_place=allow_place,
                        today=today,
                        product_type=product_type,
                        lookback_days=lookback_days,
                    )

                # --- Status log ---
                parts = []
                for sym in self.symbols:
                    ss  = self._sf_state(sym)
                    px  = self.state.last_prices.get(sym, D0)
                    parts.append(
                        f"{sym} px={px} mode={ss.get('mode')} "
                        f"sell_lvl={ss.get('sell_level')} buy_lvl={ss.get('buy_level')} "
                        f"sell_oid={ss.get('sell_oid')} buy_oid={ss.get('buy_oid')}"
                    )
                LOG.info("SF | %s", " | ".join(parts))

                # --- PnL ---
                broker_name = infer_broker_name(self.broker)
                port_val, quote_asset, port_details = compute_portfolio_value_for_symbols(
                    self.broker, self.symbols, prices, self.state)
                start_val      = ensure_portfolio_start(self.state, port_val)
                port_pnl       = port_val - start_val
                port_pnl_pct   = (port_pnl / start_val) if start_val > 0 else D0
                dd             = update_drawdown(self.state, port_val)

                se, realized, unreal, st_total, exposure, exp_pct = compute_strategy_pnl(self.state)
                ensure_today_buckets(self.state, realized_now=realized)
                realized_td       = realized_today(self.state, realized_now=realized)
                non_strategy_value = port_val - se
                non_strategy_pct   = (non_strategy_value / port_val) if port_val > 0 else D0

                deployed_cost_strategy = D0
                for s in self.symbols:
                    ss2 = self.state.symbol_states[s]
                    deployed_cost_strategy += Decimal(str(ss2.traded_qty)) * Decimal(str(ss2.traded_avg_price))

                pt = PnLPoint(
                    ts=utcnow().isoformat(), broker=broker_name, quote_asset=str(quote_asset),
                    portfolio_value=port_val, portfolio_pnl=port_pnl, portfolio_pnl_pct=port_pnl_pct,
                    strategy_equity=se, strategy_realized=realized, strategy_unrealized=unreal,
                    strategy_total=st_total, drawdown_pct=dd, exposure=exposure, exposure_pct=exp_pct,
                )
                if self._pnl_writer:
                    self._pnl_writer.append(pt)
                self._append_price_point(ts=pt.ts)
                self._update_daily_points(pt=pt)

                summary_out = {
                    "ts": pt.ts, "broker": broker_name, "quote_asset": str(quote_asset),
                    "portfolio_value": str(port_val), "portfolio_pnl": str(port_pnl),
                    "portfolio_pnl_pct": str(port_pnl_pct), "drawdown_pct": str(dd),
                    "bot": {
                        "equity": str(se), "realized_today": str(realized_td),
                        "realized_all_time": str(realized), "unrealized_now": str(unreal),
                        "total_now": str(st_total),
                    },
                    "non_strategy": {"value_est": str(non_strategy_value), "value_pct_est": str(non_strategy_pct)},
                    "deployed": {
                        "deployed_cost_strategy_now": str(deployed_cost_strategy),
                        "quote_asset": str(quote_asset),
                    },
                }
                for s in self.symbols:
                    ss2 = self.state.symbol_states[s]
                    sf_ss = self._sf_state(s)
                    summary_out.setdefault("symbols", {})[s] = {
                        "ltp": str(self.state.last_prices.get(s, D0)),
                        "mode": sf_ss.get("mode"),
                        "sell_level": sf_ss.get("sell_level"),
                        "buy_level": sf_ss.get("buy_level"),
                        "prev_close": sf_ss.get("prev_close"),
                        "sell_oid": sf_ss.get("sell_oid"),
                        "buy_oid": sf_ss.get("buy_oid"),
                        "realized_pnl": str(ss2.realized_pnl),
                    }
                self._pnl_writer.write_summary(summary_out)

            except KeyboardInterrupt:
                LOG.info("SF stopped by user.")
                for sym in self.symbols:
                    ss = self._sf_state(sym)
                    for oid_key in ("sell_oid", "buy_oid"):
                        oid = ss.get(oid_key)
                        if oid:
                            self._sf_cancel(sym, oid)
                            ss[oid_key] = None
                break
            except Exception as e:
                LOG.exception("SF loop error: %s", e)

            self.state.last_update_ts = utcnow().isoformat()
            self.state.dump(state_path)

            sleep_s = self.poll_seconds if (now >= open_dt and now < close_dt) else self.closed_poll_seconds
            try:
                time.sleep(max(int(sleep_s), 1))
            except KeyboardInterrupt:
                LOG.info("SF stopped by user (sleep).")
                for sym in self.symbols:
                    ss = self._sf_state(sym)
                    for oid_key in ("sell_oid", "buy_oid"):
                        oid = ss.get(oid_key)
                        if oid:
                            self._sf_cancel(sym, oid)
                            ss[oid_key] = None
                break

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
                self._update_extras_crypto(prices)

                # fetch open orders once
                ob = self.broker.orderbook()
                open_orders = ob.get("orderBook") or ob.get("data") or ob.get("orders") or []
                if isinstance(open_orders, dict):
                    open_orders = list(open_orders.values())

                open_ids = set()
                order_map = {}
                for o in (open_orders or []):
                    if not isinstance(o, dict):
                        continue
                    oid = str(o.get("id") or o.get("order_id") or "")
                    if not oid:
                        continue
                    order_map[oid] = o
                    status = str(o.get("status") or o.get("orderStatus") or o.get("order_status") or "").upper()
                    qty = int(o.get("qty") or o.get("quantity") or 0)
                    filled = int(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0)
                    # treat these as terminal (FYERS returns filled orders in orderbook)
                    terminal = {"TRADED", "FILLED", "COMPLETE", "REJECTED", "CANCELLED", "CANCELED"}
                    if status in terminal:
                        continue
                    if qty > 0 and filled >= qty:
                        continue
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
                    # --- partial fill detection: treat any fill > 0 as fill-event ---
                    if str(oid) in open_ids:
                        o = order_map.get(str(oid))
                        if isinstance(o, dict):
                            filled = int(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0)
                            if filled > 0 and not meta.get("partial_handled"):
                                meta["partial_handled"] = True  # prevent double counting

                                qty = _dec(filled)
                                avg_px = _dec(o.get("avgPrice") or o.get("averagePrice") or o.get("avg_price") or o.get(
                                    "tradedPrice") or 0)
                                sym = str(o.get("symbol") or o.get("tradingSymbol") or meta.get("symbol") or "")
                                side_val = o.get("side")
                                side = "BUY" if side_val in (1, "1", "BUY", "B") else "SELL"

                                # cancel remaining qty (best-effort)
                                try:
                                    self.broker.cancel_order(str(oid))
                                except Exception:
                                    pass

                                # apply fill to state
                                cum = avg_px * qty if (avg_px > 0 and qty > 0) else D0
                                self._apply_fill(sym, side, qty, avg_px, cum,
                                                 reason=str(meta.get("reason") or "managed_partial_fill"),
                                                 order_id=str(oid), status="FILLED")

                                # notify strategy as FILLED (so it advances levels)
                                if hasattr(strategy, "on_order_terminal"):
                                    term = OrderTerminal(
                                        order_id=str(oid),
                                        symbol=sym,
                                        side=side,  # type: ignore
                                        status="FILLED",
                                        filled_qty=qty,
                                        avg_price=avg_px,
                                        cum_quote_qty=cum,
                                        message="partial_fill_event",
                                        ts=utcnow(),
                                        raw=o,
                                    )
                                    try:
                                        strategy.on_order_terminal(term, meta, self.state)  # type: ignore[attr-defined]
                                    except Exception as e:
                                        LOG.warning("strategy.on_order_terminal failed: %s", e)

                                meta_map.pop(str(oid), None)
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
                # --- realized today (UTC) baseline + value ---
                ensure_today_buckets(self.state, realized_now=realized)
                realized_td = realized_today(self.state, realized_now=realized)
                # --- deployed (market now/peak) and deployed cost (strategy bucket) ---
                deployed_market = D0

                # crypto: port_details has quote_total
                if isinstance(port_details, dict) and port_details.get("quote_total") is not None:
                    quote_total = Decimal(str(port_details.get("quote_total") or "0"))
                    deployed_market = port_val - quote_total
                else:
                    # equities: sum qty_used*px from port_details
                    per = (port_details.get("per_symbol") or {}) if isinstance(port_details, dict) else {}
                    for _, d in per.items():
                        try:
                            deployed_market += Decimal(str(d.get("qty_used") or "0")) * Decimal(str(d.get("px") or "0"))
                        except Exception:
                            pass

                peak = Decimal(str(self.state.extras.get("deployed_market_peak") or "0"))
                if deployed_market > peak:
                    self.state.extras["deployed_market_peak"] = str(deployed_market)

                deployed_cost_strategy = D0
                for s in self.symbols:
                    ss = self.state.symbol_states[s]
                    deployed_cost_strategy += Decimal(str(ss.traded_qty)) * Decimal(str(ss.traded_avg_price))
                unit_map = self.state.extras.get("cycle_unit_quote_by_symbol") or {}

                def _cycles_block(store: dict) -> dict:
                    out = {"per_symbol": {}}
                    per = (store.get("per_symbol") or {})
                    for sym, rec in per.items():
                        buy_q = Decimal(str(rec.get("buy_quote") or "0"))
                        sell_q = Decimal(str(rec.get("sell_quote") or "0"))
                        cycle_q = min(buy_q, sell_q)
                        unit = unit_map.get(sym)
                        cycles_est = (cycle_q / Decimal(str(unit))) if unit else None
                        out["per_symbol"][sym] = {
                            "buy_quote": str(buy_q),
                            "sell_quote": str(sell_q),
                            "buy_qty": str(rec.get("buy_qty") or "0"),
                            "sell_qty": str(rec.get("sell_qty") or "0"),
                            "cycle_quote": str(cycle_q),
                            "cycle_unit_quote": str(unit) if unit else None,
                            "cycles_est": str(cycles_est) if cycles_est is not None else None,
                        }
                    return out

                cycles_today_store = self.state.extras.get("cycles_today") or {"per_symbol": {}}
                cycles_all_store = self.state.extras.get("cycles_all_time") or {"per_symbol": {}}

                cycles_today_out = {
                    "date_utc": self.state.extras.get("cycles_today_utc_date"),
                    **_cycles_block(cycles_today_store),
                }
                cycles_all_out = _cycles_block(cycles_all_store)

                holdings_out = {
                    "quote_total": str(port_details.get("quote_total")) if isinstance(port_details,
                                                                                      dict) and port_details.get(
                        "quote_total") is not None else None,
                    "per_symbol": (port_details.get("per_symbol") if isinstance(port_details, dict) else {}),
                }

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
                self._append_price_point(ts=pt.ts)
                self._update_daily_points(pt=pt)

                manual_map = self.state.extras.get("manual_inventory_by_symbol") or {}
                snap = {"ts": pt.ts, "broker": broker_name, "quote_asset": str(quote_asset),
                        "portfolio_value": str(port_val), "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct), "drawdown_pct": str(dd),
                        "portfolio_details": port_details, "symbols": {}, "created": {
                        "strategy_realized_today": str(realized_td),
                        "strategy_realized_all_time": str(realized),
                        "strategy_unrealized_now": str(unreal),
                        "strategy_total_now": str(st_total),
                    }, "bot": {
                        "equity": str(se),
                        "realized_today": str(realized_td),
                        "realized_all_time": str(realized),
                        "unrealized_now": str(unreal),
                        "total_now": str(st_total),
                    }, "non_strategy": {
                        "value_est": str(non_strategy_value),
                        "value_pct_est": str(non_strategy_pct),
                    }, "manual_inventory_by_symbol": manual_map, "deployed": {
                        "deployed_market_now": str(deployed_market),
                        "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                        "deployed_cost_strategy_now": str(deployed_cost_strategy),
                        "quote_asset": str(quote_asset),
                    }, "cycles_today": cycles_today_out, "cycles_all_time": cycles_all_out, "holdings": holdings_out}
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
                        "borrowed_qty": str(ss.borrowed_qty),
                        "borrowed_avg_sell": str(ss.borrowed_avg_sell),
                        "net_qty": str(ss.traded_qty - ss.borrowed_qty),
                    }
                if self._pnl_writer:
                    self._pnl_writer.write_snapshot(snap)
                    summary_out = {
                        "ts": pt.ts,
                        "portfolio_value": str(port_val),
                        "portfolio_pnl": str(port_pnl),
                        "portfolio_pnl_pct": str(port_pnl_pct),
                        "max_dd": str(self.state.extras.get("pnl_max_dd") or "0"),
                        "quote_asset": str(quote_asset),

                        "created": {
                            "strategy_realized_today": str(realized_td),
                            "strategy_realized_all_time": str(realized),
                            "strategy_unrealized_now": str(unreal),
                            "strategy_total_now": str(st_total),
                        },
                        "bot": {
                            "equity": str(se),
                            "realized_today": str(realized_td),
                            "realized_all_time": str(realized),
                            "unrealized_now": str(unreal),
                            "total_now": str(st_total),
                        },
                        "non_strategy": {
                            "value_est": str(non_strategy_value),
                            "value_pct_est": str(non_strategy_pct),
                        },
                        "manual_inventory_by_symbol": manual_map,
                        "deployed": {
                            "deployed_market_now": str(deployed_market),
                            "deployed_market_peak": str(self.state.extras.get("deployed_market_peak") or "0"),
                            "deployed_cost_strategy_now": str(deployed_cost_strategy),
                            "quote_asset": str(quote_asset),
                        },
                        "cycles_today": cycles_today_out,
                        "cycles_all_time": cycles_all_out,
                        "holdings": holdings_out,
                    }
                    self._pnl_writer.write_summary(summary_out)
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
