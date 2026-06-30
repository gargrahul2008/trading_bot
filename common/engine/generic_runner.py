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
from common.engine.overlay_pnl import OverlayPnL

def _try_start_price_feed(broker, symbols):
    """Start a MexcPriceFeed if the broker is MexcSpotClient, else return None."""
    try:
        from common.broker.mexc_spot_client import MexcPriceFeed
        if type(broker).__name__ == "MexcSpotClient":
            base_url = getattr(broker, "base_url", "https://api.mexc.com")
            feed = MexcPriceFeed(symbols, base_url=base_url, poll_ms=200)
            feed.start()
            return feed
    except Exception as e:
        LOG.warning("Could not start MexcPriceFeed: %s", e)
    return None

LOG = setup_logger("runner")

TERMINAL_STATUSES = {"FILLED", "REJECTED", "CANCELLED"}

D0 = Decimal("0")

def _dec(x: Any) -> Decimal:
    return to_decimal(x)

class GenericRunner:
    def __init__(self, *, broker: Broker, state: GlobalState, symbols: List[str], exec_cfg: ExecutionConfig,
                 trades_path: str, rejects_path: str, market_tz: str, market_open: str, market_close: str,
                 eod_cancel_time: str, poll_seconds: int, closed_poll_seconds: int, cancel_all_open_orders: bool,
                 sync_on_start: bool, adopt_broker_inventory: bool, manual_adjustments_path: str | None = None,
                 regular_market_open: str | None = None,
                 preopen_pause_start: str | None = None):
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
        self.regular_open_t = parse_hhmm(regular_market_open) if regular_market_open else self.open_t
        self.preopen_pause_t = parse_hhmm(preopen_pause_start) if preopen_pause_start else self.regular_open_t
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
        _ledger_path = os.path.join(base_dir, "mexc_ledger.csv")
        self._overlay = OverlayPnL(
            trades_path=trades_path,
            csv_path=os.path.join(base_dir, "pnl_new.csv"),
            state=state,
            ledger_path=_ledger_path if os.path.exists(_ledger_path) else None,
        )
        self._last_price_point: Dict[str, str] = {}
        self._pro_oid_qty: Dict[str, Decimal] = {}    # oid -> intended qty, for partial-fill detection
        self._pro_oid_price: Dict[str, Decimal] = {}  # oid -> limit price, for fixed-step order reuse
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

            isolated = getattr(self.exec_cfg, "isolated_cash", False)
            if isolated:
                # Multi-bot on one account: cash tracked from fills only, never sync from broker.
                self.state.extras["strategy_cash_initialized"] = True
                LOG.info("Crypto isolated_cash: skipping broker cash sync, state cash=%.4f", float(self.state.cash))
            else:
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
        if getattr(self.exec_cfg, "isolated_cash", False):
            # isolated_cash: do NOT sync from broker — cash is tracked from fills only.
            # Used for sell-first strategies where the strategy must not touch account cash.
            LOG.info("isolated_cash: skipping funds_cash() sync, state cash=%.2f", float(self.state.cash))
        else:
            try:
                raw_cash = _dec(self.broker.funds_cash())
                # For MTF strategies, state.cash tracks effective buying power = actual_cash × leverage.
                # Default leverage=0/1 means no scaling (non-MTF and MEXC).
                lev = _dec(getattr(self.exec_cfg, "mtf_leverage", "0"))
                if lev > _dec("1"):
                    self.state.cash = raw_cash * lev
                    LOG.info("MTF cash sync: actual=%.2f leverage=%.1fx buying_power=%.2f",
                             float(raw_cash), float(lev), float(self.state.cash))
                else:
                    self.state.cash = raw_cash
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
            raw_status = o.get("status") or o.get("orderStatus") or o.get("order_status")
            qty = _dec(o.get("qty") or o.get("quantity") or 0)
            filled_qty = _dec(o.get("filledQty") or o.get("tradedQty") or o.get("filled_qty") or 0)
            # Fyers returns integer status codes: 1=Cancelled, 2=Traded/Filled, 5=Rejected, 6=Pending
            # MEXC/other brokers return string statuses.
            if isinstance(raw_status, int):
                if raw_status in (1, 2, 5):  # Fyers terminal
                    continue
            else:
                status = str(raw_status or "").upper()
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
                # Use trigger price (ltp at order placement) not execution price.
                # This prevents slippage from shifting the grid — any favourable
                # slippage becomes pure bonus without pushing the next level further away.
                trigger_px = _dec(ss.pending_expected_price) if ss.pending_expected_price is not None else D0
                ss.reference_price = trigger_px if trigger_px > 0 else price
            ss.pending_order_id = None
            ss.pending_reason = None
            ss.pending_since = None

        # Track cumulative net PRO sells for max_pro_sell_qty cap
        max_pro_sell = getattr(self.exec_cfg, "max_pro_sell_qty", None)
        if max_pro_sell and qty > 0:
            _key = f"pro_net_sold_{symbol}"
            _cur = _dec(self.state.extras.get(_key, "0"))
            if reason.startswith("pro_sell"):
                self.state.extras[_key] = str(_cur + qty)
            elif reason.startswith("pro_buy"):
                self.state.extras[_key] = str(max(D0, _cur - qty))

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
        # --- Overlay PnL attribution (post-cutoff tracking) ---
        if qty > 0:
            try:
                mark = self.state.last_prices.get(symbol) or price
                self._overlay.on_fill(
                    ts=rec["ts"],
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    cum_quote=cum_quote if cum_quote > 0 else (price * qty),
                    fill_price=price,
                    mark_price=_dec(mark),
                    reason=reason,
                )
            except Exception as _oe:
                LOG.warning("Overlay PnL update failed: %s", _oe)
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

    def _migrate_renamed_symbol_extras(self) -> None:
        """On startup, detect if the configured symbol was renamed from another symbol.
        If the new symbol is missing its pro_net_sold key but exactly one orphaned
        pro_net_sold_* key exists (from the old name), migrate all symbol-keyed extras
        and symbol_states from old -> new automatically.
        """
        _PREFIXES = (
            "pro_net_sold_", "pro_buy_oids_", "pro_sell_oids_",
            "_itm_buy_placed_", "_itm_sell_placed_",
            "_pro_buy_anchor_", "_pro_sell_anchor_", "broker_base_qty_",
        )
        for sym in self.symbols:
            if self.state.extras.get(f"pro_net_sold_{sym}") is not None:
                continue
            # Find orphaned pro_net_sold_* keys not belonging to any configured symbol
            configured = {f"pro_net_sold_{s}" for s in self.symbols}
            orphans = [
                k for k in self.state.extras
                if k.startswith("pro_net_sold_") and k not in configured
            ]
            if len(orphans) != 1:
                continue
            old_sym = orphans[0][len("pro_net_sold_"):]
            LOG.warning("Symbol rename detected: migrating state from '%s' -> '%s'", old_sym, sym)
            # Migrate known prefixed extras keys
            for prefix in _PREFIXES:
                old_key = f"{prefix}{old_sym}"
                new_key = f"{prefix}{sym}"
                if old_key in self.state.extras and new_key not in self.state.extras:
                    self.state.extras[new_key] = self.state.extras.pop(old_key)
                    LOG.warning("  extras migrated: %s -> %s", old_key, new_key)
            # Migrate any partial-remaining keys: _pro_partial_remaining_{sym}_BUY/SELL_*
            for key in list(self.state.extras.keys()):
                if old_sym in key:
                    new_key = key.replace(old_sym, sym, 1)
                    if new_key not in self.state.extras:
                        self.state.extras[new_key] = self.state.extras.pop(key)
                        LOG.warning("  extras migrated: %s -> %s", key, new_key)
            # Migrate cycles_all_time.per_symbol
            cps = (self.state.extras.get("cycles_all_time") or {}).get("per_symbol") or {}
            if old_sym in cps and sym not in cps:
                cps[sym] = cps.pop(old_sym)
                LOG.warning("  cycles_all_time.per_symbol migrated: %s -> %s", old_sym, sym)
            # Migrate symbol_states entry if old still present and new is uninitialized
            if old_sym in self.state.symbol_states and sym in self.state.symbol_states:
                old_ss = self.state.symbol_states[old_sym]
                new_ss = self.state.symbol_states[sym]
                if old_ss.reference_price is not None and new_ss.reference_price is None:
                    self.state.symbol_states[sym] = old_ss
                    LOG.warning("  symbol_states migrated: %s -> %s", old_sym, sym)

    def _restore_oid_prices_from_state(self) -> None:
        """Restore _pro_oid_price from persisted state so gap-fill ref anchoring
        works correctly after a restart (limit prices survive in state.extras)."""
        for sym in self.symbols:
            for side in ("BUY", "SELL"):
                for oid in self._get_pro_oid_list(sym, side):
                    px_str = self.state.extras.get(f"_pro_oid_limitpx_{oid}")
                    if px_str:
                        self._pro_oid_price[oid] = _dec(px_str)

    def _init_reference(self, symbol: str, price: Decimal) -> None:
        ss = self.state.symbol_states[symbol]
        if ss.reference_price is None:
            ss.reference_price = price
            ss.initialized = True

    def _compute_port_val(self, prices: Dict[str, Decimal]):
        """Returns (port_val, quote_asset, port_details).
        When isolated_cash is true, derives values from THIS bot's state instead of broker totals,
        so multi-bot setups on one exchange account don't double-count.
        """
        if getattr(self.exec_cfg, "isolated_cash", False):
            total = _dec(self.state.cash)
            per_symbol: Dict[str, Any] = {}
            for sym in self.symbols:
                ss = self.state.symbol_states.get(sym)
                if ss is None:
                    continue
                px = _dec(prices.get(sym) or 0)
                qty = _dec(ss.traded_qty)
                total += qty * px
                try:
                    info = self.broker.symbol_info(sym)
                    base = info.base_asset
                except Exception:
                    base = sym[:-4] if sym.endswith(("USDC", "USDT")) else sym
                per_symbol[sym] = {"base": base, "base_total": str(qty), "px": str(px)}
            quote_asset = str(self.state.extras.get("quote_asset") or "USDC")
            details = {"quote_total": str(_dec(self.state.cash)), "per_symbol": per_symbol}
            return total, quote_asset, details
        return compute_portfolio_value_for_symbols(self.broker, self.symbols, prices, self.state)

    def _update_extras_crypto(self, prices: Dict[str, Decimal]) -> None:
        isolated = getattr(self.exec_cfg, "isolated_cash", False)
        if isolated:
            # Multi-bot on one account: derive portfolio_value from this bot's state, not broker totals.
            total = _dec(self.state.cash)
            for sym, px in prices.items():
                ss = self.state.symbol_states.get(sym)
                if ss is not None:
                    total += _dec(ss.traded_qty) * _dec(px)
            self.state.extras["portfolio_value"] = str(total)
            return
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

    def _overlay_summary(self, prices: Dict[str, Decimal]) -> dict:
        """Return overlay PnL snapshot for embedding in pnl_summary.json."""
        try:
            # Use the first symbol's price as mark (MEXC runs one symbol at a time)
            mark = D0
            for sym in self.symbols:
                p = prices.get(sym)
                if p is not None:
                    mark = _dec(p)
                    break
            o_pnl = self._overlay.current_overlay_pnl(mark)
            b_val = self._overlay.baseline_value(mark)
            return {
                "cutoff_ts": self.state.extras.get("overlay_cutoff_ts", ""),
                "baseline_qty": self.state.extras.get("overlay_baseline_qty", "0"),
                "baseline_cash": self.state.extras.get("overlay_baseline_cash", "0"),
                "baseline_value": str(b_val),
                "overlay_qty": self.state.extras.get("overlay_qty", "0"),
                "overlay_cash": self.state.extras.get("overlay_cash", "0"),
                "overlay_pnl": str(o_pnl),
                "mark_price": str(mark),
            }
        except Exception as e:
            LOG.warning("Overlay summary failed: %s", e)
            return {}

    def run_reactive(self, strategy: ReactiveStrategy, *, state_path: str) -> None:
        self.state.ensure_symbols(self.symbols)

        if self.sync_on_start:
            try:
                self.reconcile_from_broker()
            except Exception as e:
                LOG.warning("Sync-on-start failed: %s", e)

        # Start real-time WebSocket price feed (MEXC only; None for other brokers)
        price_feed = _try_start_price_feed(self.broker, self.symbols)
        if price_feed:
            LOG.info("LIVE started symbols=%s [WebSocket price feed active]", ",".join(self.symbols))
        else:
            LOG.info("LIVE started symbols=%s [REST polling mode]", ",".join(self.symbols))

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
                # Use WebSocket cached price if available; fall back to REST
                ws_prices = price_feed.get_prices() if price_feed else None
                if ws_prices:
                    prices = ws_prices
                else:
                    prices = self.broker.get_ltps(self.symbols)

                for sym, px in prices.items():
                    self.state.last_prices[sym] = _dec(px)
                    self.state.symbol_states[sym].last_mark_price = _dec(px)

                self._update_extras_crypto(prices)

                # poll pending — capture state before to detect fills
                had_pending = {s: bool(self.state.symbol_states[s].pending_order_id) for s in self.symbols}
                for sym in self.symbols:
                    self._poll_pending(sym, current_price=_dec(prices[sym]))

                # init refs
                for sym in self.symbols:
                    self._init_reference(sym, _dec(prices[sym]))

                # ── Catch-up: if a fill just cleared, immediately re-check with
                # the freshest cached price for one extra order opportunity.
                # Safe: only fires if latest ltp still satisfies the trigger —
                # if price reversed during order placement, no order is placed.
                filled_any = any(
                    had_pending[s] and not self.state.symbol_states[s].pending_order_id
                    for s in self.symbols
                )
                if filled_any and allow_new and price_feed and price_feed.connected:
                    fresh = price_feed.get_prices()
                    if fresh:
                        for sym, px in fresh.items():
                            if sym in self.state.last_prices:
                                self.state.last_prices[sym] = _dec(px)
                                self.state.symbol_states[sym].last_mark_price = _dec(px)
                        catchup_intents = [
                            it for it in strategy.on_prices(fresh, self.state, utcnow().isoformat())
                            if not (it.reason or "").startswith("rebalance_")
                        ]
                        if catchup_intents:
                            LOG.info("Catch-up: %d ladder intent(s) after fill", len(catchup_intents))
                        for it in catchup_intents:
                            self._place_intent(it, ltp=_dec(fresh[it.symbol]))

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
                port_val, quote_asset, port_details = self._compute_port_val(prices)
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
                        "overlay": self._overlay_summary(prices),
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
            if price_feed and price_feed.connected:
                # Wake up the instant a new price tick arrives (or after poll_seconds max)
                price_feed.wait_for_tick(timeout=float(sleep_s))
            else:
                time.sleep(max(int(sleep_s), 1))

    # ================================================================
    # Proactive runner helpers
    # ================================================================

    # ------------------------------------------------------------------
    # Multi-level proactive OID management
    # OIDs stored as JSON lists: pro_buy_oids_{sym}, pro_sell_oids_{sym}
    # ------------------------------------------------------------------

    def _get_pro_oid_list(self, sym: str, side: str) -> list:
        """Return list of active OIDs for this side (newest last)."""
        key = f"pro_buy_oids_{sym}" if side == "BUY" else f"pro_sell_oids_{sym}"
        val = self.state.extras.get(key)
        if isinstance(val, list):
            return [o for o in val if o]
        # backward compat: single OID stored under old key
        old_key = f"pro_buy_oid_{sym}" if side == "BUY" else f"pro_sell_oid_{sym}"
        old = self.state.extras.get(old_key)
        return [old] if old else []

    def _add_pro_oid(self, sym: str, side: str, oid: str, qty: Decimal = D0, price: Decimal = D0) -> None:
        key = f"pro_buy_oids_{sym}" if side == "BUY" else f"pro_sell_oids_{sym}"
        lst = self._get_pro_oid_list(sym, side)
        if oid not in lst:
            lst.append(oid)
        self.state.extras[key] = lst
        if qty > D0:
            self._pro_oid_qty[oid] = qty
        if price > D0:
            self._pro_oid_price[oid] = price
            # Persist limit price so it survives restarts (used for gap-fill ref anchoring)
            self.state.extras[f"_pro_oid_limitpx_{oid}"] = str(price)

    def _clear_pro_oids(self, sym: str, side: str) -> None:
        for oid in self._get_pro_oid_list(sym, side):
            self._pro_oid_qty.pop(oid, None)
            self._pro_oid_price.pop(oid, None)
            self.state.extras.pop(f"_pro_oid_limitpx_{oid}", None)
            self.state.extras.pop(f"_pro_oid_applied_{oid}", None)
        self.state.extras[f"pro_buy_oids_{sym}" if side == "BUY" else f"pro_sell_oids_{sym}"] = []
        self.state.extras.pop(f"pro_buy_oid_{sym}" if side == "BUY" else f"pro_sell_oid_{sym}", None)

    def _remove_pro_oid(self, sym: str, side: str, oid: str) -> None:
        """Remove a single OID from the list (used after a fill is confirmed)."""
        key = f"pro_buy_oids_{sym}" if side == "BUY" else f"pro_sell_oids_{sym}"
        lst = self._get_pro_oid_list(sym, side)
        if oid in lst:
            lst.remove(oid)
        self.state.extras[key] = lst
        self._pro_oid_qty.pop(oid, None)
        self._pro_oid_price.pop(oid, None)
        self.state.extras.pop(f"_pro_oid_limitpx_{oid}", None)
        self.state.extras.pop(f"_pro_oid_applied_{oid}", None)

    # Legacy single-oid shims used by status logging and snapshot
    def _get_pro_oids(self, sym: str):
        """Returns (first_buy_oid_or_None, first_sell_oid_or_None) for display."""
        bl = self._get_pro_oid_list(sym, "BUY")
        sl = self._get_pro_oid_list(sym, "SELL")
        return (bl[0] if bl else None), (sl[0] if sl else None)

    def _cancel_pro_orders(self, sym: str, side: str, partial_reason: str = "") -> None:
        """Cancel ALL resting orders on this side and recover any partial fills."""
        for oid in list(self._get_pro_oid_list(sym, side)):
            try:
                self.broker.cancel_order(oid, sym)
                LOG.info("PRO cancelled %s %s oid=%s", sym, side, oid)
            except Exception as e:
                LOG.warning("PRO cancel failed %s %s oid=%s: %s", sym, side, oid, e)
            try:
                if hasattr(self.broker, "get_order_snapshot"):
                    snap = getattr(self.broker, "get_order_snapshot")(oid, sym)
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
                    filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(sym, oid, timeout_s=30)
                    if terminal and filled_qty > D0:
                        reason = partial_reason or f"pro_{side.lower()}_partial_cancel"
                        self._apply_fill(sym, side, filled_qty, avg_px, cum_q,
                                         reason=reason, order_id=oid, status="CANCELLED",
                                         skip_ref_update=True)
                        LOG.info("PRO %s %s partial fill recovered (Fyers): qty=%s @ %s",
                                 sym, side, filled_qty, avg_px)
                    elif not terminal:
                        LOG.warning("PRO %s %s partial fill check timed out oid=%s — "
                                    "fill may be unrecovered; cash may be understated",
                                    sym, side, oid)
            except Exception as e:
                LOG.warning("PRO partial fill check after cancel failed %s %s: %s", sym, side, e)
        self._clear_pro_oids(sym, side)

    # Keep old single-cancel name as alias for backward compat with non-proactive callers
    def _cancel_pro_order(self, sym: str, side: str, partial_reason: str = "") -> None:
        self._cancel_pro_orders(sym, side, partial_reason)

    def _cancel_all_pro_orders(self, sym: str) -> None:
        """
        Cancel all resting proactive orders on both sides.
        Uses broker batch cancel (single API call) when available, falling back
        to individual cancels.  Partial fills are recovered from the batch response.
        """
        if hasattr(self.broker, "cancel_all_open_orders"):
            try:
                snaps = self.broker.cancel_all_open_orders(sym)
                # Recover any partial fills from the batch response
                all_oids = (self._get_pro_oid_list(sym, "BUY") +
                            self._get_pro_oid_list(sym, "SELL"))
                oid_side = {o: "BUY" for o in self._get_pro_oid_list(sym, "BUY")}
                oid_side.update({o: "SELL" for o in self._get_pro_oid_list(sym, "SELL")})
                for snap in snaps:
                    oid      = snap.get("order_id", "")
                    executed = _dec(snap.get("executed_qty") or 0)
                    avg_px   = _dec(snap.get("avg_price") or 0)
                    cum_q    = _dec(snap.get("cum_quote_qty") or 0)
                    side     = oid_side.get(oid) or snap.get("side", "")
                    if executed > D0 and side:
                        reason = (f"pro_buy|ref-{float(self.state.symbol_states[sym].reference_price or 0):.4f}"
                                  if side == "BUY" else
                                  f"pro_sell_partial_cancel")
                        self._apply_fill(sym, side, executed, avg_px, cum_q,
                                         reason=reason, order_id=oid,
                                         status="CANCELLED", skip_ref_update=True)
                        LOG.info("PRO %s %s batch-cancel partial fill recovered: qty=%s @ %s",
                                 sym, side, executed, avg_px)
                self._clear_pro_oids(sym, "BUY")
                self._clear_pro_oids(sym, "SELL")
                LOG.info("PRO %s batch cancel done (%d orders)", sym, len(snaps))
                return
            except Exception as e:
                LOG.warning("PRO %s batch cancel failed (%s), falling back to individual cancels", sym, e)
        # Fallback: cancel side-by-side individually
        self._cancel_pro_orders(sym, "BUY")
        self._cancel_pro_orders(sym, "SELL")

    def _check_pro_fill(self, sym: str, oid: str):
        """
        Poll broker for order status.
        Returns (filled_qty, avg_price, cum_quote, is_terminal) or None if still open.
        """
        if hasattr(self.broker, "get_order_snapshot"):
            snap = getattr(self.broker, "get_order_snapshot")(oid, sym)
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
                # Use effective (compounded) quote for consistency with normal order sizing.
                cost_per_trade = _dec(self.state.extras.get("compound_buy_quote") or cfg.buy_quote)
            target_cash = _dec(target_steps) * cost_per_trade
            available_cash = max(self.state.cash - quote_reserve, D0)
            if available_cash >= target_cash:
                return True  # already enough
            # Sell a full step's worth of ETH (not just the deficit).
            # Selling only the deficit produces sub-minimum quantities that exchanges reject;
            # selling a full step ensures the order meets venue minimum qty requirements.
            sell_qty = self._round_qty_pro(cost_per_trade / ref, strategy)
            # cap to what we actually hold
            base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            sell_qty = min(sell_qty, base_qty)
            sell_qty = self._round_qty_pro(sell_qty, strategy)
            if sell_qty <= D0:
                return False
            mode = (self.exec_cfg.order_mode or "market").lower()
            if mode == "marketable_limit":
                sl = _dec(self.exec_cfg.slippage_bps) / _dec("10000")
                limit_px = self._round_price_to_tick(ref * (Decimal("1") - sl))
                req = PlaceOrderRequest(
                    symbol=sym, side="SELL", qty=sell_qty,
                    product_type=self.exec_cfg.product_type, order_type="LIMIT",
                    limit_price=limit_px, time_in_force="GTC",
                )
            else:
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
            usdc_to_spend = deficit_eth * ref
            if usdc_to_spend > available_cash:
                usdc_to_spend = available_cash
            if usdc_to_spend <= D0:
                return False
            mode = (self.exec_cfg.order_mode or "market").lower()
            if mode == "marketable_limit":
                buy_qty = self._round_qty_pro(usdc_to_spend / ref, strategy)
                if buy_qty <= D0:
                    return False
                sl = _dec(self.exec_cfg.slippage_bps) / _dec("10000")
                limit_px = self._round_price_to_tick(ref * (Decimal("1") + sl))
                req = PlaceOrderRequest(
                    symbol=sym, side="BUY", qty=buy_qty,
                    product_type=self.exec_cfg.product_type, order_type="LIMIT",
                    limit_price=limit_px, time_in_force="GTC",
                )
            else:
                # Use quoteOrderQty (USDC to spend) for market BUY — MEXC requires this
                # format for market buy orders; passing base qty causes "quantity scale invalid".
                usdc_to_spend = usdc_to_spend.quantize(_dec("0.01"), rounding=ROUND_DOWN)
                req = PlaceOrderRequest(
                    symbol=sym, side="BUY", quote_qty=usdc_to_spend,
                    product_type=self.exec_cfg.product_type, order_type="MARKET",
                )
            oid = self.exec.place_with_adaptive_qty(req, reason="rebalance_restore")
            if not oid:
                return False
            LOG.info("PRO rebalance_sync BUY %s=%s oid=%s",
                     "qty" if mode == "marketable_limit" else "quote_qty",
                     buy_qty if mode == "marketable_limit" else usdc_to_spend, oid)
            filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(sym, oid)
            if terminal and filled_qty > 0:
                self._apply_fill(sym, "BUY", filled_qty, avg_px if avg_px > D0 else ref, cum_q,
                                 reason="rebalance_restore", order_id=oid, status="FILLED")
            return terminal

    def _place_orders_parallel(self, sym: str, orders: list, *, pre_sellable=None) -> None:
        """
        Place multiple orders concurrently using a thread pool.
        Each entry in `orders` is (side, level_k, limit_price, qty, reason).
        Results are collected and OIDs registered after all threads complete.
        Falls back to sequential placement if threading fails.
        pre_sellable: if provided, used as the sellable qty for SELL orders instead of
        fetching from broker per-thread (avoids N concurrent positions/holdings API calls).
        """
        import concurrent.futures

        import math
        _disc_pct = _dec(getattr(self.exec_cfg, "pro_disclosed_pct", D0))

        _in_preopen = False
        if _disc_pct > D0 and self.regular_open_t != self.open_t:
            import pytz
            _tz = pytz.timezone(self.market_tz)
            _now_local = utcnow().astimezone(_tz)
            _reg_open = _now_local.replace(hour=self.regular_open_t.hour, minute=self.regular_open_t.minute, second=0, microsecond=0)
            _in_preopen = _now_local < _reg_open
            if _in_preopen:
                LOG.info("PRO %s pre-open session: disclosed qty suppressed until %s", sym, self.regular_open_t)

        def _disclosed(qty: Decimal) -> int:
            if _disc_pct <= D0 or _in_preopen:
                return 0
            return math.ceil(float(qty * _disc_pct))

        def _place_one(entry):
            side, k, lvl_price, qty, reason = entry
            req = PlaceOrderRequest(
                symbol=sym, side=side, qty=qty,
                product_type=self.exec_cfg.product_type,
                order_type="LIMIT", limit_price=lvl_price, time_in_force="GTC",
                disclosed_qty=_disclosed(qty),
            )
            hint = pre_sellable if side == "SELL" else None
            oid = self.exec.place_with_adaptive_qty(req, reason=reason, sellable_hint=hint)
            return side, k, lvl_price, qty, oid

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(orders)) as pool:
                futures = [pool.submit(_place_one, entry) for entry in orders]
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        side, k, lvl_price, qty, oid = fut.result()
                        if oid:
                            self._add_pro_oid(sym, side, oid, qty, price=lvl_price)
                            LOG.info("PRO %s %s L%d placed oid=%s qty=%s @ %s disc=%s",
                                     sym, side, k, oid, qty, lvl_price, _disclosed(qty))
                    except Exception as e:
                        LOG.warning("PRO %s parallel place error: %s", sym, e)
        except Exception as e:
            LOG.warning("PRO %s parallel placement failed (%s), placing sequentially", sym, e)
            for side, k, lvl_price, qty, reason in orders:
                req = PlaceOrderRequest(
                    symbol=sym, side=side, qty=qty,
                    product_type=self.exec_cfg.product_type,
                    order_type="LIMIT", limit_price=lvl_price, time_in_force="GTC",
                    disclosed_qty=_disclosed(qty),
                )
                hint = pre_sellable if side == "SELL" else None
                oid = self.exec.place_with_adaptive_qty(req, reason=reason, sellable_hint=hint)
                if oid:
                    self._add_pro_oid(sym, side, oid, qty, price=lvl_price)
                    LOG.info("PRO %s %s L%d placed oid=%s qty=%s @ %s disc=%s",
                             sym, side, k, oid, qty, lvl_price, _disclosed(qty))

    def _smart_cancel_oid(self, sym: str, side: str, oid: str) -> None:
        """Cancel one OID and recover any partial fill (supports both MEXC and Fyers paths)."""
        try:
            self.broker.cancel_order(oid, sym)
        except Exception as e:
            LOG.warning("PRO %s %s smart-cancel oid=%s: %s", sym, side, oid, e)
        try:
            if hasattr(self.broker, "get_order_snapshot"):
                snap = getattr(self.broker, "get_order_snapshot")(oid, sym)
                if snap:
                    executed = _dec(snap.get("executed_qty") or 0)
                    avg_px = _dec(snap.get("avg_price") or 0)
                    cum_q = _dec(snap.get("cum_quote_qty") or 0)
                    if executed > D0:
                        self._apply_fill(sym, side, executed, avg_px, cum_q,
                                         reason=f"pro_{side.lower()}_partial_cancel",
                                         order_id=oid, status="CANCELLED", skip_ref_update=True)
                        oid_price = self._pro_oid_price.get(oid)
                        oid_intended = self._pro_oid_qty.get(oid, executed)
                        remaining = oid_intended - executed
                        if oid_price is not None and remaining > D0:
                            key = f"_pro_partial_remaining_{sym}_{side}_{oid_price}"
                            self.state.extras[key] = str(remaining)
                            LOG.info("PRO %s %s partial cancel oid=%s: filled=%s remaining=%s @ %s",
                                     sym, side, oid, executed, remaining, oid_price)
                        if side == "SELL":
                            # Recovered a SELL partial fill from a stale order (typically on restart).
                            # Apply a 120s sell cooldown to prevent double-selling in the same cycle.
                            cooldown_until = (utcnow() + dt.timedelta(seconds=120)).isoformat()
                            self.state.extras[f"_restart_sell_cooldown_{sym}"] = cooldown_until
                            LOG.warning("PRO %s: restart SELL partial fill recovered (oid=%s qty=%s) "
                                        "— sell cooldown active for 120s", sym, oid, executed)
            else:
                # Fyers: poll until terminal to recover partials
                filled_qty, avg_px, cum_q, terminal = self._wait_fill_blocking(sym, oid, timeout_s=30)
                if terminal and filled_qty > D0:
                    # Subtract any fill already applied via _poll_live_fills to avoid double-counting.
                    already_applied = _dec(self.state.extras.get(f"_pro_oid_applied_{oid}", "0"))
                    delta_fill = max(D0, filled_qty - already_applied)
                    if delta_fill > D0:
                        delta_cum_q = avg_px * delta_fill if avg_px > 0 else D0
                        self._apply_fill(sym, side, delta_fill, avg_px, delta_cum_q,
                                         reason=f"pro_{side.lower()}_partial_cancel",
                                         order_id=oid, status="CANCELLED", skip_ref_update=True)
                    oid_price = self._pro_oid_price.get(oid)
                    oid_intended = self._pro_oid_qty.get(oid, filled_qty)
                    remaining = oid_intended - filled_qty
                    if oid_price is not None and remaining > D0:
                        key = f"_pro_partial_remaining_{sym}_{side}_{oid_price}"
                        self.state.extras[key] = str(remaining)
                        LOG.info("PRO %s %s partial cancel oid=%s: filled=%s already_applied=%s delta=%s remaining=%s @ %s",
                                 sym, side, oid, filled_qty, already_applied, delta_fill, remaining, oid_price)
                    if side == "SELL" and delta_fill > D0:
                        # Recovered a SELL partial fill from a stale order (typically on restart).
                        # Apply a 120s sell cooldown to prevent double-selling in the same cycle.
                        cooldown_until = (utcnow() + dt.timedelta(seconds=120)).isoformat()
                        self.state.extras[f"_restart_sell_cooldown_{sym}"] = cooldown_until
                        LOG.warning("PRO %s: restart SELL partial fill recovered (oid=%s qty=%s) "
                                    "— sell cooldown active for 120s", sym, oid, delta_fill)
        except Exception as e:
            LOG.warning("PRO %s %s smart-cancel partial check failed oid=%s: %s", sym, side, oid, e)
        self._remove_pro_oid(sym, side, oid)
        LOG.info("PRO %s %s smart-cancelled oid=%s (not in new grid)", sym, side, oid)

    def _check_restart_sell_cooldown(self, sym: str) -> bool:
        """Return True if a restart sell cooldown is active for this symbol (sells should be skipped)."""
        key = f"_restart_sell_cooldown_{sym}"
        cooldown_until = self.state.extras.get(key)
        if not cooldown_until:
            return False
        try:
            if utcnow() < dt.datetime.fromisoformat(cooldown_until):
                LOG.info("PRO %s SELL: restart sell cooldown active until %s — skipping sell orders",
                         sym, cooldown_until)
                return True
            else:
                del self.state.extras[key]
                LOG.info("PRO %s SELL: restart sell cooldown expired", sym)
                return False
        except Exception:
            del self.state.extras[key]
            return False

    def _poll_live_fills(self, sym: str) -> None:
        """Detect and apply partial fills on live (kept) grid orders without cancelling them.
        Called once per tick before classifying oids as keep/cancel so that kept_sell_qty
        and available_cash reflect the true remaining unfilled qty.

        _pro_oid_qty[oid] stays as the ORIGINAL intended qty throughout (so _smart_cancel_oid
        can still compute remaining = intended - total_filled correctly).
        _pro_oid_applied_{oid} tracks how much has already been credited via this path so that
        _smart_cancel_oid only credits the delta that hasn't been applied yet.
        Only active for brokers that expose get_live_fills (currently Fyers)."""
        if not hasattr(self.broker, "get_live_fills"):
            return
        all_buy_oids = self._get_pro_oid_list(sym, "BUY")
        all_sell_oids = self._get_pro_oid_list(sym, "SELL")
        all_oids = all_buy_oids + all_sell_oids
        if not all_oids:
            return
        oid_side = {o: "BUY" for o in all_buy_oids}
        oid_side.update({o: "SELL" for o in all_sell_oids})
        try:
            live_fills = self.broker.get_live_fills(all_oids)
        except Exception as e:
            LOG.debug("PRO %s live fill poll error: %s", sym, e)
            return
        for oid, (broker_filled, avg_px) in live_fills.items():
            side = oid_side.get(oid)
            if not side:
                continue
            applied_key = f"_pro_oid_applied_{oid}"
            already_applied = _dec(self.state.extras.get(applied_key, "0"))
            delta = broker_filled - already_applied
            if delta <= D0:
                continue
            cum_q = avg_px * delta
            LOG.info("PRO %s %s live partial fill oid=%s: broker_filled=%s already_applied=%s delta=%s @ %s",
                     sym, side, oid, broker_filled, already_applied, delta, avg_px)
            self._apply_fill(sym, side, delta, avg_px, cum_q,
                             reason=f"pro_{side.lower()}_live_partial",
                             order_id=oid, status="PARTIALLY_FILLED", skip_ref_update=True)
            self.state.extras[applied_key] = str(broker_filled)
            # NOTE: _pro_oid_qty[oid] intentionally left as original intended qty.
            # The classification loop reads already_applied to get effective remaining qty.

    def _place_proactive_fixed_step(self, sym: str, strategy, price: Decimal,
                                      n_levels: int, eff_buy_quote: Decimal,
                                      eff_sell_quote: Decimal, quote_reserve: Decimal) -> None:
        """
        Fixed-step smart rebuild: diff new grid against existing tracked orders,
        cancel only those that don't belong to the new grid, place only the missing ones.
        Called by _place_proactive_orders when cfg.fixed_step is set.
        Requires _pro_oid_price to be populated (done by _place_orders_parallel).

        Capital safety invariants:
          - kept_buy_cost is subtracted from available_cash before placing new buys
          - kept_sell_qty is subtracted from base_qty before placing new sells
          - kept sell orders are limited to remaining_sell_cap during classification
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        ref = _dec(ss.reference_price) if ss.reference_price is not None else price
        step = _dec(cfg.fixed_step)

        # After a gap-fill (pre-open auction executing far from the limit):
        #   BUY gap-down: sell ref = limit price, buy anchor = fill price
        #   SELL gap-up:  buy ref = limit price, sell anchor = fill price
        # This keeps the opposite side anchored at the intended grid level while
        # placing the same-side next order relative to the actual execution price.
        _buy_anchor_str = self.state.extras.get(f"_pro_buy_anchor_{sym}")
        _sell_anchor_str = self.state.extras.get(f"_pro_sell_anchor_{sym}")
        buy_ref = _dec(_buy_anchor_str) if _buy_anchor_str else ref
        sell_ref = _dec(_sell_anchor_str) if _sell_anchor_str else ref

        # Compute new target prices for all levels on both sides
        new_buy_targets: dict = {}   # k -> price
        new_sell_targets: dict = {}  # k -> price
        for k in range(1, n_levels + 1):
            bp = self._round_price_to_tick(buy_ref - k * step)
            sp = self._round_price_to_tick(sell_ref + k * step)
            if bp > D0:
                new_buy_targets[k] = bp
            if sp > D0:
                new_sell_targets[k] = sp

        buy_target_prices = set(new_buy_targets.values())
        sell_target_prices = set(new_sell_targets.values())

        # Detect and apply any live partial fills on kept orders before classifying.
        # This ensures kept_sell_qty and available_cash reflect actual remaining unfilled qty.
        self._poll_live_fills(sym)

        # Compute sell cap before classifying (needed to limit kept sell orders)
        max_pro_sell = getattr(self.exec_cfg, "max_pro_sell_qty", None)
        remaining_sell_cap: "Decimal | None" = None
        if max_pro_sell:
            net_sold = _dec(self.state.extras.get(f"pro_net_sold_{sym}", "0"))
            remaining_sell_cap = max(D0, _dec(str(max_pro_sell)) - net_sold)

        # ---- BUY side: classify existing orders as keep vs cancel ----
        # Track kept_buy_cost so available_cash can be adjusted below.
        keep_buy_prices: set = set()
        cancel_buy_oids: list = []
        kept_buy_cost = D0
        for oid in list(self._get_pro_oid_list(sym, "BUY")):
            oid_price = self._pro_oid_price.get(oid)
            oid_qty = self._pro_oid_qty.get(oid, _dec(cfg.fixed_qty_buy))
            if oid_price is not None and oid_price in buy_target_prices:
                keep_buy_prices.add(oid_price)
                kept_buy_cost += oid_price * oid_qty
            else:
                cancel_buy_oids.append(oid)

        # ---- SELL side: classify existing orders (also enforce cap on kept orders) ----
        # kept_sell_qty is subtracted from both base_qty and remaining_sell_cap below
        # so that new orders are only placed against truly unallocated inventory/cap.
        # Use effective remaining qty (original - already live-applied) for sizing decisions.
        keep_sell_prices: set = set()
        cancel_sell_oids: list = []
        kept_sell_qty = D0
        for oid in list(self._get_pro_oid_list(sym, "SELL")):
            oid_price = self._pro_oid_price.get(oid)
            oid_qty = self._pro_oid_qty.get(oid, _dec(cfg.fixed_qty_sell))
            already_applied = _dec(self.state.extras.get(f"_pro_oid_applied_{oid}", "0"))
            effective_remaining = max(D0, oid_qty - already_applied)
            matches_grid = oid_price is not None and oid_price in sell_target_prices
            within_cap = remaining_sell_cap is None or kept_sell_qty + effective_remaining <= remaining_sell_cap
            if matches_grid and within_cap:
                keep_sell_prices.add(oid_price)
                kept_sell_qty += effective_remaining
            else:
                cancel_sell_oids.append(oid)

        # ---- Cancel non-matching / cap-exceeding orders (with partial fill recovery) ----
        for oid in cancel_buy_oids:
            self._smart_cancel_oid(sym, "BUY", oid)
        for oid in cancel_sell_oids:
            self._smart_cancel_oid(sym, "SELL", oid)

        # ---- Build list of missing orders to place ----
        orders_to_place: list = []

        # BUY: place missing levels.
        # Subtract kept_buy_cost from available_cash so we don't over-commit capital
        # that is already blocked in the broker for the kept buy orders.
        buy_missing = [(k, p) for k, p in sorted(new_buy_targets.items()) if p not in keep_buy_prices]
        available_cash = max(self.state.cash - quote_reserve - kept_buy_cost, D0)
        placed_cash = D0
        # MEXC and most exchanges reject limit BUYs placed too far above market (price band).
        # Skip BUY levels that are above the current price — they'd fire as marketable orders anyway,
        # which is not the grid's intent. The bot will retry placing them once price catches up.
        for k, bp in buy_missing:
            if price > D0 and bp > price:
                LOG.debug("PRO %s BUY L%d @ %s: above current price %s, skipping (will place when price catches up)",
                         sym, k, bp, price)
                continue
            if cfg.sizing_mode == "fixed_qty":
                qty = _dec(cfg.fixed_qty_buy)
                partial_key = f"_pro_partial_remaining_{sym}_BUY_{bp}"
                if partial_key in self.state.extras:
                    qty = _dec(self.state.extras.pop(partial_key))
                    LOG.info("PRO %s BUY L%d @ %s: re-placing partial remaining qty=%s", sym, k, bp, qty)
            elif cfg.sizing_mode == "banded_qty":
                band_mid = (bp // cfg.band_width) * cfg.band_width + cfg.band_width / Decimal("2")
                qty = self._round_qty_pro(eff_buy_quote / (band_mid or Decimal("1")), strategy)
            else:
                qty = self._round_qty_pro(eff_buy_quote / (bp or Decimal("1")), strategy)
            if qty <= D0:
                continue
            order_cost = qty * bp
            if placed_cash + order_cost > available_cash:
                # Try placing a reduced qty with whatever cash remains
                affordable_qty = self._round_qty_pro((available_cash - placed_cash) / bp, strategy)
                affordable_cost = affordable_qty * bp
                if affordable_qty > D0 and affordable_cost >= Decimal("30000"):
                    LOG.warning("PRO %s BUY L%d @ %s: insufficient cash for full qty=%s, placing reduced qty=%s",
                                sym, k, bp, qty, affordable_qty)
                    qty = affordable_qty
                    order_cost = affordable_cost
                else:
                    LOG.warning("PRO %s BUY L%d @ %s: insufficient cash, skipping remaining", sym, k, bp)
                    break
            orders_to_place.append(("BUY", k, bp, qty, f"pro_buy_L{k}|ref-{k}x{step}"))
            placed_cash += order_cost

        # SELL: place missing levels.
        # effective_sell_cap and available_for_new_sell both subtract kept quantities so that
        # new orders are only placed against inventory/cap not already committed to kept orders.
        effective_sell_cap = (None if remaining_sell_cap is None
                              else max(D0, remaining_sell_cap - kept_sell_qty))
        if effective_sell_cap is not None and effective_sell_cap <= D0:
            LOG.info("PRO %s SELL: cap fully committed by kept orders (remaining_cap=%s kept_qty=%s)",
                     sym, remaining_sell_cap, kept_sell_qty)

        sell_missing = [(k, p) for k, p in sorted(new_sell_targets.items()) if p not in keep_sell_prices]
        # Restart sell cooldown: skip placing new sell orders if a partial fill was just
        # recovered from a stale order (prevents double-selling immediately after restart).
        if sell_missing and self._check_restart_sell_cooldown(sym):
            sell_missing = []
        if sell_missing and (effective_sell_cap is None or effective_sell_cap > D0):
            base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            available_for_new_sell = max(D0, base_qty - kept_sell_qty)
            placed_sell_qty = D0
            for k, sp in sell_missing:
                if cfg.sizing_mode == "fixed_qty":
                    qty = _dec(cfg.fixed_qty_sell)
                    partial_key = f"_pro_partial_remaining_{sym}_SELL_{sp}"
                    if partial_key in self.state.extras:
                        qty = _dec(self.state.extras.pop(partial_key))
                        LOG.info("PRO %s SELL L%d @ %s: re-placing partial remaining qty=%s", sym, k, sp, qty)
                elif cfg.sizing_mode == "banded_qty":
                    band_mid = (sp // cfg.band_width) * cfg.band_width + cfg.band_width / Decimal("2")
                    qty = self._round_qty_pro(eff_sell_quote / (band_mid or Decimal("1")), strategy)
                else:
                    qty = self._round_qty_pro(eff_sell_quote / (sp or Decimal("1")), strategy)
                if qty <= D0:
                    continue
                if placed_sell_qty + qty > available_for_new_sell:
                    LOG.warning("PRO %s SELL L%d @ %s: insufficient inventory, skipping remaining", sym, k, sp)
                    break
                if effective_sell_cap is not None and placed_sell_qty + qty > effective_sell_cap:
                    LOG.info("PRO %s SELL L%d @ %s: max_pro_sell_qty cap reached, stopping", sym, k, sp)
                    break
                orders_to_place.append(("SELL", k, sp, qty, f"pro_sell_L{k}|ref+{k}x{step}"))
                placed_sell_qty += qty

        # Track ITM counts for gap-open step-down/up logic
        self.state.extras[f"_itm_buy_placed_{sym}"] = sum(
            1 for e in orders_to_place if e[0] == "BUY" and e[2] > price)
        self.state.extras[f"_itm_sell_placed_{sym}"] = sum(
            1 for e in orders_to_place if e[0] == "SELL" and e[2] < price)

        if cancel_buy_oids or cancel_sell_oids or orders_to_place:
            LOG.info("PRO %s fixed_step smart-rebuild: kept=%dB/%dS cancel=%dB/%dS place=%d",
                     sym, len(keep_buy_prices), len(keep_sell_prices),
                     len(cancel_buy_oids), len(cancel_sell_oids), len(orders_to_place))

        if orders_to_place:
            pre_sellable = None
            if any(e[0] == "SELL" for e in orders_to_place):
                try:
                    pre_sellable = self.exec.compute_broker_sellable(sym)
                except Exception as e:
                    LOG.warning("PRO %s: sellable pre-fetch failed (%s)", sym, e)
            self._place_orders_parallel(sym, orders_to_place, pre_sellable=pre_sellable)

    def _place_proactive_orders(self, sym: str, strategy, price: Decimal) -> None:
        """
        Place resting GTC LIMIT BUY and SELL orders if not already in the book.
        Supports N levels (pro_levels): level k is at ref ± k*pct/100.
        Pre-validates funds for all N orders before placing; rebalances if needed.
        Works for both fixed_quote (crypto) and fixed_qty (India equity) modes.
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        ref = _dec(ss.reference_price) if ss.reference_price is not None else price
        quote_reserve = _dec(self.exec_cfg.quote_reserve)
        n_levels = int(getattr(self.exec_cfg, "pro_levels", 1))

        # Weekend/mode overrides written to state.extras by cron scripts.
        # compound_buy_quote/sell_quote: daily compounding step (already established).
        # weekend_upper_pct / weekend_lower_pct / weekend_pro_levels: tighter grid on weekends.
        eff_upper_pct  = _dec(self.state.extras.get("weekend_upper_pct")  or cfg.upper_pct)
        eff_lower_pct  = _dec(self.state.extras.get("weekend_lower_pct")  or cfg.lower_pct)
        eff_buy_quote  = _dec(self.state.extras.get("compound_buy_quote") or cfg.buy_quote)
        eff_sell_quote = _dec(self.state.extras.get("compound_sell_quote") or cfg.sell_quote)
        n_levels = int(self.state.extras.get("weekend_pro_levels") or n_levels)

        # Safety bounds: compound step must stay within 0.3x–4x of config value.
        # Protects against state corruption, cron race conditions, or runaway compounding.
        # Weekend half-step (~0.5x) is expected; 0.3x gives headroom. 4x allows ~300% growth.
        _cfg_bq = _dec(cfg.buy_quote)
        _cfg_sq = _dec(cfg.sell_quote)
        if _cfg_bq > 0 and not (Decimal("0.3") * _cfg_bq <= eff_buy_quote <= Decimal("4") * _cfg_bq):
            LOG.error("SAFETY: compound_buy_quote=%s is outside [0.3x, 4x] of config=%s — "
                      "reverting to config value", eff_buy_quote, _cfg_bq)
            eff_buy_quote = _cfg_bq
        if _cfg_sq > 0 and not (Decimal("0.3") * _cfg_sq <= eff_sell_quote <= Decimal("4") * _cfg_sq):
            LOG.error("SAFETY: compound_sell_quote=%s is outside [0.3x, 4x] of config=%s — "
                      "reverting to config value", eff_sell_quote, _cfg_sq)
            eff_sell_quote = _cfg_sq

        # Fixed-step mode: diff new grid against existing orders, cancel only excess, place only missing.
        if getattr(cfg, "fixed_step", None):
            self._place_proactive_fixed_step(sym, strategy, price, n_levels,
                                             eff_buy_quote, eff_sell_quote, quote_reserve)
            return

        orders_to_place: list = []   # collected by both BUY and SELL blocks, placed in parallel

        # ---- BUY side: place levels 1..N if fewer than n_levels are tracked ----
        # Condition is < n_levels (not just "empty") so a dropped Level-1 OID doesn't
        # leave the bot stuck with only a Level-2 order and miss the tighter level.
        # When the list is partial we cancel the stale remainder and rebuild all levels
        # from scratch around the current ref to avoid mismatched price levels.
        if len(self._get_pro_oid_list(sym, "BUY")) < n_levels:
            buy_levels: list = []
            total_buy_cost = D0
            for k in range(1, n_levels + 1):
                lvl_price = self._round_price_to_tick(
                    ref * (Decimal("1") - k * eff_lower_pct / Decimal("100"))
                )
                if lvl_price <= D0:
                    continue
                if cfg.sizing_mode == "fixed_qty":
                    qty = _dec(cfg.fixed_qty_buy)
                elif cfg.sizing_mode == "banded_qty":
                    band_mid = (lvl_price // cfg.band_width) * cfg.band_width + cfg.band_width / Decimal("2")
                    qty = self._round_qty_pro(eff_buy_quote / band_mid, strategy)
                else:
                    qty = self._round_qty_pro(eff_buy_quote / lvl_price, strategy)
                buy_levels.append((k, lvl_price, qty))
                total_buy_cost += qty * lvl_price

            available_cash = max(self.state.cash - quote_reserve, D0)
            tracked_buy = self._get_pro_oid_list(sym, "BUY")

            placed_cash = D0
            orders_to_place: list = []
            if tracked_buy and total_buy_cost > available_cash:
                # Partial orders already placed but insufficient cash to fill all levels.
                # Accept the partial — cancel+rebuild would just loop. Skip placement entirely.
                buy_levels = []
            else:
                # Cancel partial orders only if we have enough cash to fill all levels.
                if tracked_buy and total_buy_cost <= available_cash:
                    LOG.info("PRO %s BUY: only %d/%d levels tracked — cancelling stale and rebuilding",
                             sym, len(tracked_buy), n_levels)
                    self._cancel_pro_orders(sym, "BUY")

                if total_buy_cost > available_cash and cfg.rebalance_threshold_steps > 0:
                    LOG.info("PRO %s BUY: insufficient cash (need=%s have=%s), rebalancing",
                             sym, total_buy_cost, available_cash)
                    self._rebalance_sync(sym, strategy, "buy", ref)
                    self._update_extras_crypto({sym: price})
                    available_cash = max(self.state.cash - quote_reserve, D0)
            for k, lvl_price, qty in buy_levels:
                if qty <= D0:
                    continue
                # Skip BUY levels above current market — they'd fire as marketable orders
                # (or get rejected by exchange price-band protection like MEXC's 30087).
                # The bot will retry placing them once price catches up.
                if price > D0 and lvl_price > price:
                    LOG.debug("PRO %s BUY L%d @ %s: above market %s, skipping",
                              sym, k, lvl_price, price)
                    continue
                order_cost = qty * lvl_price
                if placed_cash + order_cost > available_cash:
                    LOG.warning("PRO %s BUY L%d @ %s: insufficient cash, skipping remaining levels",
                                sym, k, lvl_price)
                    break
                orders_to_place.append((
                    "BUY", k, lvl_price, qty,
                    f"pro_buy_L{k}|ref-{k}x{float(eff_lower_pct)}%",
                ))
                placed_cash += order_cost

            # Track how many of the placed buy orders are above current market price
            # (in-the-money). Used by the post-fill step-down to shift ref by exactly
            # this count, so the rebuild doesn't repeat the same above-market levels.
            _itm = sum(1 for e in orders_to_place if e[2] > price)
            self.state.extras[f"_itm_buy_placed_{sym}"] = _itm

        # ---- SELL side: place levels 1..N if fewer than n_levels are tracked ----
        # Compute remaining sell capacity against the hard cap (if configured)
        max_pro_sell = getattr(self.exec_cfg, "max_pro_sell_qty", None)
        remaining_sell_cap: "Decimal | None" = None
        if max_pro_sell:
            net_sold = _dec(self.state.extras.get(f"pro_net_sold_{sym}", "0"))
            remaining_sell_cap = max(D0, _dec(str(max_pro_sell)) - net_sold)
            if remaining_sell_cap <= D0:
                LOG.info("PRO %s SELL: max_pro_sell_qty=%s reached (net_sold=%s) — no sell orders",
                         sym, max_pro_sell, net_sold)

        _do_place_sell = (
            len(self._get_pro_oid_list(sym, "SELL")) < n_levels
            and (remaining_sell_cap is None or remaining_sell_cap > D0)
            and not self._check_restart_sell_cooldown(sym)
        )
        if _do_place_sell:
            sell_levels: list = []
            total_sell_qty = D0
            for k in range(1, n_levels + 1):
                lvl_price = self._round_price_to_tick(
                    ref * (Decimal("1") + k * eff_upper_pct / Decimal("100"))
                )
                if lvl_price <= D0:
                    continue
                if cfg.sizing_mode == "fixed_qty":
                    qty = _dec(cfg.fixed_qty_sell)
                elif cfg.sizing_mode == "banded_qty":
                    band_mid = (lvl_price // cfg.band_width) * cfg.band_width + cfg.band_width / Decimal("2")
                    qty = self._round_qty_pro(eff_sell_quote / band_mid, strategy)
                else:
                    qty = self._round_qty_pro(eff_sell_quote / lvl_price, strategy)
                sell_levels.append((k, lvl_price, qty))
                total_sell_qty += qty

            base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
            tracked_sell = self._get_pro_oid_list(sym, "SELL")

            if tracked_sell and total_sell_qty > base_qty:
                # Partial orders already placed but insufficient inventory to fill all levels.
                # Accept the partial — cancel+rebuild would just loop. Skip placement entirely.
                pass
            else:
                pending_buys = self._get_pro_oid_list(sym, "BUY")
                if total_sell_qty > base_qty and cfg.rebalance_threshold_steps > 0 and not pending_buys:
                    # Only rebalance (buy more ETH) when no BUY orders are pending.
                    # After a sell fills, the pending BUY will naturally restore ETH when it fills.
                    # Rebalancing while a BUY is pending would over-buy ETH.
                    LOG.info("PRO %s SELL: insufficient ETH (need=%s have=%s), rebalancing",
                             sym, total_sell_qty, base_qty)
                    self._rebalance_sync(sym, strategy, "sell", ref)
                    self._update_extras_crypto({sym: price})
                    base_qty = _dec(self.state.extras.get(f"broker_base_qty_{sym}") or ss.traded_qty)
                elif total_sell_qty > base_qty and pending_buys:
                    LOG.info("PRO %s SELL: insufficient ETH (need=%s have=%s) — waiting for pending BUY to fill",
                             sym, total_sell_qty, base_qty)

                # Cancel partial orders only if we have enough inventory to fill all levels.
                if tracked_sell and total_sell_qty <= base_qty:
                    LOG.info("PRO %s SELL: only %d/%d levels tracked — cancelling stale and rebuilding",
                             sym, len(tracked_sell), n_levels)
                    self._cancel_pro_orders(sym, "SELL")

                placed_qty = D0
                for k, lvl_price, qty in sell_levels:
                    if qty <= D0:
                        continue
                    # Skip SELL levels below current market — they'd fire as marketable orders
                    # (or get rejected by exchange price-band protection).
                    if price > D0 and lvl_price < price:
                        LOG.debug("PRO %s SELL L%d @ %s: below market %s, skipping",
                                  sym, k, lvl_price, price)
                        continue
                    if placed_qty + qty > base_qty:
                        LOG.warning("PRO %s SELL L%d @ %s: insufficient inventory, skipping remaining levels",
                                    sym, k, lvl_price)
                        break
                    if remaining_sell_cap is not None and placed_qty + qty > remaining_sell_cap:
                        LOG.info("PRO %s SELL L%d @ %s: max_pro_sell_qty cap reached (remaining=%s), stopping",
                                 sym, k, lvl_price, remaining_sell_cap)
                        break
                    orders_to_place.append((
                        "SELL", k, lvl_price, qty,
                        f"pro_sell_L{k}|ref+{k}x{float(eff_upper_pct)}%",
                    ))
                    placed_qty += qty

            # Track how many of the placed sell orders are below current market price
            # (in-the-money). Used by the post-fill step-up to shift ref by exactly
            # this count, so the rebuild doesn't repeat the same below-market levels.
            _itm_sell = sum(1 for e in orders_to_place if e[0] == "SELL" and e[2] < price)
            self.state.extras[f"_itm_sell_placed_{sym}"] = _itm_sell

        # ---- Place all collected orders in parallel ----
        if orders_to_place:
            # Pre-fetch sellable once so parallel SELL threads don't each call
            # positions() + holdings() + orderbook() — that burst triggers -429.
            pre_sellable = None
            if any(entry[0] == "SELL" for entry in orders_to_place):
                try:
                    pre_sellable = self.exec.compute_broker_sellable(sym)
                except Exception as e:
                    LOG.warning("PRO %s: sellable pre-fetch failed (%s) — fetching per order", sym, e)
            self._place_orders_parallel(sym, orders_to_place, pre_sellable=pre_sellable)

    def _poll_proactive_symbol(self, sym: str, strategy, price: Decimal, allow_new: bool = True) -> None:
        """
        Check ALL BUY and SELL proactive orders for fills (supports N levels).
        Fetches open orders ONCE per tick; only calls get_order_snapshot for OIDs
        that have disappeared (terminal). Normal tick = 1 API call instead of N.
        On any fill: cancel ALL remaining orders on both sides, guard re-center, place fresh N+N.
        Also handles drift: if price drifts > lower_pct+upper_pct from ref, re-center.
        """
        ss = self.state.symbol_states[sym]
        cfg = strategy.cfg
        ref = _dec(ss.reference_price) if ss.reference_price is not None else price

        # Weekend/mode overrides (mirrors _place_proactive_orders resolution)
        eff_upper_pct = _dec(self.state.extras.get("weekend_upper_pct") or cfg.upper_pct)
        eff_lower_pct = _dec(self.state.extras.get("weekend_lower_pct") or cfg.lower_pct)

        # --- Single orderbook fetch for this tick ---
        open_oids: set = set()
        try:
            ob = self.broker.orderbook()
            for o in (ob.get("orderBook") or []):
                oid = str(o.get("id") or "")
                status = o.get("status")
                # Fyers returns all orders (open + filled + cancelled) in orderbook.
                # Only treat as "open" if not in a numeric terminal state.
                # Fyers status codes: 1=Cancelled, 2=Traded/Filled, 5=Rejected, 6=Pending
                # MEXC returns only open orders (status is string), so this filter is harmless.
                is_fyers_terminal = isinstance(status, int) and status in (1, 2, 5)
                if oid and not is_fyers_terminal:
                    open_oids.add(oid)
        except Exception as e:
            LOG.warning("PRO %s: orderbook fetch failed: %s — skipping tick", sym, e)
            return

        # Cache for _place_proactive_orders to detect cash-limited (vs stale) partial sets
        self._last_open_oids = open_oids

        def _check_terminal(side: str, oid: str) -> bool:
            """Returns True if a fill was recorded, removes OID from tracking."""
            nonlocal filled_buy, filled_sell
            if oid in open_oids:
                return False  # still open — no API call needed
            # Not in open orders → terminal: fetch details
            result = self._check_pro_fill(sym, oid)
            self._remove_pro_oid(sym, side, oid)
            if result is None:
                # Snapshot unavailable for a disappeared OID — could be a fill that raced us
                # or an exchange-side cancel we didn't initiate.  Either way, the safe action
                # is to cancel ALL remaining orders and let _place_proactive_orders rebuild,
                # rather than silently leaving a partial (e.g. only Level-2) order set which
                # causes long dead windows where Level-1 is missing.
                LOG.warning(
                    "PRO %s %s oid=%s disappeared but snapshot unavailable — "
                    "cancelling all + re-centering to restore full order set",
                    sym, side, oid,
                )
                self._cancel_all_pro_orders(sym)
                ss.reference_price = price
                return False  # no confirmed fill; _place_proactive_orders restores orders
            filled_qty, avg_px, cum_q, _ = result
            if filled_qty > D0:
                fill_px = avg_px if avg_px > D0 else price
                reason = f"pro_buy|ref-{float(eff_lower_pct)}%" if side == "BUY" else f"pro_sell|ref+{float(eff_upper_pct)}%"
                intended_qty = self._pro_oid_qty.get(oid, D0)
                is_partial = intended_qty > D0 and filled_qty < intended_qty
                if is_partial:
                    remainder = intended_qty - filled_qty
                    LOG.warning("PRO %s %s partial fill oid=%s: filled=%s of %s (remainder=%s @ intended %.4f) — ref unchanged",
                                sym, side, oid, filled_qty, intended_qty, remainder, float(fill_px))
                # Use the order's limit price as the ref anchor for SELL targets.
                # Gap fills (e.g. pre-open auction) can execute far from the limit;
                # using fill price would shift the entire grid away from intended levels.
                # For BUY targets after a gap-fill, anchor to fill price so the next
                # BUY sits below where we actually bought, not above it.
                limit_px = self._pro_oid_price.get(oid, D0)
                if limit_px > D0 and not is_partial:
                    ss.pending_expected_price = limit_px
                    if side == "BUY" and fill_px < limit_px:
                        # Gap-down: BUY filled below its limit (e.g. pre-open auction).
                        # Sell ref stays at limit; buy anchor at fill so next BUY is below where we bought.
                        self.state.extras[f"_pro_buy_anchor_{sym}"] = str(fill_px)
                        self.state.extras.pop(f"_pro_sell_anchor_{sym}", None)
                        LOG.info("PRO %s BUY gap-fill oid=%s: limit=%.4f fill=%.4f — sell ref=%.4f buy anchor=%.4f",
                                 sym, oid, float(limit_px), float(fill_px), float(limit_px), float(fill_px))
                    elif side == "SELL" and fill_px > limit_px:
                        # Gap-up: SELL filled above its limit (e.g. pre-open auction).
                        # Buy ref stays at limit; sell anchor at fill so next SELL is above where we sold.
                        self.state.extras[f"_pro_sell_anchor_{sym}"] = str(fill_px)
                        self.state.extras.pop(f"_pro_buy_anchor_{sym}", None)
                        LOG.info("PRO %s SELL gap-fill oid=%s: limit=%.4f fill=%.4f — buy ref=%.4f sell anchor=%.4f",
                                 sym, oid, float(limit_px), float(fill_px), float(limit_px), float(fill_px))
                    else:
                        # Normal fill: clear any stale gap anchors.
                        self.state.extras.pop(f"_pro_buy_anchor_{sym}", None)
                        self.state.extras.pop(f"_pro_sell_anchor_{sym}", None)
                        if fill_px != limit_px:
                            LOG.info("PRO %s %s fill oid=%s: limit=%.4f fill=%.4f — keeping ref at limit price",
                                     sym, side, oid, float(limit_px), float(fill_px))
                self._apply_fill(sym, side, filled_qty, fill_px, cum_q,
                                 reason=reason, order_id=oid, status="FILLED",
                                 skip_ref_update=is_partial)
                return True
            else:
                LOG.info("PRO %s %s terminal (no fill) oid=%s", sym, side, oid)
                return False

        filled_buy = False
        filled_sell = False

        for oid in list(self._get_pro_oid_list(sym, "BUY")):
            if _check_terminal("BUY", oid):
                filled_buy = True

        for oid in list(self._get_pro_oid_list(sym, "SELL")):
            if _check_terminal("SELL", oid):
                filled_sell = True

        # Drift re-center — configurable via pro_drift_recenter / pro_drift_pct on strategy cfg.
        # Only fires when no fills this iteration (fills take priority).
        _drift_enabled = getattr(cfg, "pro_drift_recenter", True)
        if _drift_enabled and not filled_buy and not filled_sell and ref > D0:
            drift_pct = abs(price - ref) / ref * Decimal("100")
            _custom_pct = getattr(cfg, "pro_drift_pct", None)
            drift_threshold = _dec(str(_custom_pct)) if _custom_pct is not None else (eff_lower_pct + eff_upper_pct)
            if drift_pct > drift_threshold:
                LOG.info("PRO %s drift=%.4f%% > threshold=%.4f%%, re-centering ref=%s->%s",
                         sym, float(drift_pct), float(drift_threshold), ref, price)
                self._cancel_all_pro_orders(sym)
                ss.reference_price = price
                return

        _use_fixed_step = getattr(cfg, "fixed_step", None) is not None

        # Post-fill: update ref → guard re-center → rebuild orders.
        # Fixed-step mode: skip pre-cancel; _place_proactive_orders diffs against existing orders.
        # Pct mode: cancel ALL remaining on both sides first, then place fresh N+N.
        if filled_buy and not filled_sell:
            if not _use_fixed_step:
                self._cancel_pro_orders(sym, "BUY",
                                        partial_reason=f"pro_buy|ref-{float(eff_lower_pct)}%")
                self._cancel_pro_orders(sym, "SELL",
                                        partial_reason=f"pro_sell|ref+{float(eff_upper_pct)}%")
            new_ref = _dec(ss.reference_price)
            if _use_fixed_step:
                step = _dec(cfg.fixed_step)
                l1_buy_price = new_ref - step
                new_sell_level = new_ref + step
            else:
                l1_buy_price = new_ref * (Decimal("1") - eff_lower_pct / Decimal("100"))
                new_sell_level = new_ref * (Decimal("1") + eff_upper_pct / Decimal("100"))
            if price >= new_sell_level:
                # Guard re-center: price ran past sell level after buy
                LOG.info("PRO %s BUY filled, price=%s >= new_sell=%s, re-centering", sym, price, new_sell_level)
                if _use_fixed_step:
                    self._cancel_all_pro_orders(sym)
                ss.reference_price = price
            elif price < l1_buy_price:
                # In-the-money fill: one or more orders were placed above market (gap open).
                # Step ref down by exactly the number of ITM orders that were placed so
                # the next rebuild's L1 lands below current price, stopping the loop.
                itm_count = int(self.state.extras.get(f"_itm_buy_placed_{sym}", 0))
                if itm_count > 0:
                    new_stepped_ref = new_ref
                    for _ in range(itm_count):
                        if _use_fixed_step:
                            new_stepped_ref = new_stepped_ref - _dec(cfg.fixed_step)
                        else:
                            new_stepped_ref = new_stepped_ref * (Decimal("1") - cfg.lower_pct / Decimal("100"))
                    LOG.info("PRO %s BUY %d in-the-money fill(s), stepping ref %s->%s",
                             sym, itm_count, new_ref, new_stepped_ref)
                    if _use_fixed_step:
                        self._cancel_all_pro_orders(sym)
                    ss.reference_price = new_stepped_ref
                    self.state.extras[f"_itm_buy_placed_{sym}"] = 0
            if allow_new:
                self._place_proactive_orders(sym, strategy, price)

        elif filled_sell and not filled_buy:
            if not _use_fixed_step:
                self._cancel_pro_orders(sym, "SELL",
                                        partial_reason=f"pro_sell|ref+{float(eff_upper_pct)}%")
                self._cancel_pro_orders(sym, "BUY",
                                        partial_reason=f"pro_buy|ref-{float(eff_lower_pct)}%")
            new_ref = _dec(ss.reference_price)
            if _use_fixed_step:
                step = _dec(cfg.fixed_step)
                l1_sell_price = new_ref + step
                new_buy_level = new_ref - step
            else:
                l1_sell_price = new_ref * (Decimal("1") + eff_upper_pct / Decimal("100"))
                new_buy_level = new_ref * (Decimal("1") - eff_lower_pct / Decimal("100"))
            if price <= new_buy_level:
                # Guard re-center: price dropped past buy level after sell
                LOG.info("PRO %s SELL filled, price=%s <= new_buy=%s, re-centering", sym, price, new_buy_level)
                if _use_fixed_step:
                    self._cancel_all_pro_orders(sym)
                ss.reference_price = price
            elif price > l1_sell_price:
                # In-the-money fill: one or more sell orders were placed below market (gap up).
                # Step ref up by exactly the number of ITM orders placed so the next
                # rebuild's SELL L1 lands above current price, stopping the loop.
                itm_count = int(self.state.extras.get(f"_itm_sell_placed_{sym}", 0))
                if itm_count > 0:
                    new_stepped_ref = new_ref
                    for _ in range(itm_count):
                        if _use_fixed_step:
                            new_stepped_ref = new_stepped_ref + _dec(cfg.fixed_step)
                        else:
                            new_stepped_ref = new_stepped_ref * (Decimal("1") + cfg.upper_pct / Decimal("100"))
                    LOG.info("PRO %s SELL %d in-the-money fill(s), stepping ref %s->%s",
                             sym, itm_count, new_ref, new_stepped_ref)
                    if _use_fixed_step:
                        self._cancel_all_pro_orders(sym)
                    ss.reference_price = new_stepped_ref
                    self.state.extras[f"_itm_sell_placed_{sym}"] = 0
            if allow_new:
                self._place_proactive_orders(sym, strategy, price)

        elif filled_buy and filled_sell:
            # Both sides filled in same poll (big move) — cancel all remaining, re-center on current price
            LOG.info("PRO %s both BUY+SELL filled, re-centering ref on price=%s", sym, price)
            self._cancel_pro_orders(sym, "BUY",
                                    partial_reason=f"pro_buy|ref-{float(eff_lower_pct)}%")
            self._cancel_pro_orders(sym, "SELL",
                                    partial_reason=f"pro_sell|ref+{float(eff_upper_pct)}%")
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
        self._migrate_renamed_symbol_extras()
        self._restore_oid_prices_from_state()

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

            # Pre-open pause: NSE blocks order entry/cancel between pause_start and regular open.
            # Only active when market_open < regular_market_open (i.e. pre-open trading is enabled).
            in_preopen_pause = False
            if self.open_t < self.regular_open_t:
                preopen_pause_dt = now.replace(hour=self.preopen_pause_t.hour, minute=self.preopen_pause_t.minute, second=0, microsecond=0)
                regular_open_dt = now.replace(hour=self.regular_open_t.hour, minute=self.regular_open_t.minute, second=0, microsecond=0)
                in_preopen_pause = (now >= preopen_pause_dt) and (now < regular_open_dt)
                if in_preopen_pause:
                    allow_new = False

            try:
                prices = self.broker.get_ltps(self.symbols)
                for sym, px in prices.items():
                    self.state.last_prices[sym] = _dec(px)
                    self.state.symbol_states[sym].last_mark_price = _dec(px)

                self._update_extras_crypto(prices)

                # Init references on first run
                for sym in self.symbols:
                    self._init_reference(sym, _dec(prices[sym]))

                if in_preopen_pause:
                    LOG.debug("PRO pre-open pause (%s-%s): skipping poll/place",
                              self.preopen_pause_t.strftime("%H:%M"), self.regular_open_t.strftime("%H:%M"))
                else:
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
                port_val, quote_asset, port_details = self._compute_port_val(prices)
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
                port_val, quote_asset, port_details = self._compute_port_val(prices)
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
                port_val, quote_asset, port_details = self._compute_port_val(prices)
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
