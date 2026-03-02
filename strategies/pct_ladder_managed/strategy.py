# strategies/pct_ladder_managed/strategy.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo
import datetime as dt

from common.broker.interfaces import PlaceOrderRequest, OrderTerminal, to_decimal
from common.engine.strategy_base import OrderAction

Mode = Literal["both"]  # per your requirement: always both sides
D0 = Decimal("0")


def _dec(x: Any) -> Decimal:
    return to_decimal(x)


def _round_to_tick(px: Decimal, tick: Decimal) -> Decimal:
    if tick is None or tick <= 0:
        return px
    # round to nearest tick (NSE EQ tick usually 0.05)
    return (px / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


@dataclass
class SymbolCfg:
    upper_pct: Decimal
    lower_pct: Decimal
    qty_buy: Decimal
    qty_sell: Decimal
    price_tick: Decimal = Decimal("0.05")
    disclosed_qty: Decimal = D0  # 0 => no disclosed quantity


class PctLadderManagedStrategy:
    """
    Managed (proactive) percentage ladder using resting LIMIT orders on both sides.

    Behavior:
    - ref = state.symbol_states[sym].reference_price if available, else set to current LTP at start.
    - Always keep one BUY LIMIT at ref*(1-lower_pct) and one SELL LIMIT at ref*(1+upper_pct).
    - SELL is only placed if inventory exists (state.symbol_states[sym].traded_qty > 0).
    - On ANY fill event (including partial fills, handled by runner), we:
        * set ref = fill_price
        * clear both order IDs (runner cancels remaining)
        * next tick will place a fresh BUY+SELL around new ref
    """

    NS_KEY = "pct_ladder_managed"

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg_raw = cfg
        self.market_tz = str(cfg.get("market_tz") or "Asia/Kolkata")
        self.tz = ZoneInfo(self.market_tz)

        self.symbols: List[str] = list(cfg.get("symbols") or [])
        if not self.symbols:
            raise ValueError("pct_ladder_managed: strategy.symbols is required")

        defaults = cfg.get("defaults") or {}
        def_upper = Decimal(str(defaults.get("upper_pct") or cfg.get("upper_pct") or 1))
        def_lower = Decimal(str(defaults.get("lower_pct") or cfg.get("lower_pct") or 1))
        def_qb = Decimal(str(defaults.get("fixed_qty_buy") or defaults.get("qty_buy") or 0))
        def_qs = Decimal(str(defaults.get("fixed_qty_sell") or defaults.get("qty_sell") or 0))
        def_tick = Decimal(str(defaults.get("price_tick") or "0.05"))
        def_dq = Decimal(str(defaults.get("disclosed_qty") or "0"))

        per = cfg.get("per_symbol") or {}
        self.sym_cfg: Dict[str, SymbolCfg] = {}
        for s in self.symbols:
            ps = per.get(s) or {}
            upper = Decimal(str(ps.get("upper_pct") or def_upper))
            lower = Decimal(str(ps.get("lower_pct") or def_lower))
            qb = Decimal(str(ps.get("fixed_qty_buy") or ps.get("qty_buy") or def_qb))
            qs = Decimal(str(ps.get("fixed_qty_sell") or ps.get("qty_sell") or def_qs))
            tick = Decimal(str(ps.get("price_tick") or def_tick))
            dq = Decimal(str(ps.get("disclosed_qty") or def_dq))
            self.sym_cfg[s] = SymbolCfg(
                upper_pct=upper,
                lower_pct=lower,
                qty_buy=qb,
                qty_sell=qs,
                price_tick=tick,
                disclosed_qty=dq,
            )

    # ---------------- state namespace ----------------
    def _ns(self, state) -> Dict[str, Any]:
        ns = state.extras.get(self.NS_KEY)
        if not isinstance(ns, dict):
            ns = {}
            state.extras[self.NS_KEY] = ns
        ns.setdefault("sym", {})
        return ns

    def _sym_state(self, state, symbol: str) -> Dict[str, Any]:
        ns = self._ns(state)
        smap = ns["sym"]
        if symbol not in smap or not isinstance(smap.get(symbol), dict):
            smap[symbol] = {
                "buy_order_id": None,
                "sell_order_id": None,
            }
        return smap[symbol]

    # ---------------- callbacks from runner ----------------
    def on_order_placed(self, order_id: str, meta: Dict[str, Any], state) -> None:
        sym = str(meta.get("symbol") or "")
        side = str(meta.get("side") or "").upper()
        st = self._sym_state(state, sym)
        if side == "BUY":
            st["buy_order_id"] = str(order_id)
        elif side == "SELL":
            st["sell_order_id"] = str(order_id)

    def on_order_cancelled(self, order_id: str, meta: Dict[str, Any], state) -> None:
        sym = str(meta.get("symbol") or "")
        side = str(meta.get("side") or "").upper()
        st = self._sym_state(state, sym)
        if side == "BUY" and st.get("buy_order_id") == str(order_id):
            st["buy_order_id"] = None
        if side == "SELL" and st.get("sell_order_id") == str(order_id):
            st["sell_order_id"] = None

    def on_order_terminal(self, term: OrderTerminal, meta: Dict[str, Any], state) -> None:
        # Any FILLED event (including partial fill event from runner) rebuilds ladder
        if term.status != "FILLED" or term.filled_qty <= 0:
            return

        sym = term.symbol or str(meta.get("symbol") or "")
        if not sym:
            return

        st = self._sym_state(state, sym)
        st["buy_order_id"] = None
        st["sell_order_id"] = None

        # Update reference to fill price
        fill_px = _dec(term.avg_price)
        if fill_px > 0:
            state.symbol_states[sym].reference_price = fill_px

    # ---------------- main desired actions ----------------
    def desired_actions(
        self,
        prices: Dict[str, Decimal],
        open_orders: List[dict],
        state,
        now_ts: str,
    ) -> List[OrderAction]:
        actions: List[OrderAction] = []

        open_ids = set()
        for o in open_orders or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("id") or o.get("order_id") or "")
            if oid:
                open_ids.add(oid)

        for sym in self.symbols:
            cfg = self.sym_cfg[sym]
            st = self._sym_state(state, sym)

            ltp = _dec(prices.get(sym) or 0)
            if ltp <= 0:
                continue

            ss = state.symbol_states[sym]

            # Initialize reference if missing
            if ss.reference_price is None or _dec(ss.reference_price) <= 0:
                ss.reference_price = ltp

            ref = _dec(ss.reference_price)

            # Compute ladder prices
            buy_px = _round_to_tick(ref * (Decimal("1") - cfg.lower_pct / Decimal("100")), cfg.price_tick)
            sell_px = _round_to_tick(ref * (Decimal("1") + cfg.upper_pct / Decimal("100")), cfg.price_tick)

            # Clear local order ids if no longer open (runner will handle terminal)
            boid = st.get("buy_order_id")
            soid = st.get("sell_order_id")
            if boid and str(boid) not in open_ids:
                # don't place a new one yet; runner should resolve terminal and call on_order_terminal
                pass
            if soid and str(soid) not in open_ids:
                pass

            # Place BUY if none tracked
            if not st.get("buy_order_id"):
                if cfg.qty_buy > 0 and buy_px > 0:
                    req = PlaceOrderRequest(
                        symbol=sym,
                        side="BUY",
                        qty=cfg.qty_buy,
                        order_type="LIMIT",
                        limit_price=buy_px,
                        time_in_force="DAY",
                        validity="DAY",
                        disclosed_qty=cfg.disclosed_qty if cfg.disclosed_qty and cfg.disclosed_qty > 0 else 0,
                    )
                    actions.append(
                        OrderAction(
                            kind="PLACE",
                            request=req,
                            reason=f"ladder_buy ref={ref} -{cfg.lower_pct}%",
                            meta={"symbol": sym, "side": "BUY", "ref": str(ref), "target_px": str(buy_px)},
                        )
                    )

            # Place SELL if none tracked AND we have inventory
            if not st.get("sell_order_id"):
                if cfg.qty_sell > 0 and sell_px > 0 and _dec(ss.traded_qty) > 0:
                    req = PlaceOrderRequest(
                        symbol=sym,
                        side="SELL",
                        qty=cfg.qty_sell,
                        order_type="LIMIT",
                        limit_price=sell_px,
                        time_in_force="DAY",
                        validity="DAY",
                        disclosed_qty=cfg.disclosed_qty if cfg.disclosed_qty and cfg.disclosed_qty > 0 else 0,
                    )
                    actions.append(
                        OrderAction(
                            kind="PLACE",
                            request=req,
                            reason=f"ladder_sell ref={ref} +{cfg.upper_pct}%",
                            meta={"symbol": sym, "side": "SELL", "ref": str(ref), "target_px": str(sell_px)},
                        )
                    )

        return actions


def create_strategy(strategy_cfg: dict) -> PctLadderManagedStrategy:
    return PctLadderManagedStrategy(strategy_cfg)