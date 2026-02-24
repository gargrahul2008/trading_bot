from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Literal, Optional, Tuple
from zoneinfo import ZoneInfo

from common.broker.interfaces import Broker, PlaceOrderRequest, OrderTerminal, to_decimal
from common.engine.strategy_base import OrderAction
from common.engine.anchors import fetch_prev_close

Mode = Literal["both", "buy_only", "sell_only"]
D0 = Decimal("0")

def _dec(x: Any) -> Decimal:
    return to_decimal(x)

def _local_date(now_ts: str, tz: ZoneInfo) -> str:
    try:
        t = dt.datetime.fromisoformat(now_ts)
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return t.astimezone(tz).date().isoformat()
    except Exception:
        return dt.datetime.now(tz).date().isoformat()

def _round_to_tick(px: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return px
    # round to nearest tick
    q = (px / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick
    return q

@dataclass
class SymbolCfg:
    buy_levels_pct: List[Decimal]
    sell_levels_pct: List[Decimal]
    mode: Mode
    qty_buy: Decimal
    qty_sell: Decimal
    price_tick: Decimal = Decimal("0.05")
    allow_buy_qty_cap: bool = True

class PrevCloseLevelsStrategy:
    """Proactive grid based on previous close (anchor).
    - Keeps at most one BUY limit and one SELL limit live per symbol (depending on mode).
    - Advances the filled side to the next level.
    - When a side is exhausted, it is reset after the opposite side fills.
    """

    NS_KEY = "prevclose_levels"

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg_raw = cfg
        self.market_tz = str(cfg.get("market_tz") or "Asia/Kolkata")
        self.tz = ZoneInfo(self.market_tz)

        # defaults
        defaults = cfg.get("defaults") or {}
        def_buy = [Decimal(str(x)) for x in (defaults.get("buy_levels_pct") or [])]
        def_sell = [Decimal(str(x)) for x in (defaults.get("sell_levels_pct") or [])]
        def_mode: Mode = str(defaults.get("mode") or "both")  # type: ignore
        def_qty_buy = Decimal(str(defaults.get("qty_buy") or 0))
        def_qty_sell = Decimal(str(defaults.get("qty_sell") or 0))
        def_tick = Decimal(str(defaults.get("price_tick") or "0.05"))
        def_cap = bool(defaults.get("allow_buy_qty_cap", True))

        self.symbols: List[str] = list(cfg.get("symbols") or [])

        per = cfg.get("per_symbol") or {}
        self.sym_cfg: Dict[str, SymbolCfg] = {}
        for s in self.symbols:
            ps = per.get(s) or {}
            buy = [Decimal(str(x)) for x in (ps.get("buy_levels_pct") or def_buy)]
            sell = [Decimal(str(x)) for x in (ps.get("sell_levels_pct") or def_sell)]
            # normalize: buy should be negative, sell positive (we won't enforce, but it's expected)
            buy = sorted(buy, reverse=True)   # e.g. -2.5, -4.5 (closest to 0 first)
            sell = sorted(sell)               # e.g. 2.5, 4.5 (closest to 0 first)
            mode: Mode = str(ps.get("mode") or def_mode)  # type: ignore
            qty_buy = Decimal(str(ps.get("qty_buy") or def_qty_buy))
            qty_sell = Decimal(str(ps.get("qty_sell") or def_qty_sell))
            tick = Decimal(str(ps.get("price_tick") or def_tick))
            cap = bool(ps.get("allow_buy_qty_cap", def_cap))
            self.sym_cfg[s] = SymbolCfg(
                buy_levels_pct=buy,
                sell_levels_pct=sell,
                mode=mode,
                qty_buy=qty_buy,
                qty_sell=qty_sell,
                price_tick=tick,
                allow_buy_qty_cap=cap,
            )

        self.lookback_days = int(cfg.get("anchor_lookback_days") or 10)

    # ---------- state helpers ----------
    def _ns(self, state) -> Dict[str, Any]:
        ns = state.extras.get(self.NS_KEY)
        if not isinstance(ns, dict):
            ns = {}
            state.extras[self.NS_KEY] = ns
        ns.setdefault("anchors", {})
        ns.setdefault("anchor_date", None)
        ns.setdefault("sym", {})
        ns.setdefault("pending_cancel_ids", [])
        return ns

    def _sym_state(self, state, symbol: str) -> Dict[str, Any]:
        ns = self._ns(state)
        sym_map = ns["sym"]
        if symbol not in sym_map or not isinstance(sym_map.get(symbol), dict):
            sym_map[symbol] = {
                "buy": {"idx": 0, "order_id": None, "exhausted": False},
                "sell": {"idx": 0, "order_id": None, "exhausted": False},
            }
        return sym_map[symbol]

    def _set_anchor(self, state, symbol: str, anchor: Decimal) -> None:
        ns = self._ns(state)
        ns["anchors"][symbol] = str(anchor)

    def _get_anchor(self, state, symbol: str) -> Optional[Decimal]:
        ns = self._ns(state)
        a = (ns.get("anchors") or {}).get(symbol)
        if a is None:
            return None
        try:
            return Decimal(str(a))
        except Exception:
            return None

    def _select_initial_idx(self, *, anchor: Decimal, ltp: Decimal, levels: List[Decimal], side: str) -> int:
        # side: "buy" uses d <= level; levels negative sorted desc. "sell" uses d >= level; levels positive sorted asc.
        if not levels:
            return 0
        d = (ltp / anchor - Decimal("1")) * Decimal("100") if anchor > 0 else Decimal("0")
        idx = 0
        if side == "buy":
            for i, lvl in enumerate(levels):
                if d <= lvl:
                    idx = i
        else:
            for i, lvl in enumerate(levels):
                if d >= lvl:
                    idx = i
        return idx

    # ---------- hooks called by runner ----------
    def ensure_anchor(self, broker: Broker, state, now_ts: str, prices: Dict[str, Decimal]) -> None:
        ns = self._ns(state)
        today = _local_date(now_ts, self.tz)
        if ns.get("anchor_date") == today:
            return

        # New trading day -> refresh anchors and reset progression
        old_ids: List[str] = []
        for sym in self.symbols:
            st = self._sym_state(state, sym)
            for side in ("buy", "sell"):
                oid = st[side].get("order_id")
                if oid:
                    old_ids.append(str(oid))
                st[side]["order_id"] = None
                st[side]["exhausted"] = False
                st[side]["idx"] = 0

        # request cancels for any known ids (best-effort)
        ns["pending_cancel_ids"] = list(set((ns.get("pending_cancel_ids") or []) + old_ids))

        # fetch anchors
        for sym in self.symbols:
            anchor = None
            try:
                anchor = fetch_prev_close(broker, symbol=sym, market_tz=self.market_tz, lookback_days=self.lookback_days)
            except Exception:
                # fallback: use current ltp if present
                px = prices.get(sym)
                if px is not None:
                    anchor = _dec(px)
            if anchor is None:
                continue
            self._set_anchor(state, sym, anchor)

            # choose initial indices with jump-handling
            ltp = _dec(prices.get(sym) or anchor)
            cfg = self.sym_cfg[sym]
            st = self._sym_state(state, sym)
            if cfg.buy_levels_pct:
                st["buy"]["idx"] = self._select_initial_idx(anchor=anchor, ltp=ltp, levels=cfg.buy_levels_pct, side="buy")
            if cfg.sell_levels_pct:
                st["sell"]["idx"] = self._select_initial_idx(anchor=anchor, ltp=ltp, levels=cfg.sell_levels_pct, side="sell")

            # keep reference_price aligned to anchor for logs
            try:
                state.symbol_states[sym].reference_price = anchor
            except Exception:
                pass

        ns["anchor_date"] = today

    def on_order_placed(self, order_id: str, meta: Dict[str, Any], state) -> None:
        sym = str(meta.get("symbol") or "")
        side = str(meta.get("side") or "").upper()
        lvl_side = "buy" if side == "BUY" else "sell"
        st = self._sym_state(state, sym)
        st[lvl_side]["order_id"] = str(order_id)

    def on_order_cancelled(self, order_id: str, meta: Dict[str, Any], state) -> None:
        sym = str(meta.get("symbol") or "")
        side = str(meta.get("side") or "").upper()
        lvl_side = "buy" if side == "BUY" else "sell"
        st = self._sym_state(state, sym)
        if st[lvl_side].get("order_id") == str(order_id):
            st[lvl_side]["order_id"] = None

    def on_order_terminal(self, term: OrderTerminal, meta: Dict[str, Any], state) -> None:
        sym = term.symbol or str(meta.get("symbol") or "")
        side = str(term.side or meta.get("side") or "").upper()
        lvl_side = "buy" if side == "BUY" else "sell"
        other_side = "sell" if lvl_side == "buy" else "buy"

        cfg = self.sym_cfg.get(sym)
        if cfg is None:
            return

        st = self._sym_state(state, sym)

        # clear current order_id
        if st[lvl_side].get("order_id") == term.order_id:
            st[lvl_side]["order_id"] = None

        # Only advance on FILLED with positive qty
        if term.status != "FILLED" or term.filled_qty <= 0:
            return

        levels = cfg.buy_levels_pct if lvl_side == "buy" else cfg.sell_levels_pct
        cur_idx = int(meta.get("level_idx") if meta.get("level_idx") is not None else st[lvl_side].get("idx", 0))

        next_idx = cur_idx + 1
        if next_idx >= len(levels):
            st[lvl_side]["exhausted"] = True
            st[lvl_side]["idx"] = next_idx
        else:
            st[lvl_side]["idx"] = next_idx

        # If the opposite side was exhausted, reset it after this fill
        if bool(st[other_side].get("exhausted")):
            anchor = self._get_anchor(state, sym)
            px = _dec(state.last_prices.get(sym) or state.symbol_states[sym].last_mark_price or 0)
            if anchor and px > 0:
                other_levels = cfg.sell_levels_pct if other_side == "sell" else cfg.buy_levels_pct
                st[other_side]["idx"] = self._select_initial_idx(anchor=anchor, ltp=px, levels=other_levels, side=other_side)
                st[other_side]["exhausted"] = False

    # ---------- main strategy logic ----------
    def desired_actions(self, prices: Dict[str, Decimal], open_orders: List[dict], state, now_ts: str) -> List[OrderAction]:
        actions: List[OrderAction] = []
        ns = self._ns(state)

        open_ids = set()
        for o in open_orders or []:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("id") or o.get("order_id") or "")
            if oid:
                open_ids.add(oid)

        # Handle pending cancels (from day reset)
        pending_cancel = list(ns.get("pending_cancel_ids") or [])
        if pending_cancel:
            ns["pending_cancel_ids"] = []
            for oid in pending_cancel:
                actions.append(OrderAction(kind="CANCEL", order_id=str(oid), reason="anchor_reset_cancel", meta={"reason":"anchor_reset_cancel"}))

        for sym in self.symbols:
            cfg = self.sym_cfg[sym]
            anchor = self._get_anchor(state, sym)
            if anchor is None or anchor <= 0:
                continue

            # keep reference aligned to anchor for logs
            state.symbol_states[sym].reference_price = anchor

            ltp = _dec(prices.get(sym) or 0)
            if ltp <= 0:
                continue

            st = self._sym_state(state, sym)

            # BUY side
            if cfg.mode in ("both", "buy_only") and cfg.buy_levels_pct and not st["buy"]["exhausted"]:
                buy_oid = st["buy"].get("order_id")
                if buy_oid and str(buy_oid) not in open_ids:
                    # if it's not open, runner will resolve terminal; do not place a new one yet
                    pass
                elif not buy_oid:
                    idx = int(st["buy"].get("idx", 0))
                    idx = max(0, min(idx, len(cfg.buy_levels_pct)-1))
                    lvl = cfg.buy_levels_pct[idx]
                    px = anchor * (Decimal("1") + (lvl / Decimal("100")))
                    px = _round_to_tick(px, cfg.price_tick)

                    qty = cfg.qty_buy
                    # cash cap (best-effort)
                    if cfg.allow_buy_qty_cap:
                        try:
                            max_afford = (Decimal(str(state.cash)) / px) if px > 0 else D0
                            max_afford_int = Decimal(int(max_afford))
                            qty = min(qty, max_afford_int)
                        except Exception:
                            pass

                    if qty > 0:
                        req = PlaceOrderRequest(
                            symbol=sym,
                            side="BUY",
                            qty=qty,
                            order_type="LIMIT",
                            limit_price=px,
                            time_in_force="DAY",
                            validity="DAY",
                        )
                        actions.append(OrderAction(
                            kind="PLACE",
                            request=req,
                            reason=f"anchor_buy_lvl_{lvl}",
                            meta={
                                "symbol": sym,
                                "side": "BUY",
                                "level_idx": idx,
                                "level_pct": str(lvl),
                                "anchor": str(anchor),
                                "anchor_date": str(ns.get("anchor_date") or ""),
                            }
                        ))

            # SELL side
            if cfg.mode in ("both", "sell_only") and cfg.sell_levels_pct and not st["sell"]["exhausted"]:
                sell_oid = st["sell"].get("order_id")
                if sell_oid and str(sell_oid) not in open_ids:
                    pass
                elif not sell_oid:
                    idx = int(st["sell"].get("idx", 0))
                    idx = max(0, min(idx, len(cfg.sell_levels_pct)-1))
                    lvl = cfg.sell_levels_pct[idx]
                    px = anchor * (Decimal("1") + (lvl / Decimal("100")))
                    px = _round_to_tick(px, cfg.price_tick)

                    qty = cfg.qty_sell
                    if qty > 0:
                        req = PlaceOrderRequest(
                            symbol=sym,
                            side="SELL",
                            qty=qty,
                            order_type="LIMIT",
                            limit_price=px,
                            time_in_force="DAY",
                            validity="DAY",
                        )
                        actions.append(OrderAction(
                            kind="PLACE",
                            request=req,
                            reason=f"anchor_sell_lvl_{lvl}",
                            meta={
                                "symbol": sym,
                                "side": "SELL",
                                "level_idx": idx,
                                "level_pct": str(lvl),
                                "anchor": str(anchor),
                                "anchor_date": str(ns.get("anchor_date") or ""),
                            }
                        ))

        return actions

def create_strategy(strategy_cfg: dict) -> PrevCloseLevelsStrategy:
    return PrevCloseLevelsStrategy(strategy_cfg)
