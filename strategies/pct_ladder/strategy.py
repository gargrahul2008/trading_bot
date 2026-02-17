from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

from common.logger import LOG
from common.models import FillEvent, OrderAction, StrategyContext
from common.reject_parser import parse_qty_suggestion
from common.state import load_json, atomic_write_json
from common.timeutils import now_utc, parse_iso, iso_utc


@dataclass
class SymbolState:
    reference_price: Optional[float] = None
    traded_qty: int = 0
    traded_avg_price: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class StrategyState:
    cash: float = 0.0
    symbol_states: Dict[str, SymbolState] = field(default_factory=dict)

    # reject / pause safety
    reject_events: List[str] = field(default_factory=list)  # utc iso ts
    cooldown_until: Optional[str] = None
    halted_until: Optional[str] = None
    halt_reason: Optional[str] = None

    session_date: Optional[str] = None


class PctLadderStrategy:
    """Reactive percentage ladder.

    Key behavior for your use-case:
    - `core` holdings are not used; everything is treated as *traded inventory*.
    - On sync, the strategy can adopt broker holdings as traded_qty (including T1/T2 if enabled).
    - Sell quantity is clamped to broker-reported sellable_qty (to handle T0/T1/T2).
    - If a SELL is rejected with a message that includes an allowed qty, we parse it and log it.
      The runner already clamps SELL qty to sellable_qty, and may attempt one immediate retry.
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.symbols: List[str] = list(cfg.get("symbols") or [])
        if not self.symbols:
            raise ValueError("Config must include symbols[]")

        self.paths = cfg.get("paths") or {}
        self.state_path = str(self.paths.get("state") or "state.json")
        self.trades_path = str(self.paths.get("trades") or "trades.jsonl")
        self.rejects_path = str(self.paths.get("rejects") or "rejects.jsonl")

        self.product_type = str(cfg.get("product_type") or "CNC")

        ladder = cfg.get("ladder") or {}
        self.upper_pct = float(ladder.get("upper_pct", 1.0))
        self.lower_pct = float(ladder.get("lower_pct", 1.0))
        if self.upper_pct <= 0 or self.lower_pct <= 0:
            raise ValueError("ladder.upper_pct and ladder.lower_pct must be > 0")

        sizing = cfg.get("sizing") or {}
        self.sizing_mode = str(sizing.get("mode") or "fixed_qty")
        self.fixed_buy_qty = int(sizing.get("fixed_buy_qty") or 0)
        self.fixed_sell_qty = int(sizing.get("fixed_sell_qty") or 0)
        self.buy_pct = float(sizing.get("buy_pct") or 0.0)
        self.sell_pct = float(sizing.get("sell_pct") or 0.0)
        self.sizing_base = str(sizing.get("sizing_base") or "fixed")  # fixed/cash
        self.fixed_capital = float(sizing.get("fixed_capital") or 0.0)

        beh = cfg.get("behaviour") or {}
        self.include_t_settled_for_eq = bool(beh.get("include_t_settled_for_eq", True))
        self.adopt_broker_inventory = str(beh.get("adopt_broker_inventory") or "traded")  # traded / none
        self.reset_reference_from_cost_on_sync = bool(beh.get("reset_reference_from_cost_on_sync", True))

        risk = cfg.get("risk") or {}
        self.reject_cooldown_seconds = int(risk.get("reject_cooldown_seconds", 30))
        self.reject_window_minutes = int(risk.get("reject_window_minutes", 60))
        self.max_rejects_in_window = int(risk.get("max_rejects_in_window", 5))

        self._pending: Dict[str, OrderAction] = {}

        self.state = self._load_state()
        for s in self.symbols:
            self.state.symbol_states.setdefault(s, SymbolState())

    # ---------------- persistence ----------------
    def _load_state(self) -> StrategyState:
        raw = load_json(self.state_path)
        st = StrategyState(
            cash=float(raw.get("cash") or 0.0),
            reject_events=list(raw.get("reject_events") or []),
            cooldown_until=raw.get("cooldown_until"),
            halted_until=raw.get("halted_until"),
            halt_reason=raw.get("halt_reason"),
            session_date=raw.get("session_date"),
        )
        ss_raw = raw.get("symbol_states") or {}
        if isinstance(ss_raw, dict):
            for sym, d in ss_raw.items():
                if not isinstance(d, dict):
                    continue
                st.symbol_states[str(sym)] = SymbolState(
                    reference_price=d.get("reference_price"),
                    traded_qty=int(d.get("traded_qty") or 0),
                    traded_avg_price=float(d.get("traded_avg_price") or 0.0),
                    realized_pnl=float(d.get("realized_pnl") or 0.0),
                )
        return st

    def _append_jsonl(self, path: str, rec: Dict[str, Any]) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception as e:
            LOG.warning("Failed writing %s: %s", path, e)

    def persist(self) -> None:
        snap = asdict(self.state)
        atomic_write_json(self.state_path, snap)

    def get_cash(self) -> float:
        return float(self.state.cash)

    # ---------------- pending orders plumbing ----------------
    def register_order(self, order_id: str, action: OrderAction) -> None:
        self._pending[str(order_id)] = action

    def pending_orders(self) -> Dict[str, OrderAction]:
        return dict(self._pending)

    def clear_order(self, order_id: str) -> None:
        self._pending.pop(str(order_id), None)

    # ---------------- pause / rejects ----------------
    def _parse_iso_utc(self, s: Optional[str]) -> Optional[dt.datetime]:
        if not s:
            return None
        try:
            return parse_iso(s).astimezone(dt.timezone.utc)
        except Exception:
            return None

    def _trim_rejects(self) -> None:
        window = dt.timedelta(minutes=self.reject_window_minutes)
        now = now_utc()
        keep: List[str] = []
        for ts in self.state.reject_events:
            try:
                t = parse_iso(ts).astimezone(dt.timezone.utc)
            except Exception:
                continue
            if (now - t) <= window:
                keep.append(ts)
        self.state.reject_events = keep

    def _note_reject(self, *, symbol: str, order_id: str, status: str, message: str, raw: Any) -> None:
        now = now_utc()
        self.state.reject_events.append(iso_utc(now))
        self._trim_rejects()

        cd = now + dt.timedelta(seconds=self.reject_cooldown_seconds)
        self.state.cooldown_until = iso_utc(cd)

        if len(self.state.reject_events) >= self.max_rejects_in_window:
            # halt for a long time; runner doesn't need exact EOD here
            self.state.halted_until = iso_utc(now + dt.timedelta(hours=8))
            self.state.halt_reason = f"MAX_REJECTS({len(self.state.reject_events)})"
            LOG.error("Auto-halt triggered: %s", self.state.halt_reason)

        rej = {
            "ts": iso_utc(now),
            "symbol": symbol,
            "order_id": order_id,
            "status": status,
            "message": message,
            "raw": raw,
            "cooldown_until": self.state.cooldown_until,
            "halted_until": self.state.halted_until,
            "halt_reason": self.state.halt_reason,
            "reject_count_window": len(self.state.reject_events),
        }
        self._append_jsonl(self.rejects_path, rej)

    def is_paused(self) -> bool:
        now = now_utc()
        cd = self._parse_iso_utc(self.state.cooldown_until)
        if cd and now < cd:
            return True
        hu = self._parse_iso_utc(self.state.halted_until)
        if hu and now < hu:
            return True
        return False

    # ---------------- broker sync ----------------
    def sync_from_broker(self, broker) -> None:
        """Sync for cash + holdings inventory for our symbols."""
        funds = broker.funds()
        cash = None
        fund_limit = funds.get("fund_limit") if isinstance(funds, dict) else None
        if isinstance(fund_limit, list):
            priority = {"AVAILABLE BALANCE": 0, "CLEAR BALANCE": 1, "TOTAL BALANCE": 2, "EQUITY": 3}
            best = None
            for it in fund_limit:
                if not isinstance(it, dict):
                    continue
                title = str(it.get("title") or it.get("name") or "").strip().upper()
                amt = it.get("equityAmount") or it.get("amount") or it.get("value")
                if amt is None:
                    continue
                rank = priority.get(title, 99)
                cand = (rank, float(amt))
                if best is None or cand[0] < best[0]:
                    best = cand
            if best is not None:
                cash = best[1]
        if cash is None and isinstance(funds, dict):
            cash = funds.get("cash") or funds.get("availableCash") or funds.get("available_balance")
        if cash is not None:
            self.state.cash = float(cash)

        sellable, cost, remaining = broker.get_inventory(self.symbols)#, include_t_settled=self.include_t_settled_for_eq)

        for sym in self.symbols:
            ss = self.state.symbol_states[sym]
            if self.adopt_broker_inventory == "traded":
                if ss.traded_qty == 0 and abs(ss.realized_pnl) < 1e-9:
                    ss.traded_qty = int(remaining.get(sym, 0) or 0)
                    if float(cost.get(sym, 0.0) or 0.0) > 0:
                        ss.traded_avg_price = float(cost[sym])
                    if self.reset_reference_from_cost_on_sync and ss.reference_price is None and ss.traded_avg_price > 0:
                        ss.reference_price = float(ss.traded_avg_price)

        self.persist()

    # ---------------- sizing helpers ----------------
    def _order_qty_buy(self, price: float) -> int:
        if price <= 0:
            return 0
        if self.sizing_mode == "fixed_qty":
            qty = int(self.fixed_buy_qty)
        else:
            base = self.fixed_capital if self.sizing_base == "fixed" and self.fixed_capital > 0 else float(self.state.cash)
            qty = int((self.buy_pct / 100.0) * float(base) / price)
        qty = min(qty, int(math.floor(self.state.cash / price)))
        return max(qty, 0)

    def _order_qty_sell(self, sym: str, price: float, sellable_qty: int) -> int:
        ss = self.state.symbol_states[sym]
        if price <= 0:
            return 0
        if self.sizing_mode == "fixed_qty":
            qty = int(self.fixed_sell_qty)
        else:
            base = self.fixed_capital if self.sizing_base == "fixed" and self.fixed_capital > 0 else float(self.state.cash)
            qty = int((self.sell_pct / 100.0) * float(base) / price)
        qty = min(qty, int(ss.traded_qty))
        qty = min(qty, int(sellable_qty))
        return max(qty, 0)

    # ---------------- decision logic ----------------
    def on_tick(self, ctx: StrategyContext) -> List[OrderAction]:
        actions: List[OrderAction] = []
        for sym in self.symbols:
            px = float(ctx.prices.get(sym) or 0.0)
            if px <= 0:
                continue

            ss = self.state.symbol_states[sym]

            if ss.reference_price is None:
                ss.reference_price = px

            ref = float(ss.reference_price)
            buy_thr = ref * (1.0 - self.lower_pct / 100.0)
            sell_thr = ref * (1.0 + self.upper_pct / 100.0)

            # one pending per symbol
            if any(a.symbol == sym for a in self._pending.values()):
                continue

            if px <= buy_thr:
                qty = self._order_qty_buy(px)
                if qty > 0:
                    actions.append(OrderAction(sym, "BUY", qty, f"px<=ref-{self.lower_pct:.2f}%", product_type=self.product_type))
                    continue

            if px >= sell_thr:
                sellable = int(ctx.sellable_qty.get(sym) or 0)
                qty = self._order_qty_sell(sym, px, sellable)
                if qty > 0:
                    actions.append(OrderAction(sym, "SELL", qty, f"px>=ref+{self.upper_pct:.2f}%", product_type=self.product_type))

        return actions

    def on_fill(self, fill: FillEvent) -> None:
        sym = fill.symbol
        ss = self.state.symbol_states.setdefault(sym, SymbolState())
        action = self._pending.get(fill.order_id)

        # terminal failure
        if fill.status in {"REJECTED", "CANCELLED", "CANCELED"} or fill.filled_qty <= 0:
            msg = fill.message or ""
            self._note_reject(symbol=sym, order_id=fill.order_id, status=fill.status, message=msg, raw=fill.raw)

            if action and action.side == "SELL":
                sug = parse_qty_suggestion(msg, action.qty).suggested_qty
                if sug is not None:
                    LOG.warning("SELL rejected; parsed suggestion qty=%s. (Runner clamps to sellable anyway)", sug)

            self._append_jsonl(self.trades_path, {
                "ts": fill.ts_utc,
                "event": "ORDER_TERMINAL",
                "order_id": fill.order_id,
                "symbol": sym,
                "side": fill.side,
                "status": fill.status,
                "message": fill.message,
                "raw": fill.raw,
            })
            return

        # filled
        price = float(fill.avg_price or 0.0)
        if price <= 0:
            price = float("nan")

        qty = int(fill.filled_qty)
        realized_delta = 0.0

        if fill.side == "BUY":
            if price == price:
                self.state.cash -= price * qty
            old_qty = ss.traded_qty
            new_qty = old_qty + qty
            if new_qty > 0 and price == price:
                ss.traded_avg_price = ((ss.traded_avg_price * old_qty) + (price * qty)) / new_qty if old_qty > 0 else price
            ss.traded_qty = new_qty
        else:
            sell_qty = min(qty, ss.traded_qty)
            if price == price:
                self.state.cash += price * sell_qty
                realized_delta = sell_qty * (price - ss.traded_avg_price)
                ss.realized_pnl += realized_delta
            ss.traded_qty -= sell_qty
            if ss.traded_qty <= 0:
                ss.traded_qty = 0
                ss.traded_avg_price = 0.0

        if price == price:
            ss.reference_price = price

        self._append_jsonl(self.trades_path, {
            "ts": fill.ts_utc,
            "event": "FILL",
            "order_id": fill.order_id,
            "symbol": sym,
            "side": fill.side,
            "qty": qty,
            "avg_price": fill.avg_price,
            "status": fill.status,
            "message": fill.message,
            "reason": (action.reason if action else ""),
            "realized_delta": realized_delta,
            "cash_after": self.state.cash,
            "traded_qty_after": ss.traded_qty,
            "traded_avg_after": ss.traded_avg_price,
            "realized_pnl_after": ss.realized_pnl,
            "ref_after": ss.reference_price,
            "raw": fill.raw,
        })

    # ---------------- logging ----------------
    def snapshot_line(self, prices: dict) -> str:
        parts = []
        for sym in self.symbols:
            px = float(prices.get(sym) or 0.0)
            ss = self.state.symbol_states[sym]
            ref = ss.reference_price if ss.reference_price is not None else float("nan")
            parts.append(f"{sym} px={px:.2f} ref={ref:.2f} traded={ss.traded_qty} avg={ss.traded_avg_price:.2f} R={ss.realized_pnl:.2f}")
        pause = ""
        if self.is_paused():
            pause = f" PAUSED(cd={self.state.cooldown_until} hu={self.state.halted_until} reason={self.state.halt_reason} rejects={len(self.state.reject_events)})"
        return f"cash={self.state.cash:.2f}{pause} | " + " | ".join(parts)
