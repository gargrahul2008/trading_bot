from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Literal

from common.engine.strategy_base import OrderIntent

SizingMode = Literal["pct", "fixed_qty", "fixed_quote", "fixed_percent_of_portfolio"]
SizingBase = Literal["strategy_equity", "cash", "fixed"]

D0 = Decimal("0")

def _dec(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))

@dataclass
class LadderPctConfig:
    symbols: List[str]
    upper_pct: Decimal = Decimal("1")
    lower_pct: Decimal = Decimal("1")

    sizing_mode: SizingMode = "fixed_qty"

    # pct sizing (equity-style)
    sizing_base: SizingBase = "fixed"
    fixed_capital: Decimal = D0
    buy_trade_pct: Decimal = Decimal("100")
    sell_trade_pct: Decimal = Decimal("100")

    # fixed qty
    fixed_qty_buy: Decimal = Decimal("800")
    fixed_qty_sell: Decimal = Decimal("800")

    # fixed quote notional (crypto-style, quote currency like USDT)
    buy_quote: Decimal = Decimal("50")
    sell_quote: Decimal = Decimal("50")

    # percent of portfolio (fraction of 1; 0.25 = 25%)
    buy_percent: Decimal = Decimal("0.25")
    sell_percent: Decimal = Decimal("0.25")

    qty_step: Decimal = Decimal("1")
    min_qty: Decimal = Decimal("0")

class LadderPctStrategy:
    def __init__(self, cfg: LadderPctConfig):
        self.cfg = cfg
        if cfg.upper_pct <= 0 or cfg.lower_pct <= 0:
            raise ValueError("upper_pct/lower_pct must be > 0")

    def _round_qty(self, qty: Decimal) -> Decimal:
        step = self.cfg.qty_step if self.cfg.qty_step > 0 else Decimal("1")
        # floor to step: floor(qty/step)*step
        q = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step
        if q < self.cfg.min_qty:
            return D0
        return q

    def _base_value(self, state) -> Decimal:
        if self.cfg.sizing_base == "cash":
            return _dec(state.cash)
        if self.cfg.sizing_base == "fixed":
            return _dec(self.cfg.fixed_capital) if self.cfg.fixed_capital > 0 else _dec(state.strategy_equity())
        return _dec(state.strategy_equity())

    def _portfolio_value(self, state) -> Decimal:
        pv = state.extras.get("portfolio_value")
        if pv is not None:
            try:
                return _dec(pv)
            except Exception:
                pass
        return _dec(state.strategy_equity())

    def on_prices(self, prices: Dict[str, Decimal], state, now_ts: str) -> List[OrderIntent]:
        intents: List[OrderIntent] = []
        for sym in self.cfg.symbols:
            ss = state.symbol_states[sym]
            if ss.reference_price is None:
                continue
            ref = _dec(ss.reference_price)
            ltp = _dec(prices[sym])
            buy_thr = ref * (Decimal("1") - self.cfg.lower_pct / Decimal("100"))
            sell_thr = ref * (Decimal("1") + self.cfg.upper_pct / Decimal("100"))

            if ltp <= buy_thr:
                qty = D0
                if self.cfg.sizing_mode == "fixed_qty":
                    qty = _dec(self.cfg.fixed_qty_buy)
                elif self.cfg.sizing_mode == "fixed_quote":
                    qty = _dec(self.cfg.buy_quote) / (ltp if ltp > 0 else Decimal("1e-9"))
                elif self.cfg.sizing_mode == "fixed_percent_of_portfolio":
                    pv = self._portfolio_value(state)
                    notional = _dec(self.cfg.buy_percent) * pv
                    qty = notional / (ltp if ltp > 0 else Decimal("1e-9"))
                else:
                    base = self._base_value(state)
                    order_value = (_dec(self.cfg.buy_trade_pct) / Decimal("100")) * base
                    qty = order_value / (ltp if ltp > 0 else Decimal("1e-9"))

                qty = self._round_qty(qty)
                if qty > 0:
                    intents.append(OrderIntent(sym, "BUY", qty, f"ltp<=ref-{self.cfg.lower_pct}%"))
                continue

            allow_buffer = bool(state.extras.get("use_inventory_buffer"))
            if ltp >= sell_thr and (_dec(ss.traded_qty) > 0 or allow_buffer):
                qty = D0
                if self.cfg.sizing_mode == "fixed_qty":
                    qty = _dec(self.cfg.fixed_qty_sell)
                elif self.cfg.sizing_mode == "fixed_quote":
                    qty = _dec(self.cfg.sell_quote) / (ltp if ltp > 0 else Decimal("1e-9"))
                elif self.cfg.sizing_mode == "fixed_percent_of_portfolio":
                    pv = self._portfolio_value(state)
                    notional = _dec(self.cfg.sell_percent) * pv
                    qty = notional / (ltp if ltp > 0 else Decimal("1e-9"))
                else:
                    base = self._base_value(state)
                    order_value = (_dec(self.cfg.sell_trade_pct) / Decimal("100")) * base
                    qty = order_value / (ltp if ltp > 0 else Decimal("1e-9"))

                # If buffer is OFF -> cap to strategy inventory
                if not allow_buffer:
                    qty = min(qty, _dec(ss.traded_qty))

                qty = self._round_qty(qty)
                if qty > 0:
                    intents.append(OrderIntent(sym, "SELL", qty, f"ltp>=ref+{self.cfg.upper_pct}%"))
                continue
        return intents

def create_strategy(strategy_cfg: dict) -> LadderPctStrategy:
    cfg = LadderPctConfig(
        symbols=strategy_cfg["symbols"],
        upper_pct=_dec(strategy_cfg.get("upper_pct", 1.0)),
        lower_pct=_dec(strategy_cfg.get("lower_pct", 1.0)),
        sizing_mode=str(strategy_cfg.get("sizing_mode", "fixed_qty")),
        sizing_base=str(strategy_cfg.get("sizing_base", "fixed")),
        fixed_capital=_dec(strategy_cfg.get("fixed_capital", 0) or 0),
        buy_trade_pct=_dec(strategy_cfg.get("buy_trade_pct", 100.0)),
        sell_trade_pct=_dec(strategy_cfg.get("sell_trade_pct", 100.0)),
        fixed_qty_buy=_dec(strategy_cfg.get("fixed_qty_buy", 800)),
        fixed_qty_sell=_dec(strategy_cfg.get("fixed_qty_sell", 800)),
        buy_quote=_dec(strategy_cfg.get("buy_quote_usdt", strategy_cfg.get("buy_quote", 50))),
        sell_quote=_dec(strategy_cfg.get("sell_quote_usdt", strategy_cfg.get("sell_quote", 50))),
        buy_percent=_dec(strategy_cfg.get("buy_percent", 0.25)),
        sell_percent=_dec(strategy_cfg.get("sell_percent", 0.25)),
        qty_step=_dec(strategy_cfg.get("qty_step", 1)),
        min_qty=_dec(strategy_cfg.get("min_qty", 0)),
    )
    return LadderPctStrategy(cfg)
