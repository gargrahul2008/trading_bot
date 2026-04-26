from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Literal

from common.engine.strategy_base import OrderIntent

SizingMode = Literal["pct", "fixed_qty", "fixed_quote", "fixed_percent_of_portfolio", "banded_qty"]
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

    # banded qty: qty = base_usdc / band_midprice, band_midprice = floor(price/band_width)*band_width + band_width/2
    band_width: Decimal = Decimal("100")

    # percent of portfolio (fraction of 1; 0.25 = 25%)
    buy_percent: Decimal = Decimal("0.25")
    sell_percent: Decimal = Decimal("0.25")

    qty_step: Decimal = Decimal("1")
    min_qty: Decimal = Decimal("0")

    # Auto-rebalance: emit MARKET order when one side runs low
    # 0 = disabled; e.g. threshold=4, target=8
    rebalance_threshold_steps: int = 0
    rebalance_target_steps: int = 8

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

            # --- Effective step size (compound override from state.extras) ---
            _cbq = state.extras.get("compound_buy_quote")
            _csq = state.extras.get("compound_sell_quote")
            eff_buy_quote  = _dec(_cbq) if _cbq is not None else _dec(self.cfg.buy_quote)
            eff_sell_quote = _dec(_csq) if _csq is not None else _dec(self.cfg.sell_quote)

            # --- Auto-rebalance check (takes priority over ladder) ---
            if self.cfg.rebalance_threshold_steps > 0:
                buy_quote = eff_buy_quote
                sell_quote = eff_sell_quote
                cash = _dec(state.cash)
                # Use actual broker balance if available (covers manual/external holdings too)
                broker_base = state.extras.get(f"broker_base_qty_{sym}")
                eth_qty = _dec(broker_base) if broker_base is not None else _dec(ss.traded_qty)

                usdc_steps = (cash / buy_quote) if buy_quote > 0 else Decimal("999")
                eth_steps = (eth_qty * ltp / sell_quote) if sell_quote > 0 and ltp > 0 else Decimal("999")
                threshold = Decimal(str(self.cfg.rebalance_threshold_steps))
                target = Decimal(str(self.cfg.rebalance_target_steps))

                MIN_REBAL_STEPS = Decimal("0.1")  # ignore micro-rebalances < 10% of a step

                if usdc_steps <= threshold:
                    # Cash running low → sell ETH to restore to target USDC steps
                    steps_needed = min(target - usdc_steps, Decimal("1"))  # cap at 1 ladder step
                    if steps_needed >= MIN_REBAL_STEPS and ltp > 0:
                        qty = self._round_qty(steps_needed * buy_quote / ltp)
                        if qty > 0:
                            intents.append(OrderIntent(sym, "SELL", qty, "rebalance_sell", order_type="MARKET"))
                            continue

                elif eth_steps <= threshold:
                    # ETH inventory low → buy ETH to restore to target ETH steps
                    steps_needed = min(target - eth_steps, Decimal("1"))  # cap at 1 ladder step
                    if steps_needed >= MIN_REBAL_STEPS and ltp > 0:
                        qty = self._round_qty(steps_needed * sell_quote / ltp)
                        if qty > 0:
                            intents.append(OrderIntent(sym, "BUY", qty, "rebalance_buy", order_type="MARKET"))
                            continue

            if ltp <= buy_thr:
                qty = D0
                if self.cfg.sizing_mode == "fixed_qty":
                    qty = _dec(self.cfg.fixed_qty_buy)
                elif self.cfg.sizing_mode == "fixed_quote":
                    qty = eff_buy_quote / (ltp if ltp > 0 else Decimal("1e-9"))
                elif self.cfg.sizing_mode == "banded_qty":
                    band_mid = (ltp // self.cfg.band_width) * self.cfg.band_width + self.cfg.band_width / Decimal("2")
                    qty = eff_buy_quote / (band_mid if band_mid > 0 else Decimal("1e-9"))
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
                    qty = eff_sell_quote / (ltp if ltp > 0 else Decimal("1e-9"))
                elif self.cfg.sizing_mode == "banded_qty":
                    # Use ref (last buy price) not ltp so sell qty matches buy qty even at band boundaries
                    band_mid = (ref // self.cfg.band_width) * self.cfg.band_width + self.cfg.band_width / Decimal("2")
                    qty = eff_sell_quote / (band_mid if band_mid > 0 else Decimal("1e-9"))
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
        band_width=_dec(strategy_cfg.get("band_width", 100)),
        buy_percent=_dec(strategy_cfg.get("buy_percent", 0.25)),
        sell_percent=_dec(strategy_cfg.get("sell_percent", 0.25)),
        qty_step=_dec(strategy_cfg.get("qty_step", 1)),
        min_qty=_dec(strategy_cfg.get("min_qty", 0)),
        rebalance_threshold_steps=int(strategy_cfg.get("rebalance_threshold_steps", 0)),
        rebalance_target_steps=int(strategy_cfg.get("rebalance_target_steps", 8)),
    )
    return LadderPctStrategy(cfg)
