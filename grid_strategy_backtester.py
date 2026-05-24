import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common.broker.auth_json import get_fyers_creds_from_json, list_fyers_users


@dataclass
class Lot:
    qty: int
    price: float
    tag: str = "grid"


@dataclass
class BacktestConfig:
    symbol: str = "RELIANCE"
    capital_allocated: Optional[float] = None
    chunk_qty: int = 70
    initial_qty: int = 420
    core_qty: Optional[int] = None
    initial_capital_frac: Optional[float] = None
    core_capital_frac: Optional[float] = None
    chunk_capital: Optional[float] = None
    chunk_capital_frac: Optional[float] = None
    grid_pct: float = 0.005
    min_profit_pct: float = 0.006
    fee_per_share: float = 0.0
    mtf_interest_annual: float = 0.0
    mtf_leverage: float = 1.0
    normal_capital_frac: Optional[float] = None
    caution_capital_frac: Optional[float] = None
    hard_capital_frac: Optional[float] = None
    normal_max_qty: int = 560
    caution_max_qty: int = 840
    hard_max_qty: int = 1050
    recovery_extra_sell_qty: int = 70
    recovery_extra_sell_capital: Optional[float] = None
    runway_pct: float = 0.0
    allow_repair: bool = False
    repair_profit_fraction: float = 0.50
    use_intrabar: bool = True
    intrabar_mode: str = "optimistic"


class GridStrategyBacktester:
    def __init__(self, config: BacktestConfig, improved: bool = True, starting_cash: Optional[float] = None):
        self.cfg = config
        self.improved = improved

        self.lots: List[Lot] = []
        self.cash = starting_cash if starting_cash is not None else 0.0
        self.use_cash_runway = starting_cash is not None
        self.realized_grid_pnl = 0.0
        self.total_fees = 0.0
        self.total_interest = 0.0
        self.last_buy_price: Optional[float] = None
        self.last_sell_price: Optional[float] = None
        self._awaiting_buyback = False
        self.base_anchor_price: Optional[float] = None
        self.resolved_initial_qty: Optional[int] = None
        self.resolved_core_qty: Optional[int] = None

        self.trades: List[Dict[str, Any]] = []
        self.equity: List[Dict[str, Any]] = []

    @property
    def open_qty(self) -> int:
        return sum(lot.qty for lot in self.lots)

    def capital_mode_enabled(self) -> bool:
        return self.cfg.capital_allocated is not None and self.cfg.capital_allocated > 0

    def gross_exposure_limit(self) -> Optional[float]:
        if not self.capital_mode_enabled():
            return None
        return float(self.cfg.capital_allocated) * float(self.cfg.mtf_leverage)

    def exposure_ratio(self) -> Optional[float]:
        gross_limit = self.gross_exposure_limit()
        if gross_limit is None or gross_limit <= 0:
            return None
        return self.open_cost() / gross_limit

    def current_chunk_qty(self, price: float) -> int:
        if self.cfg.chunk_capital is not None and self.cfg.chunk_capital > 0:
            return max(1, int(self.cfg.chunk_capital // price))
        if self.capital_mode_enabled() and self.cfg.chunk_capital_frac is not None and self.cfg.chunk_capital_frac > 0:
            chunk_capital = float(self.cfg.capital_allocated) * float(self.cfg.chunk_capital_frac)
            return max(1, int(chunk_capital // price))
        return self.cfg.chunk_qty

    def current_recovery_extra_sell_qty(self, price: float) -> int:
        if self.cfg.recovery_extra_sell_capital is not None and self.cfg.recovery_extra_sell_capital > 0:
            return max(1, int(self.cfg.recovery_extra_sell_capital // price))
        return self.cfg.recovery_extra_sell_qty

    def resolve_initial_qty(self, price: float) -> int:
        if self.cfg.initial_capital_frac is not None:
            if not self.capital_mode_enabled():
                raise ValueError("initial_capital_frac requires capital_allocated")
            initial_capital = float(self.cfg.capital_allocated) * float(self.cfg.initial_capital_frac)
            qty = int(initial_capital // price)
            if qty <= 0:
                raise ValueError("Initial capital allocation is too small to buy even one share")
            return qty
        return self.cfg.initial_qty

    def resolve_core_qty(self, price: float, initial_qty: int) -> int:
        if self.cfg.core_capital_frac is not None:
            if not self.capital_mode_enabled():
                raise ValueError("core_capital_frac requires capital_allocated")
            core_capital = float(self.cfg.capital_allocated) * float(self.cfg.core_capital_frac)
            qty = int(core_capital // price)
            if qty <= 0:
                raise ValueError("Core capital allocation is too small to buy even one share")
            return min(initial_qty, qty)
        if self.cfg.core_qty is None:
            return initial_qty
        return self.cfg.core_qty

    def target_core_qty(self) -> int:
        if self.resolved_core_qty is not None:
            return self.resolved_core_qty
        return self.cfg.initial_qty if self.cfg.core_qty is None else self.cfg.core_qty

    def open_qty_by_tag(self, tag: str) -> int:
        return sum(lot.qty for lot in self.lots if lot.tag == tag)

    @property
    def core_open_qty(self) -> int:
        return self.open_qty_by_tag("core")

    @property
    def grid_open_qty(self) -> int:
        return self.open_qty_by_tag("grid")

    def effective_intrabar_mode(self) -> str:
        if not self.cfg.use_intrabar:
            return "close_only"
        return self.cfg.intrabar_mode

    def mode(self) -> str:
        if not self.improved:
            return "BASELINE"
        if self.capital_mode_enabled() and self.cfg.normal_capital_frac is not None and self.cfg.caution_capital_frac is not None:
            ratio = self.exposure_ratio()
            if ratio is None:
                return "NORMAL"
            if ratio >= self.cfg.caution_capital_frac:
                return "RECOVERY"
            if ratio >= self.cfg.normal_capital_frac:
                return "CAUTION"
            return "NORMAL"
        if self.open_qty >= self.cfg.caution_max_qty:
            return "RECOVERY"
        if self.open_qty >= self.cfg.normal_max_qty:
            return "CAUTION"
        return "NORMAL"

    def avg_cost(self) -> float:
        if self.open_qty == 0:
            return 0.0
        return sum(lot.qty * lot.price for lot in self.lots) / self.open_qty

    def avg_cost_by_tag(self, tag: str) -> float:
        qty = self.open_qty_by_tag(tag)
        if qty == 0:
            return 0.0
        return self.open_cost_by_tag(tag) / qty

    def open_cost(self) -> float:
        return sum(lot.qty * lot.price for lot in self.lots)

    def open_cost_by_tag(self, tag: str) -> float:
        return sum(lot.qty * lot.price for lot in self.lots if lot.tag == tag)

    def unrealized_pnl(self, price: float) -> float:
        return sum((price - lot.price) * lot.qty for lot in self.lots)

    def unrealized_pnl_by_tag(self, price: float, tag: str) -> float:
        return sum((price - lot.price) * lot.qty for lot in self.lots if lot.tag == tag)

    def total_pnl(self, price: float) -> float:
        return self.realized_grid_pnl + self.unrealized_pnl(price) - self.total_fees - self.total_interest

    def breakeven_price(self) -> float:
        if self.open_qty == 0:
            return 0.0
        return (self.open_cost() - self.realized_grid_pnl + self.total_fees + self.total_interest) / self.open_qty

    def fee(self, qty: int) -> float:
        return qty * self.cfg.fee_per_share

    def mtf_borrowed_fraction(self) -> float:
        if self.cfg.mtf_leverage <= 1.0:
            return 0.0
        return (self.cfg.mtf_leverage - 1.0) / self.cfg.mtf_leverage

    def mtf_borrowed_amount(self) -> float:
        return self.open_cost() * self.mtf_borrowed_fraction()

    def accrue_interest(self, dt, previous_dt) -> float:
        if self.cfg.mtf_interest_annual <= 0 or self.open_qty <= 0:
            return 0.0

        elapsed_days = (pd.Timestamp(dt) - pd.Timestamp(previous_dt)).total_seconds() / 86400.0
        if elapsed_days <= 0:
            return 0.0

        borrowed_amount = self.mtf_borrowed_amount()
        if borrowed_amount <= 0:
            return 0.0

        interest = borrowed_amount * self.cfg.mtf_interest_annual * (elapsed_days / 365.0)
        if interest <= 0:
            return 0.0

        self.cash -= interest
        self.total_interest += interest
        self.trades.append({
            "datetime": dt,
            "side": "INTEREST",
            "qty": 0,
            "price": 0.0,
            "reason": "mtf carry cost accrual",
            "mode": self.mode(),
            "interest_base_amount": borrowed_amount,
            "interest_accrued": interest,
            "open_qty_after": self.open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })
        return interest

    def buy(self, dt, qty: int, price: float, reason: str, tag: str = "grid"):
        if qty <= 0:
            return

        if not self.can_buy(qty, price):
            self.trades.append({
                "datetime": dt,
                "side": "SKIP_BUY",
                "qty": qty,
                "price": price,
                "reason": "insufficient cash runway or inventory cap",
                "mode": self.mode(),
                "open_qty_after": self.open_qty,
                "realized_grid_pnl_after": self.realized_grid_pnl,
            })
            return

        fee = self.fee(qty)
        self.cash -= qty * price + fee
        self.total_fees += fee
        self.lots.append(Lot(qty=qty, price=price, tag=tag))
        self.last_buy_price = price
        self._awaiting_buyback = False

        self.trades.append({
            "datetime": dt,
            "side": "BUY",
            "qty": qty,
            "price": price,
            "reason": reason,
            "lot_tag": tag,
            "mode": self.mode(),
            "last_buy_price_after": self.last_buy_price,
            "last_sell_price_after": self.last_sell_price,
            "open_qty_after": self.open_qty,
            "core_open_qty_after": self.core_open_qty,
            "grid_open_qty_after": self.grid_open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })

    def _sell_from_lot_index(self, dt, lot_index: int, qty: int, price: float, reason: str):
        qty = min(qty, self.open_qty)
        if qty <= 0:
            return

        fee = self.fee(qty)
        self.cash += qty * price - fee
        self.total_fees += fee

        lot = self.lots[lot_index]
        matched = min(qty, lot.qty)
        realized_this_sell = (price - lot.price) * matched
        self.realized_grid_pnl += realized_this_sell
        lot.qty -= matched
        sold_from_lot_price = lot.price
        sold_lot_tag = lot.tag

        if lot.qty == 0:
            self.lots.pop(lot_index)

        self.trades.append({
            "datetime": dt,
            "side": "SELL",
            "qty": matched,
            "price": price,
            "reason": reason,
            "mode": self.mode(),
            "realized_this_sell": realized_this_sell,
            "sold_lot_price": sold_from_lot_price,
            "sold_lot_tag": sold_lot_tag,
            "last_buy_price_after": self.last_buy_price,
            "last_sell_price_after": self.last_sell_price,
            "open_qty_after": self.open_qty,
            "core_open_qty_after": self.core_open_qty,
            "grid_open_qty_after": self.grid_open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })

    def sell_tagged_lifo(self, dt, qty: int, price: float, reason: str, allowed_tags: set[str]):
        available_qty = sum(lot.qty for lot in self.lots if lot.tag in allowed_tags)
        qty = min(qty, available_qty)
        if qty <= 0:
            return

        fee = self.fee(qty)
        self.cash += qty * price - fee
        self.total_fees += fee

        remaining = qty
        realized_this_sell = 0.0
        last_sold_tag = None

        while remaining > 0 and self.lots:
            lot_index = None
            for idx in range(len(self.lots) - 1, -1, -1):
                if self.lots[idx].tag in allowed_tags:
                    lot_index = idx
                    break

            if lot_index is None:
                break

            lot = self.lots[lot_index]
            matched = min(remaining, lot.qty)
            pnl = (price - lot.price) * matched
            self.realized_grid_pnl += pnl
            realized_this_sell += pnl

            lot.qty -= matched
            remaining -= matched
            last_sold_tag = lot.tag

            if lot.qty == 0:
                self.lots.pop(lot_index)

        self.trades.append({
            "datetime": dt,
            "side": "SELL",
            "qty": qty - remaining,
            "price": price,
            "reason": reason,
            "mode": self.mode(),
            "realized_this_sell": realized_this_sell,
            "sold_lot_tag": last_sold_tag if qty - remaining > 0 else None,
            "last_buy_price_after": self.last_buy_price,
            "last_sell_price_after": self.last_sell_price,
            "open_qty_after": self.open_qty,
            "core_open_qty_after": self.core_open_qty,
            "grid_open_qty_after": self.grid_open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })

    def sell_lifo(self, dt, qty: int, price: float, reason: str):
        self.sell_tagged_lifo(dt, qty, price, reason, allowed_tags={"core", "grid"})

    def sell_grid_lifo(self, dt, qty: int, price: float, reason: str):
        self.sell_tagged_lifo(dt, qty, price, reason, allowed_tags={"grid"})
        if qty > 0:
            self.last_sell_price = price
            self._awaiting_buyback = True
            if self.trades:
                self.trades[-1]["last_sell_price_after"] = self.last_sell_price

    def next_buy_gap(self) -> float:
        mode = self.mode()
        if mode == "RECOVERY":
            return self.cfg.grid_pct * 4
        if mode == "CAUTION":
            return self.cfg.grid_pct * 2
        return self.cfg.grid_pct

    def can_buy(self, qty: Optional[int] = None, price: Optional[float] = None) -> bool:
        buy_qty = qty or 0

        if qty is not None and price is not None and self.capital_mode_enabled():
            projected_open_cost = self.open_cost() + qty * price
            gross_limit = self.gross_exposure_limit()
            if gross_limit is not None:
                hard_fraction = self.cfg.hard_capital_frac if self.cfg.hard_capital_frac is not None else 1.0
                if projected_open_cost > gross_limit * hard_fraction:
                    return False

        if self.improved and not self.capital_mode_enabled() and buy_qty > 0 and self.open_qty + buy_qty > self.cfg.hard_max_qty:
            return False

        if qty is not None and price is not None and self.use_cash_runway and not self.capital_mode_enabled():
            required_cash = qty * price + self.fee(qty)
            if self.cash < required_cash:
                return False

        return True

    def cash_usage_ratio(self, current_price: float) -> float:
        gross_limit = self.gross_exposure_limit()
        if gross_limit is not None and gross_limit > 0:
            return self.open_cost() / gross_limit
        total_equity = self.cash + self.open_qty * current_price
        if total_equity <= 0:
            return 1.0
        invested = self.open_qty * current_price
        return invested / total_equity

    def latest_grid_lot_index(self) -> Optional[int]:
        for idx in range(len(self.lots) - 1, -1, -1):
            if self.lots[idx].tag == "grid":
                return idx
        return None

    def sell_target_for_latest_lot(self) -> Optional[float]:
        lot_index = self.latest_grid_lot_index()
        if lot_index is None:
            return None
        lot = self.lots[lot_index]
        lot_target = lot.price * (1 + self.cfg.min_profit_pct)
        if self.last_sell_price is not None:
            cascade_floor = self.last_sell_price * (1 + self.cfg.grid_pct)
            lot_target = max(lot_target, cascade_floor)
        return lot_target

    def upper_sell_anchor_price(self) -> Optional[float]:
        if self.grid_open_qty <= 0:
            return None
        if self.last_sell_price is not None:
            return self.last_sell_price
        return self.base_anchor_price

    def upper_sell_target_price(self) -> Optional[float]:
        anchor = self.upper_sell_anchor_price()
        if anchor is None:
            return None
        return anchor * (1 + self.cfg.grid_pct)

    def next_buy_candidates(self) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        if self.last_buy_price is not None:
            candidates.append({
                "threshold": self.last_buy_price * (1 - self.next_buy_gap()),
                "reason": f"{self.mode().lower()} grid buy",
            })

        if self.last_sell_price is not None and self._awaiting_buyback:
            candidates.append({
                "threshold": self.last_sell_price * (1 - self.cfg.grid_pct),
                "reason": "upper grid buyback",
            })

        return candidates

    def next_sell_candidates(self) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        latest_lot_target = self.sell_target_for_latest_lot()
        if latest_lot_target is not None:
            candidates.append({
                "threshold": latest_lot_target,
                "reason": "grid target hit",
            })

        upper_target = self.upper_sell_target_price()
        if upper_target is not None:
            candidates.append({
                "threshold": upper_target,
                "reason": "upper grid trim",
            })

        return candidates

    def min_grid_qty_for_runway(self, price: float) -> int:
        if not self.cfg.runway_pct or self.cfg.runway_pct <= 0:
            return 0
        min_chunks = int(self.cfg.runway_pct / self.cfg.grid_pct)
        return min_chunks * self.current_chunk_qty(price)

    def maybe_sell(self, dt, high_price: float, close_price: float, max_sells: Optional[int] = None) -> int:
        sells_done = 0
        intrabar_mode = self.effective_intrabar_mode()

        while self.grid_open_qty > 0:
            if max_sells is not None and sells_done >= max_sells:
                return sells_done

            candidates = self.next_sell_candidates()
            if not candidates:
                return sells_done

            triggered_candidates = []
            for candidate in candidates:
                threshold = float(candidate["threshold"])
                if intrabar_mode == "close_only":
                    trigger = close_price >= threshold
                    fill_price = close_price
                else:
                    trigger = high_price >= threshold
                    fill_price = threshold

                if trigger:
                    triggered_candidates.append({
                        **candidate,
                        "fill_price": fill_price,
                    })

            if not triggered_candidates:
                return sells_done

            chosen = min(triggered_candidates, key=lambda item: float(item["threshold"]))
            sell_qty = self.current_chunk_qty(float(chosen["fill_price"]))
            reason = str(chosen["reason"])

            if self.improved and self.mode() == "RECOVERY":
                sell_qty += self.current_recovery_extra_sell_qty(float(chosen["fill_price"]))
                reason = f"recovery: {reason} + inventory reduction"

            min_grid = self.min_grid_qty_for_runway(float(chosen["fill_price"]))
            if self.grid_open_qty - sell_qty < min_grid:
                sell_qty = self.grid_open_qty - min_grid
                if sell_qty <= 0:
                    return sells_done

            self.sell_grid_lifo(dt, sell_qty, float(chosen["fill_price"]), reason)
            sells_done += 1

            if intrabar_mode in {"close_only", "one_order_per_candle", "conservative"}:
                return sells_done

        return sells_done

    def maybe_buy(self, dt, low_price: float, close_price: float) -> bool:
        if self.improved and self.mode() == "RECOVERY":
            return False
        candidates = self.next_buy_candidates()
        if not candidates:
            return False

        intrabar_mode = self.effective_intrabar_mode()
        triggered_candidates = []

        for candidate in candidates:
            threshold = float(candidate["threshold"])
            if intrabar_mode == "close_only":
                trigger = close_price <= threshold
                fill_price = close_price
            else:
                trigger = low_price <= threshold
                fill_price = threshold

            if trigger:
                triggered_candidates.append({
                    **candidate,
                    "fill_price": fill_price,
                })

        if not triggered_candidates:
            return False

        chosen = max(triggered_candidates, key=lambda item: float(item["threshold"]))
        reason = str(chosen["reason"])
        prior_trade_count = len(self.trades)
        buy_qty = self.current_chunk_qty(float(chosen["fill_price"]))
        self.buy(dt, buy_qty, float(chosen["fill_price"]), reason, tag="grid")
        return len(self.trades) > prior_trade_count and self.trades[-1]["side"] == "BUY"

    def maybe_repair(self, dt, close_price: float) -> bool:
        if not self.improved or not self.cfg.allow_repair or self.grid_open_qty <= 0:
            return False
        if self.mode() != "RECOVERY":
            return False
        if self.realized_grid_pnl <= 0:
            return False

        grid_indexes = [idx for idx, lot in enumerate(self.lots) if lot.tag == "grid"]
        if not grid_indexes:
            return False

        highest_index = max(grid_indexes, key=lambda idx: self.lots[idx].price)
        highest = self.lots[highest_index]
        if highest.price <= close_price:
            return False

        loss_per_share = highest.price - close_price
        profit_budget = self.realized_grid_pnl * self.cfg.repair_profit_fraction
        affordable_qty = int(profit_budget // loss_per_share)
        repair_chunk_qty = self.current_chunk_qty(close_price)
        repair_qty = min(repair_chunk_qty, highest.qty, affordable_qty)

        if repair_qty < repair_chunk_qty:
            return False

        self._sell_from_lot_index(
            dt,
            highest_index,
            repair_qty,
            close_price,
            "repair: use booked profit to reduce highest-cost inventory",
        )
        self.last_sell_price = close_price
        if self.trades:
            self.trades[-1]["last_sell_price_after"] = self.last_sell_price
        return True

    def mark_equity(self, dt, price: float):
        self.equity.append({
            "datetime": dt,
            "close": price,
            "mode": self.mode(),
            "open_qty": self.open_qty,
            "core_open_qty": self.core_open_qty,
            "grid_open_qty": self.grid_open_qty,
            "avg_cost": self.avg_cost(),
            "core_avg_cost": self.avg_cost_by_tag("core"),
            "grid_avg_cost": self.avg_cost_by_tag("grid"),
            "core_open_cost": self.open_cost_by_tag("core"),
            "grid_open_cost": self.open_cost_by_tag("grid"),
            "realized_grid_pnl": self.realized_grid_pnl,
            "unrealized_pnl": self.unrealized_pnl(price),
            "core_unrealized_pnl": self.unrealized_pnl_by_tag(price, "core"),
            "grid_unrealized_pnl": self.unrealized_pnl_by_tag(price, "grid"),
            "total_fees": self.total_fees,
            "total_interest": self.total_interest,
            "base_anchor_price": self.base_anchor_price,
            "last_buy_price": self.last_buy_price,
            "last_sell_price": self.last_sell_price,
            "gross_exposure_limit": self.gross_exposure_limit(),
            "current_exposure": self.open_cost(),
            "available_exposure": None if self.gross_exposure_limit() is None else self.gross_exposure_limit() - self.open_cost(),
            "exposure_ratio": self.exposure_ratio(),
            "mtf_borrowed_amount": self.mtf_borrowed_amount(),
            "mtf_borrowed_fraction": self.mtf_borrowed_fraction(),
            "total_pnl": self.total_pnl(price),
            "breakeven_price": self.breakeven_price(),
            "cash": self.cash,
            "cash_usage_ratio": self.cash_usage_ratio(price),
        })

    def validate_config(self):
        if self.cfg.chunk_qty <= 0 and (self.cfg.chunk_capital is None and self.cfg.chunk_capital_frac is None):
            raise ValueError("chunk_qty must be positive")
        if self.cfg.initial_qty <= 0:
            raise ValueError("initial_qty must be positive")
        if self.cfg.mtf_leverage < 1.0:
            raise ValueError("mtf_leverage must be greater than or equal to 1.0")
        if self.cfg.caution_max_qty < self.cfg.normal_max_qty:
            raise ValueError("caution_max_qty must be greater than or equal to normal_max_qty")
        if self.cfg.hard_max_qty < self.cfg.caution_max_qty:
            raise ValueError("hard_max_qty must be greater than or equal to caution_max_qty")
        if self.effective_intrabar_mode() not in {"optimistic", "one_order_per_candle", "conservative", "close_only"}:
            raise ValueError("intrabar_mode must be one of: optimistic, one_order_per_candle, conservative, close_only")
        if self.cfg.capital_allocated is not None and self.cfg.capital_allocated <= 0:
            raise ValueError("capital_allocated must be positive")
        for name in ("initial_capital_frac", "core_capital_frac", "chunk_capital_frac", "normal_capital_frac", "caution_capital_frac", "hard_capital_frac"):
            value = getattr(self.cfg, name)
            if value is not None and not (0 < value <= 1):
                raise ValueError(f"{name} must be between 0 and 1")
        if self.cfg.initial_capital_frac is not None and self.cfg.core_capital_frac is not None:
            if self.cfg.core_capital_frac > self.cfg.initial_capital_frac:
                raise ValueError("core_capital_frac cannot exceed initial_capital_frac")
        if self.cfg.normal_capital_frac is not None and self.cfg.caution_capital_frac is not None:
            if self.cfg.caution_capital_frac < self.cfg.normal_capital_frac:
                raise ValueError("caution_capital_frac must be greater than or equal to normal_capital_frac")
        if self.cfg.hard_capital_frac is not None and self.cfg.caution_capital_frac is not None:
            if self.cfg.hard_capital_frac < self.cfg.caution_capital_frac:
                raise ValueError("hard_capital_frac must be greater than or equal to caution_capital_frac")

    def validate_initial_buy(self, price: float):
        initial_qty = self.resolve_initial_qty(price)
        core_qty = self.resolve_core_qty(price, initial_qty)

        if initial_qty <= 0:
            raise ValueError("Resolved initial quantity must be positive")
        if core_qty <= 0:
            raise ValueError("Resolved core quantity must be positive")
        if core_qty > initial_qty:
            raise ValueError("Resolved core quantity cannot exceed resolved initial quantity")

        if self.improved and not self.capital_mode_enabled() and initial_qty > self.cfg.hard_max_qty:
            raise ValueError("initial_qty cannot exceed hard_max_qty")

        if self.use_cash_runway and not self.capital_mode_enabled():
            required_cash = initial_qty * price + self.fee(initial_qty)
            if self.cash < required_cash:
                raise ValueError("Starting cash cannot fund initial buy")

    def initialize_position(self, dt, price: float):
        self.base_anchor_price = price
        initial_qty = self.resolve_initial_qty(price)
        core_qty = self.resolve_core_qty(price, initial_qty)
        self.resolved_initial_qty = initial_qty
        self.resolved_core_qty = core_qty

        if not self.improved:
            self.buy(dt, initial_qty, price, "initial tradable buy", tag="grid")
            self.last_sell_price = price
            return

        initial_grid_qty = initial_qty - core_qty

        self.buy(dt, core_qty, price, "initial core buy", tag="core")
        if initial_grid_qty > 0:
            self.buy(dt, initial_grid_qty, price, "initial grid buy", tag="grid")
            self.last_sell_price = price

    def run(self, price_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = price_df.copy()
        self.validate_config()

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")
        else:
            df = df.reset_index().rename(columns={df.index.name or "index": "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")

        if "close" not in df.columns:
            raise ValueError("price_df must contain a 'close' column")

        if "high" not in df.columns:
            df["high"] = df["close"]
        if "low" not in df.columns:
            df["low"] = df["close"]

        first = df.iloc[0]
        first_close = float(first["close"])
        self.validate_initial_buy(first_close)
        self.initialize_position(first["datetime"], first_close)
        self.mark_equity(first["datetime"], first_close)
        previous_dt = first["datetime"]

        intrabar_mode = self.effective_intrabar_mode()

        for _, row in df.iloc[1:].iterrows():
            dt = row["datetime"]
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])

            self.accrue_interest(dt, previous_dt)

            if intrabar_mode == "conservative":
                buy_hit = any(low <= float(candidate["threshold"]) for candidate in self.next_buy_candidates())
                sell_hit = any(high >= float(candidate["threshold"]) for candidate in self.next_sell_candidates())

                acted = False
                if buy_hit:
                    acted = self.maybe_buy(dt, low, close)
                if not acted and sell_hit:
                    self.maybe_sell(dt, high, close, max_sells=1)
            else:
                max_sells = 1 if intrabar_mode == "one_order_per_candle" else None
                self.maybe_sell(dt, high, close, max_sells=max_sells)
                self.maybe_repair(dt, close)
                if intrabar_mode != "one_order_per_candle":
                    self.maybe_buy(dt, low, close)

            self.mark_equity(dt, close)
            previous_dt = dt

        return {
            "equity": pd.DataFrame(self.equity),
            "trades": pd.DataFrame(self.trades),
        }


def max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    drawdown = equity - running_max
    return float(drawdown.min())


def summarize(equity: pd.DataFrame, trades: pd.DataFrame) -> Dict[str, Any]:
    sells = trades[trades["side"] == "SELL"].copy()
    trade_events = trades[trades["side"].isin(["BUY", "SELL", "SKIP_BUY"])].copy()
    return {
        "final_total_pnl": float(equity["total_pnl"].iloc[-1]),
        "max_drawdown": max_drawdown(equity["total_pnl"]),
        "max_open_qty": int(equity["open_qty"].max()),
        "max_grid_open_qty": int(equity["grid_open_qty"].max()),
        "final_open_qty": int(equity["open_qty"].iloc[-1]),
        "final_core_open_qty": int(equity["core_open_qty"].iloc[-1]),
        "final_grid_open_qty": int(equity["grid_open_qty"].iloc[-1]),
        "final_breakeven_price": float(equity["breakeven_price"].iloc[-1]),
        "realized_grid_pnl": float(equity["realized_grid_pnl"].iloc[-1]),
        "total_interest": float(equity["total_interest"].iloc[-1]),
        "total_fees": float(equity["total_fees"].iloc[-1]),
        "number_of_trades": int(len(trade_events)),
        "number_of_sells": int(len(sells)),
        "sell_win_rate": float((sells.get("realized_this_sell", pd.Series(dtype=float)) > 0).mean()) if len(sells) else np.nan,
    }


def compare_baseline_vs_improved(price_csv_path: str, output_prefix: str = "grid_backtest"):
    prices = pd.read_csv(price_csv_path)
    prices.columns = [c.strip().lower() for c in prices.columns]

    cfg = BacktestConfig()

    baseline = GridStrategyBacktester(cfg, improved=False)
    baseline_result = baseline.run(prices)

    improved = GridStrategyBacktester(cfg, improved=True)
    improved_result = improved.run(prices)

    baseline_equity = baseline_result["equity"]
    improved_equity = improved_result["equity"]
    baseline_trades = baseline_result["trades"]
    improved_trades = improved_result["trades"]

    baseline_equity.to_csv(f"{output_prefix}_baseline_equity.csv", index=False)
    improved_equity.to_csv(f"{output_prefix}_improved_equity.csv", index=False)
    baseline_trades.to_csv(f"{output_prefix}_baseline_trades.csv", index=False)
    improved_trades.to_csv(f"{output_prefix}_improved_trades.csv", index=False)

    summary = pd.DataFrame([
        {"strategy": "baseline", **summarize(baseline_equity, baseline_trades)},
        {"strategy": "improved", **summarize(improved_equity, improved_trades)},
    ])
    summary.to_csv(f"{output_prefix}_summary.csv", index=False)
    return summary


def fetch_fyers_history(symbol: str, start: str, end: str, resolution: str = "1") -> pd.DataFrame:
    from fyers_apiv3 import fyersModel

    client_id = os.getenv("FYERS_CLIENT_ID")
    access_token = os.getenv("FYERS_ACCESS_TOKEN")

    if not client_id or not access_token:
        auth_file = os.getenv("FYERS_AUTH_FILE", "fyers_auth.json")
        user_key = os.getenv("FYERS_USER_KEY", "user1")
        auth_candidates = []
        raw_auth_path = os.path.expanduser(auth_file)
        if os.path.isabs(raw_auth_path):
            auth_candidates.append(raw_auth_path)
        else:
            auth_candidates.append(os.path.abspath(raw_auth_path))
            auth_candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), raw_auth_path))

        seen = set()
        auth_candidates = [path for path in auth_candidates if not (path in seen or seen.add(path))]
        last_error = None

        for candidate in auth_candidates:
            if not os.path.exists(candidate):
                last_error = f"auth file not found: {candidate}"
                continue
            try:
                client_id, access_token = get_fyers_creds_from_json(candidate, user_key=user_key)
                break
            except Exception as exc:
                try:
                    available_users = sorted(list_fyers_users(candidate).keys())
                except Exception:
                    available_users = []
                last_error = (
                    f"failed to load FYERS auth from {candidate} for {user_key}: {exc}. "
                    f"Available users: {available_users}"
                )
        else:
            raise RuntimeError(
                "Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN environment variables, "
                f"and FYERS auth fallback failed. Tried {auth_candidates}. Last error: {last_error}"
            )

    if not client_id or not access_token:
        raise RuntimeError(
            "Missing FYERS auth. Set FYERS_CLIENT_ID/FYERS_ACCESS_TOKEN or provide "
            "FYERS_AUTH_FILE and FYERS_USER_KEY for fyers_auth.json fallback."
        )

    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )

    # Fyers limits 1-min data to 100-day windows — fetch in chunks
    from datetime import datetime as _dt, timedelta as _td
    chunk_days = 99
    start_dt = _dt.strptime(start, "%Y-%m-%d")
    end_dt = _dt.strptime(end, "%Y-%m-%d")
    all_candles: list = []

    chunk_start = start_dt
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + _td(days=chunk_days), end_dt)
        response = fyers.history(data={
            "symbol": symbol,
            "resolution": resolution,
            "date_format": "1",
            "range_from": chunk_start.strftime("%Y-%m-%d"),
            "range_to": chunk_end.strftime("%Y-%m-%d"),
            "cont_flag": "1",
        })

        if not isinstance(response, dict) or response.get("s") != "ok":
            raise RuntimeError(f"FYERS history API error: {response}")

        candles = response.get("candles", [])
        if candles:
            all_candles.extend(candles)
            print(f"  [fyers] {chunk_start.strftime('%Y-%m-%d')} → {chunk_end.strftime('%Y-%m-%d')}: {len(candles):,} candles")

        chunk_start = chunk_end + _td(days=1)

    if not all_candles:
        raise RuntimeError("FYERS returned no candles for the requested symbol/date range.")

    df = pd.DataFrame(all_candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df = df.drop_duplicates(subset="datetime")
    return df[["datetime", "open", "high", "low", "close", "volume"]].sort_values("datetime").reset_index(drop=True)


def fetch_yfinance_history(symbol: str, start: str, end: str, interval: str = "1m") -> pd.DataFrame:
    import yfinance as yf

    data = yf.download(
        symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
    )

    if data.empty:
        raise RuntimeError("Yahoo Finance returned no data.")

    data = data.reset_index()
    data.columns = [str(c).lower().replace(" ", "_") for c in data.columns]

    datetime_col = "datetime" if "datetime" in data.columns else "date"
    data = data.rename(columns={datetime_col: "datetime"})
    return data[["datetime", "open", "high", "low", "close", "volume"]].sort_values("datetime").reset_index(drop=True)


def fetch_price_data(
    symbol: str,
    start: str,
    end: str,
    provider: str = "fyers",
    resolution: str = "1",
) -> pd.DataFrame:
    provider = provider.lower().strip()

    if provider == "fyers":
        return fetch_fyers_history(symbol=symbol, start=start, end=end, resolution=resolution)

    if provider in {"yfinance", "yf", "yahoo"}:
        interval = "1m" if resolution == "1" else f"{resolution}m" if resolution.isdigit() else "1d"
        return fetch_yfinance_history(symbol=symbol, start=start, end=end, interval=interval)

    raise ValueError("provider must be 'fyers' or 'yfinance'")


def compare_baseline_vs_improved_dynamic(
    symbol: str,
    start: str,
    end: str,
    provider: str = "fyers",
    resolution: str = "1",
    starting_cash: Optional[float] = None,
    output_prefix: Optional[str] = None,
    cfg: Optional[BacktestConfig] = None,
):
    prices = fetch_price_data(
        symbol=symbol,
        start=start,
        end=end,
        provider=provider,
        resolution=resolution,
    )

    if cfg is None:
        cfg = BacktestConfig(symbol=symbol)
    else:
        cfg.symbol = symbol

    baseline = GridStrategyBacktester(cfg, improved=False, starting_cash=starting_cash)
    baseline_result = baseline.run(prices)

    improved = GridStrategyBacktester(cfg, improved=True, starting_cash=starting_cash)
    improved_result = improved.run(prices)

    baseline_equity = baseline_result["equity"]
    improved_equity = improved_result["equity"]
    baseline_trades = baseline_result["trades"]
    improved_trades = improved_result["trades"]

    summary = pd.DataFrame([
        {"strategy": "baseline", **summarize(baseline_equity, baseline_trades)},
        {"strategy": "improved", **summarize(improved_equity, improved_trades)},
    ])

    if output_prefix is None:
        safe_symbol = symbol.replace(":", "_").replace("-", "_").replace(".", "_")
        output_prefix = f"grid_backtest_{safe_symbol}_{start}_{end}_{provider}"

    prices.to_csv(f"{output_prefix}_prices.csv", index=False)
    baseline_equity.to_csv(f"{output_prefix}_baseline_equity.csv", index=False)
    improved_equity.to_csv(f"{output_prefix}_improved_equity.csv", index=False)
    baseline_trades.to_csv(f"{output_prefix}_baseline_trades.csv", index=False)
    improved_trades.to_csv(f"{output_prefix}_improved_trades.csv", index=False)
    summary.to_csv(f"{output_prefix}_summary.csv", index=False)

    return {
        "summary": summary,
        "prices": prices,
        "baseline_equity": baseline_equity,
        "improved_equity": improved_equity,
        "baseline_trades": baseline_trades,
        "improved_trades": improved_trades,
        "output_prefix": output_prefix,
    }


def plot_backtest_result(result: Dict[str, pd.DataFrame], output_path: Optional[str] = None):
    baseline_equity = result["baseline_equity"]
    improved_equity = result["improved_equity"]

    plt.figure(figsize=(12, 6))
    plt.plot(pd.to_datetime(baseline_equity["datetime"]), baseline_equity["total_pnl"], label="Baseline")
    plt.plot(pd.to_datetime(improved_equity["datetime"]), improved_equity["total_pnl"], label="Improved")
    plt.axhline(0, linewidth=1)
    plt.title("Grid Strategy Backtest: Baseline vs Improved")
    plt.xlabel("Date & Time")
    plt.ylabel("Total PnL")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()

    if output_path is None:
        output_path = f"{result['output_prefix']}_equity_curve.png"

    plt.savefig(output_path, dpi=150)
    return output_path


def _shade_modes(ax, datetimes, modes):
    """Add colored background bands for NORMAL/CAUTION/RECOVERY modes."""
    mode_colors = {"NORMAL": "#d4edda", "CAUTION": "#fff3cd", "RECOVERY": "#f8d7da", "BASELINE": "#e2e3e5"}
    prev_mode = modes.iloc[0]
    start_dt = datetimes.iloc[0]
    for i in range(1, len(modes)):
        if modes.iloc[i] != prev_mode or i == len(modes) - 1:
            end_dt = datetimes.iloc[i]
            color = mode_colors.get(prev_mode, "#f0f0f0")
            ax.axvspan(start_dt, end_dt, alpha=0.3, color=color, linewidth=0)
            prev_mode = modes.iloc[i]
            start_dt = end_dt


def plot_diagnostics(result: Dict[str, pd.DataFrame], output_path: Optional[str] = None):
    prices = result["prices"].copy()
    improved_trades = result["improved_trades"].copy()
    baseline_equity = result["baseline_equity"].copy()
    improved_equity = result["improved_equity"].copy()

    prices["datetime"] = pd.to_datetime(prices["datetime"])
    improved_trades["datetime"] = pd.to_datetime(improved_trades["datetime"])
    baseline_equity["datetime"] = pd.to_datetime(baseline_equity["datetime"])
    improved_equity["datetime"] = pd.to_datetime(improved_equity["datetime"])

    fig, axes = plt.subplots(5, 1, figsize=(14, 18), sharex=False,
                             gridspec_kw={"height_ratios": [3, 2, 2, 2, 1]})

    # Panel 1: Price + buy/sell markers + mode background
    ax0 = axes[0]
    _shade_modes(ax0, improved_equity["datetime"], improved_equity["mode"])
    ax0.plot(prices["datetime"], prices["close"], color="black", linewidth=0.8, label="Close")
    buys = improved_trades[improved_trades["side"] == "BUY"]
    sells = improved_trades[improved_trades["side"] == "SELL"]
    if len(buys):
        ax0.scatter(buys["datetime"], buys["price"], marker="^", s=20, color="green",
                    alpha=0.7, label=f"BUY ({len(buys)})", zorder=3)
    if len(sells):
        ax0.scatter(sells["datetime"], sells["price"], marker="v", s=20, color="red",
                    alpha=0.7, label=f"SELL ({len(sells)})", zorder=3)
    from matplotlib.patches import Patch
    mode_patches = [
        Patch(facecolor="#d4edda", alpha=0.5, label="NORMAL"),
        Patch(facecolor="#fff3cd", alpha=0.5, label="CAUTION"),
        Patch(facecolor="#f8d7da", alpha=0.5, label="RECOVERY"),
    ]
    handles, labels = ax0.get_legend_handles_labels()
    ax0.legend(handles=handles + mode_patches, fontsize=8, ncol=3)
    ax0.set_title("Price + Mode Zones + Trades")
    ax0.set_ylabel("Price")

    # Panel 2: PnL equity curve
    axes[1].plot(baseline_equity["datetime"], baseline_equity["total_pnl"], label="Baseline total PnL")
    axes[1].plot(improved_equity["datetime"], improved_equity["total_pnl"], label="Improved total PnL")
    axes[1].axhline(0, linewidth=1)
    axes[1].set_title("Total PnL equity curve")
    axes[1].set_ylabel("PnL")
    axes[1].legend(fontsize=8)

    # Panel 3: PnL components
    axes[2].plot(improved_equity["datetime"], improved_equity["realized_grid_pnl"], label="Realized grid PnL")
    axes[2].plot(improved_equity["datetime"], improved_equity["unrealized_pnl"], label="Unrealized inventory PnL")
    axes[2].plot(improved_equity["datetime"], -improved_equity["total_interest"], label="Accumulated interest cost")
    axes[2].plot(improved_equity["datetime"], -improved_equity["total_fees"], label="Accumulated fee cost")
    axes[2].plot(improved_equity["datetime"], improved_equity["total_pnl"], label="Total PnL")
    axes[2].axhline(0, linewidth=1)
    axes[2].set_title("Improved strategy PnL components")
    axes[2].set_ylabel("PnL")
    axes[2].legend(fontsize=8)

    # Panel 4: Inventory with mode background
    ax3 = axes[3]
    _shade_modes(ax3, improved_equity["datetime"], improved_equity["mode"])
    ax3.fill_between(improved_equity["datetime"], 0, improved_equity["open_qty"],
                     alpha=0.3, color="steelblue", label="Total open qty")
    ax3.fill_between(improved_equity["datetime"], 0, improved_equity["core_open_qty"],
                     alpha=0.5, color="orange", label="Core qty")
    ax3.plot(improved_equity["datetime"], improved_equity["grid_open_qty"],
             color="green", linewidth=0.8, label="Grid qty")
    ax3.set_title("Inventory (grid vs core) + Mode Zones")
    ax3.set_ylabel("Quantity")
    ax3b = ax3.twinx()
    ax3b.plot(improved_equity["datetime"], improved_equity["breakeven_price"],
              color="purple", linewidth=0.8, alpha=0.6, label="Breakeven price")
    ax3b.set_ylabel("Breakeven price")
    lines1, labels1 = ax3.get_legend_handles_labels()
    lines2, labels2 = ax3b.get_legend_handles_labels()
    ax3.legend(handles=lines1 + lines2, fontsize=8, loc="upper left")

    # Panel 5: Mode timeline (compact strip)
    ax4 = axes[4]
    mode_map = {"NORMAL": 0, "CAUTION": 1, "RECOVERY": 2, "BASELINE": -1}
    mode_colors_line = {"NORMAL": "#28a745", "CAUTION": "#ffc107", "RECOVERY": "#dc3545", "BASELINE": "#6c757d"}
    mode_num = improved_equity["mode"].map(mode_map).fillna(-1)
    _shade_modes(ax4, improved_equity["datetime"], improved_equity["mode"])
    ax4.plot(improved_equity["datetime"], mode_num, color="black", linewidth=0.5)
    ax4.set_yticks([0, 1, 2])
    ax4.set_yticklabels(["NORMAL", "CAUTION", "RECOVERY"])
    ax4.set_ylim(-0.5, 2.5)
    ax4.set_title("Mode Timeline")

    for ax in axes:
        ax.tick_params(axis="x", rotation=45)

    fig.tight_layout()

    if output_path is None:
        output_path = f"{result['output_prefix']}_diagnostics.png"

    fig.savefig(output_path, dpi=150)
    return output_path


def config_to_dict(
    cfg: BacktestConfig,
    starting_cash: Optional[float],
    provider: str,
    resolution: str,
    start: str,
    end: str,
) -> Dict[str, Any]:
    return {
        "symbol": cfg.symbol,
        "start": start,
        "end": end,
        "provider": provider,
        "resolution": resolution,
        "starting_cash": starting_cash,
        "capital_allocated": cfg.capital_allocated,
        "chunk_qty": cfg.chunk_qty,
        "chunk_capital": cfg.chunk_capital,
        "chunk_capital_frac": cfg.chunk_capital_frac,
        "initial_qty": cfg.initial_qty,
        "initial_capital_frac": cfg.initial_capital_frac,
        "core_qty": cfg.core_qty if cfg.core_qty is not None else cfg.initial_qty,
        "core_capital_frac": cfg.core_capital_frac,
        "grid_pct": cfg.grid_pct,
        "min_profit_pct": cfg.min_profit_pct,
        "fee_per_share": cfg.fee_per_share,
        "mtf_interest_annual": cfg.mtf_interest_annual,
        "mtf_leverage": cfg.mtf_leverage,
        "normal_capital_frac": cfg.normal_capital_frac,
        "caution_capital_frac": cfg.caution_capital_frac,
        "hard_capital_frac": cfg.hard_capital_frac,
        "normal_max_qty": cfg.normal_max_qty,
        "caution_max_qty": cfg.caution_max_qty,
        "hard_max_qty": cfg.hard_max_qty,
        "recovery_extra_sell_qty": cfg.recovery_extra_sell_qty,
        "recovery_extra_sell_capital": cfg.recovery_extra_sell_capital,
        "allow_repair": cfg.allow_repair,
        "runway_pct": cfg.runway_pct,
        "repair_profit_fraction": cfg.repair_profit_fraction,
        "use_intrabar": cfg.use_intrabar,
        "intrabar_mode": cfg.intrabar_mode,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dynamic grid strategy backtester")
    parser.add_argument("--symbol", required=True, help="Example: NSE:RELIANCE-EQ for FYERS, RELIANCE.NS for yfinance")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--provider", default="fyers", choices=["fyers", "yfinance", "yf", "yahoo"])
    parser.add_argument("--resolution", default="1", help="FYERS: 1,5,15,D etc. yfinance: 1 means 1m")
    parser.add_argument("--starting-cash", type=float, default=None, help="Optional runway cash. Example: 1000000")
    parser.add_argument("--output-prefix", default=None)

    parser.add_argument("--capital-allocated", type=float, default=None, help="Capital allocated to this symbol before leverage")
    parser.add_argument("--chunk-qty", type=int, default=70)
    parser.add_argument("--chunk-capital", type=float, default=None, help="Optional per-trade chunk capital in rupees")
    parser.add_argument("--chunk-capital-frac", type=float, default=None, help="Optional per-trade chunk as fraction of allocated capital")
    parser.add_argument("--initial-qty", type=int, default=420)
    parser.add_argument("--initial-capital-frac", type=float, default=None, help="Optional initial deployment as fraction of allocated capital")
    parser.add_argument("--core-qty", type=int, default=None, help="Protected core holding quantity. Defaults to initial_qty")
    parser.add_argument("--core-capital-frac", type=float, default=None, help="Optional protected core as fraction of allocated capital")
    parser.add_argument("--grid-pct", type=float, default=0.005, help="0.005 means 0.5 percent")
    parser.add_argument("--min-profit-pct", type=float, default=0.006, help="0.006 means 0.6 percent target for sell")
    parser.add_argument("--fee-per-share", type=float, default=0.0)
    parser.add_argument("--mtf-interest-annual", type=float, default=0.0, help="Example: 0.12 means 12 percent annual carry")
    parser.add_argument("--mtf-leverage", type=float, default=1.0, help="Example: 3.0 means interest is charged on 2/3 of inventory cost")
    parser.add_argument("--normal-capital-frac", type=float, default=None, help="Exposure ratio where improved mode enters CAUTION")
    parser.add_argument("--caution-capital-frac", type=float, default=None, help="Exposure ratio where improved mode enters RECOVERY")
    parser.add_argument("--hard-capital-frac", type=float, default=None, help="Max exposure ratio allowed for new buys")
    parser.add_argument("--normal-max-qty", type=int, default=560)
    parser.add_argument("--caution-max-qty", type=int, default=840)
    parser.add_argument("--hard-max-qty", type=int, default=1050)
    parser.add_argument("--recovery-extra-sell-qty", type=int, default=70)
    parser.add_argument("--recovery-extra-sell-capital", type=float, default=None, help="Optional extra recovery sell size in rupees")
    parser.add_argument("--allow-repair", action="store_true")
    parser.add_argument("--repair-profit-fraction", type=float, default=0.50)
    parser.add_argument(
        "--intrabar-mode",
        default="optimistic",
        choices=["optimistic", "one_order_per_candle", "conservative", "close_only"],
        help="Intrabar execution policy for OHLC candles",
    )
    parser.add_argument("--close-only", action="store_true", help="Alias for --intrabar-mode close_only")

    args = parser.parse_args()

    intrabar_mode = "close_only" if args.close_only else args.intrabar_mode

    cfg = BacktestConfig(
        symbol=args.symbol,
        capital_allocated=args.capital_allocated,
        chunk_qty=args.chunk_qty,
        chunk_capital=args.chunk_capital,
        chunk_capital_frac=args.chunk_capital_frac,
        initial_qty=args.initial_qty,
        initial_capital_frac=args.initial_capital_frac,
        core_qty=args.core_qty,
        core_capital_frac=args.core_capital_frac,
        grid_pct=args.grid_pct,
        min_profit_pct=args.min_profit_pct,
        fee_per_share=args.fee_per_share,
        mtf_interest_annual=args.mtf_interest_annual,
        mtf_leverage=args.mtf_leverage,
        normal_capital_frac=args.normal_capital_frac,
        caution_capital_frac=args.caution_capital_frac,
        hard_capital_frac=args.hard_capital_frac,
        normal_max_qty=args.normal_max_qty,
        caution_max_qty=args.caution_max_qty,
        hard_max_qty=args.hard_max_qty,
        recovery_extra_sell_qty=args.recovery_extra_sell_qty,
        recovery_extra_sell_capital=args.recovery_extra_sell_capital,
        allow_repair=args.allow_repair,
        repair_profit_fraction=args.repair_profit_fraction,
        use_intrabar=intrabar_mode != "close_only",
        intrabar_mode=intrabar_mode,
    )

    result = compare_baseline_vs_improved_dynamic(
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        provider=args.provider,
        resolution=args.resolution,
        starting_cash=args.starting_cash,
        output_prefix=args.output_prefix,
        cfg=cfg,
    )
    chart_path = plot_backtest_result(result)
    diagnostics_path = plot_diagnostics(result)

    config_df = pd.DataFrame([config_to_dict(
        cfg=cfg,
        starting_cash=args.starting_cash,
        provider=args.provider,
        resolution=args.resolution,
        start=args.start,
        end=args.end,
    )])
    config_path = f"{result['output_prefix']}_config.csv"
    config_df.to_csv(config_path, index=False)

    print("\nCONFIG USED")
    print(config_df.T.to_string(header=False))
    print("\nSUMMARY")
    print(result["summary"].to_string(index=False))
    print(f"\nSaved outputs with prefix: {result['output_prefix']}")
    print(f"Saved equity chart: {chart_path}")
    print(f"Saved diagnostics chart: {diagnostics_path}")
    print(f"Saved config: {config_path}")
