"""Fixed-point grid ladder calculation, cost validation, and qty adjustment."""

import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

LOG = logging.getLogger("bzg.grid")

NSE_TICK_SIZE = 0.05


@dataclass
class GridInfo:
    grid_gap_points: float = 0
    grid_levels: List[float] = field(default_factory=list)
    total_levels: int = 0
    grids_to_bottom: int = 0

    order_qty: int = 0
    order_value: float = 0
    gross_cycle_profit: float = 0
    round_trip_cost: float = 0
    net_cycle_profit: float = 0

    # Bar range metrics
    avg_bar_range_15m: Optional[float] = None
    avg_bar_range_5m: Optional[float] = None

    # Qty adjustment fields
    required_qty_for_min_profit: int = 0
    suggested_order_qty: int = 0
    suggested_order_value: float = 0
    max_affordable_order_qty: int = 0
    qty_adjusted: bool = False
    qty_adjustment_reason: str = ""

    # Capital allocation
    allocated_capital: float = 0
    cash_reserve: float = 0
    max_downside_buy_value: float = 0
    initial_buy_value: float = 0
    initial_buy_qty: int = 0
    initial_buy_used_value: float = 0
    unused_cash: float = 0

    viable: bool = False
    grid_status: str = "NOT_EVALUATED"
    reject_reason: Optional[str] = None


def round_to_tick(price: float, tick: float = NSE_TICK_SIZE) -> float:
    return round(round(price / tick) * tick, 4)


def calculate_grid_gap_from_bar_range(
    avg_bar_range: float,
    gap_multiplier: float = 0.75,
) -> float:
    raw_gap = avg_bar_range * gap_multiplier
    return round_to_tick(raw_gap)


def build_grid_ladder(range_low: float, range_high: float, gap: float) -> List[float]:
    levels = []
    price = round_to_tick(range_low)
    while price <= range_high + gap * 0.01:
        levels.append(round(price, 4))
        price += gap
    return levels


def calculate_round_trip_cost(price: float, qty: int, config: Dict) -> float:
    costs = config.get("costs", {})
    value = price * qty

    brokerage = costs.get("brokerage_per_order", 20) * 2
    stt_buy = value * costs.get("stt_buy_pct", 0.1) / 100
    stt_sell = value * costs.get("stt_sell_pct", 0.025) / 100
    exchange_txn = value * 2 * costs.get("exchange_txn_pct", 0.00345) / 100
    gst_pct = costs.get("gst_pct", 18.0) / 100
    gst = (brokerage + exchange_txn) * gst_pct
    sebi = value * 2 / 10_000_000 * costs.get("sebi_per_crore", 10)
    stamp_buy = value * costs.get("stamp_duty_buy_pct", 0.015) / 100
    slippage = value * 2 * costs.get("slippage_pct", 0.05) / 100

    return brokerage + stt_buy + stt_sell + exchange_txn + gst + sebi + stamp_buy + slippage


def _compute_capital_allocation(
    cmp: float,
    order_qty: int,
    grids_to_bottom: int,
    ladder: List[float],
    capital_per_slot: float,
    reserve_buffer_pct: float,
) -> dict:
    downside_levels = [l for l in ladder if l < cmp - 0.01]
    reserve_levels = downside_levels[-grids_to_bottom:] if grids_to_bottom > 0 else []
    max_downside_buy_value = sum(order_qty * lvl for lvl in reserve_levels) if reserve_levels else 0
    cash_reserve = max_downside_buy_value * (1 + reserve_buffer_pct)

    initial_buy_value = capital_per_slot - cash_reserve
    initial_buy_qty = int(initial_buy_value // cmp) if initial_buy_value > 0 else 0
    initial_buy_used = initial_buy_qty * cmp
    unused_cash = capital_per_slot - cash_reserve - initial_buy_used

    return {
        "allocated_capital": capital_per_slot,
        "cash_reserve": round(cash_reserve, 2),
        "max_downside_buy_value": round(max_downside_buy_value, 2),
        "initial_buy_value": round(initial_buy_value, 2),
        "initial_buy_qty": initial_buy_qty,
        "initial_buy_used_value": round(initial_buy_used, 2),
        "unused_cash": round(unused_cash, 2),
    }


def _derive_order_qty_from_capital(
    cmp: float,
    grids_to_bottom: int,
    ladder: List[float],
    capital_per_slot: float,
    reserve_buffer_pct: float,
) -> int:
    downside_levels = [l for l in ladder if l < cmp - 0.01]
    reserve_levels = downside_levels[-grids_to_bottom:] if grids_to_bottom > 0 else []

    if not reserve_levels:
        return int(capital_per_slot // cmp)

    avg_reserve_price = sum(reserve_levels) / len(reserve_levels)
    total_reserve_cost_per_share = avg_reserve_price * len(reserve_levels) * (1 + reserve_buffer_pct)
    available_per_share = cmp + total_reserve_cost_per_share
    max_qty = int(capital_per_slot / available_per_share)
    return max(max_qty, 1)


def adjust_gap_for_range_coverage(
    bar_range_gap: float,
    range_low: float,
    range_high: float,
    capital_per_slot: float,
    min_order_value: float,
) -> float:
    range_size = range_high - range_low
    if range_size <= 0 or min_order_value <= 0:
        return bar_range_gap

    max_levels = int(capital_per_slot // min_order_value)
    if max_levels <= 0:
        return bar_range_gap

    levels_at_bar_gap = range_size / bar_range_gap if bar_range_gap > 0 else float("inf")
    if levels_at_bar_gap <= max_levels:
        return bar_range_gap

    widened_gap = round_to_tick(range_size / max_levels)
    LOG.info("Gap widened: %.2f → %.2f (range=%.1f, max_levels=%d)",
             bar_range_gap, widened_gap, range_size, max_levels)
    return widened_gap


def derive_qty_for_full_range(
    ladder: List[float],
    capital_per_slot: float,
    reserve_buffer_pct: float,
) -> int:
    if not ladder:
        return 0
    total_level_value = sum(ladder)
    qty = int(capital_per_slot / (total_level_value * (1 + reserve_buffer_pct)))
    return max(qty, 1)


def evaluate_grid(
    range_low: float,
    range_high: float,
    cmp: float,
    capital_per_slot: float,
    config: Dict,
    bar_range_data: Optional[Dict[str, Optional[float]]] = None,
) -> GridInfo:
    gcfg = config.get("grid", {})
    brcfg = config.get("bar_range", {})

    min_net_profit = gcfg.get("min_expected_net_profit_per_cycle", 50)
    reserve_buffer_pct = gcfg.get("reserve_charges_buffer_pct", 0.10)
    almost_viable_pct = gcfg.get("almost_viable_pct", 80)

    primary_tf = brcfg.get("primary_timeframe", "15")
    gap_multiplier = brcfg.get("gap_multiplier", 0.75)

    info = GridInfo()

    if bar_range_data:
        info.avg_bar_range_15m = bar_range_data.get("15")
        info.avg_bar_range_5m = bar_range_data.get("5")

    min_order_value = gcfg.get("min_order_value", 100000)

    primary_bar_range = bar_range_data.get(primary_tf) if bar_range_data else None
    if not primary_bar_range or primary_bar_range <= 0:
        info.reject_reason = f"no {primary_tf}min bar range data"
        info.grid_status = "NO_BAR_RANGE_DATA"
        return info

    bar_range_gap = calculate_grid_gap_from_bar_range(primary_bar_range, gap_multiplier)
    if bar_range_gap <= 0:
        info.reject_reason = "grid gap is zero after rounding"
        info.grid_status = "GRID_NOT_COST_SAFE"
        return info

    gap = adjust_gap_for_range_coverage(
        bar_range_gap, range_low, range_high, capital_per_slot, min_order_value,
    )

    ladder = build_grid_ladder(range_low, range_high, gap)
    grids_to_bottom = math.ceil((cmp - range_low) / gap) if gap > 0 else 0

    info.grid_gap_points = round(gap, 4)
    info.grid_levels = ladder
    info.total_levels = len(ladder)
    info.grids_to_bottom = grids_to_bottom

    order_qty = derive_qty_for_full_range(ladder, capital_per_slot, reserve_buffer_pct)

    gross = order_qty * gap
    rt_cost = calculate_round_trip_cost(cmp, order_qty, config)
    net = gross - rt_cost

    info.order_qty = order_qty
    info.order_value = round(order_qty * cmp, 2)
    info.gross_cycle_profit = round(gross, 2)
    info.round_trip_cost = round(rt_cost, 2)
    info.net_cycle_profit = round(net, 2)

    # Capital allocation
    alloc = _compute_capital_allocation(cmp, order_qty, grids_to_bottom,
                                        ladder, capital_per_slot, reserve_buffer_pct)
    for k, v in alloc.items():
        setattr(info, k, v)

    # Check net profit
    if net < min_net_profit:
        almost_threshold = min_net_profit * (almost_viable_pct / 100)
        if net >= almost_threshold:
            info.grid_status = "GRID_ALMOST_VIABLE"
            info.reject_reason = (
                f"net profit ₹{net:.0f} is {net/min_net_profit*100:.0f}% of min ₹{min_net_profit}"
            )
        else:
            info.grid_status = "GRID_NOT_COST_SAFE"
            info.reject_reason = f"net profit ₹{net:.0f} < min ₹{min_net_profit}"
        return info

    # All checks passed
    info.viable = True
    info.grid_status = "GRID_VALID"
    return info
