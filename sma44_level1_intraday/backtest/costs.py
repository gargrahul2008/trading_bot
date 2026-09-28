"""Transaction cost model for the 44-SMA scanner strategy.

All cost effects (brokerage, taxes, slippage) are collapsed into a single
`costs` amount subtracted from the trade's raw (pre-cost) gross P&L, matching
the trade-journal schema in the spec (one aggregate `costs` column). This is
mathematically equivalent to adjusting entry/exit prices by slippage and then
re-deriving P&L, since slippage always works against the trader on both legs.
"""
from __future__ import annotations

from ..config.schema import CostsConfig


def compute_trade_costs(
    *,
    entry_price: float,
    exit_price: float,
    qty: float,
    costs_cfg: CostsConfig,
) -> float:
    entry_turnover = abs(entry_price) * qty
    exit_turnover = abs(exit_price) * qty
    total_turnover = entry_turnover + exit_turnover

    brokerage = 2 * costs_cfg.brokerage_per_order + (costs_cfg.brokerage_pct / 100.0) * total_turnover
    taxes = (costs_cfg.taxes_pct / 100.0) * total_turnover

    avg_price = (abs(entry_price) + abs(exit_price)) / 2.0
    slippage_per_unit = costs_cfg.slippage_points + avg_price * (costs_cfg.slippage_pct / 100.0)
    slippage_cost = slippage_per_unit * qty * 2  # applied on both entry and exit legs

    return brokerage + taxes + slippage_cost
