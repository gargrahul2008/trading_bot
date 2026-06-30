from __future__ import annotations

from typing import Iterable

import pandas as pd

from .types import Trade


def trades_to_frame(trades: Iterable[Trade]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "symbol": trade.symbol,
                "side": trade.side.value,
                "quantity": trade.quantity,
                "effective_quantity": trade.effective_quantity,
                "lot_size": trade.lot_size,
                "entry_time": trade.entry_time,
                "exit_time": trade.exit_time,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "buy_turnover": (trade.entry_price * trade.effective_quantity)
                if trade.side.value == "LONG"
                else (trade.exit_price * trade.effective_quantity),
                "sell_turnover": (trade.exit_price * trade.effective_quantity)
                if trade.side.value == "LONG"
                else (trade.entry_price * trade.effective_quantity),
                "gross_pnl": trade.gross_pnl,
                "brokerage": trade.brokerage,
                "stt": trade.stt,
                "exchange_charges": trade.exchange_charges,
                "gst": trade.gst,
                "sebi_charges": trade.sebi_charges,
                "stamp_duty": trade.stamp_duty,
                "fees": trade.fees,
                "entry_slippage": trade.entry_slippage,
                "exit_slippage": trade.exit_slippage,
                "slippage": trade.slippage,
                "total_cost": trade.fees + trade.slippage,
                "net_pnl": trade.net_pnl,
                "gross_pnl_per_share": trade.gross_pnl / trade.effective_quantity if trade.effective_quantity else 0.0,
                "net_pnl_per_share": trade.net_pnl / trade.effective_quantity if trade.effective_quantity else 0.0,
                "cost_per_share": (trade.fees + trade.slippage) / trade.effective_quantity if trade.effective_quantity else 0.0,
                "gross_bps": (
                    (trade.gross_pnl / (trade.entry_price * trade.effective_quantity)) * 10_000.0
                    if trade.entry_price and trade.effective_quantity
                    else 0.0
                ),
                "net_bps": (
                    (trade.net_pnl / (trade.entry_price * trade.effective_quantity)) * 10_000.0
                    if trade.entry_price and trade.effective_quantity
                    else 0.0
                ),
                "cost_bps": (
                    (((trade.fees + trade.slippage) / (trade.entry_price * trade.effective_quantity)) * 10_000.0)
                    if trade.entry_price and trade.effective_quantity
                    else 0.0
                ),
                "breakeven_move_required": (
                    ((trade.fees + trade.slippage) / trade.effective_quantity) if trade.effective_quantity else 0.0
                ),
                "strategy_name": trade.strategy_name,
                "strategy_reason": trade.strategy_reason,
                "exit_reason": trade.exit_reason,
                "is_partial": trade.is_partial,
            }
            for trade in trades
        ]
    )
    return frame


def daily_pnl_report(trades: Iterable[Trade]) -> pd.DataFrame:
    trade_frame = trades_to_frame(trades)
    if trade_frame.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "trades",
                "gross_pnl",
                "brokerage",
                "stt",
                "exchange_charges",
                "gst",
                "sebi_charges",
                "stamp_duty",
                "fees",
                "slippage",
                "total_cost",
                "net_pnl",
            ]
        )
    report = (
        trade_frame.assign(trade_date=lambda df: pd.to_datetime(df["exit_time"]).dt.date)
        .groupby("trade_date", as_index=False)
        .agg(
            trades=("symbol", "count"),
            gross_pnl=("gross_pnl", "sum"),
            brokerage=("brokerage", "sum"),
            stt=("stt", "sum"),
            exchange_charges=("exchange_charges", "sum"),
            gst=("gst", "sum"),
            sebi_charges=("sebi_charges", "sum"),
            stamp_duty=("stamp_duty", "sum"),
            fees=("fees", "sum"),
            slippage=("slippage", "sum"),
            total_cost=("total_cost", "sum"),
            net_pnl=("net_pnl", "sum"),
        )
    )
    return report
