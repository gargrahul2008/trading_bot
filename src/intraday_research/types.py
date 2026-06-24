from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional

import pandas as pd


class SignalSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass(frozen=True)
class InstrumentSpec:
    quantity_mode: Literal["units", "lots"] = "units"
    lot_size: int = 1
    instrument_type: Literal["equity", "index"] = "equity"


@dataclass(frozen=True)
class CombinedSignal:
    timestamp: pd.Timestamp
    symbol: str
    final_decision: SignalSide
    final_score: float
    price: float
    stop_price: Optional[float]
    target_price: Optional[float]
    explanation: str
    strategy_name: str

    @property
    def side(self) -> SignalSide:
        return self.final_decision

    @property
    def strength(self) -> float:
        return self.final_score

    @property
    def reason(self) -> str:
        return self.explanation


@dataclass
class Position:
    symbol: str
    side: SignalSide
    quantity: int
    effective_quantity: int
    lot_size: int
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: Optional[float]
    target_price: Optional[float]
    entry_reason: str
    strategy_name: str


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: SignalSide
    quantity: int
    effective_quantity: int
    lot_size: int
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    gross_pnl: float
    fees: float
    slippage: float
    net_pnl: float
    brokerage: float
    stt: float
    exchange_charges: float
    gst: float
    sebi_charges: float
    stamp_duty: float
    entry_slippage: float
    exit_slippage: float
    strategy_name: str
    strategy_reason: str
    exit_reason: str

    @property
    def pnl(self) -> float:
        return self.net_pnl
