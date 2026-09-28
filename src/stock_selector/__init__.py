"""Bullish stock-selection engine for the BTST strategy.

Selection and ranking only — execution, capital allocation and portfolio
logic live in a separate module.
"""
from .config import (
    DATA_MODE_PRE_CLOSE_SNAPSHOT,
    DATA_MODE_PREVIOUS_COMPLETED_DAY,
    MARKET_STATE_OFF,
    MARKET_STATE_ON,
    SelectorConfig,
)
from .data_provider import DataProvider, InMemoryDataProvider
from .models import (
    PreCloseSnapshot,
    SelectionResult,
    STATUS_CONTINUE_ONLY,
    STATUS_ENTRY_ELIGIBLE,
    STATUS_HARD_FAIL,
    STATUS_MARKET_BLOCKED,
    STATUS_REJECTED,
    STATUS_WATCHLIST,
    UniverseEntry,
)
from .selector import BullishStockSelector

__all__ = [
    'BullishStockSelector',
    'DataProvider',
    'InMemoryDataProvider',
    'PreCloseSnapshot',
    'SelectionResult',
    'SelectorConfig',
    'UniverseEntry',
    'DATA_MODE_PRE_CLOSE_SNAPSHOT',
    'DATA_MODE_PREVIOUS_COMPLETED_DAY',
    'MARKET_STATE_ON',
    'MARKET_STATE_OFF',
    'STATUS_ENTRY_ELIGIBLE',
    'STATUS_MARKET_BLOCKED',
    'STATUS_CONTINUE_ONLY',
    'STATUS_WATCHLIST',
    'STATUS_REJECTED',
    'STATUS_HARD_FAIL',
]
