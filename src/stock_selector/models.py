"""Typed inputs and outputs for the bullish stock selector.

Optional (``None``) boolean flags on :class:`UniverseEntry` mean "data not
available" — the configured fail-open / fail-closed policy then decides how
the flag is treated. ``False`` / ``True`` are authoritative values.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pandas as pd


@dataclass
class UniverseEntry:
    """Point-in-time description of one tradable security."""
    symbol: str
    exchange: str = 'NSE'
    series: str = 'EQ'
    sector: str | None = None
    industry: str | None = None
    universe_name: str = ''
    listing_date: date | None = None
    tradable: bool | None = True
    btst_eligible: bool | None = None
    restricted_security: bool | None = None
    corporate_action_risk: bool | None = None
    # scheduled results / major event between the cutoff and the next-morning exit window
    event_risk: bool | None = None
    # the next session is an ex-date that mechanically alters the price
    ex_date_next_session: bool | None = None


@dataclass
class PreCloseSnapshot:
    """Intraday state of one symbol at the selection cutoff (never after it)."""
    timestamp: datetime
    session_open: float
    high_to_cutoff: float
    low_to_cutoff: float
    last_price: float
    cumulative_volume: float
    vwap: float | None = None
    bid_price: float | None = None
    ask_price: float | None = None
    upper_band: float | None = None
    lower_band: float | None = None
    circuit_locked: bool | None = None


@dataclass
class SelectionResult:
    """Everything :meth:`BullishStockSelector.run` produces for one timestamp."""
    full_snapshot_dataframe: pd.DataFrame
    entry_candidates_dataframe: pd.DataFrame
    continuation_candidates_dataframe: pd.DataFrame
    rejected_dataframe: pd.DataFrame
    diagnostic_summary: dict[str, Any]
    report_paths: dict[str, str] = field(default_factory=dict)


# status values, in assignment priority order
STATUS_HARD_FAIL = 'HARD_FAIL'
STATUS_ENTRY_ELIGIBLE = 'ENTRY_ELIGIBLE'
STATUS_MARKET_BLOCKED = 'MARKET_BLOCKED'
STATUS_CONTINUE_ONLY = 'CONTINUE_ONLY'
STATUS_WATCHLIST = 'WATCHLIST'
STATUS_REJECTED = 'REJECTED'
