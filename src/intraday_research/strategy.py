from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

SIGNAL_COLUMNS = [
    "timestamp",
    "symbol",
    "direction",
    "strength",
    "reason",
    "stop_loss",
    "target",
    "strategy_name",
]


class Strategy(ABC):
    """Base interface for signal-producing strategies."""

    name: str

    @abstractmethod
    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return strategy signal rows for the supplied feature-rich data."""
