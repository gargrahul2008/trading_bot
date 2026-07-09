from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def make_full_session_frame(
    trade_date: str = "2026-06-01",
    symbol: str = "RELIANCE",
    price_fn: Callable[[int, pd.Timestamp], tuple[float, float, float, float, float]] | None = None,
) -> pd.DataFrame:
    timestamps = pd.date_range(
        start=f"{trade_date} 09:15:00",
        end=f"{trade_date} 15:30:00",
        freq="1min",
    )

    rows: list[list[object]] = []
    for idx, timestamp in enumerate(timestamps):
        if price_fn is None:
            base = 100.0 + (idx * 0.05)
            open_price = base
            high_price = base + 0.5
            low_price = base - 0.5
            close_price = base + 0.1
            volume = 1000.0 + idx
        else:
            open_price, high_price, low_price, close_price, volume = price_fn(idx, timestamp)
        rows.append([timestamp.strftime("%Y-%m-%d %H:%M:%S"), symbol, open_price, high_price, low_price, close_price, volume])

    return pd.DataFrame(
        rows,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
