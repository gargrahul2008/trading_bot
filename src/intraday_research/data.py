from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Final

import pandas as pd

from .session import SESSION_END, SESSION_START, SESSION_TIMEZONE

REQUIRED_COLUMNS: Final[set[str]] = {
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


class MarketDataLoader:
    """Load and validate 1-minute intraday market data."""

    def load(self, path: str | Path) -> pd.DataFrame:
        source = Path(path)
        if source.suffix.lower() == ".csv":
            frame = pd.read_csv(source)
        elif source.suffix.lower() in {".parquet", ".pq"}:
            frame = pd.read_parquet(source)
        else:
            raise ValueError(f"Unsupported file format: {source.suffix}")
        return self.prepare(frame)

    def prepare(self, frame: pd.DataFrame, filter_session: bool = True) -> pd.DataFrame:
        missing_columns = REQUIRED_COLUMNS.difference(frame.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

        prepared = frame.copy()
        prepared["timestamp"] = self._parse_timestamps(prepared["timestamp"])

        numeric_columns = ["open", "high", "low", "close", "volume"]
        prepared[numeric_columns] = prepared[numeric_columns].apply(pd.to_numeric, errors="raise")

        prepared = prepared.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        prepared["trade_date"] = prepared["timestamp"].dt.date

        if filter_session:
            session_mask = prepared["timestamp"].dt.time.between(SESSION_START, SESSION_END)
            prepared = prepared.loc[session_mask].reset_index(drop=True)

        self._validate_duplicates(prepared)
        if filter_session:
            self._validate_missing_candles(prepared)
        self._validate_ohlcv(prepared)
        return prepared

    @staticmethod
    def _parse_timestamps(series: pd.Series) -> pd.Series:
        try:
            parsed = pd.to_datetime(series, errors="raise")
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid timestamp values in input data.") from exc

        if parsed.isna().any():
            raise ValueError("Timestamp column contains null values.")

        if parsed.dt.tz is None:
            return parsed.dt.tz_localize(SESSION_TIMEZONE)
        return parsed.dt.tz_convert(SESSION_TIMEZONE)

    @staticmethod
    def _validate_duplicates(frame: pd.DataFrame) -> None:
        duplicate_mask = frame.duplicated(subset=["symbol", "timestamp"], keep=False)
        if duplicate_mask.any():
            duplicates = frame.loc[duplicate_mask, ["symbol", "timestamp"]].head(5).to_dict("records")
            raise ValueError(f"Duplicate candles detected: {duplicates}")

    @staticmethod
    def _validate_missing_candles(frame: pd.DataFrame) -> None:
        missing_examples: list[dict[str, object]] = []
        for (symbol, trade_date), group in frame.groupby(["symbol", "trade_date"], sort=False):
            session_start = pd.Timestamp(
                datetime.combine(trade_date, SESSION_START),
                tz=SESSION_TIMEZONE,
            )
            max_timestamp = pd.Timestamp(group["timestamp"].max())
            max_time = max_timestamp.time()
            if max_time == SESSION_END:
                expected_end_time = SESSION_END
            elif max_time == (datetime.combine(trade_date, SESSION_END) - timedelta(minutes=1)).time():
                # Many 1-minute vendors timestamp the last regular-session bar at 15:29,
                # representing the 15:29:00-15:29:59 interval before the 15:30 close.
                expected_end_time = max_time
            else:
                expected_end_time = max_time

            session_end = pd.Timestamp(
                datetime.combine(trade_date, expected_end_time),
                tz=SESSION_TIMEZONE,
            )
            expected = pd.date_range(start=session_start, end=session_end, freq="1min")
            actual = pd.DatetimeIndex(group["timestamp"])
            missing = expected.difference(actual)
            if not missing.empty:
                missing_examples.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "missing_count": len(missing),
                        "first_missing": missing[0].isoformat(),
                    }
                )

        if missing_examples:
            raise ValueError(f"Missing 1-minute candles detected: {missing_examples[:5]}")

    @staticmethod
    def _validate_ohlcv(frame: pd.DataFrame) -> None:
        invalid_price_mask = (
            frame[["open", "high", "low", "close"]].isna().any(axis=1)
            | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
            | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
            | (frame["low"] > frame["high"])
            | (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
        )
        invalid_volume_mask = frame["volume"].isna() | (frame["volume"] < 0)
        invalid_mask = invalid_price_mask | invalid_volume_mask

        if invalid_mask.any():
            invalid_rows = frame.loc[
                invalid_mask,
                ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
            ].head(5)
            raise ValueError(f"Invalid OHLCV rows detected: {invalid_rows.to_dict('records')}")
