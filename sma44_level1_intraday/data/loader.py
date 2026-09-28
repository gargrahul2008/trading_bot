from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from intraday_research.provider import MarketDataProvider

AssetType = Literal["equity", "index", "crypto"]


def bare_symbol(fyers_symbol: str) -> str:
    """Strip a Fyers-qualified symbol's exchange prefix/suffix, preserving any
    interior hyphens in the ticker itself — e.g. 'NSE:BAJAJ-AUTO-EQ' ->
    'BAJAJ-AUTO', 'NSE:NIFTY50-INDEX' -> 'NIFTY50'.

    A naive `symbol.split('-')[0]` (as opposed to this) silently truncates
    any ticker that itself contains a hyphen — 'NSE:BAJAJ-AUTO-EQ' would
    become just 'BAJAJ', which load_symbol_data then (wrongly) re-qualifies
    as 'NSE:BAJAJ-EQ', a symbol that doesn't exist. Already-bare symbols
    (no ':') are returned unchanged.
    """
    s = fyers_symbol.split(":", 1)[1] if ":" in fyers_symbol else fyers_symbol
    for suffix in ("-EQ", "-INDEX"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s


def infer_asset_type(symbol: str) -> AssetType:
    """Best-effort asset-type guess from a bare symbol name. Callers with more
    context (e.g. a CLI --crypto/--index flag) should pass asset_type explicitly
    instead of relying on this."""
    upper = symbol.upper()
    if upper.endswith("USDT"):
        return "crypto"
    if upper in {"NIFTY50", "BANKNIFTY", "FINNIFTY"}:
        return "index"
    return "equity"


def load_symbol_data(
    symbol: str,
    start: str,
    end: str,
    *,
    asset_type: AssetType | None = None,
    provider: MarketDataProvider | None = None,
    repo_root: Path | None = None,
    verbose: bool = False,
    tolerate_partial_sessions: bool = True,
) -> pd.DataFrame:
    """Load a prepared 1-minute OHLCV frame for one symbol via the existing
    MarketDataProvider (auto-fetches from Fyers/Binance if the local cache under
    data/fyers or data/binance doesn't cover the requested range).

    Returns columns: timestamp (Asia/Kolkata tz-aware), symbol, open, high, low,
    close, volume, trade_date.

    tolerate_partial_sessions (default True): don't hard-fail on days that
    legitimately lack the full 09:15–15:30 candle set — e.g. Diwali Muhurat and
    other special short sessions. Set False to restore the strict completeness
    check (raises on any day with missing 1-minute candles).
    """
    resolved_asset_type = asset_type or infer_asset_type(symbol)
    provider = provider or MarketDataProvider(repo_root=repo_root, verbose=verbose)
    return provider.load(
        symbol, resolved_asset_type, start, end,
        validate_missing_candles=not tolerate_partial_sessions,
    )


def load_symbol_daily_data(
    symbol: str,
    start: str,
    end: str,
    *,
    asset_type: AssetType | None = None,
    provider: MarketDataProvider | None = None,
    repo_root: Path | None = None,
    verbose: bool = False,
) -> pd.DataFrame:
    """Load Fyers' NATIVE daily candles for one symbol — the exchange's
    official daily close (a VWAP of the last 30 minutes of trading), not the
    last-traded-price a 1-minute-bar resample would give you. Cached
    separately from 1-minute data (data/fyers_daily/, vs data/fyers/).

    Returns the same schema as load_symbol_data (timestamp, symbol, open,
    high, low, close, volume, trade_date), one row per trading day. Not
    available for crypto (24/7, no exchange VWAP-close convention to match —
    use load_symbol_data's 1-minute data resampled to daily instead).
    """
    resolved_asset_type = asset_type or infer_asset_type(symbol)
    if resolved_asset_type == "crypto":
        raise ValueError("load_symbol_daily_data is Fyers-only; crypto has no native daily/VWAP-close feed")
    provider = provider or MarketDataProvider(repo_root=repo_root, verbose=verbose)
    return provider.load(symbol, resolved_asset_type, start, end, resolution="D")


def load_multi_symbol_data(
    symbols: list[str],
    start: str,
    end: str,
    *,
    asset_type: AssetType | None = None,
    provider: MarketDataProvider | None = None,
    repo_root: Path | None = None,
    verbose: bool = False,
    tolerate_partial_sessions: bool = True,
) -> pd.DataFrame:
    """Load and concatenate 1-minute OHLCV data for multiple symbols."""
    provider = provider or MarketDataProvider(repo_root=repo_root, verbose=verbose)
    frames = [
        load_symbol_data(
            symbol, start, end,
            asset_type=asset_type, provider=provider, verbose=verbose,
            tolerate_partial_sessions=tolerate_partial_sessions,
        )
        for symbol in symbols
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_date"])
    return pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
