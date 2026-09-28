"""Wires data -> resample -> scanner -> signals -> backtest engine together.

Each stage is independently testable (see resample.py, scanner.py, signals.py,
backtest/engine.py); this module only does the plumbing so a caller (CLI
script, notebook, test) can go from raw 1-minute OHLCV straight to a trade
journal in one call.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .backtest.engine import ScannerBacktester
from .config.schema import StrategyConfig
from .exits import compute_atr, compute_confirmed_swings, resolve_exit_mode
from .resample import is_daily_timeframe, is_weekly_timeframe, resample_ohlc
from .signals import generate_scan_candidates


def prepare_symbol_bars(
    raw_1min: pd.DataFrame, config: StrategyConfig, *, repo_root: Path | None = None,
) -> pd.DataFrame:
    """Build one symbol's bars at the single configured timeframe
    (config.timeframes.timeframe — see scanner.py's module docstring for why
    there is no separate trend/entry timeframe split any more) and attach
    whatever the EXECUTION layer needs (ATR / confirmed swings for the optional
    ATR-chandelier exit). The 44-SMA / slope / touch-state columns the SCANNER
    needs are computed inside scanner.scan_history itself — that is the
    detection logic's own concern, not this plumbing function's.

    For "1D"/"1W", this does NOT resample `raw_1min` — it fetches Fyers'
    NATIVE daily candles instead (see data/loader.load_symbol_daily_data) and,
    for weekly, resamples those. The exchange's official daily close is a
    VWAP of the last 30 minutes of trading, not the last-traded-price a
    1-minute resample would give you; on any given day these can differ by
    up to ~1%, which is large enough to flip a borderline touch/breach
    decision even though it barely moves a 44-bar SMA. `raw_1min` is still
    used to bound the date range for the native fetch (and is the only path
    for intraday timeframes, where Fyers has no matching custom resolution).
    """
    if raw_1min.empty:
        return pd.DataFrame()

    symbol = raw_1min["symbol"].iloc[0]
    market = config.market
    tf = config.timeframes

    if is_daily_timeframe(tf.timeframe) or is_weekly_timeframe(tf.timeframe):
        start = raw_1min["timestamp"].min().date().isoformat()
        end = raw_1min["timestamp"].max().date().isoformat()
        bars = _prepare_native_daily_bars(
            symbol, tf.timeframe, market, repo_root, start=start, end=end, raw_1min=raw_1min,
        )
    else:
        bars = resample_ohlc(
            raw_1min, tf.timeframe,
            continuous_session=market.continuous_session,
            session_start=market.market_start_time, session_end=market.market_end_time,
        )
        bars = bars.rename(columns={"bar_open_time": "timestamp"})
        bars["symbol"] = symbol
        bars["trade_date"] = bars["timestamp"].dt.tz_convert(market.timezone).dt.date

    if bars.empty:
        return bars

    # ATR + most-recent CONFIRMED swing high/low, needed by the ATR Chandelier
    # trailing stop. Both are strictly backward-looking (see exits.py).
    if resolve_exit_mode(config.execution) == "atr_chandelier":
        ex = config.execution
        bars["atr"] = compute_atr(bars, ex.atr_length)
        recent_low, recent_high = compute_confirmed_swings(bars, ex.swing_lookback)
        bars["recent_swing_low"] = recent_low
        bars["recent_swing_high"] = recent_high

    return bars


def _prepare_native_daily_bars(
    symbol: str, timeframe: str, market, repo_root: Path | None,
    *, start: str, end: str, raw_1min: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """`start`/`end` (ISO date strings) bound the native-daily fetch directly
    — the caller does NOT need to have loaded any 1-minute data to call this
    (that's the whole point: a scanner-only workflow on "1D"/"1W" never needs
    1-minute bars at all — see prepare_symbol_bars_for_scan). `raw_1min` is
    only required for crypto, which has no native daily/VWAP-close feed to
    fetch and must fall back to resampling 1-minute data instead.
    """
    from .data.loader import bare_symbol, infer_asset_type, load_symbol_daily_data

    bare = bare_symbol(symbol)
    asset_type = infer_asset_type(bare)
    if asset_type == "crypto":
        if raw_1min is None or raw_1min.empty:
            raise ValueError("crypto daily/weekly bars need 1-minute data (no native daily feed) — pass raw_1min")
        # No exchange-official VWAP daily close to fetch for crypto — fall
        # back to resampling the (already 24/7) 1-minute data, same as before.
        raw_daily = resample_ohlc(
            raw_1min, "1D", continuous_session=True,
            session_start=market.market_start_time, session_end=market.market_end_time,
        )
        daily = raw_daily.rename(columns={"bar_open_time": "timestamp"})
    else:
        daily = load_symbol_daily_data(bare, start, end, asset_type=asset_type, repo_root=repo_root)
        if daily.empty:
            return pd.DataFrame()
    daily = daily[["timestamp", "open", "high", "low", "close", "volume"]]

    if is_weekly_timeframe(timeframe):
        # _resample_weekly only needs timestamp/open/high/low/close/volume —
        # no trade_date required, and it works identically whether the input
        # rows are 1-minute or (as here) already-daily.
        weekly = resample_ohlc(
            daily, "1W",
            continuous_session=market.continuous_session,
            session_start=market.market_start_time, session_end=market.market_end_time,
        )
        bars = weekly.rename(columns={"bar_open_time": "timestamp"})
    else:
        bars = daily.copy()

    bars["symbol"] = symbol
    bars["trade_date"] = bars["timestamp"].dt.tz_convert(market.timezone).dt.date
    return bars.sort_values("timestamp").reset_index(drop=True)


def prepare_symbol_bars_for_scan(
    symbol: str, start: str, end: str, config: StrategyConfig, *,
    asset_type: str | None = None, repo_root: Path | None = None, verbose: bool = False,
) -> pd.DataFrame:
    """Build one symbol's bars for a SCANNER-ONLY workflow (scan_history /
    scan_symbol — no backtest engine, no order execution) straight from plain
    start/end date strings, WITHOUT ever loading 1-minute data when the
    configured timeframe is "1D"/"1W" and the symbol isn't crypto.

    This is the difference between this function and prepare_symbol_bars:
    prepare_symbol_bars takes already-loaded 1-minute bars because the
    caller (e.g. the backtest engine, which also needs 1-minute-timeframe
    ATR/chandelier-exit precision) already has them — but a pure scan never
    needs 1-minute data at all for "1D"/"1W", only the native-daily fetch
    does. Loading it anyway (as every earlier version of the universe-scan
    notebook did) meant a wasted incremental 1-minute catch-up fetch for
    EVERY symbol, EVERY run — the single largest cost in a "we already have
    the data" re-run. For intraday timeframes (or crypto, which has no
    native daily feed to skip to), this still has to load 1-minute data,
    exactly as prepare_symbol_bars would.
    """
    from .data.loader import bare_symbol, infer_asset_type, load_symbol_data

    bare = bare_symbol(symbol) if ":" in symbol else symbol
    resolved_asset_type = asset_type or infer_asset_type(bare)
    tf = config.timeframes.timeframe

    if (is_daily_timeframe(tf) or is_weekly_timeframe(tf)) and resolved_asset_type != "crypto":
        bars = _prepare_native_daily_bars(symbol, tf, config.market, repo_root, start=start, end=end)
    else:
        raw = load_symbol_data(
            bare, start, end, asset_type=resolved_asset_type, repo_root=repo_root, verbose=verbose,
        )
        if raw.empty:
            return pd.DataFrame()
        bars = prepare_symbol_bars(raw, config, repo_root=repo_root)

    if bars.empty:
        return bars
    if resolve_exit_mode(config.execution) == "atr_chandelier":
        ex = config.execution
        bars["atr"] = compute_atr(bars, ex.atr_length)
        recent_low, recent_high = compute_confirmed_swings(bars, ex.swing_lookback)
        bars["recent_swing_low"] = recent_low
        bars["recent_swing_high"] = recent_high
    return bars


def run_backtest(
    symbol_raw_1min: dict[str, pd.DataFrame], config: StrategyConfig, *, repo_root: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Run the full pipeline across one or more symbols and return the combined
    trade journal + rejected-signal log. `symbol_raw_1min` maps symbol -> raw
    1-minute OHLCV frame (timestamp, symbol, open, high, low, close, volume,
    trade_date). `repo_root` is only used for a "1D"/"1W" config timeframe,
    to locate the native-daily Fyers cache (see prepare_symbol_bars) —
    defaults to auto-detecting from cwd, same as MarketDataProvider."""
    bar_frames: list[pd.DataFrame] = []
    candidate_frames: list[pd.DataFrame] = []

    for symbol, raw in symbol_raw_1min.items():
        bars = prepare_symbol_bars(raw, config, repo_root=repo_root)
        if bars.empty:
            continue
        bar_frames.append(bars)
        candidates = generate_scan_candidates(bars, config)
        if not candidates.empty:
            candidate_frames.append(candidates)

    combined_bars = pd.concat(bar_frames, ignore_index=True) if bar_frames else pd.DataFrame()
    combined_candidates = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()

    engine = ScannerBacktester(config)
    result = engine.run(combined_bars, combined_candidates)
    result["candidate_signals"] = combined_candidates
    return result
