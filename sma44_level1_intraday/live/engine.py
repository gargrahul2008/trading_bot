"""Live-poller trigger/exit resolution. Entry is a thin wrapper straight
onto backtest/engine.py's own tested ScannerBacktester._resolve_pending_trigger
— reused, not reimplemented, so a live alert can never disagree with what
the backtest would do on the identical price path. Exit is a hand-mirrored
copy of ScannerBacktester._resolve_exit/_resolve_priority for
exit_mode="fixed" ONLY — the backtest's atr_chandelier/candle_trail modes
carry per-bar trailing STATE that only exists inside a full engine run and
isn't reproduced here; scripts/live_scan_after_market.py warns loudly if the
notebook's config uses either of those, since live exit-tracking would
silently be wrong for them.
"""
from __future__ import annotations

from typing import Optional

from ..backtest.engine import ScannerBacktester


def check_trigger(signal: dict, running_bar: dict) -> tuple[bool, bool]:
    """(triggered, invalidated) against the running day's reconstructed
    open/high/low/close bar — see ScannerBacktester._resolve_pending_trigger
    for the full rule (same-bar trigger-vs-invalidation ambiguity, etc.)."""
    return ScannerBacktester._resolve_pending_trigger(signal, running_bar)


def check_fixed_exit(
    direction: str, stop_loss: float, target: float, running_bar: dict, same_candle_priority: str,
) -> Optional[tuple[float, str]]:
    """Mirrors backtest/engine.py's _resolve_exit(mode="fixed") /
    _resolve_priority for one OPEN position. Returns (exit_price,
    exit_reason) or None if neither SL nor target has been touched yet by
    the running bar's high/low so far."""
    long = direction == "LONG"
    hit_sl = (running_bar["low"] <= stop_loss) if long else (running_bar["high"] >= stop_loss)
    hit_tgt = (running_bar["high"] >= target) if long else (running_bar["low"] <= target)
    if not hit_sl and not hit_tgt:
        return None
    if hit_sl and not hit_tgt:
        return stop_loss, "sl"
    if hit_tgt and not hit_sl:
        return target, "target"
    if same_candle_priority == "sl_first":
        return stop_loss, "sl"
    if same_candle_priority == "target_first":
        return target, "target"
    # use_ohlc_path: same bullish/bearish-bar-shape assumption the backtest
    # engine itself uses when it can't see the true intrabar path.
    bullish_bar = running_bar["close"] >= running_bar["open"]
    if long:
        return (stop_loss, "sl") if bullish_bar else (target, "target")
    return (target, "target") if bullish_bar else (stop_loss, "sl")


def compute_qty(config, signal: dict, current_equity: float) -> float:
    """Thin wrapper onto ScannerBacktester._compute_qty (same sizing rule
    the backtest itself uses). NOTE: `current_equity` here is a static
    number the caller supplies (the live poller passes
    config.risk.initial_capital) — it does NOT track real running P&L across
    live trades the way the backtest's own equity curve does. That's exactly
    right for "fixed_qty" and "capital_based with capital_per_trade_amount
    set" (the notebook's current mode — capital_per_trade_amount ignores
    current_equity entirely), but WRONG for percentage-of-equity sizing
    (capital_per_trade_pct / risk_per_trade_pct without an *_amount override)
    — see live_scan_after_market.py's own warning for that case."""
    return ScannerBacktester(config)._compute_qty(signal, current_equity)
