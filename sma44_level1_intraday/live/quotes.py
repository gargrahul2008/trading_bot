"""Thin quote wrapper over common.broker.fyers_client.FyersClient for the live
poller.

fetch_day_bars is what the poller should use: Fyers' /quotes response already
carries the session's open_price/high_price/low_price next to lp, so the day's
true range comes straight from the broker in the SAME call an LTP fetch would
make -- no extra request, no extra rate-limit cost. Rebuilding high/low from
repeated lp samples instead silently understates the range whenever price
spikes and retraces between two polls, which is exactly how the 2026-09-29
IWP entry was missed (broker high 42.40, lp-reconstructed high 41.40, trigger
42.15 never seen).

fetch_ltps is kept for the lp-only fallback path.
"""
from __future__ import annotations

from common.broker.fyers_client import FyersClient


def fetch_ltps(client: FyersClient, symbols: list[str], *, chunk_size: int = 50) -> dict[str, float]:
    """LTP for every symbol, chunked (Fyers' quotes endpoint caps how many
    symbols one call can carry) — a failed chunk is logged and simply
    missing from the result rather than aborting every other chunk."""
    out: dict[str, float] = {}
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            prices = client.get_ltps(chunk)
        except Exception as e:
            print(f"[quotes] LTP fetch failed for chunk {chunk[:3]}{'...' if len(chunk) > 3 else ''}: {e}")
            continue
        out.update({k: float(v) for k, v in prices.items()})
    return out


def fetch_day_bars(client: FyersClient, symbols: list[str], *, chunk_size: int = 50) -> dict[str, dict]:
    """{symbol: {"open","high","low","close"}} for the CURRENT session, as the
    broker itself reports it — chunked and failure-tolerant exactly like
    fetch_ltps. A symbol missing from the result (failed chunk, or a payload
    without all four fields) is simply absent, so the caller can fall back to
    its lp-derived bar for that symbol alone."""
    out: dict[str, dict] = {}
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            bars = client.get_day_ohlc(chunk)
        except Exception as e:
            print(f"[quotes] day-OHLC fetch failed for chunk {chunk[:3]}{'...' if len(chunk) > 3 else ''}: {e}")
            continue
        out.update({sym: {k: float(v) for k, v in bar.items()} for sym, bar in bars.items()})
    return out
