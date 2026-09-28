"""Thin LTP-fetch wrapper over common.broker.fyers_client.FyersClient for the
live poller. Deliberately LTP-only (get_ltps), not a richer OHLC quote — see
scripts/live_scan_poll.py's own docstring for why the day's running
high/low/open has to be reconstructed from repeated polls instead."""
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
