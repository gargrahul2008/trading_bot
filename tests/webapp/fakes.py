"""A gateway that answers from canned data and counts calls, so the polling
schedule can be tested without a broker."""
from __future__ import annotations

from typing import Any, Dict, List


class FakeGateway:
    def __init__(self) -> None:
        self.calls: Dict[str, int] = {}
        self.fail_with: Dict[str, Exception] = {}
        self.order_rows: List[Dict[str, Any]] = []

    def _record(self, name: str):
        self.calls[name] = self.calls.get(name, 0) + 1
        exc = self.fail_with.get(name)
        if exc is not None:
            raise exc

    def positions(self):
        self._record("positions")
        return [{"symbol": "NSE:RELIANCE-EQ", "net_qty": 70.0, "kind": "positional"}]

    def orders(self):
        self._record("orders")
        return [dict(row) for row in self.order_rows]

    def holdings(self):
        self._record("holdings")
        return [{"symbol": "NSE:RELIANCE-EQ", "qty": 420.0}]

    def funds(self):
        self._record("funds")
        return {"available": 585876.0}

    def trades(self):
        self._record("trades")
        return [{"trade_id": "t1", "symbol": "NSE:RELIANCE-EQ", "qty": 70.0}]


class FixedSession:
    """A session pinned to one phase, so tests do not depend on the wall clock."""

    def __init__(self, intervals: Dict[str, float], phase: str = "live") -> None:
        self._intervals = intervals
        self._phase = phase

    def intervals(self, now=None):
        return self._intervals

    def phase(self, now=None):
        return self._phase


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds
