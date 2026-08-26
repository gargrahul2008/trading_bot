"""The polling loop: what to fetch, how often, and what to drop when quota runs short.

The schedule comes from the session (see session.py) and every call is spent out
of the shared budget (see budget.py). When the budget refuses, the section is
simply not refreshed on this tick — it ages, the dashboard shows it ageing, and
the bots keep their headroom. That ordering is the point: sections are attempted
in priority order, so a squeeze costs us the holdings refresh long before it
costs us the order book.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from webapp.agent.attribution import Attribution
from webapp.agent.budget import Budget, is_rate_limit
from webapp.agent.book import Book
from webapp.agent.session import Session

LOG = logging.getLogger("agent.poller")

# Attempted in this order whenever more than one is due. Positions and orders
# are what you act on; funds and holdings can wait.
PRIORITY = ("positions", "orders", "trades", "funds", "holdings")

# The tradebook is not on a fixed schedule: it is pulled when the order book
# shows a new fill (we need the exact traded price for P&L matching) and swept
# occasionally in case a fill was missed between polls.
TRADES_SWEEP_SECONDS = 60.0

TICK_SECONDS = 0.5


class Poller:
    def __init__(
        self,
        gateway: Any,
        book: Book,
        *,
        budget: Optional[Budget] = None,
        session: Optional[Session] = None,
        attribution: Optional[Attribution] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.gateway = gateway
        self.book = book
        self.budget = budget or Budget()
        self.session = session or Session()
        self.attribution = attribution
        self._clock = clock

        self._due: Dict[str, float] = {name: 0.0 for name in PRIORITY}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._seen_fills: Dict[str, float] = {}
        self.phase: Optional[str] = None
        self.ticks = 0

    # ── fetchers ────────────────────────────────────────────────────────────
    def _fetch(self, name: str) -> None:
        if name == "positions":
            self.book.set("positions", self.gateway.positions())
        elif name == "orders":
            self.book.set("orders", self._orders_with_attribution())
        elif name == "holdings":
            self.book.set("holdings", self.gateway.holdings())
        elif name == "funds":
            self.book.set("funds", self.gateway.funds())
        elif name == "trades":
            self.book.set("trades", self.gateway.trades())
        else:  # pragma: no cover - PRIORITY is the only caller
            raise KeyError(name)

    def _orders_with_attribution(self) -> List[Dict[str, Any]]:
        orders = self.gateway.orders()
        if self.attribution is None:
            return orders
        self.attribution.refresh()
        now = time.time()
        for order in orders:
            placed = order.get("epoch")
            age = (now - placed) if isinstance(placed, (int, float)) and placed else None
            order.update(self.attribution.label(order, age))
        return orders

    def _note_fills(self, orders: List[Dict[str, Any]]) -> bool:
        """True when an order has filled more than the last time we looked.

        Filled quantity is compared rather than status, so a partial fill that
        grows is caught as well as one that completes.
        """
        changed = False
        seen = {}
        for order in orders:
            oid = order.get("order_id")
            if not oid:
                continue
            filled = float(order.get("filled_qty") or 0.0)
            if filled > self._seen_fills.get(oid, 0.0):
                changed = True
            seen[oid] = filled
        # Rebuilt from the current order book rather than accumulated, so the
        # map empties itself when the broker clears the book at the next open
        # instead of growing for as long as the process lives.
        self._seen_fills = seen
        return changed

    # ── loop ────────────────────────────────────────────────────────────────
    def tick(self) -> None:
        now = self._clock()
        intervals = self.session.intervals()
        self.phase = self.session.phase()
        self.ticks += 1
        # Staleness is relative to the cadence we are meant to be running at,
        # which changes with the session.
        self.book.set_tolerances(intervals)

        for name in PRIORITY:
            if now < self._due.get(name, 0.0):
                continue
            # The tradebook has no interval of its own; it is scheduled by a
            # detected fill, and swept at TRADES_SWEEP_SECONDS as a backstop.
            interval = intervals.get(name, TRADES_SWEEP_SECONDS)

            if not self.budget.take():
                # Out of allowance. Leave the section due so it goes first on the
                # next tick rather than waiting a full interval.
                continue

            try:
                self._fetch(name)
                if name == "orders":
                    orders = self.book.section("orders").data or []
                    if self._note_fills(orders):
                        # Pull the tradebook now: the exact traded price is only
                        # there, and it is what the P&L matching needs. Setting
                        # it due makes it the next section this same tick, since
                        # "trades" follows "orders" in PRIORITY.
                        self._due["trades"] = 0.0
            except Exception as exc:  # broker errors must not kill the loop
                message = str(exc)
                self.book.fail(name, message)
                if is_rate_limit(exc):
                    self.budget.penalise()
                    LOG.warning("%s: rate limited on %s — backing off", self.book.user, name)
                else:
                    LOG.warning("%s: %s refresh failed: %s", self.book.user, name, message)

            self._due[name] = self._clock() + interval

    def run(self) -> None:
        LOG.info("%s: poller started", self.book.user)
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # pragma: no cover - belt and braces
                LOG.exception("%s: poller tick failed", self.book.user)
            self._stop.wait(TICK_SECONDS)
        LOG.info("%s: poller stopped", self.book.user)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self.run, name="agent-poller", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None

    def expedite(self, name: str) -> None:
        """Make a section due at once.

        Called after an order action: the book is out of date the moment we
        place, modify or cancel something, and the person who did it should see
        the effect on the next tick rather than a full interval later.
        """
        if name in self._due:
            self._due[name] = 0.0

    def status(self) -> Dict[str, Any]:
        now = self._clock()
        return {
            "phase": self.phase,
            "ticks": self.ticks,
            "intervals": self.session.intervals(),
            "next_due_in_s": {
                name: round(max(due - now, 0.0), 1) for name, due in self._due.items()
            },
            "budget": self.budget.snapshot(),
        }
