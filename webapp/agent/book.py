"""The agent's in-memory view of one account.

Each section is refreshed on its own cadence and carries the time it was
fetched. Nothing here is ever served without that timestamp: a position shown
without its age is how you act on a number that stopped being true two minutes
ago. `stale` is computed against the section's own tolerance so the UI can grey
a figure out rather than presenting it as current.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional


class Section:
    """One polled resource: its last good payload, when it arrived, and whether
    the most recent attempt failed.

    A failed refresh never discards the last good data — it keeps serving, but
    ageing, with the error attached. Losing the screen entirely because one poll
    timed out is worse than showing a figure that is visibly ten seconds old.
    """

    def __init__(self, name: str, stale_after: float) -> None:
        self.name = name
        self.stale_after = float(stale_after)
        self.data: Any = None
        self.fetched_at: Optional[float] = None
        self.error: Optional[str] = None
        self.error_at: Optional[float] = None
        self.ok_count = 0
        self.fail_count = 0

    def set(self, data: Any, *, now: Optional[float] = None) -> None:
        self.data = data
        self.fetched_at = time.time() if now is None else now
        self.error = None
        self.ok_count += 1

    def fail(self, message: str, *, now: Optional[float] = None) -> None:
        self.error = message
        self.error_at = time.time() if now is None else now
        self.fail_count += 1

    def age_s(self, now: Optional[float] = None) -> Optional[float]:
        if self.fetched_at is None:
            return None
        return max((time.time() if now is None else now) - self.fetched_at, 0.0)

    def is_stale(self, now: Optional[float] = None) -> bool:
        age = self.age_s(now)
        return True if age is None else age > self.stale_after

    def as_dict(self, *, include_data: bool = True) -> Dict[str, Any]:
        age = self.age_s()
        out: Dict[str, Any] = {
            "as_of": self.fetched_at,
            "age_s": None if age is None else round(age, 2),
            "stale": self.is_stale(),
            "stale_after_s": self.stale_after,
            "source": "rest",
            "error": self.error,
            "ok_count": self.ok_count,
            "fail_count": self.fail_count,
        }
        if include_data:
            out["data"] = self.data
        return out


class Book:
    """All sections for one account, behind one lock.

    The poller writes; the HTTP server reads. Both are threads in the same
    process, so a plain lock is enough — and every read returns a snapshot, so a
    response can never show two sections caught mid-update against each other.
    """

    #: name -> starting tolerance. These are replaced at runtime by
    #: `set_tolerances`, because the poll interval changes with the market
    #: session: 3s while it is open, 60s when it is closed, 15 minutes on a
    #: holiday. A fixed tolerance would mark every section stale the moment the
    #: market shut, which reads as "the agent is broken" when it is simply idle.
    STALE_AFTER = {
        "positions": 10.0,
        "orders": 10.0,
        "funds": 90.0,
        "holdings": 180.0,
        "trades": 180.0,
    }

    #: A section is stale after this many missed polls. Two, so one lost poll is
    #: tolerated and a second is not.
    STALE_AFTER_MISSES = 3.0

    #: Never tolerate less than this, however fast the cadence.
    MIN_STALE_AFTER = 10.0

    def __init__(self, user: str) -> None:
        self.user = user
        self._lock = threading.RLock()
        self._sections: Dict[str, Section] = {
            name: Section(name, stale_after) for name, stale_after in self.STALE_AFTER.items()
        }
        self.started_at = time.time()

    def section(self, name: str) -> Section:
        return self._sections[name]

    def set_tolerances(self, intervals: Dict[str, float]) -> None:
        """Scale each section's staleness tolerance to its current poll interval.

        Called by the poller as the session changes, so "stale" always means
        "later than the agent should have refreshed this", not "older than some
        fixed number of seconds".
        """
        with self._lock:
            for name, section in self._sections.items():
                interval = intervals.get(name)
                if interval:
                    section.stale_after = max(
                        interval * self.STALE_AFTER_MISSES, self.MIN_STALE_AFTER
                    )

    def set(self, name: str, data: Any) -> None:
        with self._lock:
            self._sections[name].set(data)

    def fail(self, name: str, message: str) -> None:
        with self._lock:
            self._sections[name].fail(message)

    def get(self, name: str) -> Dict[str, Any]:
        with self._lock:
            return self._sections[name].as_dict()

    def snapshot(self) -> Dict[str, Any]:
        """Every section at once — what the dashboard polls."""
        with self._lock:
            return {
                "user": self.user,
                "started_at": self.started_at,
                "sections": {name: sec.as_dict() for name, sec in self._sections.items()},
            }

    def health(self) -> Dict[str, Any]:
        """Section metadata without payloads, for /health and for deciding
        whether the account is showing live data at all."""
        with self._lock:
            sections = {
                name: sec.as_dict(include_data=False) for name, sec in self._sections.items()
            }
        # "Live" means the two sections you would act on are both fresh. Funds
        # and holdings ageing is normal and must not raise an alarm.
        live = not (sections["positions"]["stale"] or sections["orders"]["stale"])
        return {
            "user": self.user,
            "live": live,
            "uptime_s": round(time.time() - self.started_at, 1),
            "sections": sections,
        }

    def symbols(self) -> List[str]:
        """Symbols currently held or in an open position — the set worth
        watching. Read from the raw broker payloads rather than a derived copy,
        so nothing is missed when a shape changes."""
        with self._lock:
            found = set()
            for name in ("positions", "holdings"):
                rows = self._sections[name].data or []
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, dict):
                        sym = str(row.get("symbol") or "").strip()
                        if sym:
                            found.add(sym)
            return sorted(found)
