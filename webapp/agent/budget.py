"""Rate budget for the agent's Fyers calls.

Fyers publishes its limits per *app*, and on pratibha's app four bot runs are
already spending ~96 requests a minute. The agent shares that quota, so it is
capped well below the app limit and — critically — it is the side that yields:
a bot missing a poll can miss a fill, whereas the agent missing one shows a
figure a few seconds old. See generic_runner.py:2431 for the -429 we already hit
in production.

Everything here is monotonic-clock based and thread-safe; the poller calls
`take()` from its loop and `penalise()` from the error path.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

# What the agent may spend, leaving the rest of the app's per-minute quota to
# the bots. The steady-state schedule in poller.py costs about 45/min, so this
# leaves room for on-demand tradebook pulls and for order actions.
DEFAULT_PER_MIN = 60.0

# After a -429 the agent stops calling entirely for this long, then runs at a
# reduced rate for a while. Both are deliberately generous: the recovery path
# must not be what pushes the app back over the limit.
COOLDOWN_SECONDS = 30.0
THROTTLE_SECONDS = 300.0
THROTTLE_FACTOR = 0.5


class Budget:
    """A leaky-bucket allowance, in requests per minute.

    `take()` never blocks. A caller that is refused simply skips this tick and
    tries again on the next one — the poller is a loop, so a refusal costs
    freshness rather than correctness.
    """

    def __init__(
        self,
        per_min: float = DEFAULT_PER_MIN,
        burst: int = 5,
        clock: Any = time.monotonic,
    ) -> None:
        if per_min <= 0:
            raise ValueError("per_min must be positive")
        self._per_min = float(per_min)
        self._burst = float(max(burst, 1))
        self._clock = clock
        self._tokens = self._burst
        self._updated = clock()
        self._lock = threading.Lock()

        self._cooldown_until = 0.0
        self._throttle_until = 0.0

        # Observability for /health — operators need to see whether the agent is
        # being throttled before they wonder why the screen is stale.
        self.taken = 0
        self.refused = 0
        self.rate_limited = 0
        self.last_429_at: Optional[float] = None

    # ── internals ───────────────────────────────────────────────────────────
    def _rate_per_sec(self, now: float) -> float:
        per_min = self._per_min
        if now < self._throttle_until:
            per_min *= THROTTLE_FACTOR
        return per_min / 60.0

    def _refill(self, now: float) -> None:
        # Allowance does not accrue while cooling down. Without this the bucket
        # would refill to a full burst during the pause, and the first thing we
        # did after a -429 would be to fire five requests at the broker that
        # just told us to stop.
        since = max(self._updated, self._cooldown_until)
        elapsed = max(now - since, 0.0)
        self._updated = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate_per_sec(now))

    # ── api ─────────────────────────────────────────────────────────────────
    def take(self, cost: float = 1.0) -> bool:
        """Consume `cost` requests' worth of allowance. False means "not now"."""
        with self._lock:
            now = self._clock()
            if now < self._cooldown_until:
                self.refused += 1
                return False
            self._refill(now)
            if self._tokens < cost:
                self.refused += 1
                return False
            self._tokens -= cost
            self.taken += 1
            return True

    def penalise(self) -> None:
        """Called when the broker answers -429. Stop, then run at half rate."""
        with self._lock:
            now = self._clock()
            self._cooldown_until = now + COOLDOWN_SECONDS
            self._throttle_until = now + THROTTLE_SECONDS
            self._tokens = 0.0
            self._updated = now
            self.rate_limited += 1
            self.last_429_at = now

    def cooling_down(self) -> bool:
        with self._lock:
            return self._clock() < self._cooldown_until

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = self._clock()
            self._refill(now)
            return {
                "per_min": self._per_min,
                "effective_per_min": self._rate_per_sec(now) * 60.0,
                "tokens": round(self._tokens, 2),
                "taken": self.taken,
                "refused": self.refused,
                "rate_limited": self.rate_limited,
                "cooling_down": now < self._cooldown_until,
                "cooldown_remaining_s": max(round(self._cooldown_until - now, 1), 0.0),
                "throttled": now < self._throttle_until,
            }


def is_rate_limit(exc: BaseException) -> bool:
    """Fyers signals a rate limit as code -429 in the response envelope, which
    our client re-raises as a BrokerError carrying that response. The text
    check is the fallback for shapes we have not seen."""
    resp = getattr(exc, "resp", None)
    if isinstance(resp, dict):
        code = resp.get("code") or resp.get("s_code")
        try:
            if int(code) in (-429, 429):
                return True
        except (TypeError, ValueError):
            pass
    text = str(exc).lower()
    return "-429" in text or "too many request" in text or "rate limit" in text
