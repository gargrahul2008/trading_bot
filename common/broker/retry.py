from __future__ import annotations
import random, time
from typing import Callable, TypeVar
from common.broker.interfaces import RetryableError

T = TypeVar("T")

def _retry_sleep(attempt: int, base: float, cap: float) -> float:
    return min(cap, base * (2 ** attempt)) * (0.7 + random.random() * 0.6)

def _is_rate_limited(e: Exception) -> bool:
    """True if the error is an HTTP 429 / rate-limit response. Fyers returns e.g.
    {'s':'error','code':429,'message':'Bad request'} on the data API; BrokerError carries
    that dict in .resp."""
    resp = getattr(e, "resp", None)
    if isinstance(resp, dict):
        code = resp.get("code")
        if code == 429 or str(code) == "429":
            return True
        msg = str(resp.get("message") or "").lower()
        return "too many" in msg or "rate limit" in msg
    return False

def with_retries(fn: Callable[[], T], *, max_retries: int, base_sleep: float,
                 max_sleep: float, logger, rate_limit_sleep: float = 15.0) -> T:
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if _is_rate_limited(e):
                # HARD back-off on 429: hammering with fast retries only keeps the rate limit
                # saturated. Sleep long (jittered — also desyncs bots), then abort this call so
                # the caller's next poll cycle retries. Logged distinctly so 429 events are
                # visible/measurable (the SDK doesn't expose Retry-After headers, so we log resp).
                sleep_s = rate_limit_sleep * (0.7 + random.random() * 0.6)
                logger.warning("RATE_LIMIT (429): backing off %.1fs then aborting call. resp=%s",
                               sleep_s, getattr(e, "resp", None))
                time.sleep(sleep_s)
                break
            sleep_s = _retry_sleep(attempt, base_sleep, max_sleep)
            logger.warning("Retryable error (%s). attempt=%s sleep=%.2fs", type(e).__name__, attempt + 1, sleep_s)
            time.sleep(sleep_s)
    raise RetryableError(f"Failed after {max_retries} retries. last_error={last_exc!r}")
