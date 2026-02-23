from __future__ import annotations
import random, time
from typing import Callable, TypeVar
from common.broker.interfaces import RetryableError

T = TypeVar("T")

def _retry_sleep(attempt: int, base: float, cap: float) -> float:
    return min(cap, base * (2 ** attempt)) * (0.7 + random.random() * 0.6)

def with_retries(fn: Callable[[], T], *, max_retries: int, base_sleep: float, max_sleep: float, logger) -> T:
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            sleep_s = _retry_sleep(attempt, base_sleep, max_sleep)
            logger.warning("Retryable error (%s). attempt=%s sleep=%.2fs", type(e).__name__, attempt + 1, sleep_s)
            time.sleep(sleep_s)
    raise RetryableError(f"Failed after {max_retries} retries. last_error={last_exc!r}")
