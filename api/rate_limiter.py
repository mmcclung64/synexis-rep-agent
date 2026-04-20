"""In-memory sliding-window rate limiter, keyed by partner key.

Alpha/Beta single-instance deployment only. When this moves to a
multi-instance or multi-worker setup, swap the dict for Redis ZRANGEBYSCORE.
"""
from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict

from fastapi import HTTPException, status


RATE_LIMIT_PER_HOUR = int(os.getenv("RATE_LIMIT_PER_HOUR", "100"))
WINDOW_SECONDS = 3600


class SlidingWindowLimiter:
    def __init__(self, limit: int = RATE_LIMIT_PER_HOUR, window_sec: int = WINDOW_SECONDS) -> None:
        self.limit = limit
        self.window = window_sec
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str) -> None:
        """Record a request and raise 429 if the key has exceeded its limit."""
        now = time.time()
        with self._lock:
            bucket = self._buckets[key]
            cutoff = now - self.window
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= self.limit:
                retry_after = int(bucket[0] + self.window - now) + 1
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit ({self.limit}/hour) exceeded.",
                    headers={"Retry-After": str(max(1, retry_after))},
                )
            bucket.append(now)


_default_limiter = SlidingWindowLimiter()


def check_rate_limit(partner_key: str) -> None:
    _default_limiter.check(partner_key)
