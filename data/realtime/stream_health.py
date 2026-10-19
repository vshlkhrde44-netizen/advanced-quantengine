"""
====================================================

QuantEngine

Stream Health

Phase 6D -- Real-time multi-symbol subscriptions.

Staleness detection for a single subscription. A feed can
be socket-connected yet silent; STALE is the state that
reports that exact condition. Both forming and closed
candles keep the subscription fresh (a live ticker never
goes quiet), but only closed candles are forwarded.

Deterministic under an injectable clock (tests use a fake
clock; production uses time.time).
====================================================
"""

import time


class StreamHealth:

    def __init__(self, max_stale_seconds=30.0, clock=None):

        self.max_stale_seconds = max_stale_seconds
        self._clock = clock or time.time
        self.last_seen = 0.0

    def touch(self, now=None):

        self.last_seen = now if now is not None else self._clock()

    def stale(self, now=None):

        if self.last_seen <= 0.0:

            return False

        now = now if now is not None else self._clock()

        return (now - self.last_seen) > self.max_stale_seconds