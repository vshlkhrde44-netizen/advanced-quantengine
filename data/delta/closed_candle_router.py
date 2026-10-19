"""Closed-candle-only boundary for the new normalized Delta data path.

The router is intentionally a small adapter rather than a runtime switch.
It accepts an ``IngestResult`` from ``DeltaPublicEventStream`` and invokes the
consumer only when that result contains a causally confirmed candle.  Forming,
rejected, duplicate and non-candle events never reach the consumer.
"""

from __future__ import annotations

from collections.abc import Callable

from data.delta.market_events import NormalizedCandle
from data.delta.public_event_stream import IngestResult


class ClosedCandleRouter:
    def __init__(self, consumer: Callable[[NormalizedCandle], None]):
        self.consumer = consumer
        self.delivered = 0
        self.suppressed = 0

    def route(self, result: IngestResult) -> bool:
        candle = result.closed_candle
        if (
            candle is None
            or not candle.closed
            or result.data_gap_status != "CONTINUOUS"
        ):
            self.suppressed += 1
            return False
        self.consumer(candle)
        self.delivered += 1
        return True
