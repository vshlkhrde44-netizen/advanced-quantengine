"""
====================================================

QuantEngine

Candle Router

Phase 6D -- Real-time multi-symbol subscriptions.

Integrity gate between the live transport and strategy
logic. Guarantees the strategy contract that matters for
BREAKOUT:

  * a CLOSED candle is delivered at most once
  * closed candles are delivered in chronological order
  * FORMING candles are never delivered to strategy logic

Duplicate and out-of-order candles are counted, not
forwarded, so a misbehaving feed degrades observability
instead of silently corrupting setup timing.

The router keeps a bounded recent-timestamp memory that
survives a reconnect: a symbol resubscription that replays
an already-delivered candle is rejected as a duplicate, and
anything older than the last delivered candle is rejected
as out-of-order. No state reset is needed at the reconnect
boundary.
====================================================
"""

from collections import deque


class CandleRouter:

    def __init__(self, symbol, max_dedupe=256):

        self.symbol = symbol

        self._seen = set()
        self._seen_order = deque()
        self._max_dedupe = max_dedupe

        self.last_delivered_ts = None

        self.delivered = 0
        self.duplicates = 0
        self.out_of_order = 0
        self.forming_skipped = 0

    def _remember(self, timestamp):

        self._seen.add(timestamp)
        self._seen_order.append(timestamp)

        while len(self._seen_order) > self._max_dedupe:

            old = self._seen_order.popleft()
            self._seen.discard(old)

    def route(self, candle):
        """Returns the candle iff it is a closed candle that
        may be forwarded to strategy logic. Returns None for
        forming candles, duplicates, and out-of-order candles
        (each counted separately)."""

        if not candle.closed:

            self.forming_skipped += 1
            return None

        timestamp = candle.timestamp

        if timestamp in self._seen:

            self.duplicates += 1
            return None

        if self.last_delivered_ts is not None and timestamp < self.last_delivered_ts:

            self.out_of_order += 1
            return None

        self._remember(timestamp)
        self.last_delivered_ts = timestamp
        self.delivered += 1

        return candle