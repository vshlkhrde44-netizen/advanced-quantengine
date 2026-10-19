"""Causal finality tracker for candles that carry an explicit start timestamp.

No wall-clock close inference is made.  A forming candle becomes closed only
when a later start timestamp is actually observed for the same venue,
instrument and resolution.  If the feed pauses, the last candle stays
forming, which is conservative for strategy dispatch.
"""

from __future__ import annotations

from dataclasses import replace

from data.delta.market_events import NormalizedCandle


class CandleFinalityTracker:
    def __init__(self):
        self._forming: dict[tuple[str, str, str], NormalizedCandle] = {}
        self.rejected = 0

    @staticmethod
    def _key(candle: NormalizedCandle) -> tuple[str, str, str]:
        provenance = candle.provenance
        return provenance.venue, provenance.instrument, candle.resolution

    def ingest(self, candle: NormalizedCandle) -> NormalizedCandle | None:
        """Return the newly confirmed previous candle, if any.

        A candle without ``cst`` cannot be finalized by this tracker because
        its time identity is unknown.  It is rejected rather than bucketed
        from provider publish time.
        """
        start = candle.candle_start_timestamp
        if start is None:
            self.rejected += 1
            return None
        key = self._key(candle)
        previous = self._forming.get(key)
        if previous is None:
            self._forming[key] = candle
            return None
        previous_start = previous.candle_start_timestamp
        assert previous_start is not None
        if start < previous_start:
            self.rejected += 1
            return None
        if start == previous_start:
            self._forming[key] = candle
            return None
        self._forming[key] = candle
        return replace(previous, closed=True)

    def forming(self, venue: str, instrument: str, resolution: str) -> NormalizedCandle | None:
        return self._forming.get((venue, instrument, resolution))
