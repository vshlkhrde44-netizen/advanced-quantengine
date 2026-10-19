"""Bounded in-memory journal of accepted normalized public market events."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterator

from data.delta.market_events import (
    NormalizedCandle, NormalizedL1Book, NormalizedL2Book, NormalizedTrade,
)


MarketEvent = NormalizedTrade | NormalizedCandle | NormalizedL1Book | NormalizedL2Book


@dataclass(frozen=True, slots=True)
class StoredEvent:
    ordinal: int
    event: MarketEvent


class NormalizedEventStore:
    """Append-only, bounded event history for one process lifetime.

    It stores typed normalized objects, not raw WebSocket payloads.  Durable
    persistence/provenance retention is a later Phase-1 requirement; this
    class establishes the canonical in-memory event-stream contract first.
    """

    def __init__(self, max_events: int = 10_000):
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        self._events: deque[StoredEvent] = deque(maxlen=max_events)
        self._next_ordinal = 0

    def append(self, event: MarketEvent) -> StoredEvent:
        self._next_ordinal += 1
        stored = StoredEvent(self._next_ordinal, event)
        self._events.append(stored)
        return stored

    def all(self) -> tuple[StoredEvent, ...]:
        return tuple(self._events)

    def for_instrument(self, venue: str, instrument: str) -> tuple[StoredEvent, ...]:
        return tuple(
            item for item in self._events
            if item.event.provenance.venue == venue
            and item.event.provenance.instrument == instrument
        )

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[StoredEvent]:
        return iter(self._events)
