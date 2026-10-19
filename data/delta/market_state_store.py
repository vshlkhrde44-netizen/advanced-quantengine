"""Authoritative in-memory state for validated Delta public market events.

This is a data-foundation state store, not strategy state.  It consumes only
events emitted by ``data.delta.market_events`` and makes no trade-direction,
sequence, candle-close, or missing-volume inference.  Those facts remain
explicitly unavailable until an adapter supplies documented evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from data.delta.market_events import (
    NormalizedCandle, NormalizedL1Book, NormalizedL2Book, NormalizedTrade,
)

BookEvent = NormalizedL1Book | NormalizedL2Book


@dataclass(frozen=True, slots=True)
class DataQuality:
    latest_receipt_timestamp: int | None
    stale: bool | None
    sequence_status: str
    gap_status: str
    rejected_events: int


@dataclass(frozen=True, slots=True)
class AuthoritativeMarketState:
    venue: str
    instrument: str
    last_trade: NormalizedTrade | None
    candles: dict[str, NormalizedCandle]
    volume_available: bool | None
    delta: None = None
    cumulative_delta: None = None
    order_book: BookEvent | None = None
    data_quality: DataQuality | None = None


@dataclass
class _MutableState:
    venue: str
    instrument: str
    last_trade: NormalizedTrade | None = None
    candles: dict[str, NormalizedCandle] = field(default_factory=dict)
    order_book: BookEvent | None = None
    gap_status: str = "UNKNOWN"
    latest_receipt_timestamp: int | None = None
    rejected_events: int = 0


class MarketStateStore:
    """One authoritative state record per ``(venue, instrument)``.

    A candle update replaces only the same resolution's snapshot when its
    exchange publish timestamp advances.  Identical/older snapshots are
    rejected deterministically.  Trades have no documented trade ID or
    sequence in the public compact frame, so they are never deduplicated by
    timestamp: doing so could silently drop legitimate same-time trades.
    """

    def __init__(self):
        self._states: dict[tuple[str, str], _MutableState] = {}

    @staticmethod
    def _key(event: NormalizedTrade | NormalizedCandle | BookEvent) -> tuple[str, str]:
        p = event.provenance
        return p.venue, p.instrument

    def _state(self, event: NormalizedTrade | NormalizedCandle | BookEvent) -> _MutableState:
        key = self._key(event)
        if key not in self._states:
            self._states[key] = _MutableState(*key)
        return self._states[key]

    @staticmethod
    def _touch(state: _MutableState, receipt_timestamp: int) -> None:
        previous = state.latest_receipt_timestamp
        state.latest_receipt_timestamp = (
            receipt_timestamp if previous is None else max(previous, receipt_timestamp)
        )

    def apply_trade(self, event: NormalizedTrade) -> bool:
        state = self._state(event)
        state.last_trade = event
        self._touch(state, event.provenance.receipt_timestamp)
        return True

    def apply_candle(self, event: NormalizedCandle) -> bool:
        state = self._state(event)
        previous = state.candles.get(event.resolution)
        if previous is not None:
            same_candle = (
                event.candle_start_timestamp is not None
                and event.candle_start_timestamp == previous.candle_start_timestamp
            )
            if same_candle:
                previous_publish = previous.provenance.provider_publish_timestamp
                current_publish = event.provenance.provider_publish_timestamp
                if current_publish is None or (
                    previous_publish is not None and current_publish <= previous_publish
                ):
                    state.rejected_events += 1
                    return False
            elif event.provenance.exchange_timestamp <= previous.provenance.exchange_timestamp:
                state.rejected_events += 1
                return False
        state.candles[event.resolution] = event
        self._touch(state, event.provenance.receipt_timestamp)
        return True

    def apply_order_book(self, event: BookEvent) -> bool:
        state = self._state(event)
        previous = state.order_book
        if previous is not None and event.provenance.exchange_timestamp <= previous.provenance.exchange_timestamp:
            state.rejected_events += 1
            return False
        state.order_book = event
        self._touch(state, event.provenance.receipt_timestamp)
        return True

    def set_gap_status(self, venue: str, instrument: str, status: str) -> None:
        """Record adapter data quality without synthesizing market facts."""
        state = self._states.get((venue, instrument))
        if state is None:
            raise KeyError("cannot set quality for unknown instrument")
        if status not in ("UNKNOWN", "CONTINUOUS", "DATA_GAP_UNREPAIRED"):
            raise ValueError("invalid gap status")
        state.gap_status = status

    def snapshot(
        self, venue: str, instrument: str, *, now: int | None = None,
        stale_after_seconds: int | None = None,
    ) -> AuthoritativeMarketState | None:
        state = self._states.get((venue, instrument))
        if state is None:
            return None
        stale: bool | None = None
        if stale_after_seconds is not None and state.latest_receipt_timestamp is not None:
            if stale_after_seconds < 0 or now is None:
                raise ValueError("staleness requires now and a non-negative threshold")
            stale = now - state.latest_receipt_timestamp > stale_after_seconds
        volumes = [c.volume for c in state.candles.values()]
        volume_available: bool | None = bool(volumes) and all(v is not None for v in volumes)
        return AuthoritativeMarketState(
            venue=state.venue,
            instrument=state.instrument,
            last_trade=state.last_trade,
            candles=dict(state.candles),
            volume_available=volume_available,
            order_book=state.order_book,
            data_quality=DataQuality(
                latest_receipt_timestamp=state.latest_receipt_timestamp,
                stale=stale,
                sequence_status="UNAVAILABLE",
                gap_status=state.gap_status,
                rejected_events=state.rejected_events,
            ),
        )
