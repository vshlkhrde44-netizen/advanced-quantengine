"""Raw Delta public-frame ingress for the Phase-1 normalized event store.

This is deliberately a pure, caller-driven dispatcher: network transport
owns receipt timing and reconnects; this layer owns schema dispatch,
normalization and state mutation.  It does not route any event to strategy or
execution code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from data.delta.market_events import (
    InvalidMarketFrame,
    NormalizedCandle,
    normalize_candlestick_frame,
    normalize_l1_book_frame,
    normalize_l2_book_frame,
    normalize_trade_frame,
)
from data.delta.candle_finality import CandleFinalityTracker
from data.delta.candle_continuity import CandleContinuityTracker
from data.delta.event_store import NormalizedEventStore
from data.delta.market_state_store import MarketStateStore


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: Literal["APPLIED", "DUPLICATE_OR_OLD", "REJECTED", "UNHANDLED"]
    event_type: str
    reason: str = ""
    closed_candle: NormalizedCandle | None = None
    data_gap_status: str = "UNKNOWN"


class DeltaPublicEventStream:
    """Normalize documented Delta public frames into one state store."""

    def __init__(self, store: MarketStateStore | None = None,
                 event_store: NormalizedEventStore | None = None,
                 *, venue: str = "DELTA"):
        self.store = store if store is not None else MarketStateStore()
        self.event_store = event_store if event_store is not None else NormalizedEventStore()
        self.venue = venue
        self.finality = CandleFinalityTracker()
        self.continuity = CandleContinuityTracker()
        self._stats = {"applied": 0, "duplicates_or_old": 0, "rejected": 0, "unhandled": 0}

    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def ingest(self, frame: Any, *, receipt_timestamp: Any) -> IngestResult:
        if not isinstance(frame, dict):
            return self._result("REJECTED", "UNKNOWN", "frame is not a mapping")
        channel = frame.get("type")
        try:
            if channel == "trades":
                event = normalize_trade_frame(frame, receipt_timestamp=receipt_timestamp, venue=self.venue)
                applied = self.store.apply_trade(event)
            elif isinstance(channel, str) and channel.startswith("candlestick_"):
                event = normalize_candlestick_frame(frame, receipt_timestamp=receipt_timestamp, venue=self.venue)
                applied = self.store.apply_candle(event)
                if applied:
                    self.continuity.ingest(event)
                    closed_candle = self.finality.ingest(event)
                    data_gap_status = self.continuity.status(
                        event.provenance.venue, event.provenance.instrument,
                        event.resolution,
                    )["status"]
                    self.store.set_gap_status(
                        event.provenance.venue,
                        event.provenance.instrument,
                        data_gap_status,
                    )
                else:
                    closed_candle = None
                    data_gap_status = "UNKNOWN"
            elif channel == "ob_l1":
                event = normalize_l1_book_frame(frame, receipt_timestamp=receipt_timestamp, venue=self.venue)
                applied = self.store.apply_order_book(event)
            elif channel == "ob_l2":
                event = normalize_l2_book_frame(frame, receipt_timestamp=receipt_timestamp, venue=self.venue)
                applied = self.store.apply_order_book(event)
            else:
                return self._result("UNHANDLED", str(channel or "UNKNOWN"), "unsupported public channel")
        except InvalidMarketFrame as error:
            return self._result("REJECTED", str(channel or "UNKNOWN"), str(error))
        if applied:
            self.event_store.append(event)
        return self._result(
            "APPLIED" if applied else "DUPLICATE_OR_OLD", str(channel),
            closed_candle=closed_candle if channel and str(channel).startswith("candlestick_") else None,
            data_gap_status=data_gap_status if channel and str(channel).startswith("candlestick_") else "UNKNOWN",
        )

    def repair_historical_candles(
        self, symbol: str, resolution: str, candles, *,
        receipt_timestamp: int, venue: str | None = None,
    ) -> dict:
        """Record validated historical gap evidence and clear exact gaps.

        Repaired candles are appended to the normalized event journal but are
        deliberately not routed through causal finality or strategy dispatch: a
        backfill restores continuity; it does not create retroactive live signals.
        """
        from data.delta.market_events import EventProvenance, NormalizedCandle

        venue_name = venue or self.venue
        window = __import__("data.candle_builder", fromlist=["TIMEFRAME_SECONDS"]).TIMEFRAME_SECONDS.get(resolution)
        if window is None or window <= 0:
            return {"accepted": 0, "rejected": 0, "ranges_repaired": 0,
                    "reason": "unsupported resolution"}
        gap_status = self.continuity.status(venue_name, symbol, resolution)
        allowed_starts = set()
        for gap in gap_status["gaps"]:
            allowed_starts.update(
                range(gap.after_start + window, gap.before_start, window)
            )
        normalized = []
        rejected = 0
        for candle in list(candles or []):
            try:
                stamp = int(candle.timestamp)
                if stamp <= 0 or stamp % window != 0 or stamp not in allowed_starts:
                    raise ValueError("candle is outside an outstanding gap")
                values = tuple(float(getattr(candle, name)) for name in ("open", "high", "low", "close", "volume"))
                if any(value <= 0 for value in values):
                    raise ValueError("candle values must be positive")
                open_, high, low, close, volume = values
                if high < max(open_, close) or low > min(open_, close) or high < low:
                    raise ValueError("invalid candle geometry")
                normalized.append(NormalizedCandle(
                    provenance=EventProvenance(
                        venue=venue_name, instrument=symbol, event_type="CANDLE",
                        exchange_timestamp=stamp, receipt_timestamp=int(receipt_timestamp),
                        source_channel="history_rest", sequence=None,
                        provider_publish_timestamp=None,
                    ),
                    resolution=resolution, open=open_, high=high, low=low,
                    close=close, volume=volume, price_basis="TRADED_PRICE",
                    candle_start_timestamp=stamp,
                ))
            except (AttributeError, TypeError, ValueError, OverflowError):
                rejected += 1
        normalized.sort(key=lambda item: item.candle_start_timestamp or 0)
        unique = []
        seen = set()
        for event in normalized:
            stamp = event.candle_start_timestamp
            if stamp in seen:
                rejected += 1
                continue
            seen.add(stamp)
            unique.append(event)
            self.event_store.append(event)
        repair = self.continuity.repair(
            venue_name, symbol, resolution,
            [event.candle_start_timestamp for event in unique if event.candle_start_timestamp is not None],
        )
        try:
            self.store.set_gap_status(
                venue_name, symbol,
                self.continuity.status(venue_name, symbol, resolution)["status"],
            )
        except KeyError:
            pass
        return {
            "accepted": len(unique), "rejected": rejected,
            "ranges_repaired": repair["ranges_repaired"],
            "reason": repair["reason"],
            "candles": tuple(unique),
        }

    def _result(self, status: IngestResult.status, event_type: str, reason: str = "",
                closed_candle: NormalizedCandle | None = None,
                data_gap_status: str = "UNKNOWN") -> IngestResult:
        key = {
            "APPLIED": "applied", "DUPLICATE_OR_OLD": "duplicates_or_old",
            "REJECTED": "rejected", "UNHANDLED": "unhandled",
        }[status]
        self._stats[key] += 1
        return IngestResult(status=status, event_type=event_type, reason=reason,
                            closed_candle=closed_candle, data_gap_status=data_gap_status)
