"""Fail-closed bridge from normalized public data to the legacy candle type.

This module is intentionally not wired into ``DeltaSubscriptionStream`` yet.
It makes the runtime boundary explicit: only a causally confirmed,
continuous, traded-price candle with an exchange-reported volume can be
represented as the legacy ``core.history.candle.Candle``.  Mark prices and
unknown volume are market-state facts, not strategy candles.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.history.candle import Candle
from data.delta.market_events import NormalizedCandle
from data.delta.public_event_stream import IngestResult


@dataclass(frozen=True, slots=True)
class RuntimeCandleDecision:
    candle: Candle | None
    reason: str


def to_runtime_candle(result: IngestResult, *, expected_resolution: str) -> RuntimeCandleDecision:
    """Convert one finality result without filling in unavailable facts.

    The caller must retain the decision reason as observability.  A rejected
    conversion is deliberately not a synthetic zero-volume candle.
    """
    candle = result.closed_candle
    if result.status != "APPLIED" or candle is None or not candle.closed:
        return RuntimeCandleDecision(None, "candle is not causally closed")
    if result.data_gap_status != "CONTINUOUS":
        return RuntimeCandleDecision(None, "candle continuity is not verified")
    if candle.resolution != expected_resolution:
        return RuntimeCandleDecision(None, "candle resolution does not match subscription")
    if candle.price_basis != "TRADED_PRICE":
        return RuntimeCandleDecision(None, "mark-price candle cannot enter strategy runtime")
    if candle.volume is None:
        return RuntimeCandleDecision(None, "traded candle volume is unavailable")
    timestamp = candle.candle_start_timestamp
    if timestamp is None:
        return RuntimeCandleDecision(None, "candle start timestamp is unavailable")
    return RuntimeCandleDecision(
        Candle(
            timestamp=timestamp,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            closed=True,
            symbol=candle.provenance.instrument,
        ),
        "accepted",
    )
