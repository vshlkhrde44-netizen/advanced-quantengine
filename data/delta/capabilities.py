"""Truthful feature-capability assessment from authoritative market state."""

from __future__ import annotations

from dataclasses import dataclass

from data.delta.market_state_store import AuthoritativeMarketState


AVAILABLE = "AVAILABLE"
UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class FeatureCapability:
    status: str
    reason: str


@dataclass(frozen=True, slots=True)
class MarketDataCapabilities:
    traded_ohlcv: FeatureCapability
    volume_analysis: FeatureCapability
    l1_spread: FeatureCapability
    l2_order_book: FeatureCapability
    order_book_imbalance: FeatureCapability
    signed_delta: FeatureCapability
    footprint: FeatureCapability


def _yes(reason: str) -> FeatureCapability:
    return FeatureCapability(AVAILABLE, reason)


def _no(reason: str) -> FeatureCapability:
    return FeatureCapability(UNAVAILABLE, reason)


def assess_capabilities(state: AuthoritativeMarketState | None) -> MarketDataCapabilities:
    """Declare evidence availability; never manufacture a feature value."""
    if state is None:
        absent = _no("no authoritative market state")
        return MarketDataCapabilities(absent, absent, absent, absent, absent, absent, absent)

    candles = tuple(state.candles.values())
    traded = [
        candle for candle in candles
        if candle.price_basis == "TRADED_PRICE" and candle.volume is not None
    ]
    ohlcv = _yes("traded-price candle with reported volume") if traded else _no(
        "no traded-price candle with reported volume"
    )
    volume = ohlcv if traded else _no("volume requires traded-price candle volume")
    book = state.order_book
    l1 = _yes("best bid/ask snapshot present") if book is not None else _no("no order-book snapshot")
    l2 = _yes("L2 depth snapshot present") if book is not None and hasattr(book, "bids") else _no(
        "no L2 depth snapshot"
    )
    imbalance = _no(
        "snapshot-only book has no verified sequence/incremental reconstruction"
    )
    delta = _no(
        "public trades have no verified aggressor-side classification"
    )
    footprint = _no(
        "footprint requires classified trade flow and a verified price ladder"
    )
    return MarketDataCapabilities(ohlcv, volume, l1, l2, imbalance, delta, footprint)
