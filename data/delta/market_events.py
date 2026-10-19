"""Validated, provenance-preserving Delta public-market event normalizers.

This module is deliberately independent from the legacy ticker-to-candle
transport.  It implements only documented public-frame shapes and preserves
unknown or unavailable facts as ``None``.  In particular, Delta's public
``trades`` frame exposes maker/taker role, not aggressor side, so it must not
be used as signed delta without an independently documented classifier.

Reference (accessed 2026-09-14):
https://docs.delta.exchange/ — public ``trades`` and ``candlestick_*``
channels.  Deployment against the repository's configured Global endpoint
remains EXTERNAL VERIFICATION REQUIRED.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Literal

from core.common.timestamp import normalize_timestamp


class InvalidMarketFrame(ValueError):
    """A frame does not meet the documented normalization contract."""


def _positive(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise InvalidMarketFrame(f"{field} must be numeric") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise InvalidMarketFrame(f"{field} must be finite and positive")
    return parsed


def _timestamp(value: Any, field: str) -> int:
    try:
        return normalize_timestamp(value)
    except (TypeError, ValueError) as error:
        raise InvalidMarketFrame(f"{field} is invalid") from error


def _provider_timestamp(value: Any, field: str) -> int:
    """Retain a positive raw provider timestamp for same-second ordering.

    ``normalize_timestamp`` deliberately reduces µs/ns to seconds for the
    canonical clock.  That resolution is insufficient to order two provider
    publishes for one forming candle, so the original integer is retained in
    addition to canonical exchange time.
    """
    try:
        parsed = int(float(value))
    except (TypeError, ValueError) as error:
        raise InvalidMarketFrame(f"{field} is invalid") from error
    if parsed <= 0:
        raise InvalidMarketFrame(f"{field} is invalid")
    return parsed


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidMarketFrame("symbol is missing")
    return value.strip()


@dataclass(frozen=True, slots=True)
class EventProvenance:
    venue: str
    instrument: str
    event_type: str
    exchange_timestamp: int
    receipt_timestamp: int
    source_channel: str
    sequence: int | None
    provider_publish_timestamp: int | None = None
    normalization_status: Literal["VALID"] = "VALID"
    gap_status: Literal["UNKNOWN"] = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    provenance: EventProvenance
    price: float
    quantity: float
    maker_taker_role: Literal["maker", "taker"] | None
    aggressor_side: None = None


@dataclass(frozen=True, slots=True)
class NormalizedCandle:
    provenance: EventProvenance
    resolution: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    price_basis: Literal["TRADED_PRICE", "MARK_PRICE"]
    candle_start_timestamp: int | None = None
    closed: bool = False


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: float
    quantity: float


@dataclass(frozen=True, slots=True)
class NormalizedL1Book:
    provenance: EventProvenance
    bid_price: float
    bid_quantity: float
    ask_price: float
    ask_quantity: float


@dataclass(frozen=True, slots=True)
class NormalizedL2Book:
    provenance: EventProvenance
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


def normalize_trade_frame(
    frame: dict[str, Any], *, receipt_timestamp: Any, venue: str = "DELTA"
) -> NormalizedTrade:
    """Normalize one documented Delta public ``trades`` WebSocket frame.

    The documented compact fields are ``p`` (price), ``s`` (size), ``r``
    (buyer role), ``sy`` (symbol), ``t`` (trade timestamp), and ``ts``
    (server publish timestamp).  ``r`` is preserved but never converted to
    a buy/sell aggressor classification.
    """
    if not isinstance(frame, dict) or frame.get("type") != "trades":
        raise InvalidMarketFrame("expected a Delta trades frame")
    symbol = _symbol(frame.get("sy"))
    role = frame.get("r")
    maker_taker_role = {"m": "maker", "t": "taker"}.get(role)
    if role is not None and maker_taker_role is None:
        raise InvalidMarketFrame("trade buyer role is invalid")
    provenance = EventProvenance(
        venue=venue,
        instrument=symbol,
        event_type="TRADE",
        exchange_timestamp=_timestamp(frame.get("t"), "trade timestamp"),
        receipt_timestamp=_timestamp(receipt_timestamp, "receipt timestamp"),
        source_channel="trades",
        sequence=None,
        provider_publish_timestamp=_provider_timestamp(frame.get("ts"), "publish timestamp"),
    )
    return NormalizedTrade(
        provenance=provenance,
        price=_positive(frame.get("p"), "trade price"),
        quantity=_positive(frame.get("s"), "trade size"),
        maker_taker_role=maker_taker_role,
    )


def normalize_candlestick_frame(
    frame: dict[str, Any], *, receipt_timestamp: Any, venue: str = "DELTA"
) -> NormalizedCandle:
    """Normalize one documented Delta ``candlestick_<resolution>`` frame.

    A normal symbol represents a traded-price candle.  A ``MARK:`` symbol is
    explicitly retained as a mark-price candle; it is never silently treated
    as traded OHLCV.  Missing volume remains ``None`` (rather than zero).
    The documented sample has no close/final flag, so the event remains
    forming until a venue-specific close contract is independently verified.
    """
    if not isinstance(frame, dict):
        raise InvalidMarketFrame("candlestick frame is not a mapping")
    channel = frame.get("type")
    if not isinstance(channel, str) or not channel.startswith("candlestick_"):
        raise InvalidMarketFrame("expected a Delta candlestick frame")
    symbol = _symbol(frame.get("sy"))
    resolution = frame.get("res")
    if not isinstance(resolution, str) or not resolution:
        raise InvalidMarketFrame("candle resolution is missing")
    open_ = _positive(frame.get("o"), "candle open")
    high = _positive(frame.get("h"), "candle high")
    low = _positive(frame.get("l"), "candle low")
    close = _positive(frame.get("c"), "candle close")
    if high < max(open_, close) or low > min(open_, close) or high < low:
        raise InvalidMarketFrame("candle OHLC geometry is impossible")
    volume: float | None = None
    if "v" in frame and frame["v"] is not None:
        volume = _positive(frame["v"], "candle volume")
    price_basis: Literal["TRADED_PRICE", "MARK_PRICE"] = (
        "MARK_PRICE" if symbol.startswith("MARK:") else "TRADED_PRICE"
    )
    candle_start = None
    if frame.get("cst") is not None:
        candle_start = _timestamp(frame.get("cst"), "candle start timestamp")
    publish_timestamp = _timestamp(frame.get("ts"), "publish timestamp")
    raw_publish_timestamp = _provider_timestamp(frame.get("ts"), "publish timestamp")
    provenance = EventProvenance(
        venue=venue,
        instrument=symbol,
        event_type="CANDLE",
        exchange_timestamp=candle_start if candle_start is not None else publish_timestamp,
        receipt_timestamp=_timestamp(receipt_timestamp, "receipt timestamp"),
        source_channel=channel,
        sequence=None,
        provider_publish_timestamp=raw_publish_timestamp,
    )
    return NormalizedCandle(
        provenance=provenance,
        resolution=resolution,
        open=open_, high=high, low=low, close=close,
        volume=volume,
        price_basis=price_basis,
        candle_start_timestamp=candle_start,
    )


def _book_provenance(frame: dict[str, Any], *, receipt_timestamp: Any,
                     venue: str, channel: str) -> EventProvenance:
    return EventProvenance(
        venue=venue, instrument=_symbol(frame.get("sy")),
        event_type="ORDER_BOOK",
        exchange_timestamp=_timestamp(frame.get("lts", frame.get("ts")), "book timestamp"),
        receipt_timestamp=_timestamp(receipt_timestamp, "receipt timestamp"),
        source_channel=channel, sequence=None,
        provider_publish_timestamp=_provider_timestamp(frame.get("ts"), "publish timestamp"),
    )


def normalize_l1_book_frame(frame: dict[str, Any], *, receipt_timestamp: Any,
                            venue: str = "DELTA") -> NormalizedL1Book:
    """Normalize a documented compact Delta ``ob_l1`` snapshot frame."""
    if not isinstance(frame, dict) or frame.get("type") != "ob_l1":
        raise InvalidMarketFrame("expected a Delta ob_l1 frame")
    bid = _positive(frame.get("bp"), "best bid")
    ask = _positive(frame.get("ap"), "best ask")
    if bid >= ask:
        raise InvalidMarketFrame("best bid must be below best ask")
    return NormalizedL1Book(
        provenance=_book_provenance(frame, receipt_timestamp=receipt_timestamp,
                                    venue=venue, channel="ob_l1"),
        bid_price=bid, bid_quantity=_positive(frame.get("bs"), "best bid size"),
        ask_price=ask, ask_quantity=_positive(frame.get("as"), "best ask size"),
    )


def _levels(raw: Any, side: str) -> tuple[BookLevel, ...]:
    if not isinstance(raw, list) or not raw:
        raise InvalidMarketFrame(f"{side} levels are missing")
    levels: list[BookLevel] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise InvalidMarketFrame(f"{side} level is invalid")
        levels.append(BookLevel(_positive(item[0], f"{side} price"),
                                _positive(item[1], f"{side} size")))
    prices = [level.price for level in levels]
    if prices != sorted(prices, reverse=(side == "bid")) or len(set(prices)) != len(prices):
        raise InvalidMarketFrame(f"{side} levels are not strictly ordered")
    return tuple(levels)


def normalize_l2_book_frame(frame: dict[str, Any], *, receipt_timestamp: Any,
                            venue: str = "DELTA") -> NormalizedL2Book:
    """Normalize a documented compact Delta ``ob_l2`` top-book snapshot."""
    if not isinstance(frame, dict) or frame.get("type") != "ob_l2":
        raise InvalidMarketFrame("expected a Delta ob_l2 frame")
    bids, asks = _levels(frame.get("b"), "bid"), _levels(frame.get("a"), "ask")
    if bids[0].price >= asks[0].price:
        raise InvalidMarketFrame("order book is crossed")
    return NormalizedL2Book(
        provenance=_book_provenance(frame, receipt_timestamp=receipt_timestamp,
                                    venue=venue, channel="ob_l2"),
        bids=bids, asks=asks,
    )
