"""Verified-candle subscription contract, intentionally independent of sockets.

The legacy ticker transport has a large operational surface.  This coordinator
is the narrow migration target: it creates the documented candle subscription
payload and routes a raw symbol-scoped frame to one fail-closed runtime adapter.
Socket connect/reconnect/resubscribe behaviour remains outside this module.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from core.history.candle import Candle
from data.delta.public_event_stream import IngestResult
from data.delta.runtime_stream_adapter import DeltaRuntimeStreamAdapter


def candle_subscription_payload(symbols: Iterable[str], resolution: str) -> dict:
    """Build the documented deterministic public-candle subscription payload."""
    if not isinstance(resolution, str) or not resolution:
        raise ValueError("resolution is required")
    normalized = sorted(set(symbols))
    if not normalized or any(not isinstance(symbol, str) or not symbol for symbol in normalized):
        raise ValueError("at least one non-empty symbol is required")
    return {
        "type": "subscribe",
        "payload": {
            "channels": [{
                "name": "candlestick_" + resolution,
                "symbols": normalized,
            }],
        },
    }


def is_observed_subscription_snapshot(
    frame: Any, *, symbols: Iterable[str], resolution: str,
) -> bool:
    """Recognize the 2026-09-16 Delta India public subscription echo.

    This is deliberately narrow: it validates only the observed
    ``{"type": "subscriptions", "channels": [...]}`` control envelope.
    It does not declare a socket healthy, imply Global endpoint compatibility,
    or replace the post-connect candle rollover gate.
    """
    if not isinstance(frame, dict) or frame.get("type") != "subscriptions":
        return False
    channels = frame.get("channels")
    if not isinstance(channels, list):
        return False
    expected = sorted(set(symbols))
    return any(
        isinstance(channel, dict)
        and channel.get("name") == "candlestick_" + resolution
        and channel.get("symbols") == expected
        for channel in channels
    )


class VerifiedCandleSubscription:
    """One-resolution in-process demultiplexer for verified candle frames."""

    def __init__(self, resolution: str):
        if not isinstance(resolution, str) or not resolution:
            raise ValueError("resolution is required")
        self.resolution = resolution
        self._adapters: dict[str, DeltaRuntimeStreamAdapter] = {}
        self._stats: Counter[str] = Counter()

    def subscribe(self, symbol: str, on_closed_candle: Callable[[Candle], None]) -> None:
        self._adapters[symbol] = DeltaRuntimeStreamAdapter(
            symbol, self.resolution, on_closed_candle,
        )

    def unsubscribe(self, symbol: str) -> None:
        self._adapters.pop(symbol, None)

    def payload(self) -> dict:
        return candle_subscription_payload(self._adapters, self.resolution)

    def ingest(self, frame: Any, *, receipt_timestamp: Any) -> IngestResult | None:
        """Route a raw frame; control frames and unowned symbols are observable drops."""
        if not isinstance(frame, dict):
            self._stats["malformed_frames"] += 1
            return None
        channel = frame.get("type")
        if channel in {"heartbeat", "subscribed", "success", "pong", "ping"}:
            self._stats["control_frames"] += 1
            return None
        symbol = frame.get("sy")
        adapter = self._adapters.get(symbol)
        if adapter is None:
            self._stats["unowned_or_malformed_frames"] += 1
            return None
        self._stats["routed_frames"] += 1
        return adapter.ingest(frame, receipt_timestamp=receipt_timestamp)

    def stats(self) -> dict[str, object]:
        return {
            **dict(self._stats),
            "symbols": {
                symbol: adapter.stats()
                for symbol, adapter in sorted(self._adapters.items())
            },
        }
