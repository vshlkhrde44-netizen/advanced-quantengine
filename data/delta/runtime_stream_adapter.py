"""Transport-agnostic fail-closed feed adapter for the legacy candle callback.

Network code supplies raw Delta frames and its receipt timestamp.  This
adapter owns the normalized stream, causal finality, continuity gate and
runtime-candle conversion.  It intentionally has no socket lifecycle or
strategy imports, so it can be proven in PAPER mode before a transport swap.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from typing import Any

from core.history.candle import Candle
from data.delta.public_event_stream import DeltaPublicEventStream, IngestResult
from data.delta.runtime_candle_bridge import to_runtime_candle


class DeltaRuntimeStreamAdapter:
    """Deliver verified closed candles for exactly one symbol and resolution."""

    def __init__(
        self,
        symbol: str,
        resolution: str,
        on_closed_candle: Callable[[Candle], None],
        *,
        stream: DeltaPublicEventStream | None = None,
    ):
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol is required")
        if not isinstance(resolution, str) or not resolution:
            raise ValueError("resolution is required")
        self.symbol = symbol
        self.resolution = resolution
        self.on_closed_candle = on_closed_candle
        self.stream = stream if stream is not None else DeltaPublicEventStream()
        self._drops: Counter[str] = Counter()
        self.delivered = 0
        self.callback_errors = 0
        self.last_callback_error = ""

    def ingest(self, frame: Any, *, receipt_timestamp: Any) -> IngestResult:
        """Ingest one frame and call the consumer only after every gate passes."""
        result = self.stream.ingest(frame, receipt_timestamp=receipt_timestamp)
        decision = to_runtime_candle(result, expected_resolution=self.resolution)
        runtime_candle = decision.candle
        if runtime_candle is None:
            self._drops[decision.reason] += 1
            return result
        if runtime_candle.symbol != self.symbol:
            self._drops["candle is for another subscription symbol"] += 1
            return result
        try:
            self.on_closed_candle(runtime_candle)
        except Exception as error:  # consumer errors must not kill ingress
            self.callback_errors += 1
            self.last_callback_error = str(error)[:200]
            self._drops["runtime callback raised"] += 1
            return result
        self.delivered += 1
        return result

    def stats(self) -> dict[str, object]:
        return {
            "delivered": self.delivered,
            "callback_errors": self.callback_errors,
            "last_callback_error": self.last_callback_error,
            "drops": dict(self._drops),
            "stream": self.stream.stats(),
        }
