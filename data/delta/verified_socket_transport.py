"""Fail-closed verified Global Delta public-candle socket transport.

Phase-2 migration target (not yet production-wired):

    Global Delta WebSocket
      -> normalized events (DeltaPublicEventStream)
      -> VerifiedCandleSubscription
      -> DeltaRuntimeStreamAdapter
      -> CandleSessionGate (per symbol, post-connect resynchronization)
      -> MarketStateStore
      -> VerifiedStrategyDispatcher
      -> strategy callback (closed candles only)

Safety contract:

* The transport never synthesizes candles, never treats mark-price as
  traded OHLCV, and never delivers forming/gapped/volume-less candles.
* A socket open is never sufficient for delivery.  Every (re)connect
  revokes readiness via ``CandleSessionGate``; delivery resumes only after
  a fresh forming baseline plus exactly one continuous rollover.
* Production connection is refused until Global evidence is verified:
  ``require_global_evidence_bundle`` must point at a capture bundle for
  ``wss://public-socket.delta.exchange`` that passes
  ``scripts/verify_delta_global_evidence.py`` for the requested symbols.
  Until then every network entry point raises ``RuntimeError`` fail-closed.
* Network I/O is injected via ``socket_factory`` so unit tests exercise the
  full gate chain with a fake socket and zero network.

This module does not open any socket at import time.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from core.common.timestamp import normalize_timestamp_or_none
from core.history.candle import Candle
from data.delta.candle_session_gate import CandleSessionGate
from data.delta.market_state_store import (
    AuthoritativeMarketState,
    MarketStateStore,
)
from data.delta.public_event_stream import DeltaPublicEventStream
from data.delta.runtime_candle_bridge import to_runtime_candle
from data.delta.verified_candle_subscription import (
    VerifiedCandleSubscription,
    candle_subscription_payload,
)
from data.delta.verified_strategy_dispatch import VerifiedStrategyDispatcher


GLOBAL_PUBLIC_SOCKET = "wss://public-socket.delta.exchange"
GLOBAL_EVIDENCE_ENV_VAR = "DELTA_GLOBAL_EVIDENCE_BUNDLE"


class GlobalEvidenceMissing(RuntimeError):
    """Verified Global evidence has not been supplied; transport stays closed."""


def verified_evidence_bundle_path(explicit: str | None = None) -> str | None:
    """Resolve the configured Global evidence bundle path, if any."""
    import os

    if explicit:
        return explicit
    raw = os.getenv(GLOBAL_EVIDENCE_ENV_VAR, "")
    return raw.strip() or None


def check_global_evidence(
    bundle_path: str | None,
    *,
    symbols: Iterable[str],
    resolution: str = "1m",
) -> None:
    """Raise fail-closed unless a verified Global bundle exists for symbols.

    The bundle must be a single-session ``quantengine.delta-public-capture.v1``
    document for the exact Global endpoint covering the requested symbols.
    India endpoint or non-universe symbols never satisfy this gate.  A
    reconnect observation bundle is supplementary evidence only and does not
    satisfy this gate on its own.
    """
    import json as _json
    from pathlib import Path as _Path

    from scripts.verify_delta_global_evidence import verify_single_session

    if not bundle_path:
        raise GlobalEvidenceMissing(
            "Global Delta evidence is not configured; set %s to a verified "
            "Global capture bundle (wss://public-socket.delta.exchange). "
            "Legacy ticker-derived candles remain disabled."
            % GLOBAL_EVIDENCE_ENV_VAR
        )
    path = _Path(bundle_path)
    if not path.is_file():
        raise GlobalEvidenceMissing(
            "Global evidence bundle not found: %r" % bundle_path
        )
    try:
        bundle = _json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise GlobalEvidenceMissing(
            "Global evidence bundle unreadable: %r (%s)" % (bundle_path, error)
        ) from error
    if not isinstance(bundle, dict):
        raise GlobalEvidenceMissing("Global evidence bundle is not a mapping")
    verdict = verify_single_session(
        bundle, required_symbols=tuple(sorted(set(symbols))),
        resolution=resolution,
    )
    if not verdict.passed:
        failures = "; ".join(
            "%s: %s" % (check.name, check.detail)
            for check in verdict.failures()
        )
        raise GlobalEvidenceMissing(
            "Global evidence bundle %r does not satisfy the Phase-1 bar: %s"
            % (bundle_path, failures)
        )


class VerifiedSocketTransport:
    """One-resolution verified candle transport for explicitly listed symbols.

    The caller supplies ``on_closed_candle`` (strategy-safe delivery) and an
    optional ``socket_factory``.  The default factory uses the synchronous
    ``websockets`` client against the Global public socket; tests inject a
    fake.  No connection is opened until :meth:`connect` is called, and
    :meth:`connect` refuses until Global evidence is verified.
    """

    def __init__(
        self,
        symbols: Iterable[str],
        on_closed_candle: Callable[[Candle], None],
        *,
        resolution: str = "1m",
        endpoint: str = GLOBAL_PUBLIC_SOCKET,
        venue: str = "DELTA_GLOBAL",
        evidence_bundle: str | None = None,
        socket_factory: Callable[[str], Any] | None = None,
        history_fetcher: Callable[[str, str, int, int], Iterable[Candle]] | None = None,
        on_dispatch_record: Callable[[Any], None] | None = None,
    ):
        normalized = sorted({s for s in symbols if isinstance(s, str) and s})
        if not normalized:
            raise ValueError("at least one non-empty symbol is required")
        if not isinstance(resolution, str) or not resolution:
            raise ValueError("resolution is required")
        if endpoint != GLOBAL_PUBLIC_SOCKET:
            raise ValueError(
                "verified Global transport requires %r; got %r"
                % (GLOBAL_PUBLIC_SOCKET, endpoint)
            )
        self.symbols = normalized
        self.resolution = resolution
        self.endpoint = endpoint
        self.venue = venue
        self.evidence_bundle = verified_evidence_bundle_path(evidence_bundle)
        self._socket_factory = socket_factory
        self._history_fetcher = history_fetcher
        self.subscription = VerifiedCandleSubscription(resolution)
        # Per-symbol stream + session gate.  The subscription owns one
        # DeltaRuntimeStreamAdapter per symbol; the stream store behind each
        # adapter is the per-symbol MarketStateStore view used for dispatch
        # state.  Gates enforce post-connect resynchronization independently.
        self._gates: dict[str, CandleSessionGate] = {}
        self._dispatchers: dict[str, VerifiedStrategyDispatcher] = {}
        self._stats: Counter[str] = Counter()
        for symbol in normalized:
            stream = DeltaPublicEventStream(venue=venue)
            from data.delta.runtime_stream_adapter import (
                DeltaRuntimeStreamAdapter,
            )

            adapter = DeltaRuntimeStreamAdapter(
                symbol, resolution, lambda _candle: None, stream=stream
            )
            # Bypass the adapter's direct callback: delivery goes through the
            # session gate + dispatch gate below.  The adapter still owns
            # normalization/finality/continuity/bridge decisions.
            self.subscription._adapters[symbol] = adapter
            self._gates[symbol] = CandleSessionGate(resolution)
            self._dispatchers[symbol] = VerifiedStrategyDispatcher(
                lambda candle, _symbol=symbol: on_closed_candle(candle),
                on_decision=on_dispatch_record,
            )
        self._on_closed_candle = on_closed_candle
        self._socket: Any | None = None
        self.connected = False

    # -- evidence-gated lifecycle --------------------------------------

    def subscription_payload(self) -> dict:
        """Deterministic subscribe payload: candles + trades for all symbols."""
        candle = candle_subscription_payload(self.symbols, self.resolution)
        channels = list(candle["payload"]["channels"])
        channels.append({"name": "trades", "symbols": list(self.symbols)})
        return {"type": "subscribe", "payload": {"channels": channels}}

    def connect(self, *, timeout: float = 15.0) -> None:
        """Open the Global socket after verifying evidence. Fail closed."""
        check_global_evidence(
            self.evidence_bundle, symbols=self.symbols,
            resolution=self.resolution,
        )
        if self.connected:
            return
        factory = self._socket_factory
        if factory is None:
            from websockets.sync.client import connect as _connect

            socket = _connect(self.endpoint, open_timeout=min(timeout, 15))
        else:
            socket = factory(self.endpoint)
        try:
            socket.send(json.dumps(self.subscription_payload()))
        except Exception:
            try:
                socket.close()
            except Exception:
                pass
            raise
        self._socket = socket
        self.connected = True
        for gate in self._gates.values():
            gate.on_connected()
        self._stats["connects"] += 1

    def close(self) -> None:
        socket, self._socket = self._socket, None
        was_connected = self.connected
        self.connected = False
        for gate in self._gates.values():
            gate.on_disconnected()
        if socket is not None:
            try:
                socket.close()
            except Exception:
                self._stats["close_errors"] += 1
        if was_connected:
            self._stats["disconnects"] += 1

    # -- ingress --------------------------------------------------------

    def ingest(self, frame: Any, *, receipt_timestamp: Any) -> dict[str, Any]:
        """Route one raw frame through verify -> gate -> dispatch chain.

        Returns a small decision dict for observability.  Never raises on
        malformed market data; transport-level failures are the caller's.
        """
        if not self.connected:
            self._stats["dropped_not_connected"] += 1
            return {"delivered": False, "reason": "transport is not connected"}
        if not isinstance(frame, dict):
            self._stats["malformed_frames"] += 1
            return {"delivered": False, "reason": "frame is not a mapping"}
        channel = frame.get("type")
        if channel in {"heartbeat", "subscribed", "success", "pong", "ping",
                       "subscriptions"}:
            self._stats["control_frames"] += 1
            return {"delivered": False, "reason": "control frame observed"}
        symbol = frame.get("sy")
        if not isinstance(symbol, str) or symbol not in self.subscription._adapters:
            self._stats["unowned_or_malformed_frames"] += 1
            return {"delivered": False, "reason": "unowned symbol"}
        adapter = self.subscription._adapters[symbol]
        result = adapter.stream.ingest(frame, receipt_timestamp=receipt_timestamp)
        # Session-gate resynchronization on every candle start observation.
        if isinstance(channel, str) and channel.startswith("candlestick_"):
            start = normalize_timestamp_or_none(frame.get("cst"))
            gate = self._gates[symbol]
            decision = gate.observe_candle_start(start)
            if not decision.ready:
                self._stats["gated_until_resync"] += 1
                return {"delivered": False, "reason": decision.reason}
        decision_bridge = to_runtime_candle(
            result, expected_resolution=self.resolution
        )
        runtime_candle = decision_bridge.candle
        if runtime_candle is None:
            self._stats["bridge_rejections"] += 1
            return {"delivered": False, "reason": decision_bridge.reason}
        if runtime_candle.symbol != symbol:
            self._stats["bridge_rejections"] += 1
            return {
                "delivered": False,
                "reason": "candle is for another subscription symbol",
            }
        state = self._snapshot(symbol)
        gate_decision = self._dispatchers[symbol].dispatch(runtime_candle, state)
        if not gate_decision.allowed:
            self._stats["dispatch_rejections"] += 1
            return {"delivered": False, "reason": gate_decision.reason}
        self._stats["delivered"] += 1
        return {"delivered": True, "reason": "accepted"}

    def repair_gaps(self, symbol: str) -> dict[str, Any]:
        """Recover verified-feed gaps from an explicit historical-data adapter.

        The adapter must return genuine historical candles for the exact gap
        window.  Missing/partial data leaves the gate closed.  Backfilled bars
        restore continuity and journal evidence but are never retroactively
        dispatched to strategy.
        """
        if symbol not in self.subscription._adapters:
            return {"symbol": symbol, "status": "REJECTED", "reason": "unowned symbol"}
        if self._history_fetcher is None:
            self._stats["gap_repair_unavailable"] += 1
            return {
                "symbol": symbol, "status": "DATA_GAP_UNREPAIRED",
                "reason": "no historical-data adapter wired",
            }
        adapter = self.subscription._adapters[symbol]
        stream = adapter.stream
        status = stream.continuity.status(self.venue, symbol, self.resolution)
        if status["status"] == "CONTINUOUS":
            return {"symbol": symbol, "status": "CONTINUOUS", "ranges_repaired": 0}
        total = {"symbol": symbol, "status": "DATA_GAP_UNREPAIRED",
                 "ranges_seen": len(status["gaps"]), "ranges_repaired": 0,
                 "candles_repaired": 0, "rejected": 0, "unrepaired": []}
        for gap in status["gaps"]:
            try:
                candles = self._history_fetcher(
                    symbol, self.resolution, gap.after_start, gap.before_start
                )
                report = stream.repair_historical_candles(
                    symbol, self.resolution, candles,
                    receipt_timestamp=gap.before_start, venue=self.venue,
                )
            except Exception as exc:  # noqa: BLE001 - recovery stays fail-closed
                total["unrepaired"].append(dict(
                    after_start=gap.after_start, before_start=gap.before_start,
                    missing_candles=gap.missing_candles, reason=type(exc).__name__,
                ))
                continue
            total["ranges_repaired"] += int(report["ranges_repaired"])
            total["candles_repaired"] += int(report["accepted"])
            total["rejected"] += int(report["rejected"])
            if report["ranges_repaired"] == 0:
                total["unrepaired"].append(dict(
                    after_start=gap.after_start, before_start=gap.before_start,
                    missing_candles=gap.missing_candles, reason=report["reason"],
                ))
        remaining = stream.continuity.status(
            self.venue, symbol, self.resolution
        )
        total["status"] = remaining["status"]
        if total["status"] == "CONTINUOUS":
            self._stats["gaps_repaired"] += total["ranges_repaired"]
        else:
            self._stats["gaps_unrepaired"] += 1
        return total

    def _snapshot(self, symbol: str) -> AuthoritativeMarketState | None:
        adapter = self.subscription._adapters.get(symbol)
        if adapter is None:
            return None
        store: MarketStateStore = adapter.stream.store
        return store.snapshot(self.venue, symbol)

    def stats(self) -> dict[str, Any]:
        return {
            **dict(self._stats),
            "connected": self.connected,
            "symbols": list(self.symbols),
            "resolution": self.resolution,
            "endpoint": self.endpoint,
            "gates": {
                symbol: gate.status() for symbol, gate in self._gates.items()
            },
            "dispatch": {
                symbol: dispatcher.stats()
                for symbol, dispatcher in self._dispatchers.items()
            },
        }
