"""Fail-closed post-reconnect gate for an evidenced candle subscription.

The compact public Delta channel does not document a subscription acknowledgement
or public sequence number.  Consequently a reconnect cannot be called healthy
merely because a socket opens.  This gate requires two consecutive observed
candle-start values in the requested resolution after every reconnect.  The
first establishes a new forming baseline; the second proves a forward rollover.
"""

from __future__ import annotations

from dataclasses import dataclass


_RESOLUTION_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
    "12h": 43200, "1d": 86400, "1w": 604800,
}


@dataclass(frozen=True, slots=True)
class SessionGateDecision:
    accepted: bool
    ready: bool
    reason: str


class CandleSessionGate:
    """Track one symbol/resolution subscription across disconnect epochs."""

    def __init__(self, resolution: str):
        try:
            self.window_seconds = _RESOLUTION_SECONDS[resolution]
        except KeyError as error:
            raise ValueError("unsupported candle resolution") from error
        self.resolution = resolution
        self.connected = False
        self.ready = False
        self._baseline_start: int | None = None
        self.reconnects = 0
        self.rejected = 0

    def on_connected(self) -> None:
        """Start a new evidence epoch; socket-open alone never makes it ready."""
        if self.connected:
            return
        if self._baseline_start is not None or self.ready:
            self.reconnects += 1
        self.connected = True
        self.ready = False
        self._baseline_start = None

    def on_disconnected(self) -> None:
        """Immediately revoke readiness until a complete resynchronization."""
        self.connected = False
        self.ready = False
        self._baseline_start = None

    def observe_candle_start(self, start: int | None) -> SessionGateDecision:
        """Accept only a continuous two-start post-connect observation.

        Same-start updates keep the forming baseline. A backward start or a gap
        rejects the observation and leaves delivery blocked. No wall-clock
        inference or implicit resubscription acknowledgement is used.
        """
        if not self.connected:
            self.rejected += 1
            return SessionGateDecision(False, False, "socket is not connected")
        if not isinstance(start, int) or start <= 0:
            self.rejected += 1
            return SessionGateDecision(False, False, "candle start is unavailable")
        baseline = self._baseline_start
        if baseline is None:
            self._baseline_start = start
            return SessionGateDecision(True, False, "forming baseline observed")
        if start == baseline:
            return SessionGateDecision(True, self.ready, "forming update observed")
        if start < baseline:
            self.rejected += 1
            return SessionGateDecision(False, False, "out-of-order candle start")
        if start - baseline != self.window_seconds:
            self.rejected += 1
            self.ready = False
            self._baseline_start = start
            return SessionGateDecision(False, False, "candle gap requires a new resynchronization")
        self._baseline_start = start
        self.ready = True
        return SessionGateDecision(True, True, "continuous rollover observed")

    def status(self) -> dict[str, object]:
        return {
            "connected": self.connected,
            "ready": self.ready,
            "resolution": self.resolution,
            "baseline_start": self._baseline_start,
            "reconnects": self.reconnects,
            "rejected": self.rejected,
        }
