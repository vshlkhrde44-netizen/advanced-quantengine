"""
====================================================

QuantEngine

REST Polling Candle Transport (paper preview)

A public-market-data transport that implements the exact
6D transport contract:

    transport.subscribe(symbol, on_candle, on_down) -> None
    transport.unsubscribe(symbol)                    -> None
    transport.close()                                -> None

but sources candles from the PUBLIC Delta REST history
endpoint (DeltaHistory.candles: /v2/history/candles — no
authentication, no trading authority) instead of the
legacy ticker-mark socket feed.

Causal contract (explicit):

  * CLOSED bars only: a bar with open-time T on timeframe F
    is delivered only when T + F <= now (exchange clock via
    the injectable clock). The forming bar is NEVER emitted.
  * Ordered + deduplicated: per-symbol watermark of the last
    delivered bar; polls deliver only strictly newer bars in
    ascending order. Reconnect/resubscribe resumes from the
    watermark — never a duplicate, never a gap-fill guess.
  * Every delivered candle is tagged symbol=<symbol> and
    closed=True (the Candle default); provenance is venue
    REST history, never ticker marks.
  * Bounded failure handling: consecutive poll failures past
    max_failures invoke on_down() (the worker owns reconnect
    via resubscribe, per ReconnectPolicy); the poll thread
    never loops unboundedly and never raises into the engine.

This transport has NO order-submission capability by
construction (it imports no exchange write path).

Paper-preview use:
    build_paper_engine(transport_factory=lambda:
        RestPollingCandleTransport(...))

====================================================
"""

from __future__ import annotations

import threading
import time
from collections import deque

TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class RestPollingCandleTransport:
    """Poll public REST history; emit closed candles only."""

    def __init__(
        self,
        timeframe="1m",
        history=None,
        clock=None,
        poll_interval_seconds=15.0,
        lookback_days=1,
        max_failures=5,
        recent_limit=240,
    ):
        from config.settings import settings

        self.timeframe = timeframe or settings.TIMEFRAME
        if self.timeframe not in TIMEFRAME_SECONDS:
            raise ValueError(
                "unsupported preview timeframe %r (supported: %s)"
                % (self.timeframe, sorted(TIMEFRAME_SECONDS)))
        self.bar_seconds = TIMEFRAME_SECONDS[self.timeframe]

        if history is not None:
            self.history = history
        else:
            from core.exchanges.delta.delta_history import (
                DeltaHistory,
            )
            self.history = DeltaHistory()

        self.clock = clock or time.time
        self.poll_interval = max(5.0, float(poll_interval_seconds))
        self.lookback_days = max(1, int(lookback_days))
        self.max_failures = max(1, int(max_failures))
        self.recent_limit = max(1, int(recent_limit))

        self._lock = threading.Lock()
        self._routes: dict[str, tuple] = {}
        self._watermarks: dict[str, int] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._stopped: dict[str, bool] = {}
        self._recent: dict[str, deque] = {}
        self._consecutive_failures: dict[str, int] = {}
        self._polls: dict[str, int] = {}
        self._delivered: dict[str, int] = {}
        self._last_ok: dict[str, float] = {}
        self._last_error: dict[str, str] = {}
        self._closed = False

    # --------------------------------------------------
    # Transport contract
    # --------------------------------------------------

    def subscribe(self, symbol, on_candle, on_down):
        """Validate quickly (one probe fetch), then start the
        per-symbol daemon poll thread. Raises on probe failure
        so the worker retries per ReconnectPolicy."""
        if self._closed:
            raise RuntimeError("transport is closed")
        if not symbol or not str(symbol).strip():
            raise ValueError("symbol is required")
        if not callable(on_candle) or not callable(on_down):
            raise TypeError("on_candle and on_down are required")
        symbol = str(symbol).strip()

        # Probe: fail fast (and loudly) when history is
        # unreachable instead of starting a dead thread.
        self._fetch(symbol)

        with self._lock:
            self._routes[symbol] = (on_candle, on_down)
            self._stopped[symbol] = False
            self._consecutive_failures[symbol] = 0
            self._polls[symbol] = 0
            self._delivered.setdefault(symbol, 0)
            self._recent.setdefault(
                symbol, deque(maxlen=self.recent_limit))
            thread = threading.Thread(
                target=self._poll_loop,
                args=(symbol,),
                name="rest-candle-poll-%s" % symbol,
                daemon=True,
            )
            self._threads[symbol] = thread
            thread.start()

    def unsubscribe(self, symbol):
        with self._lock:
            self._stopped[str(symbol)] = True
            self._routes.pop(str(symbol), None)

    def close(self):
        with self._lock:
            self._closed = True
            for symbol in self._stopped:
                self._stopped[symbol] = True
            self._routes.clear()

    # --------------------------------------------------
    # Poll loop
    # --------------------------------------------------

    def _fetch(self, symbol):
        return self.history.candles(
            symbol,
            self.timeframe,
            days=self.lookback_days,
        )

    def _poll_loop(self, symbol):
        while True:
            with self._lock:
                if self._stopped.get(symbol, True) or self._closed:
                    return
                route = self._routes.get(symbol)
            if route is None:
                return
            on_candle, on_down = route
            try:
                bars = self._fetch(symbol)
            except Exception as exc:  # noqa: BLE001 (bounded)
                failures = self._note_failure(
                    symbol, "%s: %s" % (type(exc).__name__, exc))
                if failures >= self.max_failures:
                    with self._lock:
                        self._stopped[symbol] = True
                    try:
                        on_down()
                    except Exception:  # noqa: BLE001 (isolated)
                        pass
                    return
                time.sleep(self.poll_interval)
                continue
            self._note_success(symbol)
            try:
                self._deliver_new(symbol, bars or [], on_candle)
            except Exception:  # noqa: BLE001 (isolated)
                pass
            # Bounded sleep in small slices so unsubscribe/close
            # take effect promptly.
            deadline = self.clock() + self.poll_interval
            while self.clock() < deadline:
                with self._lock:
                    if self._stopped.get(symbol, True) or self._closed:
                        return
                time.sleep(min(1.0, deadline - self.clock()))

    def _deliver_new(self, symbol, bars, on_candle):
        """Emit strictly-new CLOSED bars in ascending order."""
        now = self.clock()
        with self._lock:
            watermark = self._watermarks.get(symbol)
        fresh = []
        for bar in bars or []:
            try:
                stamp = int(getattr(bar, "timestamp", 0) or 0)
            except (TypeError, ValueError):
                continue
            if stamp <= 0:
                continue
            if stamp + self.bar_seconds > now:
                continue  # forming bar: never emit
            if watermark is not None and stamp <= watermark:
                continue  # already delivered
            fresh.append((stamp, bar))
        fresh.sort(key=lambda item: item[0])
        for stamp, bar in fresh:
            try:
                bar.symbol = symbol
            except Exception:  # noqa: BLE001 (slots guard)
                pass
            try:
                bar.closed = True
            except Exception:  # noqa: BLE001 (slots guard)
                pass
            try:
                on_candle(bar)
            except Exception:  # noqa: BLE001 (isolated)
                continue
            with self._lock:
                self._watermarks[symbol] = stamp
                self._recent[symbol].append(bar)
                self._delivered[symbol] = (
                    self._delivered.get(symbol, 0) + 1)

    def _note_failure(self, symbol, error):
        with self._lock:
            failures = self._consecutive_failures.get(symbol, 0) + 1
            self._consecutive_failures[symbol] = failures
            self._polls[symbol] = self._polls.get(symbol, 0) + 1
            self._last_error[symbol] = str(error)[:200]
            return failures

    def _note_success(self, symbol):
        now = self.clock()
        with self._lock:
            self._consecutive_failures[symbol] = 0
            self._polls[symbol] = self._polls.get(symbol, 0) + 1
            self._last_ok[symbol] = now
            self._last_error.pop(symbol, None)

    # --------------------------------------------------
    # Observability (dashboard + health)
    # --------------------------------------------------

    def recent_closed(self, symbol, limit=60):
        """Last delivered closed candles (oldest-first)."""
        with self._lock:
            bars = list(self._recent.get(str(symbol), ()))
        limit = max(1, int(limit))
        return bars[-limit:]

    def stats(self, symbol=None):
        """Per-symbol (or all-symbol) poll/delivery counters."""
        with self._lock:
            symbols = ([str(symbol)] if symbol
                       else sorted(set(list(self._polls)
                                       + list(self._routes))))
            out = {}
            for name in symbols:
                out[name] = {
                    "polls": self._polls.get(name, 0),
                    "delivered": self._delivered.get(name, 0),
                    "watermark": self._watermarks.get(name),
                    "failures": self._consecutive_failures.get(
                        name, 0),
                    "last_ok": self._last_ok.get(name),
                    "last_error": self._last_error.get(name),
                    "subscribed": name in self._routes,
                }
            return out
