"""
====================================================

QuantEngine

Delta Subscription Stream

Phase 6D -- Real-time multi-symbol subscriptions.

A multiplexed transport that follows Delta's actual
websocket subscription model: ONE physical connection and
ONE `channels` payload that lists EVERY symbol currently
desired. Adding or dropping a symbol re-sends the full
subscription payload over the same socket -- the worker
layer never opens a per-symbol socket.

    transport = DeltaSubscriptionStream()
    transport.subscribe("BTCUSDT", on_candle, on_down)
    transport.subscribe("ETHUSDT", on_candle, on_down)
    transport.unsubscribe("ETHUSDT")

This is a **legacy reference-price transport**. Each subscribed symbol keeps
its own CandleBuilder driven by ticker marks. Its derived candles must never
be treated as traded OHLCV or routed into the production strategy runtime.
The default real-time manager now rejects this source fail-closed until a
venue-verified traded-candle transport is integrated. Intra-bucket ticks emit
a throttled forming heartbeat (closed=False) for legacy observability only.

On any connection failure the transport signals on_down()
for every route; the SubscriptionWorkers then own reconnect
(they re-enter transport.subscribe(), which restarts this
pump). Imports the `websockets` sync client lazily so the
orchestration layer stays offline-testable.

This is production network glue (Phase 6E wires it into the
engine loop); unit tests drive the transport contract via a
fake instead.
====================================================
"""

import json
import threading

from core.common.timestamp import normalize_timestamp
from core.history.candle import Candle

WS_URL = "wss://public-socket.delta.exchange"


class DeltaSubscriptionStream:

    # Phase 12: explicit gap-repair state. Ranges the live
    # feed skipped are recorded per symbol (bounded); repair
    # is an explicit, deterministic REST backfill through an
    # injectable fetcher, never an inference. Without a
    # fetcher the stream reports DATA_GAP_UNREPAIRED and no
    # downstream consumer may treat the series as continuous.
    _MAX_UNREPAIRED_RANGES = 64

    def __init__(self, timeframe="1m", ws_url=WS_URL,
                 history_fetcher=None):

        from config.settings import settings
        from data.candle_builder import CandleBuilder

        self.timeframe = timeframe or settings.TIMEFRAME
        self.ws_url = ws_url
        self._history_fetcher = history_fetcher

        self._builders = {}
        self._last_form = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._pump_thread = None
        self._ws = None
        self._handlers = {}

        # Phase 11C operational counters (all guarded by
        # _lock where mutated off the pump thread).
        self._stats = {
            "frames_received": 0,
            "malformed_frames": 0,
            "unknown_frames": 0,
            "heartbeat_frames": 0,
            "closed_candles": 0,
            "candle_gaps": 0,
            "pump_restarts": 0,
            "disconnects": 0,
        }
        # Stage 4: per-symbol last CLOSED candle timestamp for gap
        # DETECTION (Phase 12 adds explicit repair below).
        self._last_closed_ts = {}
        self._unrepaired_gaps = {}
        # Stage 1-3: liveness tracking (data-driven heartbeat; no
        # WS ping/pong on the public ticker feed — staleness is the
        # liveness signal, see data/realtime/stream_health.py).
        self._last_heartbeat_ts = None

    def _note(self, key, amount=1):

        with self._lock:

            self._stats[key] = self._stats.get(key, 0) + amount

    def stats(self):
        """Snapshot of transport counters."""

        with self._lock:

            return dict(self._stats)

    def _gaps(self):
        """Unrepaired-gap map, created lazily so test harnesses
        that bypass __init__ (bare __new__ + manual fields)
        degrade to working gap tracking instead of crashing."""

        gaps = self.__dict__.get("_unrepaired_gaps")

        if gaps is None:

            gaps = self._unrepaired_gaps = {}

        return gaps

    def gap_status(self):
        """Gap durability report (Phase 12).

        Returns {"status": "CONTINUOUS"} when no symbol has
        an outstanding unrepaired range, else
        {"status": "DATA_GAP_UNREPAIRED", "symbols": {...}}.
        Downstream consumers must not treat a symbol's series
        as continuous while it appears in `symbols`.
        """

        with self._lock:

            pending = {
                symbol: {
                    "unrepaired_ranges": [
                        dict(entry) for entry in entries
                    ],
                    "unrepaired_candles": sum(
                        int(entry.get("missed_candles", 0))
                        for entry in entries
                    ),
                }
                for symbol, entries in
                self._gaps().items()
                if entries
            }

        if not pending:

            return {"status": "CONTINUOUS", "symbols": {}}

        return {"status": "DATA_GAP_UNREPAIRED",
                "symbols": pending}

    def repair_gaps(self, symbol, fetcher=None):
        """Deterministic gap backfill for one symbol.

        For every recorded unrepaired range: fetch candidate
        bars via `fetcher(symbol, after_ts, before_ts)` (or
        the stream's wired history_fetcher), then normalize
        (same unit rules as the live path), validate
        (finite/positive/consistent OHLC, strictly inside the
        range, bucket-aligned), sort, and dedupe. Repaired
        ranges clear; anything unfetched or invalid stays
        unrepaired -- gaps are never interpolated or
        fabricated. Returns a deterministic report dict.
        Repaired Candles are RETURNED (closed=True), never
        merged into the live forward-only builders.
        """

        source = (
            fetcher
            if fetcher is not None
            else self._history_fetcher
        )

        with self._lock:

            ranges = list(
                self._gaps().get(symbol, []))

        report = {
            "symbol": symbol,
            "ranges_seen": len(ranges),
            "ranges_repaired": 0,
            "candles_repaired": 0,
            "candles_rejected": 0,
            "repaired": [],
            "unrepaired": [],
            "reason": "",
        }

        if not ranges:

            report["reason"] = "no unrepaired ranges"
            return report

        if source is None:

            report["reason"] = (
                "DATA_GAP_UNREPAIRED: no history fetcher wired"
            )
            report["unrepaired"] = [
                dict(entry) for entry in ranges
            ]
            return report

        try:
            window = 60
            from data.candle_builder import TIMEFRAME_SECONDS
            window = int(
                TIMEFRAME_SECONDS.get(self.timeframe, 60))
        except Exception:  # noqa: BLE001 (fail closed below)
            window = 60

        if window <= 0:
            window = 60

        for entry in ranges:

            after_ts = entry.get("after_ts")
            before_ts = entry.get("before_ts")

            try:
                rows = source(symbol, after_ts, before_ts)
                rows = list(rows or [])
            except Exception as exc:  # noqa: BLE001
                report["unrepaired"].append(dict(entry))
                report["reason"] = (
                    "fetcher failed: %s" % type(exc).__name__
                )
                continue

            repaired, rejected = self._normalize_repair_rows(
                rows, after_ts, before_ts, window, symbol)

            report["candles_repaired"] += len(repaired)
            report["candles_rejected"] += rejected
            report["repaired"].extend(repaired)

            if repaired:

                report["ranges_repaired"] += 1

                with self._lock:

                    live = self._gaps().get(symbol, [])

                    if entry in live:

                        live.remove(entry)

            else:

                report["unrepaired"].append(dict(entry))

        if not report["unrepaired"] and not report["reason"]:

            report["reason"] = "all ranges repaired"

        elif report["unrepaired"] and not report["reason"]:

            report["reason"] = "DATA_GAP_UNREPAIRED"

        return report

    @staticmethod
    def _normalize_repair_rows(rows, after_ts, before_ts,
                               window, symbol):
        """Normalize + validate one range's fetched rows."""

        import math

        repaired = []
        rejected = 0
        seen = set()

        for row in rows:

            if not isinstance(row, dict):
                rejected += 1
                continue

            try:
                ts = normalize_timestamp(row.get("time"))
                open_ = float(row.get("open"))
                high = float(row.get("high"))
                low = float(row.get("low"))
                close = float(row.get("close"))
                volume = float(row.get("volume", 0.0))
            except (TypeError, ValueError, KeyError):
                rejected += 1
                continue

            if not (
                math.isfinite(open_) and math.isfinite(high)
                and math.isfinite(low) and math.isfinite(close)
                and math.isfinite(volume)
            ):
                rejected += 1
                continue

            if (
                min(open_, high, low, close) <= 0
                or high < max(open_, close)
                or low > min(open_, close)
                or high < low
                or volume < 0
                # Strictly inside the missed range: never
                # rewrite a candle the live feed delivered.
                or not (after_ts < ts < before_ts)
                # Bucket-aligned to the stream timeframe.
                or (ts % window) != 0
                or ts in seen
            ):
                rejected += 1
                continue

            seen.add(ts)

            repaired.append(
                Candle(
                    timestamp=ts,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    closed=True,
                    symbol=symbol,
                )
            )

        repaired.sort(key=lambda c: c.timestamp)

        return repaired, rejected

    # ==================================================

    def _ensure_builder(self, symbol):

        if symbol not in self._builders:

            from data.candle_builder import CandleBuilder

            self._builders[symbol] = CandleBuilder(timeframe=self.timeframe)

        return self._builders[symbol]

    def _subscribe_payload(self):

        symbols = sorted(self._desired_symbols())

        return {

            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": "ticker",
                        "symbols": symbols,
                    }
                ]
            },

        }

    def _desired_symbols(self):

        with self._lock:

            return set(self._handlers)

    def _emit_ticker(self, raw):
        """Parses one Delta ticker message and fans it out to the
        owning route. Emits closed candles on boundary rolls and a
        throttled forming heartbeat in between.

        Phase 11C: a malformed frame is REJECTED (counted in
        stats) and never reaches strategy logic; it never
        raises into the pump loop, so one bad frame cannot
        kill the multiplexed connection for every symbol."""

        if not isinstance(raw, dict):
            self._note("malformed_frames")
            return

        msg_type = raw.get("type")
        if msg_type in ("heartbeat", "subscribed", "success",
                        "pong", "ping"):
            # Stage 1-3: provider keepalive/control frames are
            # liveness, not data and not "unknown". Counted
            # separately so they never pollute unknown_frames and
            # never reach strategy logic.
            self._note("heartbeat_frames")
            with self._lock:
                import time as _time
                self._last_heartbeat_ts = _time.time()
            return

        if msg_type != "ticker":

            self._note("unknown_frames")
            return

        try:
            ticks = raw["d"]
            ticker = ticks[0] if isinstance(ticks, list) else None
            if not isinstance(ticker, dict):
                raise KeyError("d[0]")
            symbol = ticker["s"]
            mark = float(ticker["m"])
            if not isinstance(symbol, str) or not symbol:
                raise ValueError("symbol")
        except (KeyError, TypeError, ValueError, IndexError):
            self._note("malformed_frames")
            return

        if mark != mark or mark in (float("inf"), float("-inf")) or mark <= 0:
            self._note("malformed_frames")
            return

        try:
            canonical_ts = normalize_timestamp(raw["ts"])
        except (ValueError, TypeError, KeyError):
            self._note("malformed_frames")
            return

        self._note("frames_received")

        builder = self._ensure_builder(symbol)
        finished = builder.update(canonical_ts, mark)

        if finished is not None:

            self._note("closed_candles")

            # Stage 4 gap DETECTION (minimum fail-closed
            # observability inside the existing architecture):
            # closed candles for one symbol must advance by exactly
            # the builder window. A forward jump means missed
            # candle(s) the feed will never repair (no sequence
            # numbers, no REST backfill) — count them loudly and
            # keep the stream running. Repair stays NOT_IMPLEMENTED
            # (documented YELLOW); downstream staleness/health owns
            # any trading halt policy.
            try:
                window = int(getattr(builder, "window_seconds", 0)
                             or 0)
                ts = int(finished.timestamp)
            except (TypeError, ValueError):
                window, ts = 0, 0
            if window > 0 and ts > 0:
                with self._lock:
                    previous = self._last_closed_ts.get(symbol)
                    self._last_closed_ts[symbol] = ts
                if (previous is not None and ts > previous
                        and (ts - previous) > window):
                    missed = (ts - previous) // window - 1
                    if missed > 0:
                        self._note("candle_gaps", missed)
                        # Phase 12: record the unrepaired range
                        # (bounded per symbol) so gap_status()
                        # can report DATA_GAP_UNREPAIRED until
                        # repair_gaps() clears it with real data.
                        ranges = self._gaps().setdefault(
                            symbol, [])
                        ranges.append({
                            "after_ts": previous,
                            "before_ts": ts,
                            "missed_candles": missed,
                        })
                        del ranges[
                            :-self._MAX_UNREPAIRED_RANGES]

            # CandleBuilder already bucketed the timestamp (seconds).
            self._route(

                Candle(

                    timestamp=finished.timestamp,
                    open=finished.open,
                    high=finished.high,
                    low=finished.low,
                    close=finished.close,
                    volume=finished.volume,
                    closed=True,
                    symbol=symbol,

                )

            )

            return

        sec = canonical_ts
        bucket = sec - (sec % builder.window_seconds)

        if (sec - self._last_form.get(symbol, 0)) >= 1:

            self._last_form[symbol] = sec

            self._route(

                Candle(

                    timestamp=bucket,
                    open=mark,
                    high=mark,
                    low=mark,
                    close=mark,
                    volume=0.0,
                    closed=False,
                    symbol=symbol,

                )

            )

    def _route(self, candle):

        with self._lock:

            handler = self._handlers.get(candle.symbol)

        if handler is not None:

            on_candle, on_down = handler

            on_candle(candle)

    # ==================================================

    def _pump_loop(self):

        from websockets.sync.client import connect

        while not self._stop.is_set():

            try:

                with connect(self.ws_url) as ws:

                    with self._lock:

                        self._ws = ws

                    ws.send(json.dumps(self._subscribe_payload()))

                    for message in ws:

                        if self._stop.is_set():

                            break

                        # Phase 11C: per-message isolation. One
                        # malformed/invalid frame is rejected and
                        # counted; it never kills the pump for the
                        # other symbols on this connection.
                        try:
                            self._emit_ticker(json.loads(message))
                        except Exception:
                            self._note("malformed_frames")

            except Exception:

                # Pump exits on any failure; workers own reconnect.
                pass

            finally:

                with self._lock:

                    self._ws = None

                if not self._stop.is_set():
                    self._note("disconnects")
                    self._signal_down()

            if self._stop.is_set():

                break

            self._note("pump_restarts")
            # Stage 1-3: bounded backoff between reconnect attempts so
            # a hard-down provider cannot hot-spin the pump thread.
            # Stop-aware wait keeps close() responsive.
            self._stop.wait(0.5)

    def _signal_down(self):

        with self._lock:

            handlers = list(self._handlers.values())

            for symbol in list(self._handlers):

                self._last_form.pop(symbol, None)

        # Reconnect is per route: each worker decides its own retry.
        for on_candle, on_down in handlers:

            on_down()

    # ==================================================

    def subscribe(self, symbol, on_candle, on_down):

        with self._lock:

            self._handlers[symbol] = (on_candle, on_down)

        self._ensure_pump()

        self._resubscribe()

    def unsubscribe(self, symbol):

        with self._lock:

            self._handlers.pop(symbol, None)

        if not self._desired_symbols():

            self.close()
            return

        self._resubscribe()

    def _ensure_pump(self):

        with self._lock:

            if (self._pump_thread is not None
                    and self._pump_thread.is_alive()):
                return

            self._stop.clear()
            self._pump_thread = threading.Thread(
                target=self._pump_loop,
                daemon=True,
            )
            self._pump_thread.start()

    def _resubscribe(self):

        with self._lock:

            ws = self._ws

        if ws is None:

            return

        try:

            ws.send(json.dumps(self._subscribe_payload()))

        except Exception:

            # The pump will notice the dead socket and signal down.
            pass

    def close(self):

        self._stop.set()

        with self._lock:

            ws = self._ws
            self._ws = None
            self._handlers = {}

        self._builders = {}
        self._last_form = {}

        if ws is not None:

            try:

                ws.close()

            except Exception:

                pass
