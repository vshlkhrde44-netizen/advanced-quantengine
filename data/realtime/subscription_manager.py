"""
====================================================

QuantEngine

Multi-Symbol Subscription Manager

Phase 6D -- Real-time multi-symbol subscriptions.

Delivers real-time market data independently to every
eligible instrument. The manager owns a single multiplexed
transport (Delta multiplexes many symbol subscriptions over
one websocket connection) plus one SubscriptionWorker per
logical subscription.

    manager = MultiSymbolSubscriptionManager(transport_factory=factory)
    manager.sync(universe)      # ADD / REMOVE / KEEP diff
    manager.poll()              # pump scheduled retries + staleness
    manager.statuses()          # {symbol: SubscriptionState}

Universe changes are reconciled in place -- instruments newly
eligible are subscribed, instruments that become ineligible or
delisted are unsubscribed -- with NO restart. The diff is:

    desired subscriptions - current subscriptions
        -> ADD (newly eligible)
        -> REMOVE (ineligible / delisted / failed-heal)
        -> KEEP (unchanged, same worker object retained)

Failure isolation: every per-symbol operation is wrapped so one
worker's failure is recorded on that worker and in manager.errors
while every other subscription continues untouched.

Phase 7 (BREAKOUT directional robustness) stays a separate
research milestone: this layer adds no strategy, execution, or
portfolio logic.
====================================================
"""

from data.realtime.reconnect_policy import ReconnectPolicy
from data.realtime.stream_worker import SubscriptionWorker
from data.realtime.subscription_state import SubscriptionStatus
import time


def default_transport_factory():
    """Return the production-safe default market-data transport.

    The historical default converted Delta ticker marks into legacy candles.
    Ticker marks are neither traded OHLCV nor an evidenced candle-finality
    source, so strategy execution must not receive them by default.  A future
    socket implementation may replace this only after it is bound to the
    verified public-candle contract for the configured venue and symbols.
    """

    return _AuthoritativeCandleTransportRequired()


class _AuthoritativeCandleTransportRequired:
    """Fail closed at the legacy transport seam without opening a socket."""

    _MESSAGE = (
        "authoritative traded-candle transport is not configured; "
        "legacy ticker-derived candles are disabled"
    )

    def subscribe(self, symbol, on_candle, on_down):
        del symbol, on_candle, on_down
        raise RuntimeError(self._MESSAGE)

    def unsubscribe(self, symbol):
        del symbol

    def close(self):
        return None


class MultiSymbolSubscriptionManager:

    def __init__(

        self,
        transport_factory=None,
        clock=None,
        policy=None,
        candles_callback=None,
        max_stale_seconds=30.0,
        max_dedupe=256,

    ):

        self.transport_factory = transport_factory or default_transport_factory
        self.clock = clock or time.time
        self.policy = policy or ReconnectPolicy()
        self.candles_callback = candles_callback
        self.max_stale_seconds = max_stale_seconds
        self.max_dedupe = max_dedupe

        self._transport = None
        self._workers = {}
        self._instruments = {}

        self.errors = []

    # --------------------------------------------------

    @property
    def transport(self):

        return self._transport

    @property
    def symbols(self):

        return tuple(sorted(self._workers))

    @property
    def count(self):

        return len(self._workers)

    # --------------------------------------------------

    def _ensure_transport(self):
        """Builds the multiplexed transport once and reuses it
        across every universe pass (KEEP the connection)."""

        if self._transport is None:

            self._transport = self.transport_factory()

        return self._transport

    def _normalize(self, universe):
        """Accepts a MarketUniverse snapshot (Phase 6B) or any
        iterable of instrument-like objects exposing `.symbol`.
        The eligible cut is the ONLY source of symbols -- never a
        hardcoded list."""

        if hasattr(universe, "eligible_instruments"):

            instruments = universe.eligible_instruments()

        else:

            instruments = universe

        desired = {}

        for instrument in instruments:

            symbol = getattr(instrument, "symbol", None)

            if symbol:

                desired[symbol] = instrument

        return desired

    # --------------------------------------------------

    def sync(self, universe):
        """Reconcile subscriptions against the latest universe
        snapshot. Idempotent; safe to call on every refresh pass.
        Returns {"added": [...], "removed": [...], "kept": [...]}."""

        desired = self._normalize(universe)

        added = []
        removed = []
        kept = []

        for symbol in list(self._workers):

            worker = self._workers[symbol]

            want_gone = symbol not in desired

            want_heal = worker.state.status == SubscriptionStatus.FAILED

            if want_gone or want_heal:

                try:

                    self.unsubscribe(symbol)
                    removed.append(symbol)

                except Exception as error:

                    self.errors.append((symbol, "remove", error))

        for symbol, instrument in desired.items():

            if symbol in self._workers:

                kept.append(symbol)
                self._instruments[symbol] = instrument
                continue

            try:

                self._add_worker(symbol, instrument)
                added.append(symbol)

            except Exception as error:

                self.errors.append((symbol, "add", error))

        self._instruments = dict(desired)

        return {
            "added": added,
            "removed": removed,
            "kept": kept,
        }

    def _add_worker(self, symbol, instrument):

        transport = self._ensure_transport()

        worker = SubscriptionWorker(

            symbol=symbol,
            transport=transport,
            clock=self.clock,
            policy=self.policy,
            max_stale_seconds=self.max_stale_seconds,
            max_dedupe=self.max_dedupe,
            on_closed_candle=self.candles_callback,

        )

        self._workers[symbol] = worker
        self._instruments[symbol] = instrument

        worker.start()

        return worker

    def subscribe(self, symbol, instrument=None):
        """Direct single-symbol subscribe (primarily for tests and
        for the scanner handoff). Equivalent to sync() over one
        instrument."""

        if symbol in self._workers:

            if self._workers[symbol].state.status != SubscriptionStatus.FAILED:

                return self._workers[symbol]

            self.unsubscribe(symbol)

        instrument = instrument or getattr(self, "_instruments", {}).get(symbol)

        return self._add_worker(symbol, instrument)

    def unsubscribe(self, symbol):
        """Remove one subscription: status -> STOPPED, unsubscribed
        from the shared transport, worker discarded."""

        worker = self._workers.pop(symbol, None)

        if worker is None:

            return

        worker.stop()

        self._instruments.pop(symbol, None)

    # --------------------------------------------------

    def status(self, symbol):

        worker = self._workers.get(symbol)

        return worker.state if worker is not None else None

    def statuses(self):

        return {

            symbol: worker.state
            for symbol, worker in self._workers.items()

        }

    # --------------------------------------------------

    def poll(self):
        """Pump every worker: fire scheduled reconnect attempts and
        apply STALE transitions. One worker's failure (including
        ReconnectExhausted) is isolated and recorded."""

        for symbol, worker in list(self._workers.items()):

            try:

                worker.poll()

            except Exception as error:

                self.errors.append((symbol, "poll", error))

    # --------------------------------------------------

    def stop_all(self):
        """Stop every subscription and close the shared transport."""

        for symbol in list(self._workers):

            try:

                self.unsubscribe(symbol)

            except Exception as error:

                self.errors.append((symbol, "stop", error))

        if self._transport is not None:

            try:

                self._transport.close()

            except Exception as error:

                self.errors.append(("transport", "close", error))

            self._transport = None

        self._instruments = {}
