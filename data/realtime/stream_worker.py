"""
====================================================

QuantEngine

Subscription Worker

Phase 6D -- Real-time multi-symbol subscriptions.

One logical subscription per eligible instrument, driven
by the explicit lifecycle state machine. The worker is
event-driven and non-blocking: no thread sleeps inside it.
Reconnect attempts are scheduled as clock deadlines and
fired by poll(), so one failing subscription can never
stall another (failure isolation).

Transport contract (duck typed):

    transport.subscribe(symbol, on_candle, on_down) -> None
    transport.unsubscribe(symbol)                    -> None
    transport.close()                                -> None

subscribe() raises on failure (retried by this worker);
on_candle(candle) / on_down() are the transport->worker
callbacks. Multiple logical symbols may share one physical
connection: Delta multiplexes symbol subscriptions over a
single websocket, so the transport is a subscription
multiplexer, not a per-symbol socket.

Lifecycle:

    STOPPED --start()--> CONNECTING --ok--> LIVE
    LIVE --on_down()--> RECONNECTING --retry ok--> LIVE
    RECONNECTING --budget exhausted--> FAILED
    LIVE --no data for max_stale--> STALE --tick--> LIVE
====================================================
"""

from data.realtime.reconnect_policy import ReconnectExhausted
from data.realtime.reconnect_policy import ReconnectPolicy
from data.realtime.candle_router import CandleRouter
from data.realtime.stream_health import StreamHealth
from data.realtime.subscription_state import SubscriptionStatus
from data.realtime.subscription_state import SubscriptionState
from data.realtime.subscription_state import assert_valid_transition
import time


class SubscriptionWorker:

    def __init__(

        self,
        symbol,
        transport,
        clock=None,
        policy=None,
        max_stale_seconds=30.0,
        max_dedupe=256,
        on_closed_candle=None,

    ):

        self.symbol = symbol
        self.transport = transport
        self.clock = clock or time.time
        self.policy = policy or ReconnectPolicy()
        self.router = CandleRouter(symbol=symbol, max_dedupe=max_dedupe)
        self.health = StreamHealth(
            max_stale_seconds=max_stale_seconds,
            clock=clock,
        )
        self.on_closed_candle = on_closed_candle

        self.state = SubscriptionState(symbol=symbol)
        self._next_retry = 0.0

        # Phase 11C: downstream (strategy/risk/execution)
        # failures delivered through on_closed_candle are
        # isolated here and counted; they never propagate
        # into the transport callback chain where they could
        # kill the multiplexed pump for every symbol.
        self.callback_errors = 0
        self.last_callback_error = ""

    # --------------------------------------------------

    def _now(self):

        if self.clock is None:

            return 0.0

        return self.clock()

    def _set_status(self, target):

        assert_valid_transition(self.state.status, target)

        self.state.status = target

    def _schedule_retry(self, delay_seconds):

        self._next_retry = self._now() + delay_seconds

    # --------------------------------------------------

    def start(self):
        """STOPPED/FAILED -> CONNECTING -> LIVE. Safe to call
        again from LIVE/STALE/RECONNECTING (no-op: the
        subscription already exists)."""

        if self.state.status in (SubscriptionStatus.LIVE,
                                 SubscriptionStatus.CONNECTING,
                                 SubscriptionStatus.RECONNECTING,
                                 SubscriptionStatus.STALE):
            return

        self._set_status(SubscriptionStatus.CONNECTING)

        try:

            self.transport.subscribe(
                self.symbol,
                self._on_candle,
                self._on_down,
            )

        except Exception as error:

            self.state.reconnect_attempts = 1
            self.state.last_error = str(error)
            self._set_status(SubscriptionStatus.RECONNECTING)
            self._schedule_retry(self.policy.next_delay(attempt=1))
            return

        self.state.connected_at = self._now()
        self.health.touch(now=self._now())
        self.state.reconnect_attempts = 0
        self._set_status(SubscriptionStatus.LIVE)

    # --------------------------------------------------

    def stop(self):
        """Any state -> STOPPED. Unsubscribes from the
        transport (best effort) and cancels pending retries."""

        if self.state.status == SubscriptionStatus.STOPPED:

            return

        self._set_status(SubscriptionStatus.STOPPED)
        self.state.reconnect_attempts = 0
        self._next_retry = 0.0

        try:

            self.transport.unsubscribe(self.symbol)

        except Exception as error:

            self.state.last_error = str(error)

    # --------------------------------------------------

    def _on_down(self):
        """Transport signal: this symbol's stream dropped or the
        underlying connection died. Only a live/current
        subscription reacts; a stopped one ignores the signal."""

        if self.state.status not in (SubscriptionStatus.LIVE,
                                     SubscriptionStatus.STALE):
            return

        self.state.reconnect_attempts = 1
        self.state.last_error = "stream disconnected"
        self._set_status(SubscriptionStatus.RECONNECTING)
        self._schedule_retry(self.policy.next_delay(attempt=1))

    # --------------------------------------------------

    def _on_candle(self, candle):
        """Inbound candle from the transport. Forming ticks keep
        the subscription fresh but never reach strategy logic;
        closed candles pass through the integrity router."""

        if not self.symbol_matches(candle):

            return

        self.health.touch(now=self._now())
        self.state.last_seen_at = self._now()
        self.state.received += 1

        if self.state.status == SubscriptionStatus.STALE:

            self._set_status(SubscriptionStatus.LIVE)

        routed = self.router.route(candle)

        if routed is None:

            # Keep the observable counters honest about *why* a
            # candle was dropped (duplicate / out-of-order /
            # forming) by copying them from the router view.
            self.state.duplicates = self.router.duplicates
            self.state.out_of_order = self.router.out_of_order
            self.state.forming_skipped = self.router.forming_skipped
            return

        self.state.delivered = self.router.delivered
        self.state.last_closed_at = self._now()
        self.state.duplicates = self.router.duplicates
        self.state.out_of_order = self.router.out_of_order

        if self.on_closed_candle is not None:

            try:

                self.on_closed_candle(routed)

            except Exception as error:

                self.callback_errors += 1
                self.last_callback_error = str(error)
                self.state.last_error = (
                    "downstream error: {}".format(error)[:200]
                )


    def symbol_matches(self, candle):
        """A multiplexed transport scopes candles per handler, so a
        symbol field that disagrees with this worker is a routing
        bug worth dropping. Untagged candles (symbol == "") are
        accepted from the per-handler scope."""

        symbol = getattr(candle, "symbol", "")

        if not symbol:

            return True

        return symbol == self.symbol

    # --------------------------------------------------

    def _attempt_reconnect(self):

        if self.policy.should_give_up(attempt=self.state.reconnect_attempts):

            self.state.last_error = "reconnect budget exhausted"
            self._set_status(SubscriptionStatus.FAILED)
            raise ReconnectExhausted(self.symbol)

        try:

            self.transport.subscribe(
                self.symbol,
                self._on_candle,
                self._on_down,
            )

        except Exception as error:

            self.state.reconnect_attempts += 1
            self.state.last_error = str(error)
            self._schedule_retry(
                self.policy.next_delay(attempt=self.state.reconnect_attempts)
            )
            return

        self.state.connected_at = self._now()
        self.health.touch(now=self._now())
        self.state.reconnect_attempts = 0
        self.state.last_error = ""
        self._set_status(SubscriptionStatus.LIVE)

    # --------------------------------------------------

    def poll(self):
        """Clock-driven pump: fires scheduled reconnect attempts
        and applies the STALE transition. Call from the engine's
        cadence loop (or tests, which advance a fake clock)."""

        if self.state.status == SubscriptionStatus.RECONNECTING:

            if self._now() >= self._next_retry:

                self._attempt_reconnect()

        if self.state.status == SubscriptionStatus.LIVE:

            if self.health.stale(now=self._now()):

                self._set_status(SubscriptionStatus.STALE)