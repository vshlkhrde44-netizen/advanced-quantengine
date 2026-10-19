"""
====================================================

QuantEngine

Subscription State

Phase 6D -- Real-time multi-symbol subscriptions.

Explicit lifecycle states for a single logical symbol
subscription. The state machine is driven by
SubscriptionWorker; this module only declares the states
and the observable per-subscription metrics so a
production operator can tell WHERE a subscription is in
its lifecycle without inferring anything from implicit
booleans.

    status: SubscriptionStatus
        STOPPED     never subscribed / stopped on purpose
        CONNECTING  transport.subscribe() in flight
        LIVE        subscribed and receiving data
        RECONNECTING  lost the stream, retrying with backoff
        STALE       subscribed but no data within the window
        FAILED      reconnect budget exhausted / fatal error

There is deliberately no `is_connected` boolean: a
subscription that is STALE is still technically connected
to a socket, and reporting that as `True` hides the real
production problem.
====================================================
"""

from dataclasses import dataclass


class SubscriptionStatus:

    STOPPED = "STOPPED"
    CONNECTING = "CONNECTING"
    LIVE = "LIVE"
    RECONNECTING = "RECONNECTING"
    STALE = "STALE"
    FAILED = "FAILED"


# Transition matrix the worker uses to catch programming
# errors early. (status) -> states it may move to next.
_VALID_NEXT = {

    SubscriptionStatus.STOPPED: {

        SubscriptionStatus.CONNECTING,
        SubscriptionStatus.STOPPED,

    },

    SubscriptionStatus.CONNECTING: {

        SubscriptionStatus.LIVE,
        SubscriptionStatus.RECONNECTING,
        SubscriptionStatus.STOPPED,

    },

    SubscriptionStatus.LIVE: {

        SubscriptionStatus.RECONNECTING,
        SubscriptionStatus.STALE,
        SubscriptionStatus.STOPPED,

    },

    SubscriptionStatus.RECONNECTING: {

        SubscriptionStatus.LIVE,
        SubscriptionStatus.CONNECTING,
        SubscriptionStatus.FAILED,
        SubscriptionStatus.STOPPED,

    },

    SubscriptionStatus.STALE: {

        SubscriptionStatus.LIVE,
        SubscriptionStatus.RECONNECTING,
        SubscriptionStatus.STOPPED,

    },

    SubscriptionStatus.FAILED: {

        SubscriptionStatus.CONNECTING,
        SubscriptionStatus.STOPPED,

    },

}


def assert_valid_transition(current, target):

    allowed = _VALID_NEXT.get(current, set())

    if target not in allowed:

        raise ValueError(

            f"invalid subscription transition {current!r} -> {target!r}"

        )


@dataclass(slots=True)
class SubscriptionState:

    """Immutable-by-convention snapshot of one symbol's
    subscription lifecycle and data-integrity counters."""

    symbol: str

    status: str = SubscriptionStatus.STOPPED

    connected_at: float = 0.0

    last_seen_at: float = 0.0

    last_closed_at: float = 0.0

    received: int = 0

    delivered: int = 0

    duplicates: int = 0

    out_of_order: int = 0

    forming_skipped: int = 0

    reconnect_attempts: int = 0

    last_error: str = ""