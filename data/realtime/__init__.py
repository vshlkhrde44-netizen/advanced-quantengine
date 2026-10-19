"""
====================================================

QuantEngine

Realtime subscriptions (Phase 6D)

Real-time market data delivered independently to every
eligible instrument pipeline, with explicit subscription
lifecycle states and closed-candle integrity guarantees.

    manager = MultiSymbolSubscriptionManager(...)
    manager.sync(universe)   # ADD / REMOVE / KEEP
    manager.poll()           # retries + staleness pump

No strategy, execution, or portfolio allocation logic
lives here.
====================================================
"""

from data.realtime.candle_router import CandleRouter
from data.realtime.reconnect_policy import ReconnectExhausted
from data.realtime.reconnect_policy import ReconnectPolicy
from data.realtime.stream_health import StreamHealth
from data.realtime.stream_worker import SubscriptionWorker
from data.realtime.subscription_manager import MultiSymbolSubscriptionManager
from data.realtime.subscription_state import SubscriptionState
from data.realtime.subscription_state import SubscriptionStatus
from data.realtime.subscription_state import assert_valid_transition

__all__ = [
    "CandleRouter",
    "MultiSymbolSubscriptionManager",
    "ReconnectExhausted",
    "ReconnectPolicy",
    "StreamHealth",
    "SubscriptionState",
    "SubscriptionStatus",
    "SubscriptionWorker",
    "assert_valid_transition",
]