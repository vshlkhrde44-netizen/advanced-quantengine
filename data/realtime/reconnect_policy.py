"""
====================================================

QuantEngine

Reconnect Policy

Phase 6D -- Real-time multi-symbol subscriptions.

Exponential backoff for a single subscription's reconnect
loop. Deterministic by default (jitter disabled) so tests
and production can reason about exact deadlines.

    policy = ReconnectPolicy(base_delay=1.0, factor=2.0,
                             max_delay=60.0)
    policy.next_delay(attempt=1)      # 1.0
    policy.next_delay(attempt=2)      # 2.0
    policy.next_delay(attempt=3)      # 4.0
    policy.should_give_up(attempt)    # False

`max_attempts` caps the retry budget; when exhausted the
worker marks the subscription FAILED instead of looping
forever.
====================================================
"""


class ReconnectExhausted(Exception):

    """Raised by SubscriptionWorker when a subscription's
    reconnection budget is exhausted."""


class ReconnectPolicy:

    def __init__(

        self,
        base_delay=1.0,
        factor=2.0,
        max_delay=60.0,
        max_attempts=None,
        jitter=0.0,
        random=None,

    ):

        self.base_delay = base_delay
        self.factor = factor
        self.max_delay = max_delay
        self.max_attempts = max_attempts
        self.jitter = jitter
        self._random = random

    def next_delay(self, attempt):
        """Backoff deadline for the given 1-indexed attempt.
        First attempt returns base_delay; each further attempt
        multiplies until `max_delay`."""

        delay = self.base_delay * (self.factor ** (attempt - 1))

        delay = min(delay, self.max_delay)

        if self.jitter > 0.0 and self._random is not None:

            delay += self._random.uniform(0.0, self.jitter)

        return delay

    def should_give_up(self, attempt):
        """True once the subscription has used its retry budget."""

        if self.max_attempts is None:

            return False

        return attempt >= self.max_attempts