"""Authoritative-data gate immediately before verified strategy delivery.

This boundary deliberately accepts an already converted runtime candle and an
authoritative normalized market-state snapshot.  It cannot be retrofitted onto
the legacy ticker callback because that callback lacks provenance and quality
state. Future verified socket wiring must use this dispatcher, not bypass it.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from core.history.candle import Candle
from core.risk.market_data_gate import MarketDataGateDecision, evaluate_market_data
from data.delta.market_state_store import AuthoritativeMarketState


@dataclass(frozen=True, slots=True)
class StrategyDispatchRecord:
    allowed: bool
    reason: str
    symbol: str
    candle_timestamp: int | None
    state_venue: str | None
    state_instrument: str | None


class VerifiedStrategyDispatcher:
    def __init__(
        self,
        callback: Callable[[Candle], None], *, require_volume: bool = True,
        on_decision: Callable[[StrategyDispatchRecord], None] | None = None,
    ):
        self.callback = callback
        self.require_volume = require_volume
        self.on_decision = on_decision
        self.delivered = 0
        self.callback_errors = 0
        self._rejected: Counter[str] = Counter()

    def dispatch(
        self, candle: Candle | None, state: AuthoritativeMarketState | None,
    ) -> MarketDataGateDecision:
        """Deliver only closed runtime candles with an allowed data snapshot."""
        decision = evaluate_market_data(state, require_volume=self.require_volume)
        if not decision.allowed:
            self._rejected[decision.reason] += 1
            self._record(decision, candle, state)
            return decision
        if candle is None or not candle.closed:
            decision = MarketDataGateDecision(False, "MARKET_DATA_CANDLE_NOT_CLOSED")
            self._rejected[decision.reason] += 1
            self._record(decision, candle, state)
            return decision
        try:
            self.callback(candle)
        except Exception:
            self.callback_errors += 1
            decision = MarketDataGateDecision(False, "MARKET_DATA_CALLBACK_ERROR")
            self._rejected[decision.reason] += 1
            self._record(decision, candle, state)
            return decision
        self.delivered += 1
        self._record(decision, candle, state)
        return decision

    def _record(
        self, decision: MarketDataGateDecision, candle: Candle | None,
        state: AuthoritativeMarketState | None,
    ) -> None:
        if self.on_decision is None:
            return
        try:
            self.on_decision(StrategyDispatchRecord(
                allowed=decision.allowed,
                reason=decision.reason,
                symbol=getattr(candle, "symbol", "") or "",
                candle_timestamp=getattr(candle, "timestamp", None),
                state_venue=getattr(state, "venue", None),
                state_instrument=getattr(state, "instrument", None),
            ))
        except Exception:
            # Observability must never make an otherwise safe feed executable.
            self.callback_errors += 1
            self._rejected["MARKET_DATA_OBSERVABILITY_ERROR"] += 1

    def stats(self) -> dict[str, object]:
        return {
            "delivered": self.delivered,
            "callback_errors": self.callback_errors,
            "rejected": dict(self._rejected),
        }
