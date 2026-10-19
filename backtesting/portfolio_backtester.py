"""
====================================================

QuantEngine

Portfolio Backtester

Runs one canonical Backtester per symbol across a
multi-symbol history dict.

====================================================
"""

from backtesting.backtester import Backtester

from core.backtesting.trade_simulator import TradeSimulator

from core.events.event_bus import EventBus


class PortfolioBacktester:

    def __init__(
        self,
        pipeline_factory,
        risk_manager=None,
        portfolio_state=None,
    ):

        self.pipeline_factory = pipeline_factory

        self.risk_manager = risk_manager

        # Phase 8B: optional unified portfolio state (SharedCapital +
        # risk budget + reservation ledger). When provided, the
        # per-symbol gate/release run through the SHARED state's
        # deterministic allocator lifecycle -- capital is reserved on
        # accept and released on close. When None (default), behavior
        # is unchanged (legacy risk_manager gate).
        self.portfolio_state = portfolio_state

        self.engines = {}

        # symbol -> reservation_id held while that symbol's position
        # is open (single-position-per-symbol keying).
        self._reservation_by_symbol = {}

    # -------------------------------------------------

    def _make_engine(self, pipeline, event_bus):

        simulator = TradeSimulator()

        if self.risk_manager is not None or self.portfolio_state is not None:

            # Wire the shared portfolio cap into each symbol's
            # Backtester. The gate registers exposure when a trade
            # opens; the release frees it when a trade closes.
            # Symbols are replayed sequentially in the order they
            # appear in the run() history dict (the existing
            # architecture), so gating order is dict order.
            return Backtester(
                pipeline,
                event_bus,
                simulator,
                portfolio_gate=self._portfolio_gate,
                portfolio_release=self._portfolio_release,
            )

        return Backtester(
            pipeline,
            event_bus,
            simulator,
        )

    # -------------------------------------------------

    def _portfolio_gate(self, order):

        if self.portfolio_state is not None:

            # Phase 8B: shared-capital allocation lifecycle. Pure
            # decision (can_accept) then bookkeeping (reserve). A
            # declined order reserves nothing and consumes no capital.
            from core.risk.portfolio_state import CapitalCandidate

            candidate = CapitalCandidate.from_order(order)

            if not self.portfolio_state.can_accept(candidate):

                return False

            rid = self.portfolio_state.reserve(candidate)

            symbol = getattr(order, "symbol", None)

            self._reservation_by_symbol[symbol] = rid

            return True

        risk = getattr(order, "risk_amount", 0.0)

        if risk <= 0:

            risk = getattr(order, "expected_loss", 0.0)

        allowed = self.risk_manager.can_open(
            risk,
            symbol=getattr(order, "symbol", None),
            position_value=getattr(
                order, "position_value", 0.0
            ),
        )

        if not allowed:
            return False

        self.risk_manager.register_trade(
            risk,
            symbol=getattr(order, "symbol", None),
            position_value=getattr(
                order, "position_value", 0.0
            ),
        )

        return True

    # -------------------------------------------------

    def _portfolio_release(self, closed_trade):

        if self.portfolio_state is not None:

            # Phase 8B: release the closed trade's reservation,
            # folding its realized PnL into shared equity. A symbol
            # without a reservation (e.g. opened before the state was
            # active, or a stale release) is a harmless no-op.
            symbol = getattr(closed_trade, "symbol", None)

            rid = self._reservation_by_symbol.pop(symbol, None)

            if rid is not None:

                self.portfolio_state.release(
                    rid,
                    pnl=getattr(closed_trade, "pnl", 0.0),
                )

            return

        risk = getattr(closed_trade, "risk_amount", 0.0)

        self.risk_manager.close_trade(
            risk,
            symbol=getattr(closed_trade, "symbol", None),
            pnl=getattr(closed_trade, "pnl", 0.0),
        )

    # -------------------------------------------------

    def add_symbol(self, symbol):

        pipeline, event_bus = self.pipeline_factory(symbol)

        engine = self._make_engine(pipeline, event_bus)

        self.engines[symbol] = engine

        return engine

    # -------------------------------------------------

    def register(self, symbol, module=None):

        # Kept for interface compatibility with legacy
        # callers. The canonical Backtester has no per-engine
        # plug-in registry, so a module is only accepted (and
        # ignored) when the symbol already has an engine;
        # otherwise one is created with the configured factory.
        if symbol not in self.engines:
            self.add_symbol(symbol)

        return self.engines[symbol]

    # -------------------------------------------------

    def run(self, history, journal=None):

        if self.risk_manager is not None or self.portfolio_state is not None:

            # Each run is a fresh portfolio: any exposure left from
            # trades still open when a previous run ended must not
            # leak into the next run. Backtester.reset() recreates
            # each symbol's account and simulator, so the shared
            # cap/state is reset to match.
            if self.risk_manager is not None:

                self.risk_manager.reset()

            if self.portfolio_state is not None:

                self.portfolio_state.reset()

                self._reservation_by_symbol.clear()

        processed = {}

        for symbol, bars in history.items():

            if symbol not in self.engines:

                self.add_symbol(symbol)

            processed[symbol] = self.engines[symbol].run(
                bars, journal=journal,
            )

        return processed