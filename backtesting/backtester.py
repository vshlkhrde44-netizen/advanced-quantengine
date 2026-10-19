"""
====================================================

QuantEngine

Canonical Institutional Backtester

Architecture Version 4.0

The Backtester is ONLY responsible for replaying
historical candles through the Event Bus.

Pipeline execution is entirely event-driven.

====================================================
"""

from config import risk_settings

from core.events.candle_event import CandleEvent
from core.common.logging_config import get_logger
from core.execution.entry_mode import active_entry_modes
from core.execution.entry_model import (
    EXECUTION_LIMIT,
    EXECUTION_NEXT_BAR_OPEN,
    ENTRY_MODE_MARKET_CONFIRMATION,
    BreakoutContinuationEntryModel,
    get_entry_model,
)
from core.risk.account import Account
from core.execution.order_registry import FILLED
from core.backtesting.accounting import (
    risk_reward_allowed,
    directional_geometry_ok,
    order_rr,
    realized_rr,
)
from core.risk.risk_engine import kill_switch_active

log = get_logger(__name__)


class Backtester:
    """
    Canonical event-driven replay engine.

        Historical Candle
                │
                ▼
            CandleEvent
                │
                ▼
             EventBus
                │
                ▼
      Institutional Pipeline
                │
                ▼
        Decision / Execution
                │
                ▼
           Risk Engine
                │
                ▼
         Trade Simulator
                │
                ▼
          Account Booking
    """

    def __init__(
        self,
        pipeline,
        event_bus,
        simulator,
        account=None,
        portfolio_gate=None,
        portfolio_release=None,
        cost_spec=None,
        funding_schedule=None,
    ):

        self.pipeline = pipeline

        self.event_bus = event_bus

        self.simulator = simulator

        if funding_schedule is not None:
            from core.execution.economics import FundingSchedule
            if not isinstance(funding_schedule, FundingSchedule):
                raise TypeError("funding_schedule must be a FundingSchedule")
            self.simulator.funding_schedule = funding_schedule
        self.funding_schedule = funding_schedule

        # Optional portfolio-level risk hooks (see
        # PortfolioBacktester). portfolio_gate(order) -> bool is
        # consulted right before a new trade would open; a False
        # verdict drops the order. portfolio_release(closed_trade)
        # is invoked after a closed trade is booked, so a portfolio
        # cap can release its exposure. Both default to None and are
        # then never called -- behavior is unchanged without a gate.
        self.portfolio_gate = portfolio_gate

        self.portfolio_release = portfolio_release

        # Phase 4 verified correction (opt-in cost seam): when a
        # CostSpec-like object (`.compute(position_value) ->
        # (commission, slippage)`) is supplied, breakout-resized
        # fills re-mint costs from the resized notional so the
        # booked trade cannot carry setup-quantity costs. When None
        # (default) order costs pass through verbatim -- legacy
        # behavior is byte-identical.
        # When no explicit backtest cost spec is supplied, consume the
        # canonical production cost authority if it is configured with
        # non-zero economics.  Preserve the explicit-zero legacy baseline
        # as ``None`` so hand-authored order costs remain byte-identical
        # under the documented zero-cost mode.
        if cost_spec is None:
            from core.risk.costs import from_settings
            configured_costs = from_settings()
            if (configured_costs.per_trade != 0.0
                    or configured_costs.rate != 0.0
                    or configured_costs.slippage_bps != 0.0):
                self.cost_spec = configured_costs
            else:
                self.cost_spec = None
        else:
            self.cost_spec = cost_spec

        self._starting_balance = (
            account.state.starting_balance
            if account is not None
            else risk_settings.STARTING_BALANCE
        )

        self.account = (
            account
            if account is not None
            else Account(
                starting_balance=self._starting_balance
            )
        )

        self.total_bars = 0

        # Phase 4E pending-order capacity metrics (regression
        # report): average + maximum live pending orders per bar.
        self._pending_max = 0

        self._pending_sum = 0

        self._pending_samples = 0

        # Phase 4F STEP 8 explanation: the bar (and the per-direction
        # breakdown AT that bar) on which the pending-order maximum
        # was measured, so the report can DEBATE WHY the funnel
        # peaked there.
        self._pending_max_bar = 0

        self._pending_max_directions = {}

    # ==================================================
    # Lifecycle
    # ==================================================

    def reset(self):

        self.total_bars = 0

        self._pending_max = 0

        self._pending_sum = 0

        self._pending_samples = 0

        self._pending_max_bar = 0

        self._pending_max_directions = {}

        self.pipeline.reset()

        self.simulator.reset()

        # Phase 11B: reset IN PLACE so the caller's Account
        # object (passed at construction) is the one booked
        # into -- replacing it here used to leave callers
        # reading a stale, never-booked account.
        self.account.reset(self._starting_balance)

    # ==================================================
    # Replay
    # ==================================================

    def run(
        self,
        candles,
        journal=None,
    ):

        self.reset()

        context = self.pipeline.context

        for candle in candles:

            # ---------------------------------------------
            # New pipeline cycle
            # ---------------------------------------------

            context.reset_cycle()

            # ---------------------------------------------
            # Update market state
            # ---------------------------------------------

            context.market.timestamp = candle.timestamp

            context.market.symbol = getattr(
                candle,
                "symbol",
                context.market.symbol,
            )

            context.market.timeframe = getattr(
                candle,
                "timeframe",
                context.market.timeframe,
            )

            context.market.mark_price = candle.close

            context.market.best_bid = candle.close

            context.market.best_ask = candle.close

            context.market.spread = 0.0

            # ---------------------------------------------
            # Publish the account's live state into the
            # pipeline context so RiskEngine sizes from the
            # current equity, never a default AccountState.
            # ---------------------------------------------

            context.account = self.account.state

            # ---------------------------------------------
            # Publish Candle Event
            # publish() performs dispatch().
            # ---------------------------------------------

            self.event_bus.publish(

                CandleEvent(

                    candle=candle,

                    symbol=context.market.symbol,

                    timeframe=context.market.timeframe,

                )

            )

            # ---------------------------------------------
            # Pending-order pass (Phase 4D STEP 4): for every
            # eligible pending order -- eligibility (no same-candle
            # look-ahead) -> invalidation (structural stop breach)
            # -> entry touch fill. Runs AFTER the pipeline created
            # this candle's orders but BEFORE open positions are
            # managed.
            # ---------------------------------------------

            self._execute_signal(candle)

            before = self.simulator.registry.count()

            self.simulator.update(
                candle,
            )

            self._book_closed_trades(before)

            # ---------------------------------------------
            # Phase 4D STEP 4 (10) / STEP 5: expire at the very
            # END of the candle processing pass, bar-anchored. A
            # pending order whose current bar >= expires_at_bar is
            # terminaled here -- AFTER this candle's fill and
            # invalidation checks, so the expiry candle itself is
            # still the fill's last chance. The ORDER_EXPIRED
            # diagnostics carry the full bar metadata.
            # ---------------------------------------------

            order_registry = getattr(
                context, "order_registry", None
            )

            if order_registry is not None:

                expired = order_registry.advance(
                    bar=context.current_index,
                    timestamp=candle.timestamp,
                )

                for order in expired:

                    # Phase 4E excursion diagnostics: the expiry
                    # log must reflect the order's WHOLE eligible
                    # lifetime (init on the first eligible candle,
                    # rolled every eligible candle) so the reader
                    # can verify whether the market ever reached
                    # the entry. entry_distance_from_nearest_price
                    # is 0.0 whenever min_low <= entry <= max_high,
                    # otherwise the distance to the nearer extreme.
                    min_low = getattr(
                        order, "min_low_since_creation", None
                    )

                    max_high = getattr(
                        order, "max_high_since_creation", None
                    )

                    entry = float(getattr(order, "entry", 0.0))

                    entry_distance = None

                    if (
                        min_low is not None
                        and max_high is not None
                    ):

                        if min_low <= entry <= max_high:

                            entry_distance = 0.0

                        else:

                            entry_distance = min(
                                abs(entry - min_low),
                                abs(entry - max_high),
                            )

                    order.entry_distance_from_nearest_price = (
                        entry_distance
                    )

                    log.info(
                        "ORDER_EXPIRED setup_id=%s order_id=%s "
                        "created_bar=%s eligible_from_bar=%s "
                        "expired_bar=%s waited_bars=%s "
                        "max_wait_bars=%s entry=%.2f "
                        "min_low_since_creation=%s "
                        "max_high_since_creation=%s "
                        "bars_pending=%s "
                        "entry_distance_from_nearest_price=%s",
                        getattr(order, "setup_id", "") or "",
                        getattr(order, "order_id", "") or "",
                        getattr(order, "created_bar", None),
                        getattr(order, "eligible_from_bar", None),
                        context.current_index,
                        getattr(order, "waited_bars", 0),
                        order_registry.max_wait_bars,
                        entry,
                        min_low,
                        max_high,
                        getattr(order, "bars_pending", 0),
                        entry_distance,
                    )

            # ---------------------------------------------
            # Phase 4F STEP 9: reconcile the economic lifecycle
            # against terminal orders (a fingerprint that just
            # EXPIRED / INVALIDATED enters its re-entry COOLDOWN
            # before the next candle's structural discoveries are
            # funneled). Then Phase 4F STEP 3: roll the entry
            # tracker's post-creation retracement / continuation
            # measurements. Both are DIAGNOSTIC -- they never
            # influence orders or fills.
            # ---------------------------------------------

            economic_registry = getattr(
                context, "economic_registry", None
            )

            if (
                economic_registry is not None
                and order_registry is not None
            ):

                economic_registry.sync_lifecycles(
                    order_registry,
                    bar=context.current_index,
                )

            entry_tracker = getattr(
                context, "entry_tracker", None
            )

            if entry_tracker is not None:

                entry_tracker.advance(
                    candle,
                    bar=context.current_index,
                    order_registry=order_registry,
                )

            # Phase 4G extension STEP: roll the entry shadow's three-model
            # comparison (LIMIT_RETEST / MARKET_CONFIRMATION /
            # BREAKOUT_CONTINUATION) on the same one-candle cadence
            # as the tracker. DIAGNOSTIC ONLY -- gated on
            # ENABLE_ENTRY_SHADOW, never consulted by an engine.
            entry_shadow = getattr(
                context, "entry_shadow", None
            )

            if entry_shadow is not None:

                entry_shadow.advance(
                    candle,
                    bar=context.current_index,
                    order_registry=order_registry,
                )

            # Phase 4H STEP: roll the CausalValidator (frozen-setup
            # breakout validation + MARKET_CONFIRMATION rejection
            # diagnosis) on the same one-candle cadence as the shadow.
            # DIAGNOSTIC ONLY -- gated on ENABLE_CAUSAL_VALIDATION,
            # never consulted by an engine, and produces NO order.
            causal_validation = getattr(
                context, "causal_validation", None
            )

            if causal_validation is not None:

                causal_validation.advance(
                    candle,
                    bar=context.current_index,
                    order_registry=order_registry,
                )

            self.total_bars += 1

            # ---------------------------------------------
            # Phase 4E pending-order capacity metrics (for the
            # regression report): average / maximum live pending
            # orders, measured right after the expiry pass so the
            # current candle's terminal events have settled.
            # ---------------------------------------------

            if order_registry is not None:

                live = len(order_registry.pending())

                if live > self._pending_max:

                    self._pending_max = live

                    self._pending_max_bar = context.current_index

                    self._pending_max_directions = {}

                    for o in order_registry.pending():

                        d = (o.direction or "").upper()

                        self._pending_max_directions[d] = (
                            self._pending_max_directions.get(d, 0)
                            + 1
                        )

                self._pending_sum += live

                self._pending_samples += 1

            # ---------------------------------------------
            # Phase 9A research journal: OBSERVER OF THE
            # SETTLED BAR. When a ResearchJournal is attached,
            # read the authoritative context + trade registry
            # and append research events. Purely observational --
            # default None keeps behaviour identical; failures are
            # swallowed + logged by the journal itself (never
            # raises into the trading path).
            # ---------------------------------------------
            if journal is not None:

                journal.on_bar_observed(
                    context,
                    candle,
                    registry=self.simulator.registry,
                    provenance="BACKTEST",
                    bar=context.current_index,
                )

        if journal is not None:
            journal.finalize()

        return self.simulator.registry

    # ==================================================
    # Signal -> Order
    # ==================================================

    def _execute_signal(
        self,
        candle,
    ):

        context = self.pipeline.context

        bar = context.current_index

        order_registry = getattr(
            context, "order_registry", None
        )

        # Registry-managed pending orders own the primary path
        # (Phase 4D STEP 4: eligibility -> invalidation -> fill).
        # Phase 4E: this pass runs EVERY candle -- including while
        # a position is open -- so pending orders keep rolling
        # their lifetime excursion (min/max since creation) and
        # their invalidation checks; only the FILL is gated on the
        # single-position model. (Before, _execute_signal returned
        # immediately when a trade was open, freezing every pending
        # order's excursion at whatever the last candle recorded --
        # the source of identical stale min/max diagnostics.)
        if order_registry is not None:

            if self._process_pending_orders(
                candle, order_registry, bar
            ):
                return

            # No registered order filled this candle. Fall through
            # to the legacy branch for scripted orders that carry
            # no registered order_id (the legacy harness scripts
            # orders onto a real PipelineContext whose registry is
            # simply unused by them).

        # Single-position model: the legacy scripted path may only
        # open a trade when no position is already live.
        if self.simulator.current_trade is not None:

            return

        # =====================================================
        # Legacy scripted path (no persistent registry): the
        # risk-validated order / execution signal produced this
        # candle fills on the SAME candle it was authored for,
        # exactly as pre-Phase-4C. Scripted candles in the legacy
        # test harness are authored around the order, so there is
        # no bias to remove here -- the no-look-ahead eligibility
        # delay applies only to the registry-managed lifecycle.
        # =====================================================

        order = getattr(
            context,
            "order",
            None,
        )

        if order is None:

            order = getattr(
                context,
                "execution_signal",
                None,
            )

        if order is None:

            return

        # A registered order with an order_id is exclusively
        # handled by the pending-order pass above -- it can never
        # also fill through this legacy branch (consume-once).
        order_id = getattr(order, "order_id", "") or ""

        if order_registry is not None and order_id:

            return

        # Phase 12 legacy-path gate: scripted orders that carry
        # no registered order_id historically bypassed every
        # safety check (RR, kill switch, geometry). The
        # canonical reported backtest must never silently use
        # an ungated path, so the mandatory gates run here
        # too -- a hand-injected order that fails them simply
        # never fills. (Exposure/portfolio gating below is
        # unchanged.)
        if kill_switch_active(
            getattr(self.account, "state", None)
        ):

            log.error(
                "LEGACY_FILL_BLOCKED_KILL_SWITCH setup_id=%s",
                getattr(order, "setup_id", "") or "",
            )

            return

        if not directional_geometry_ok(
            getattr(order, "direction", ""),
            getattr(order, "entry", 0.0),
            getattr(order, "stop_loss", 0.0),
            getattr(order, "take_profit", 0.0),
        ):

            log.error(
                "LEGACY_FILL_BLOCKED_GEOMETRY setup_id=%s",
                getattr(order, "setup_id", "") or "",
            )

            return

        legacy_rr = order_rr(
            getattr(order, "entry", 0.0),
            getattr(order, "stop_loss", 0.0),
            getattr(order, "take_profit", 0.0),
        )

        exit_mode = getattr(order, "exit_mode", risk_settings.EXIT_MODE_FIXED_TARGET)
        if (
            legacy_rr is None
            or not risk_reward_allowed(legacy_rr, exit_mode=exit_mode,
                                       symbol=getattr(order, "symbol", None))
        ):

            log.error(
                "LEGACY_FILL_BLOCKED_RR setup_id=%s rr=%s",
                getattr(order, "setup_id", "") or "",
                legacy_rr,
            )

            return

        if not self._candle_touches_entry(candle, order):

            return

        if self.portfolio_gate is not None and not self.portfolio_gate(order):

            return

        trade = self.simulator.open_trade(
            order,
            candle.timestamp,
            quantity=getattr(order, "quantity", 1.0),
            commission=getattr(order, "commission", 0.0),
            slippage=getattr(order, "slippage", 0.0),
        )

        if trade is not None:

            self._log_order_filled(order, None, candle, bar)

    # ==================================================
    # Pending-order pass (Phase 4D STEP 4)
    # ==================================================

    def _process_pending_orders(
        self,
        candle,
        order_registry,
        bar,
    ):

        """
        Per-candle order processing, executed in the documented
        order for every registry-managed pending order:

            1. skip ineligible -- a PENDING order may only be
               checked on bars in
               [eligible_from_bar, expires_at_bar]; on the creation
               candle itself it is skipped (no same-candle
               look-ahead). Excursion tracking never includes the
               creation candle.
            2. update pending  -- Phase 4E lifetime excursion:
               initialized on the FIRST eligible candle from this
               candle's range, then rolled every eligible candle
               (min_low_since_creation = min(..., candle.low);
               max_high_since_creation = max(..., candle.high);
               bars_pending += 1). Runs even while a position is
               open -- the diagnostics must reflect the WHOLE
               eligible lifetime, not the last candle before a
               position opened.
            3. invalidation    -- a bullish order invalidates when
               candle.low <= stop_loss (bearish: candle.high >=
               stop_loss). The structural stop IS the invalidation
               level, so a thesis break is INVALIDATED, never
               counted as an expiration. Same-candle ambiguity
               resolves to the conservative side: invalidation
               before entry-fill, so a candle that both reaches
               entry and breaks the stop does NOT fill;
            4. entry reached   -- low <= entry <= high (inclusive);
            5. fill            -- gated on the single-position
               model (at most one live trade, at most one PENDING
               order fills per candle); the order is consumed
               exactly once via mark_filled.

        Returns True when a fill happened this candle.
        """

        filled = False

        low = getattr(candle, "low", None)

        high = getattr(candle, "high", None)

        for order in sorted(
            order_registry.pending(),
            key=lambda o: -float(getattr(o, "score", 0.0)),
        ):

            # ---- 1. skip ineligible ----------------------------

            eligible_from = getattr(
                order, "eligible_from_bar", None
            )

            if (
                eligible_from is not None
                and bar < eligible_from
            ):

                continue

            # ---- 2. lifetime excursion (Phase 4E) -------------
            #
            # Eligible candle only: the creation candle is
            # ineligible under the no-look-ahead rule and must
            # contribute nothing to the highs/lows. Init on the
            # first eligible candle, then roll every eligible
            # candle -- never computed only at expiry.

            if low is not None and high is not None:

                order.bars_pending = (
                    getattr(order, "bars_pending", 0) + 1
                )

                minim = getattr(
                    order, "min_low_since_creation", None
                )

                order.min_low_since_creation = (
                    min(minim, low)
                    if minim is not None
                    else low
                )

                maxim = getattr(
                    order, "max_high_since_creation", None
                )

                order.max_high_since_creation = (
                    max(maxim, high)
                    if maxim is not None
                    else high
                )

            # ---- 3. invalidation -------------------------------

            if low is not None or high is not None:

                stop = float(getattr(order, "stop_loss", 0.0))

                if getattr(
                    order, "direction", ""
                ).upper() == "BEARISH":

                    breached = (
                        high is not None and high >= stop
                    )

                    reason = (
                        "bearish structural stop (OB high) "
                        "breached"
                    )

                    price = high

                else:

                    breached = (
                        low is not None and low <= stop
                    )

                    reason = (
                        "bullish structural stop (OB low) "
                        "breached"
                    )

                    price = low

                if breached:

                    order_registry.invalidate(
                        order,
                        reason=reason,
                        bar=bar,
                        price=price,
                        level=stop,
                    )

                    log.info(
                        "ORDER_INVALIDATED setup_id=%s "
                        "order_id=%s created_bar=%s "
                        "eligible_from_bar=%s invalidated_bar=%s "
                        "reason=%s price=%s "
                        "invalidation_level=%.2f",
                        getattr(order, "setup_id", "") or "",
                        getattr(order, "order_id", "") or "",
                        getattr(order, "created_bar", None),
                        getattr(order, "eligible_from_bar", None),
                        bar,
                        reason,
                        price,
                        stop,
                    )

                    continue

            # ---- 3b. breakout continuation (Phase 5B) -------
            #
            # A NEXT_BAR_OPEN order (BREAKOUT_CONTINUATION) never
            # fills through the resting-limit touch below. It is
            # decided by the canonical BreakoutContinuationEntryModel
            # under the Phase 4H causal rules:
            #
            #   RULE 2 -- trigger at close on the (eligible) trigger
            #       candle: BULLISH high > E AND close > E / BEARISH
            #       low < E AND close < E. The structural stop breach
            #       already ran above (3.), so a same-bar breach
            #       invalidates instead of triggering -- the exact
            #       backtester step-3-before-step-4 order.
            #   RULE 3 -- a trigger at bar N fills at the OPEN of
            #       bar N+1 (market fill, no range check, executes
            #       even when N+1 gaps against the position).
            #
            # The trigger decision is evaluated once per eligible
            # candle and stamped onto the order; the fill consumes
            # the order exactly once on the entry bar.

            if (
                getattr(order, "entry_execution", "")
                == EXECUTION_NEXT_BAR_OPEN
            ):

                if getattr(order, "breakout_triggered", False):

                    if (
                        getattr(order, "breakout_trigger_bar", None)
                        is not None
                        and bar
                        == order.breakout_trigger_bar + 1
                    ):

                        entry_price = self._next_bar_open_price(
                            candle
                        )

                        # Erosion gate (configurable active policy): the
                        # market fill moved the entry AFTER the creation
                        # gates ran. An actual executable geometry below
                        # the ACTIVE minimum-R policy must not enter the
                        # trading path: the fill is refused before the
                        # order is resized or restamped (the frozen setup
                        # snapshot stays intact and the order keeps riding
                        # toward expiry/invalidation) and the erosion is
                        # logged. Stops/targets are never moved to
                        # manufacture compliance. No hardcoded floor: the
                        # bound is trading_policy() for this symbol
                        # (global or exact-instrument override).
                        # Phase 13 fix (gap-semantics hole): the
                        # unsigned ratio alone cannot see a fill
                        # that gapped THROUGH the stop (LONG fill
                        # at/below stop, SHORT at/above stop) yet
                        # still reads above the minimum -- an inverted
                        # fill that books perverse economics. The
                        # directional geometry check runs FIRST.
                        if not directional_geometry_ok(
                            getattr(order, "direction", ""),
                            entry_price,
                            float(order.stop_loss or 0.0),
                            float(order.take_profit or 0.0),
                        ):

                            log.error(
                                "ORDER_SKIPPED_INVERTED_FILL "
                                "setup_id=%s order_id=%s "
                                "entry=%.2f stop=%.2f target=%.2f",
                                getattr(order, "setup_id", "") or "",
                                getattr(order, "order_id", "") or "",
                                entry_price,
                                float(order.stop_loss or 0.0),
                                float(order.take_profit or 0.0),
                            )

                            continue

                        actual_rr = order_rr(
                            entry_price,
                            float(order.stop_loss or 0.0),
                            float(order.take_profit or 0.0),
                        )

                        # BREAKOUT_CONTINUATION fills through this gate under
                        # the same active minimum-R policy as every other
                        # path: the actual executable entry must satisfy
                        # trading_policy() for the order's symbol after
                        # the next-bar-open move.
                        from core.risk.trading_policy import trading_policy
                        order_symbol = getattr(order, "symbol", "")
                        active_policy = trading_policy(order_symbol)
                        policy_min_rr = float(active_policy.min_rr)
                        policy_max_rr = float(active_policy.max_rr)
                        if actual_rr is None or not risk_reward_allowed(
                            actual_rr, exit_mode="FIXED_TARGET", symbol=order_symbol
                        ):

                            log.error(
                                "ORDER_SKIPPED_RR_EROSION "
                                "setup_id=%s order_id=%s "
                                "actual_rr=%s min_rr=%.2f max_rr=%.2f",
                                getattr(order, "setup_id", "") or "",
                                getattr(order, "order_id", "") or "",
                                actual_rr,
                                policy_min_rr,
                                policy_max_rr,
                            )

                            continue

                        self._resize_for_actual_entry(order, entry_price)

                        order.entry = entry_price

                        # Stage 4: the market fill moved the entry
                        # AFTER the creation gates ran (decision + risk
                        # revalidation both saw setup prices). Stamp
                        # the REALIZED RR truthfully (signed by side
                        # via accounting.realized_rr):
                        # `risk_reward` keeps the gated order-time
                        # value; `actual_risk_reward` records what
                        # the fill gave.
                        order.actual_risk_reward = realized_rr(
                            getattr(order, "direction", ""),
                            entry_price,
                            float(order.stop_loss or 0.0),
                            float(order.take_profit or 0.0),
                        )

                        order.terminal_bar = bar

                        order.filled_bar = bar

                        order.waited_bars = max(
                            bar
                            - (
                                getattr(
                                    order, "eligible_from_bar", None
                                )
                                or bar
                            ),
                            0,
                        )

                        log.info(
                            "BREAKOUT_ENTRY setup_id=%s order_id=%s "
                            "triggered_bar=%s trigger_close=%.2f "
                            "entry_bar=%s entry_open=%.2f "
                            "actual_risk=%.2f realized_rr=%.4f",
                            getattr(order, "setup_id", "") or "",
                            getattr(order, "order_id", "") or "",
                            getattr(order, "breakout_trigger_bar", None),
                            float(
                                getattr(
                                    order,
                                    "breakout_trigger_close",
                                    0.0,
                                )
                                or 0.0
                            ),
                            bar,
                            entry_price,
                            abs(entry_price - order.stop_loss),
                            float(getattr(order, "actual_risk_reward",
                                          0.0) or 0.0),
                        )

                        # Fall through to the mode gate + fill so the
                        # single-position, active-mode and portfolio
                        # gates still apply to this market fill.
                    else:

                        # Trigger already stamped but the fill bar is
                        # still ahead: keep the order pending.
                        continue

                else:

                    # Not yet triggered: decide this candle with the
                    # canonical model. The order itself is the frozen
                    # setup snapshot (identity + economics carried by
                    # RiskEngine).
                    model = get_entry_model(
                        getattr(order, "entry_mode", "")
                    )

                    if not isinstance(
                        model, BreakoutContinuationEntryModel
                    ):

                        model = BreakoutContinuationEntryModel()

                    decision = model.decide(
                        order, candle, bar, timestamp=candle.timestamp
                    )

                    if not getattr(decision, "triggered", False):

                        continue

                    order.breakout_triggered = True

                    order.breakout_trigger_bar = bar

                    order.breakout_trigger_close = float(
                        getattr(candle, "close", None)
                        or getattr(order, "entry", 0.0)
                    )

                    log.info(
                        "BREAKOUT_TRIGGER setup_id=%s order_id=%s "
                        "bar=%s direction=%s entry_level=%.2f "
                        "close=%.2f stop=%.2f target=%.2f",
                        getattr(order, "setup_id", "") or "",
                        getattr(order, "order_id", "") or "",
                        bar,
                        getattr(order, "direction", "") or "",
                        float(getattr(order, "entry", 0.0) or 0.0),
                        order.breakout_trigger_close,
                        float(getattr(order, "stop_loss", 0.0) or 0.0),
                        float(getattr(order, "take_profit", 0.0) or 0.0),
                    )

                    continue

            # ---- 4. entry reachable ----------------------------

            if not self._candle_touches_entry(candle, order):

                continue

            # ---- 4a. canonical trigger gate (Phase 5C) ------
            #
            # MARKET_CONFIRMATION enters through the resting-limit
            # channel (execution_type_for == LIMIT) but must NOT
            # fill on a mere touch: its canonical model requires
            # the trigger candle to CLOSE BEYOND the reference
            # level (reached AND closed beyond E). Without this
            # gate an active MARKET_CONFIRMATION run would silently
            # execute as LIMIT_RETEST -- exactly the research-vs-
            # production mismatch Phase 5C forbids. LIMIT_RETEST
            # (pure resting touch) is untouched.

            # PHASE 5C: a mode-less order (orders created outside
            # the canonical router) is anchored to the LIMIT_RETEST
            # baseline, NEVER silently re-mapped to the currently
            # configured ENTRY_MODE -- routing a stranger into a
            # different execution model would be the exact invisible
            # substitution the router contract forbids.
            mode = getattr(
                order, "entry_mode", ""
            ) or "LIMIT_RETEST"

            if mode == ENTRY_MODE_MARKET_CONFIRMATION:

                if not self._market_confirmation_decided(
                    order, candle, bar
                ):

                    continue

            # ---- 4b. entry-mode gate (Phase 4F STEP 7) ------
            #
            # Only ACTIVE entry modes may actually fill. An order
            # whose (undeployed) mode is not active stays PARKED:
            # its excursions and invalidation check still roll every
            # eligible candle, but the fill primitive is refused --
            # honest diagnostics for a model that is being measured,
            # not executed.

            if mode not in active_entry_modes():

                continue

            # ---- 5. fill (single position, exactly once) -------

            # Phase 4E: while a trade is open no further fill may
            # open a second position; the pending order simply
            # keeps riding (excursions + invalidation above) until
            # the position closes or it expires/invalidates.
            if self.simulator.current_trade is not None:

                continue

            if self.portfolio_gate is not None and not self.portfolio_gate(order):

                continue

            # Phase 12 kill-switch coverage (existing
            # exposure): an active kill switch blocks pending
            # fills as well as creation. Excursions and
            # invalidation above still roll (diagnostics stay
            # honest); only the fill is refused. The order
            # keeps riding toward expiry/invalidation while
            # the switch is active.
            if kill_switch_active(
                getattr(self.account, "state", None)
            ):

                log.error(
                    "PENDING_FILL_BLOCKED_KILL_SWITCH "
                    "setup_id=%s order_id=%s",
                    getattr(order, "setup_id", "") or "",
                    getattr(order, "order_id", "") or "",
                )

                continue

            trade = self.simulator.open_trade(
                order,
                candle.timestamp,
                quantity=getattr(order, "quantity", 1.0),
                commission=getattr(order, "commission", 0.0),
                slippage=getattr(order, "slippage", 0.0),
            )

            if trade is None:

                continue

            order_registry.mark_filled(order, trade)

            order.trade_id = getattr(trade, "trade_id", None)

            order.filled_bar = bar

            order.terminal_bar = bar

            order.waited_bars = max(
                bar - (getattr(order, "eligible_from_bar", None) or bar),
                0,
            )

            setup = getattr(order, "setup", None)

            if setup is not None and getattr(
                setup, "lifecycle", ""
            ) in ("ORDER_CREATED", "ACCEPTED", "NEW", ""):

                setup.lifecycle = FILLED

            self._log_order_filled(order, trade, candle, bar)

            filled = True

            # Phase 4E: do NOT return here. At most one order MAY
            # fill per candle (the single-position gate now blocks
            # every later order), but the REMAINING pending orders
            # must still roll their lifetime excursion and their
            # invalidation check on this candle -- otherwise a fill
            # would freeze every other pending order's diagnostics
            # for one candle.

            continue

        return filled

    # ==================================================
    # Fill diagnostics (Phase 4D STEP 7)
    # ==================================================

    def _log_order_filled(self, order, trade, candle, bar):

        setup_id = getattr(order, "setup_id", "") or ""

        order_id = getattr(order, "order_id", "") or ""

        trade_id = (
            getattr(trade, "trade_id", None)
            if trade is not None
            else None
        )

        low = getattr(candle, "low", None)

        high = getattr(candle, "high", None)

        eligible_from = getattr(order, "eligible_from_bar", None)

        created_bar = getattr(order, "created_bar", None)

        ok = ""

        if (
            order_id
            and created_bar is not None
            and eligible_from is not None
        ):

            # prove low <= entry <= high on the fill bar.
            ok = " ok" if (
                low is not None
                and high is not None
                and low <= order.entry <= high
            ) else ""

        log.info(
            "ORDER_FILLED setup_id=%s order_id=%s "
            "trade_id=%s created_bar=%s filled_bar=%s "
            "waited_bars=%s entry=%.2f candle_low=%s "
            "candle_high=%s ts=%s%s",
            setup_id,
            order_id,
            trade_id or "",
            created_bar,
            bar,
            getattr(order, "waited_bars", 0),
            float(getattr(order, "entry", 0.0)),
            low,
            high,
            candle.timestamp,
            ok,
        )

    # ==================================================
    # Entry Validation (Phase 4C Step 8)
    # ==================================================

    def _candle_touches_entry(self, candle, order):

        entry = getattr(order, "entry", 0.0)

        high = getattr(candle, "high", None)

        low = getattr(candle, "low", None)

        # Unspecified candle range: no OHLC to validate against
        # (e.g. pure tick tests) -- treat as a touch so legacy
        # behavior is unchanged outside the backtester.
        if low is None or high is None:

            return True

        if entry <= 0:

            return True

        return low <= entry <= high

    # --------------------------------------------------

    def _market_confirmation_decided(self, order, candle, bar):
        """
        Phase 5C canonical trigger gate for MARKET_CONFIRMATION:
        the order fills through the resting channel ONLY on a
        candle whose range crossed the reference level AND whose
        close is beyond it in the trade's direction -- the exact
        MarketConfirmationEntryModel semantics. Anything else
        keeps the order parked (measurement, not a LIMIT fill).
        """

        model = get_entry_model(
            ENTRY_MODE_MARKET_CONFIRMATION
        )

        decision = model.decide(
            order, candle, bar, timestamp=getattr(candle, "timestamp", None)
        )

        return bool(getattr(decision, "triggered", False))

    # ==================================================
    # Breakout actual-entry economics (Phase 5B)
    # ==================================================

    @staticmethod
    def _next_bar_open_price(candle):
        """
        The market fill price for a NEXT_BAR_OPEN order: the OPEN of
        the entry bar (the first price reachable after the trigger
        close). Falls back to the close only when the candle carries
        no open (defensive; fabricated tick harnesses).
        """

        open_ = getattr(candle, "open", None)

        if open_ is not None:

            return float(open_)

        close = getattr(candle, "close", None)

        if close is not None:

            return float(close)

        return 0.0

    def _resize_for_actual_entry(self, order, entry_price):
        """
        Phase 5B: the BREAKOUT position is re-sized against the
        ACTUAL entry -- the next-bar OPEN -- so the risk amount
        RiskEngine froze is expressed over the real
        |open - stop| distance, not the setup's resting-boundary
        distance. This is what "real risk-sizing at the open entry"
        means: a gaped-open fill does not silently multiply the
        dollar risk inside the trade machinery.

        quantity_actual = risk_amount / |actual_entry - stop|

        Falls back to the order's existing quantity when the actual
        distance is unusable (degenerate stop, zero risk amount), so
        a bad fill never produces a zero/insane size.
        """

        if entry_price <= 0:

            return

        stop = float(getattr(order, "stop_loss", 0.0) or 0.0)

        distance = abs(entry_price - stop)

        risk_amount = float(
            getattr(order, "risk_amount", 0.0) or 0.0
        )

        if distance <= 0 or risk_amount <= 0:

            return

        quantity = risk_amount / distance

        if quantity <= 0:

            return

        order.quantity = quantity

        position_value = quantity * entry_price

        balance = (
            self.account.state.equity
            if self.account.state.equity > 0
            else self.account.state.balance
        )

        if balance and balance > 0:

            order.risk_percent = risk_amount / balance

        order.expected_loss = risk_amount

        order.position_value = position_value

        # 9Q-EXEC-01: Audit after resize — dust, precision, allocation cap via PositionManager
        try:
            corrected, reasons = self.simulator.position_manager.audit(order)
            if corrected.quantity <= 0:
                # Dust or invalid after resize → discard order (fail-soft)
                order.quantity = 0
                order.position_value = 0
                order.valid = False
                order.reason = "breakout resize dust/invalid: " + "; ".join(reasons) if reasons else "dust"
                return
            order.quantity = corrected.quantity
            order.position_value = corrected.position_value
            if hasattr(corrected, "valid"):
                order.valid = corrected.valid
                order.reason = corrected.reason
            # Phase 4 verified correction: re-mint costs from the
            # ACTUAL fill notional (resized quantity x actual entry),
            # not the setup-quantity notional they were minted from
            # at order creation. Same CostSpec formula, fresh inputs.
            # Only when the caller supplied an explicit spec; without
            # one the order's costs pass through verbatim (legacy).
            spec = getattr(self, "cost_spec", None)
            compute = getattr(spec, "compute", None)
            if callable(compute):
                commission, slippage = compute(
                    float(corrected.quantity) * float(entry_price)
                )
                order.commission = commission
                order.slippage = slippage
        except Exception:
            pass

    # ==================================================
    # Account Booking
    # ==================================================

    def _book_closed_trades(self, before):

        closed_now = self.simulator.registry.all()[before:]

        if not closed_now:
            return

        # Single-position model: the simulator closes at most
        # one trade per update() pass. Enforce it loudly instead
        # of the old all()[-1]-on-any-growth form, which used to
        # silently under-book if a cycle ever closed more than
        # one trade. The delta slice guarantees exactly the
        # trades closed this cycle are booked -- nothing more.
        if len(closed_now) != 1:
            raise RuntimeError(
                "Backtester: expected exactly one trade closed "
                f"per cycle, got {len(closed_now)}."
            )

        closed = closed_now[0]

        from core.backtesting.accounting import classify_pnl

        kind = classify_pnl(closed.pnl)
        if kind == "WIN":
            self.account.book_profit(closed.pnl)
        elif kind == "LOSS":
            self.account.book_loss(-closed.pnl)
        else:
            self.account.book_breakeven()

        if self.portfolio_release is not None:

            self.portfolio_release(closed)

    # ==================================================
    # Results
    # ==================================================

    @property
    def trades(self):

        return self.simulator.registry

    # --------------------------------------------------

    @property
    def completed_trades(self):

        return self.simulator.registry.all()

    # --------------------------------------------------

    @property
    def processed_bars(self):

        return self.total_bars

    # --------------------------------------------------

    @property
    def pending_orders_average(self):

        if self._pending_samples <= 0:

            return 0.0

        return self._pending_sum / self._pending_samples

    # --------------------------------------------------

    @property
    def pending_orders_max(self):

        return self._pending_max

    # --------------------------------------------------

    @property
    def pending_orders_max_bar(self):

        return self._pending_max_bar

    # --------------------------------------------------

    @property
    def pending_orders_max_directions(self):

        return dict(self._pending_max_directions)