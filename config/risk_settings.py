"""
====================================================

QuantEngine

Risk Settings

====================================================
"""

# -------------------------------------------------
# Account
# -------------------------------------------------

STARTING_BALANCE = 2000

# -------------------------------------------------
# Position Risk
# -------------------------------------------------

RISK_PER_TRADE = 0.01

# Maximum position NOTIONAL per trade (quote currency),
# enforced identically by PositionSizer.calculate and
# PositionManager.audit (11E-Prep H1). Unit: notional
# (entry * quantity), NOT margin — leverage scales margin,
# never this cap. The execution cap is additionally bounded
# by the account balance itself (min(balance, this value)).

MAX_ALLOCATION_PER_TRADE = 500

# Quantity hygiene applied by the PositionManager at the
# execution choke point (TradeSimulator.open_trade): quantities
# are rounded to QUANTITY_PRECISION decimals, and any quantity
# smaller than MIN_QUANTITY is treated as dust and dropped.
QUANTITY_PRECISION = 8

MIN_QUANTITY = 1e-6

# Pending-limit-order lifecycle (Phase 4D integrity).
#
# Until Phase 4D, aging was call-count based: OrderRegistry.advance()
# bumped wait_bars on the SAME candle that created the order, so a
# brand new order counted as already old, and expiry ran BEFORE the
# candle's fill check, so an order whose entry was touched on its
# expiry candle would expire without the chance to fill. Since Phase
# 4D the lifecycle is bar-anchored on explicit metadata:
#
#     eligible_from_bar = created_bar + ENTRY_DELAY_BARS
#     expires_at_bar     = eligible_from_bar + MAX_WAIT_BARS
#
# An order is checked for fill / invalidation only on bars in
# [eligible_from_bar, expires_at_bar], and is EXPIRED at the very END
# of a candle processing pass whose bar >= expires_at_bar (so the
# expiry candle still gets its fill/invalidation check first). It
# fills only when a candle of that window trades through its entry
# (low <= entry <= high) and is INVALIDATED only when price breaches
# the structural stop (bullish order: candle.low <= stop_loss;
# bearish order: candle.high >= stop_loss).
MAX_WAIT_BARS = 30

# Conservative no-look-ahead rule (Phase 4D STEP 3): an order created
# from candle N's close is not eligible for fill until candle N+1.
# The signal exists only at the close of N, so filling inside N's
# intrabar range would be a same-candle look-ahead. 0 disables the
# delay for replay engines where the caller guarantees no bias.
ENTRY_DELAY_BARS = 1

# Maximum concurrent pending (unfilled, non-terminal) orders the
# RiskEngine may keep alive across the pipeline (Phase 4D STEP 8).
# Accepted setups are never silently discarded: they are simply
# queued and a slot frees when an order fills, expires or is
# invalidated. Still subject to the per-setup one-order rule and
# the exposure ceiling below. 8 is an execution-CAPACITY default
# (not a strategy threshold): with the no-look-ahead eligibility
# delay + bar-anchored 30-bar windows, a too-small cap starves the
# funnel so created orders rarely overlap an entry-touch window
# (verified on the 5000-bar regression -- 3 produced 77 orders /
# 0 fills; 8 restores a meaningful funnel). MAX_WAIT_BARS and all
# strategy thresholds are unchanged.
MAX_PENDING_ORDERS = 8

# Ceiling for aggregate notional at risk from unresolved (pending)
# orders as a fraction of current equity. Guards against stacking
# multiple pending limit orders whose combined exposure exceeds the
# account. The PositionSizer sizes each order from entry/stop
# distance (quantity = risk_amount / stop_distance), so a single
# market-sized order naturally carries notional multiple times the
# account equity for tight 1m stops. This is a RUNAWAY-PENDING
# backstop, NOT a per-order cap: it must clear the full slot
# capacity or it silently suppresses the funnel it is meant to back
# stop. 25.0 == 2500% of current equity, i.e. 8 market-sized pending
# orders with headroom (8 x ~2.5x equity), so with MAX_PENDING_
# ORDERS = 8 it never binds under normal sizing (verified on the
# 5000-bar regression -- the earlier 10.0 default was a per-order
# gate in practice and skipped 391 accept-ready setups).
MAX_TOTAL_EXPOSURE_PCT = 25.0

# -------------------------------------------------
# Pending Order Capacity Policy (Phase 4F STEP 8)
# -------------------------------------------------

# Multi-axis pending-order capacity. Phase 4E made the pending-slot
# pool spend on INDEPENDENT economic ideas; Phase 4F adds explicit
# per-axis ceilings so overlapping, correlated exposure can never
# consume capacity silently. ExecutionEngine and RiskEngine both
# read these. MAX_PENDING_ORDERS is kept as the legacy alias (also
# 8) so pre-4F embeddings that only set it keep working.
MAX_PENDING_ORDERS_TOTAL = 8

MAX_PENDING_ORDERS_PER_SYMBOL = 8

MAX_PENDING_ORDERS_PER_DIRECTION = 4

# One canonical setup per execution zone (cluster) is already
# STRUCTURALLY guaranteed by the EconomicSetupRegistry -- a
# per-cluster cap of 1 is asserted by tests, never re-implemented
# as an invisible loop inside an execution function.
MAX_PENDING_ORDERS_PER_CLUSTER = 1

# -------------------------------------------------
# Entry Feasibility (Phase 4F)
# -------------------------------------------------

# ATR-normalized entry-feasibility classification thresholds:
#
#     ENTRY_IMMEDIATE      distance <= 0.10 x ATR (price is AT/inside
#                          the entry right now)
#     ENTRY_NEAR           <= 0.35 x ATR (one plausible retrace)
#     ENTRY_RETRACE        <= 0.75 x ATR (normal retrace expected)
#     ENTRY_DEEP_RETRACE   <= 1.50 x ATR (deep retrace)
#     ENTRY_TOO_FAR        >  1.50 x ATR (feasibility doubtful)
#
# Normalized to ATR -- never to a hard-coded BTC point distance --
# so the classification is symbol-agnostic (an 106-point excursion
# on a 77600 BTC entry is ~1.06 ATRs, not a magic number).
ENTRY_FEASIBILITY_IMMEDIATE_ATR = 0.10

ENTRY_FEASIBILITY_NEAR_ATR = 0.35

ENTRY_FEASIBILITY_RETRACE_ATR = 0.75

ENTRY_FEASIBILITY_DEEP_ATR = 1.50

# Explicit multi-mode entry architecture (Phase 4F STEP 6). The
# entry model is a first-class value carried on the SetupCandidate
# -> OrderIntent -> Trade -> backtest result chain. ACTIVE_ENTRY_MODES
# names the modes that may actually fill in the backtester; an order
# whose entry_mode is not active stays parked (honest diagnostics,
# no fills).
#
# PHASE 5D PRODUCTION DEFAULT: BREAKOUT_CONTINUATION is now the
# deliberately selected canonical production entry model, matching
# the strategy the Phase 4H causal validation green-lit (NOT the
# LIMIT_RETEST baseline). LIMIT_RETEST remains the canonical
# baseline fallback for mode-less legacy orders and stays selectable
# for comparison runs. The router NEVER silently substitutes a
# mode -- selection is always explicit config.
ENTRY_MODE = "BREAKOUT_CONTINUATION"

ACTIVE_ENTRY_MODES = ("BREAKOUT_CONTINUATION",)

# Diagnostic window for future retracement / continuation tracking
# after a setup is discovered (POST creation bar). 60 == 2 x
# MAX_WAIT_BARS, so an EXPIRED order's continuation is measured
# across a full extra window beyond its expiry bar. Measurement
# only -- it can never leak into signals.
ENTRY_TRACKING_WINDOW_BARS = 60

# A MISSED_CONTINUATION is diagnosed when a LIMIT_RETEST order never
# reached its entry AND price nonetheless moved at least this
# fraction of the way from entry to target in the trade direction
# (e.g. a bullish idea whose price ran 50% toward the TP without
# retracing to the entry). Diagnostic only, not a trade signal.
MISSED_CONTINUATION_TARGET_FRACTION = 0.5

# Master switch for Phase 4F diagnostics. When False, the
# EntryTracker.track / advance passes are skipped entirely so a run
# behaves exactly like Phase 4E (proven by the no-leak test).
ENABLE_ENTRY_DIAGNOSTICS = True

# -------------------------------------------------
# Structural Freshness & Entry Eligibility (Phase 4G)
# -------------------------------------------------

# STALE_ZONE boundary: a structurally unbroken zone older than
# this many bars may never mint a new LIMIT_RETEST order, rejected
# with reason=STALE_ZONE. BASELINE (STATUS 103): 500 on 1m was
# chosen MEASURE-FIRST -- it rejects only genuinely ancient zones
# while the ORDER BLOCK FRESHNESS REPORT (STEP 11) captures the
# real age distribution for a later, data-driven calibration.
# BOUND BY THE PHASE DISCIPLINE: do not raise to boost trade activity.
MAX_ORDER_BLOCK_AGE_BARS = 500

MAX_ORDER_BLOCK_AGE_BARS_BY_TIMEFRAME = {
    "1m": 500,
    "5m": 500,
    "15m": 500,
    "1h": 500,
    "4h": 500,
    "1d": 500,
}

# FRESH -> ACTIVE boundary for the derived freshness state
# (aligned with the report's first age bucket 0-5). A zone whose
# age is at or below this is FRESH; older-but-within-max is ACTIVE.
FRESH_ORDER_BLOCK_AGE_BARS = 5

# TOO_FAR_FROM_MARKET boundary: a LIMIT_RETEST entry farther than
# this many ATRs from the current price is rejected BEFORE a pending
# order is created (reason=TOO_FAR_FROM_MARKET). BASELINE 30 ATRs
# (STATUS 103): the Phase 4F regression medians sat near 60 ATRs and
# 99.2% of tracked entries came back TOO_FAR, so 30 removes the
# unreachable tail while keeping plausibly-reachable retests alive.
MAX_LIMIT_ENTRY_DISTANCE_ATR = 30.0

# INSUFFICIENT_RISK_WIDTH boundary: an entry/stop width below this
# fraction of ATR is rejected on EVERY setup path (reason=
# INSUFFICIENT_RISK_WIDTH), including a RiskEngine defense-in-depth
# check that runs even if a candidate bypasses the builder gate.
# BASELINE 0.10 ATR (STATUS 103): scripted tests stop-widths are
# ~0.67 ATR (>= 10x margin); the live 1.0-point / 237R case is
# ~0.003 ATR and is eliminated.
MIN_RISK_WIDTH_ATR = 0.10

# WEAK_DISPLACEMENT / TOO_MANY_TOUCHES measurement thresholds.
# Phase 4G BASELINE (STATUS 103): these are DIAGNOSTIC-ONLY --
# they are computed, carried as zone_diagnostics and reported by the
# ENTRY ELIGIBILITY REPORT, but they NEVER block a setup this phase.
# Set MIN to 0 / MAX to 0 to fully disable the diagnostics.
MIN_ORDER_BLOCK_DISPLACEMENT = 50.0

MAX_ORDER_BLOCK_TOUCHES = 3

# STEP 9/10 master switch: structural zone clustering before setup
# generation. When True, overlapping duplicate zones of one candle
# collapse to ONE canonical zone, so identical duplicates (the
# OB-791..796 family) can never each mint a candidate.
ENABLE_STRUCTURAL_CLUSTERING = True

# -------------------------------------------------
# Entry Model Shadow Comparison (Phase 4G extension / STATUS 104)
# -------------------------------------------------

# Master switch for the Phase 4H EntryShadow: a three-model entry
# REACHABILITY comparison (LIMIT_RETEST / MARKET_CONFIRMATION /
# BREAKOUT_CONTINUATION) plus the entry-distance-in-ATR vs the fixed
# MAX_WAIT_BARS expiry relationship, measured over EVERY
# trade-eligible structural setup. When False, observe()/advance()
# are skipped entirely so a run behaves exactly like Phase 4G (the
# no-leak test proves it).
ENABLE_ENTRY_SHADOW = True

# Which models the shadow MEASURES. LIMIT_RETEST is always measured
# (it is the executable baseline); these name the shadow-only modes
# allowed beside it. This NEVER changes execution: ACTIVE_ENTRY_MODES
# stays ("LIMIT_RETEST",) for the whole phase -- the comparison is
# diagnostic and reads the same future price action the tracker does.
SHADOW_ENTRY_MODES = (
    "MARKET_CONFIRMATION",
    "BREAKOUT_CONTINUATION",
)

# -------------------------------------------------
# Causal Breakout Continuation Validation (Phase 4H / STATUS 105)
# -------------------------------------------------

# Master switch for the Phase 4H CausalValidator: re-runs the
# BREAKOUT_CONTINUATION hypothesis under strict causal rules (frozen
# setup, trigger at the trigger candle's close, entry at the OPEN of
# the trigger + 1, deterministic intrabar SL/TP ambiguity handling)
# and reports performance metrics (win rate / profit factor /
# expectancy / average R / MAE-MFE / max drawdown) plus a
# MARKET_CONFIRMATION rejection-stage diagnosis. When False,
# observe()/advance() are skipped entirely so a run behaves exactly
# like the Phase 4G extension (the no-leak test proves it). The
# validator NEVER mints or fills an order: BREAKOUT_CONTINUATION
# stays a shadow until the user reviews the phase report.
ENABLE_CAUSAL_VALIDATION = True

# Deterministic intrabar SL/TP ambiguity policy (Phase 4H rule 4):
# when one candle spans both the stop and the target, the favorable
# outcome is never assumed.
#     CONSERVATIVE  stop first (LOSS)  -- Phase 4H PRIMARY
#     OPTIMISTIC    target first (WIN)
#     EXCLUDE       the ambiguous trade is dropped from W/L
# The report always renders all three policies from the recorded
# CONSERVATIVE outcome + ambiguity flag; this selects the primary.
INTRABAR_POLICY = "CONSERVATIVE"

# -------------------------------------------------
# Economic Re-entry Cooldown (Phase 4F STEP 9)
# -------------------------------------------------

# After an economic idea EXPIRES or is INVALIDATED, it may not be
# re-ordered for REENTRY_COOLDOWN_BARS after its terminal bar, even
# if a NEW structural order block later discovers the same
# tick-normalized economics. Prevents the periodic re-minting of
# endlessly-expiring identical ideas. Time-based only: once the
# cooldown passes, a genuinely new structural setup at the same
# prices is legal. FILLED ideas get no cooldown (their outcome was
# realized).
REENTRY_COOLDOWN_BARS = 30

# -------------------------------------------------
# Economic Setup Deduplication (Phase 4E)
# -------------------------------------------------

# Phase 4E economic deduplication is the SECOND deduplication
# layer, on top of the "one setup == one order" structural rule:
# two DIFFERENT order blocks that describe the SAME trade idea
# (identical economics, e.g. OB-954 and OB-956 both entry 77616 /
# stop 77631.5 / target 77455.42) must never each mint a pending
# order. The economic fingerprint normalizes entry/stop/target to
# the instrument's tick grid, so equal economics always collapses
# to one pending order. Clustering further merges NEAR-identical
# zones (e.g. entry 77616 vs 77620 on the same stop/target) within
# the configurable tolerance below.
#
# Crescent: thresholds are expressed as tick multiples, percentage
# distance and an ATR fraction -- NEVER as hard-coded point
# distances for a single symbol. The effective tolerance at a
# price level is:
#
#     tolerance = max(
#         tick * ECONOMIC_CLUSTER_TICKS,
#         price * ECONOMIC_CLUSTER_PCT,
#         atr   * ECONOMIC_CLUSTER_ATR_FRACTION,
#     )
#
# With every knob at 0 only EXACT fingerprint dedup applies (no
# clustering).
ECONOMIC_TICK_SIZE = 0.5

ECONOMIC_CLUSTER_TICKS = 8

ECONOMIC_CLUSTER_PCT = 0.001

ECONOMIC_CLUSTER_ATR_FRACTION = 0.0

AUTO_COMPOUND = True

LEVERAGE = 10.0

# -------------------------------------------------
# Execution Costs
# -------------------------------------------------

# Round-trip (entry + exit) commission: a fixed dollar
# amount plus a rate applied to the position value.

COMMISSION_PER_TRADE = 0.0

COMMISSION_RATE = 0.0

# Round-trip slippage in basis points of the position
# value. Applied to the bookkeeping PnL of each closed
# trade. 0.0 means fills are exact.

SLIPPAGE_BPS = 0.0

# -------------------------------------------------
# Paper Venue Cost Profile (Stage 4)
# -------------------------------------------------
# The production defaults above are intentionally neutral
# (explicit zero-cost). A zero-cost profile is NOT acceptable
# evidence for any paper profitability claim. This SEPARATE
# paper-only profile lets paper certification run explicit,
# stated costs WITHOUT touching production defaults or frozen
# research ladders (11E/11F/11G COST_SCENARIOS_*, never read
# here, never modified here).
#
# No venue fee numbers are invented here: the defaults below
# reproduce the neutral zero profile, and ANY nonzero claim
# must come from explicit operator configuration (environment
# overrides) or an explicitly labeled scenario. Until a
# venue-specific policy is provisioned:
#     PAPER_COST_VENUE_REALISTIC = False  (CONFIGURATION_REQUIRED)
# Every paper report MUST state the exact profile used.

import os as _os

PAPER_COMMISSION_PER_TRADE = float(
    _os.getenv("QE_PAPER_COMMISSION_PER_TRADE", "0.0")
)

PAPER_COMMISSION_RATE = float(
    _os.getenv("QE_PAPER_COMMISSION_RATE", "0.0")
)

PAPER_SLIPPAGE_BPS = float(
    _os.getenv("QE_PAPER_SLIPPAGE_BPS", "0.0")
)

PAPER_COST_PROFILE_NAME = _os.getenv(
    "QE_PAPER_COST_PROFILE_NAME", "explicit-zero-neutral"
)

# True ONLY when an operator provisions venue-realistic values
# above. False (default) means paper economics are neutral and
# must never be cited as venue-realistic performance.
PAPER_COST_VENUE_REALISTIC = (
    _os.getenv("QE_PAPER_COST_VENUE_REALISTIC", "false").strip().lower()
    in ("1", "true", "yes", "on")
)

# -------------------------------------------------
# Kill Switch (Stage 4 seam)
# -------------------------------------------------
# AccountState.kill_switch / trading_enabled are the in-memory
# operator controls; this env var is the restart-persistent
# equivalent. Any truthy value forces the kill state across
# restarts; clearing it requires explicit operator action
# (unset + clear in-memory flags). RiskEngine (mandatory for
# every order) enforces all three; nothing bypasses it.
KILL_SWITCH_ENV_VAR = "QE_KILL_SWITCH"

# -------------------------------------------------
# Drawdown Limits
# -------------------------------------------------

MAX_DAILY_LOSS = 0.02

MAX_WEEKLY_LOSS = 0.05

MAX_MONTHLY_LOSS = 0.10

# -------------------------------------------------
# Profit Protection
# -------------------------------------------------

PROFIT_PROTECTION = True

PROFIT_LOCK_PERCENT = 30

# -------------------------------------------------
# Position Limits
# -------------------------------------------------

MAX_OPEN_POSITIONS = 1

MAX_PORTFOLIO_RISK = 0.03

MAX_CORRELATED_RISK = 0.02

# -------------------------------------------------
# Trade Quality
# -------------------------------------------------

MIN_CONFIDENCE = 80

# Frozen Level-3 research/OOS minimum. Do not change: 9M strategy identity
# and historical certification artifacts intentionally remain 1:6.
MIN_RISK_REWARD = 6.0

# Runtime trading-policy defaults. The authoritative mutable policy is
# core.risk.trading_policy, which supports exact instrument overrides from
# the authenticated frontend/API. These constants remain compatibility
# defaults for legacy callers only.
TRADING_MIN_RISK_REWARD = 1.0
TRADING_MAX_RISK_REWARD = 20.0

EXIT_MODE_FIXED_TARGET = "FIXED_TARGET"
EXIT_MODE_TRAILING_STOP = "TRAILING_STOP"
TRAILING_STOP_ENABLED = True
TRAILING_ACTIVATION_R = 1.0
TRAILING_DISTANCE_R = 0.5

# -------------------------------------------------
# Modes
# -------------------------------------------------

PAPER_MODE = True

LIVE_MODE = False