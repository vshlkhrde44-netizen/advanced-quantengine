"""
====================================================

QuantEngine

Institutional Market State

Single Source Of Truth

====================================================
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MarketState:

    # =====================================================
    # Identity
    # =====================================================

    symbol: str = ""

    timeframe: str = ""

    timestamp: int = 0

    # =====================================================
    # Market Structure
    # =====================================================

    trend: str = "UNKNOWN"

    internal_trend: str = "UNKNOWN"

    external_trend: str = "UNKNOWN"

    market_regime: str = "UNKNOWN"

    # =====================================================
    # Swing Structure
    # =====================================================

    major_high: float = 0.0

    major_low: float = 0.0

    minor_high: float = 0.0

    minor_low: float = 0.0

    market_leg_high: float = 0.0

    market_leg_low: float = 0.0

    equilibrium: float = 0.0

    premium: bool = False

    discount: bool = False

    # =====================================================
    # Events
    # =====================================================

    bos: bool = False

    choch: bool = False

    mss: bool = False

    sweep: bool = False

    displacement: bool = False

    # =====================================================
    # Smart Money
    # =====================================================

    order_blocks: list = field(default_factory=list)

    breaker_blocks: list = field(default_factory=list)

    mitigation_blocks: list = field(default_factory=list)

    fvg: list = field(default_factory=list)

    liquidity: list = field(default_factory=list)

    imbalance: list = field(default_factory=list)

    # =====================================================
    # Fibonacci
    # =====================================================

    fib: Any = None

    fib_extension: Any = None

    ote: bool = False

    # =====================================================
    # Volume
    # =====================================================

    volume_profile: Any = None

    vwap: float = 0.0

    cvd: float = 0.0

    delta: float = 0.0

    footprint: Any = None

    # =====================================================
    # CPR
    # =====================================================

    cpr: Any = None

    # =====================================================
    # Gann
    # =====================================================

    gann: Any = None

    # =====================================================
    # Patterns
    # =====================================================

    harmonic: list = field(default_factory=list)

    elliott: list = field(default_factory=list)

    chart_patterns: list = field(default_factory=list)

    candlestick_patterns: list = field(default_factory=list)

    # =====================================================
    # Sessions
    # =====================================================

    active_session: str = ""

    session_high: float = 0.0

    session_low: float = 0.0

    # =====================================================
    # Trend Filters
    # =====================================================

    ema20: float = 0.0

    ema50: float = 0.0

    ema100: float = 0.0

    ema200: float = 0.0

    # =====================================================
    # ATR / Volatility
    # =====================================================

    atr: float = 0.0

    volatility: float = 0.0

    # =====================================================
    # Scores
    # =====================================================

    confidence: float = 0.0

    confluence_score: float = 0.0

    quality_score: float = 0.0

    # =====================================================
    # Strategy
    # =====================================================

    recommended_strategy: str = ""

    recommended_direction: str = ""

    entry_price: float = 0.0

    stop_loss: float = 0.0

    take_profit: float = 0.0

    rr: float = 0.0

    # =====================================================
    # Events
    # =====================================================

    events: list = field(default_factory=list)