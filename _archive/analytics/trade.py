"""
====================================================

QuantEngine

Sprint 6

Completed Trade

====================================================
"""

from dataclasses import dataclass


@dataclass
class Trade:

    symbol: str

    side: str

    quantity: float

    entry: float

    exit: float

    pnl: float

    strategy: str

    win: bool