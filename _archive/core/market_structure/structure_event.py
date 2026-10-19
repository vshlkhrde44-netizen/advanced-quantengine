"""
====================================================

QuantEngine

Institutional Structure Event

====================================================
"""

from dataclasses import dataclass
from enum import Enum


class StructureType(Enum):

    HIGH = "HIGH"
    LOW = "LOW"

    HH = "HH"
    HL = "HL"

    LH = "LH"
    LL = "LL"

    BOS_BULL = "BOS_BULL"

    BOS_BEAR = "BOS_BEAR"

    CHOCH_BULL = "CHOCH_BULL"

    CHOCH_BEAR = "CHOCH_BEAR"

    MSS_BULL = "MSS_BULL"

    MSS_BEAR = "MSS_BEAR"


@dataclass(slots=True)

class StructureEvent:

    type: StructureType

    price: float

    timestamp: int

    index: int