"""
====================================================

QuantEngine

Build : 4.1
Module : Bar Series

Provides fast access to OHLC data.

====================================================
"""

from typing import List


class BarSeries:

    def __init__(self, candles):

        self.candles = candles

    def open(self) -> List[float]:

        return [c.open for c in self.candles]

    def high(self) -> List[float]:

        return [c.high for c in self.candles]

    def low(self) -> List[float]:

        return [c.low for c in self.candles]

    def close(self) -> List[float]:

        return [c.close for c in self.candles]

    def count(self):

        return len(self.candles)

    def last(self):

        if not self.candles:
            return None

        return self.candles[-1]