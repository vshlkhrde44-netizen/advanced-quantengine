"""
====================================================

QuantEngine

Build : 3.3
Module : Strategy Runner

Runs the active strategy whenever a candle closes.

====================================================
"""


class StrategyRunner:

    def __init__(self, strategy, indicators):

        self.strategy = strategy
        self.indicators = indicators

    def on_new_candle(self, candle):

        snapshot = self.indicators.snapshot()

        signal = self.strategy.evaluate(
            candle,
            snapshot,
        )

        return signal