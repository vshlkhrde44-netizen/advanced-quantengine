"""
====================================================

QuantEngine

Sprint 7

Backtest Logger

====================================================
"""


class BacktestLogger:

    def on_bar(self, candle):

        print(

            candle.timestamp,

            candle.close,

        )