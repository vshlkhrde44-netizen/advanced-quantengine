from config.settings import settings

from data.candle_builder import CandleBuilder
from data.historical_loader import HistoricalLoader
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.liquidity import LiquidityDetector
from smartmoney.liquidity_sweep import LiquiditySweepDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

liquidity = LiquidityDetector().detect(swings)

events = LiquiditySweepDetector().detect(
    bars,
    liquidity
)

print()

print("Liquidity Sweeps")

print("=" * 60)

for event in events:

    print(
        event.direction,
        event.sweep_price,
        event.candle_timestamp
    )

loader.close()