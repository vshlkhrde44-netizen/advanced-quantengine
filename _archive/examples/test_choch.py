from config.settings import settings

from data.candle_builder import CandleBuilder
from data.historical_loader import HistoricalLoader
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.market_structure import MarketStructure
from smartmoney.choch import CHoCHDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

structure = MarketStructure().analyze(swings)

events = CHoCHDetector().detect(structure)

print()

print("Detected CHoCH")

print("=" * 50)

for event in events:

    print(

        event.direction,

        event.price,

        event.timestamp,

    )

loader.close()