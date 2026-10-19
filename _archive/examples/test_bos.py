from config.settings import settings

from data.candle_builder import CandleBuilder
from data.historical_loader import HistoricalLoader
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.market_structure import MarketStructure
from smartmoney.bos import BOSDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

structure = MarketStructure().analyze(swings)

bos_events = BOSDetector().detect(structure)

print()

print("Detected BOS Events")

print("=" * 60)

for bos in bos_events[-20:]:

    print(
        bos.direction,
        bos.price,
        bos.timestamp,
    )

loader.close()