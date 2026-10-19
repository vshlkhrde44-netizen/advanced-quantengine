from config.settings import settings

from data.historical_loader import HistoricalLoader
from data.candle_builder import CandleBuilder
from data.bar_series import BarSeries

from smartmoney.displacement_detector import DisplacementDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

events = DisplacementDetector().detect(bars)

print()

print("Displacement Events")

print("=" * 60)

for event in events:

    print(
        event.direction,
        f"Range: {event.range:.2f}",
        f"Body: {event.body:.2f}",
        event.timestamp,
    )

loader.close()