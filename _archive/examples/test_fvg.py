from config.settings import settings

from data.historical_loader import HistoricalLoader
from data.candle_builder import CandleBuilder
from data.bar_series import BarSeries

from smartmoney.fvg_detector import FVGDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

detector = FVGDetector()

gaps = detector.detect(bars)

print()

print("Fair Value Gaps")

print("=" * 60)

for gap in gaps[-20:]:

    print(

        gap.direction,

        f"{gap.bottom:.2f} -> {gap.top:.2f}",

        gap.timestamp,

        "Filled:", gap.filled

    )

loader.close()