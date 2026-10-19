from config.settings import settings

from data.candle_builder import CandleBuilder
from data.historical_loader import HistoricalLoader
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.liquidity import LiquidityDetector


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

zones = LiquidityDetector().detect(swings)

print()

print("Liquidity Zones")

print("=" * 60)

for zone in zones[-20:]:

    print(
        zone.type,
        round(zone.price, 2),
        zone.first_timestamp,
        zone.second_timestamp,
    )

loader.close()