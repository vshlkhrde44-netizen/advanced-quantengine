from config.settings import settings

from data.historical_loader import HistoricalLoader
from data.candle_builder import CandleBuilder
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.liquidity_pool_builder import LiquidityPoolBuilder


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

builder_pool = LiquidityPoolBuilder()

pools = builder_pool.build(swings)

print()

print("Liquidity Pools")

print("=" * 60)

for pool in pools:

    print(

        pool.type,

        round(pool.price, 2),

        "Touches:", pool.touches,

        "Active:", pool.active,

    )

loader.close()