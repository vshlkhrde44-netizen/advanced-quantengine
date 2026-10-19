from config.settings import settings

from data.historical_loader import HistoricalLoader
from data.candle_builder import CandleBuilder
from data.bar_series import BarSeries

from smartmoney.swing_detector import SwingDetector
from smartmoney.market_structure import MarketStructure
from smartmoney.bos import BOSDetector
from smartmoney.order_block_detector import OrderBlockDetector
from smartmoney.order_block_manager import OrderBlockManager


loader = HistoricalLoader()

builder = CandleBuilder()

history = loader.candles(settings.SYMBOLS[0])

builder.load_history(
    list(reversed(history["result"]))
)

bars = BarSeries(builder.history)

swings = SwingDetector().detect(bars)

structure = MarketStructure().analyze(swings)

bos = BOSDetector().detect(structure)

blocks = OrderBlockDetector().detect(bars, bos)

blocks = OrderBlockManager().update(bars, blocks)

print()

print("ORDER BLOCK STATUS")

print("=" * 70)

for block in blocks:

    print(

        block.direction,

        f"{block.low:.2f}-{block.high:.2f}",

        "Touches:", block.touches,

        "Mitigated:", block.mitigated,

        "Invalidated:", block.invalidated,

    )

loader.close()