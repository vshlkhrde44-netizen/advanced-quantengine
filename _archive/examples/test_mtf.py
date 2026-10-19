from strategy.timeframe_signal import TimeframeSignal
from strategy.mtf_engine import MultiTimeframeEngine


signals = [

    TimeframeSignal("1m", "BUY", 85, "VERY HIGH"),

    TimeframeSignal("5m", "BUY", 82, "VERY HIGH"),

    TimeframeSignal("15m", "BUY", 90, "VERY HIGH"),

    TimeframeSignal("1h", "BUY", 87, "VERY HIGH"),

    TimeframeSignal("4h", "BUY", 91, "VERY HIGH"),

]

engine = MultiTimeframeEngine()

result = engine.evaluate(signals)

print()

print("MULTI TIMEFRAME RESULT")

print("=" * 50)

print(result)