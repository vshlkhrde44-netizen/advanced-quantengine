from strategy.confluence_engine import ConfluenceEngine

engine = ConfluenceEngine()

signal = engine.evaluate(

    bos=True,

    choch=True,

    orderblock=True,

    fvg=True,

    liquidity=False,

    displacement=True,

)

print()

print("TRADE SIGNAL")

print("=" * 50)

print("Direction :", signal.direction)

print("Score     :", signal.score)

print("Confidence:", signal.confidence)

print("Reasons   :", signal.reasons)
