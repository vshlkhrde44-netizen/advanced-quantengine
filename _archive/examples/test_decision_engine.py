from strategy.strategy_manager import StrategyManager
from strategy.confluence_engine import ConfluenceEngine
from strategy.decision_engine import TradeDecisionEngine


manager = StrategyManager()

strategy = manager.get("ICT")

engine = ConfluenceEngine(strategy.weights)

signal = engine.evaluate(

    bos=True,

    choch=True,

    orderblock=True,

    fvg=True,

    liquidity=True,

    displacement=True,

)

decision = TradeDecisionEngine().decide(

    signal,

    strategy,

)

print()

print("=" * 60)

print("TRADE DECISION")

print("=" * 60)

print("Action      :", decision.action)

print("Confidence  :", decision.confidence)

print("Score       :", decision.score)

print("Strategy    :", decision.strategy)

print("Reasons")

for reason in decision.reasons:

    print(" •", reason)