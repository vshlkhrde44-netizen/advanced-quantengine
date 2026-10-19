from strategy.strategy_manager import StrategyManager
from strategy.confluence_engine import ConfluenceEngine
from strategy.decision_engine import TradeDecisionEngine

from execution.execution_engine import ExecutionEngine
from risk.risk_manager import RiskManager


manager = StrategyManager()

strategy = manager.get("ICT")

signal = ConfluenceEngine(

    strategy.weights

).evaluate(

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

risk = RiskManager(

    account_balance=10000,

    risk_percent=1,

)

qty = risk.position_size(

    entry_price=64000,

    stop_loss=63950,

)

order = ExecutionEngine().execute(

    decision,

    symbol="BTCUSDT",

    entry=64000,

    stop=63950,

    target=64200,

    quantity=qty,

)

print()

print("=" * 60)

print("PAPER ORDER")

print("=" * 60)

print(order)