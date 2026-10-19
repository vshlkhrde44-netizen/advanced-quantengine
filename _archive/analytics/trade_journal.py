"""
====================================================

QuantEngine

Institutional Trade Journal

====================================================
"""

from collections import defaultdict

from analytics.trade import Trade


class TradeJournal:
    """
    Institutional Trade Journal.

    Central repository for completed trades.

    Future modules:
        • Performance Analytics
        • AI Optimizer
        • Strategy Ranking
        • Walk Forward Analysis
        • Portfolio Reports
    """

    # -------------------------------------------------

    def __init__(self):

        self.trades = []

    # -------------------------------------------------

    def add(self, trade: Trade):

        self.trades.append(trade)

    # -------------------------------------------------

    def history(self):

        return list(self.trades)

    # -------------------------------------------------

    def total_trades(self):

        return len(self.trades)

    # -------------------------------------------------

    def winners(self):

        return sum(

            1

            for trade in self.trades

            if trade.win

        )

    # -------------------------------------------------

    def losers(self):

        return self.total_trades() - self.winners()

    # -------------------------------------------------

    def total_profit(self):

        return sum(

            trade.pnl

            for trade in self.trades

        )

    # -------------------------------------------------

    def average_profit(self):

        if not self.trades:

            return 0.0

        return self.total_profit() / self.total_trades()

    # -------------------------------------------------

    def average_win(self):

        wins = [

            trade.pnl

            for trade in self.trades

            if trade.pnl > 0

        ]

        if not wins:

            return 0.0

        return sum(wins) / len(wins)

    # -------------------------------------------------

    def average_loss(self):

        losses = [

            trade.pnl

            for trade in self.trades

            if trade.pnl < 0

        ]

        if not losses:

            return 0.0

        return sum(losses) / len(losses)

    # -------------------------------------------------

    def profit_factor(self):

        gross_profit = sum(

            t.pnl

            for t in self.trades

            if t.pnl > 0

        )

        gross_loss = abs(

            sum(

                t.pnl

                for t in self.trades

                if t.pnl < 0

            )

        )

        if gross_loss == 0:

            return float("inf")

        return gross_profit / gross_loss

    # -------------------------------------------------

    def expectancy(self):

        if not self.trades:

            return 0.0

        win_rate = self.win_rate() / 100

        loss_rate = 1 - win_rate

        return (

            win_rate * self.average_win()

            +

            loss_rate * self.average_loss()

        )

    # -------------------------------------------------

    def win_rate(self):

        if not self.trades:

            return 0.0

        return (

            self.winners()

            / self.total_trades()

            * 100

        )

    # -------------------------------------------------

    def strategy_summary(self):

        summary = defaultdict(

            lambda: {

                "trades": 0,

                "wins": 0,

                "pnl": 0.0,

            }

        )

        for trade in self.trades:

            strategy = getattr(

                trade,

                "strategy",

                "Unknown",

            )

            item = summary[strategy]

            item["trades"] += 1

            item["pnl"] += trade.pnl

            if trade.win:

                item["wins"] += 1

        return dict(summary)

    # -------------------------------------------------

    def regime_summary(self):

        summary = defaultdict(

            lambda: {

                "trades": 0,

                "wins": 0,

                "pnl": 0.0,

            }

        )

        for trade in self.trades:

            regime = getattr(

                trade,

                "regime",

                "Unknown",

            )

            item = summary[regime]

            item["trades"] += 1

            item["pnl"] += trade.pnl

            if trade.win:

                item["wins"] += 1

        return dict(summary)

    # -------------------------------------------------

    def reset(self):

        self.trades.clear()