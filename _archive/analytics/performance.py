"""
====================================================

QuantEngine

Institutional Performance Analytics

====================================================
"""

from math import sqrt


class Performance:

    # -------------------------------------------------

    def trades(self, journal):

        return journal.history()

    # -------------------------------------------------

    def equity_curve(self, journal):

        equity = 0.0

        curve = []

        for trade in self.trades(journal):

            equity += trade.pnl

            curve.append(equity)

        return curve

    # -------------------------------------------------

    def drawdown_curve(self, journal):

        peak = 0.0

        curve = []

        equity = 0.0

        for trade in self.trades(journal):

            equity += trade.pnl

            peak = max(peak, equity)

            curve.append(peak - equity)

        return curve

    # -------------------------------------------------

    def average_win(self, journal):

        wins = [

            t.pnl

            for t in self.trades(journal)

            if t.pnl > 0

        ]

        return sum(wins) / len(wins) if wins else 0.0

    # -------------------------------------------------

    def average_loss(self, journal):

        losses = [

            t.pnl

            for t in self.trades(journal)

            if t.pnl < 0

        ]

        return sum(losses) / len(losses) if losses else 0.0

    # -------------------------------------------------

    def profit_factor(self, journal):

        gross_profit = sum(

            t.pnl

            for t in self.trades(journal)

            if t.pnl > 0

        )

        gross_loss = abs(

            sum(

                t.pnl

                for t in self.trades(journal)

                if t.pnl < 0

            )

        )

        if gross_loss == 0:

            return float("inf")

        return gross_profit / gross_loss

    # -------------------------------------------------

    def expectancy(self, journal):

        total = journal.total_trades()

        if total == 0:

            return 0.0

        return journal.total_profit() / total

    # -------------------------------------------------

    def max_drawdown(self, journal):

        curve = self.drawdown_curve(journal)

        return max(curve) if curve else 0.0

    # -------------------------------------------------

    def largest_win(self, journal):

        wins = [

            t.pnl

            for t in self.trades(journal)

            if t.pnl > 0

        ]

        return max(wins) if wins else 0.0

    # -------------------------------------------------

    def largest_loss(self, journal):

        losses = [

            t.pnl

            for t in self.trades(journal)

            if t.pnl < 0

        ]

        return min(losses) if losses else 0.0

    # -------------------------------------------------

    def consecutive_wins(self, journal):

        best = 0

        current = 0

        for trade in self.trades(journal):

            if trade.pnl > 0:

                current += 1

                best = max(best, current)

            else:

                current = 0

        return best

    # -------------------------------------------------

    def consecutive_losses(self, journal):

        best = 0

        current = 0

        for trade in self.trades(journal):

            if trade.pnl < 0:

                current += 1

                best = max(best, current)

            else:

                current = 0

        return best

    # -------------------------------------------------

    def sharpe_ratio(self, journal):

        values = [

            t.pnl

            for t in self.trades(journal)

        ]

        if len(values) < 2:

            return 0.0

        mean = sum(values) / len(values)

        variance = sum(

            (x - mean) ** 2

            for x in values

        ) / (len(values) - 1)

        std = sqrt(variance)

        if std == 0:

            return 0.0

        return mean / std

    # -------------------------------------------------

    def sortino_ratio(self, journal):

        values = [

            t.pnl

            for t in self.trades(journal)

        ]

        if len(values) < 2:

            return 0.0

        mean = sum(values) / len(values)

        downside = [

            x

            for x in values

            if x < 0

        ]

        if not downside:

            return float("inf")

        downside_dev = sqrt(

            sum(x * x for x in downside)

            / len(downside)

        )

        if downside_dev == 0:

            return 0.0

        return mean / downside_dev

    # -------------------------------------------------

    def calmar_ratio(self, journal):

        dd = self.max_drawdown(journal)

        if dd == 0:

            return float("inf")

        return journal.total_profit() / dd

    # -------------------------------------------------

    def report(self, journal):

        print()

        print("=" * 70)

        print("INSTITUTIONAL PERFORMANCE REPORT")

        print("=" * 70)

        print(f"Trades              : {journal.total_trades()}")

        print(f"Winners             : {journal.winners()}")

        print(f"Losers              : {journal.losers()}")

        print(f"Win Rate            : {journal.win_rate():.2f}%")

        print(f"Net Profit          : {journal.total_profit():.2f}")

        print(f"Average Win         : {self.average_win(journal):.2f}")

        print(f"Average Loss        : {self.average_loss(journal):.2f}")

        print(f"Profit Factor       : {self.profit_factor(journal):.2f}")

        print(f"Expectancy          : {self.expectancy(journal):.2f}")

        print(f"Largest Win         : {self.largest_win(journal):.2f}")

        print(f"Largest Loss        : {self.largest_loss(journal):.2f}")

        print(f"Max Drawdown        : {self.max_drawdown(journal):.2f}")

        print(f"Sharpe Ratio        : {self.sharpe_ratio(journal):.2f}")

        print(f"Sortino Ratio       : {self.sortino_ratio(journal):.2f}")

        print(f"Calmar Ratio        : {self.calmar_ratio(journal):.2f}")

        print(f"Best Win Streak     : {self.consecutive_wins(journal)}")

        print(f"Worst Loss Streak   : {self.consecutive_losses(journal)}")