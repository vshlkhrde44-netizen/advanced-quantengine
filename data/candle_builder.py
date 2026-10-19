"""
====================================================

QuantEngine

Institutional Candle Builder

Builds completed bars and updates BarSeries

====================================================
"""

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import math

from core.bars.bar import Bar
from core.bars.bar_series import BarSeries
from core.common.timestamp import normalize_timestamp


# Bucket size in seconds for each supported timeframe.
TIMEFRAME_SECONDS = {

    "1m": 60,

    "3m": 180,

    "5m": 300,

    "15m": 900,

    "30m": 1800,

    "1h": 3600,

    "2h": 7200,

    "4h": 14400,

    "6h": 21600,

    "12h": 43200,

    "1d": 86400,

}


@dataclass(slots=True)
class Candle:

    timestamp: int

    open: float
    high: float
    low: float
    close: float

    volume: float = 0.0


class CandleBuilder:

    def __init__(self, max_history: int = 1000, timeframe: str = "1m"):

        self.timeframe = timeframe

        seconds = TIMEFRAME_SECONDS.get(timeframe)

        if seconds is None:

            raise ValueError(
                f"Unsupported timeframe: {timeframe!r}"
            )

        self.window_seconds = seconds

        self.current = None

        # Concurrency: protect current/history/series. Use RLock so
        # same thread can re-enter; held only for short critical
        # sections, never across callbacks.
        self._lock = threading.RLock()

        # max_history is deliberately a constructor parameter,
        # not a hardcoded constant: a live trading context wants
        # a small bounded window (memory-safe, fixed cost), but
        # a backtest loading paginated history (see
        # data/historical_loader.py's candles_paginated()) needs
        # this to be at least as large as the number of candles
        # it loads, or load_history() will silently keep only
        # the most recent `max_history` of whatever was passed
        # in, which looks identical to "the API only returned N
        # candles" from the caller's side.
        self.history = deque(maxlen=max_history)

        # NEW
        self.series = BarSeries(max_bars=max(max_history, 5000))

        # Phase 11C operational counters: invalid ticks
        # (non-numeric / NaN / infinite / non-positive price /
        # negative volume) and stale ticks (bucket older than
        # the current bucket -- time must never move
        # backwards) are rejected and counted, never applied.
        self.rejected_ticks = 0
        self.stale_ticks = 0

    # =====================================================

    def stats(self):
        """Snapshot of builder integrity counters."""

        with self._lock:
            return {
                "completed": len(self.history),
                "rejected_ticks": self.rejected_ticks,
                "stale_ticks": self.stale_ticks,
                "current_ts": (
                    self.current.timestamp
                    if self.current is not None
                    else None
                ),
            }

    # =====================================================

    def update(

        self,

        timestamp,

        price: float,

        volume: float = 0.0,

    ):
        """
        Update with a tick. `timestamp` may be seconds, ms, µs, or ns
        (any unit) — normalized to canonical UTC seconds at the
        entry. Returns finished Candle on bucket roll, else None.
        Thread-safe: holds lock only for state mutation.
        """

        # Normalize at entry (adapter should already have normalized,
        # but we defend here so historical seconds and live µs both work)
        try:
            ts = normalize_timestamp(timestamp)
        except (ValueError, TypeError, AttributeError):
            # Invalid timestamp → ignore tick (fail-soft, no candle)
            with self._lock:
                self.rejected_ticks += 1
            return None

        # Phase 11C market-data validation: price must be a
        # finite positive number, volume a finite
        # non-negative number. Invalid ticks are rejected
        # and counted -- they must never create or corrupt a
        # candle, and must never generate false trades.
        try:
            price_value = float(price)
            volume_value = float(volume)
        except (TypeError, ValueError):
            with self._lock:
                self.rejected_ticks += 1
            return None

        if (
            price_value != price_value
            or price_value in (float("inf"), float("-inf"))
            or price_value <= 0
            or volume_value != volume_value
            or volume_value in (float("inf"), float("-inf"))
            or volume_value < 0
        ):
            with self._lock:
                self.rejected_ticks += 1
            return None

        price = price_value
        volume = volume_value

        bucket = ts - (ts % self.window_seconds)

        with self._lock:
            if self.current is None:

                self.current = Candle(

                    timestamp=bucket,

                    open=price,

                    high=price,

                    low=price,

                    close=price,

                    volume=volume,

                )

                return None

            if bucket != self.current.timestamp:

                if bucket < self.current.timestamp:
                    # Phase 11C: stale tick from an older bucket
                    # (late/out-of-order tick after a roll). Time
                    # must never move backwards: ignore the tick,
                    # count it, keep the current bucket intact --
                    # no finished candle, no duplicate event.
                    self.stale_ticks += 1
                    return None

                finished = self.current

                self.history.append(finished)

                self.series.append(

                    Bar(

                        timestamp=finished.timestamp,

                        open=finished.open,

                        high=finished.high,

                        low=finished.low,

                        close=finished.close,

                        volume=finished.volume,

                    )

                )

                self.current = Candle(

                    timestamp=bucket,

                    open=price,

                    high=price,

                    low=price,

                    close=price,

                    volume=volume,

                )

                return finished

            self.current.high = max(

                self.current.high,

                price,

            )

            self.current.low = min(

                self.current.low,

                price,

            )

            self.current.close = price

            self.current.volume += volume

            return None

    # =====================================================

    def load_history(

        self,

        candles,

    ):

        with self._lock:
            self.history.clear()
            self.series.clear()
            self.current = None

            for index, candle in enumerate(candles):

                # Phase 11C: bulk history is validated at the
                # typed boundary -- a malformed bar fails the
                # load with an indexed ValueError instead of a
                # bare KeyError, and never silently corrupts the
                # series with a partial window.
                if not isinstance(candle, dict):
                    raise ValueError(
                        "load_history: entry {} is not a "
                        "mapping: {!r}".format(
                            index, type(candle).__name__
                        )
                    )

                try:
                    volume = float(
                        candle.get(
                            "volume",
                            0.0,
                        )
                    )
                    # Phase 12 timestamp normalization: the
                    # bulk path applies the SAME unit
                    # normalization as the streaming update()
                    # path (seconds / ms / us / ns by
                    # magnitude) BEFORE bucketing. A raw
                    # millisecond timestamp previously
                    # corrupted every downstream bucket
                    # silently; now it normalizes or the load
                    # fails with an indexed ValueError.
                    timestamp = normalize_timestamp(
                        candle["time"]
                    )
                    open_ = float(candle["open"])
                    high = float(candle["high"])
                    low = float(candle["low"])
                    close = float(candle["close"])
                except (KeyError, TypeError, ValueError) as error:
                    raise ValueError(
                        "load_history: entry {} has invalid "
                        "OHLCV ({}).".format(
                            index, type(error).__name__
                        )
                    ) from error

                # 11E-Prep C1-E: OHLC must be finite as well as
                # positive/consistent. NaN comparisons are all
                # False and Inf passes every range check, so
                # without this the bulk path accepts bars the
                # streaming update() path rejects.
                if (
                    not (
                        math.isfinite(open_)
                        and math.isfinite(high)
                        and math.isfinite(low)
                        and math.isfinite(close)
                    )
                    or min(open_, high, low, close) <= 0
                    or high < max(open_, close)
                    or low > min(open_, close)
                    or high < low
                    or volume != volume
                    or volume in (float("inf"), float("-inf"))
                    or volume < 0
                ):
                    raise ValueError(
                        "load_history: entry {} has impossible "
                        "OHLCV (open={} high={} low={} "
                        "close={} volume={}).".format(
                            index, open_, high, low, close,
                            volume,
                        )
                    )

                c = Candle(

                    timestamp=timestamp,

                    open=open_,

                    high=high,

                    low=low,

                    close=close,

                    volume=volume,

                )

                self.history.append(c)

                self.series.append(

                    Bar(

                        timestamp=c.timestamp,

                        open=c.open,

                        high=c.high,

                        low=c.low,

                        close=c.close,

                        volume=c.volume,

                    )

                )

    # =====================================================

    def last(

        self,

        n=1,

    ):

        with self._lock:
            if n == 1:

                if self.history:

                    return self.history[-1]

                return None

            return list(self.history)[-n:]

    # =====================================================

    def count(self):

        with self._lock:
            return len(self.history)

    # =====================================================

    def clear(self):

        with self._lock:
            self.history.clear()

            self.series.clear()

            self.current = None

    # =====================================================

    def print_last(self):

        candle = self.last()

        if candle is None:

            print("No completed candles.")

            return

        print()

        print("=" * 50)

        print(

            datetime.fromtimestamp(

                candle.timestamp

            ).strftime(

                "%Y-%m-%d %H:%M"

            )

        )

        print("=" * 50)

        print(f"Open   : {candle.open:.2f}")

        print(f"High   : {candle.high:.2f}")

        print(f"Low    : {candle.low:.2f}")

        print(f"Close  : {candle.close:.2f}")

        print(f"Volume : {candle.volume:.2f}")