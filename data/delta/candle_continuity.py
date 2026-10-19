"""Explicit continuity tracking for normalized candles.

This tracker observes candle-start timestamps only.  It never backfills,
interpolates, or treats provider silence as a zero-volume candle.
"""

from __future__ import annotations

from dataclasses import dataclass

from data.candle_builder import TIMEFRAME_SECONDS
from data.delta.market_events import NormalizedCandle


@dataclass(frozen=True, slots=True)
class CandleGap:
    venue: str
    instrument: str
    resolution: str
    after_start: int
    before_start: int
    missing_candles: int


class CandleContinuityTracker:
    def __init__(self):
        self._latest: dict[tuple[str, str, str], int] = {}
        self._gaps: dict[tuple[str, str, str], list[CandleGap]] = {}
        self.rejected = 0

    @staticmethod
    def _key(candle: NormalizedCandle) -> tuple[str, str, str]:
        p = candle.provenance
        return p.venue, p.instrument, candle.resolution

    def ingest(self, candle: NormalizedCandle) -> CandleGap | None:
        start = candle.candle_start_timestamp
        window = TIMEFRAME_SECONDS.get(candle.resolution)
        if start is None or window is None:
            self.rejected += 1
            return None
        key = self._key(candle)
        previous = self._latest.get(key)
        if previous is None:
            self._latest[key] = start
            return None
        if start <= previous:
            if start < previous:
                self.rejected += 1
            return None
        self._latest[key] = start
        elapsed = start - previous
        if elapsed <= window:
            return None
        gap = CandleGap(
            venue=key[0], instrument=key[1], resolution=key[2],
            after_start=previous, before_start=start,
            missing_candles=(elapsed // window) - 1,
        )
        self._gaps.setdefault(key, []).append(gap)
        return gap

    def repair(
        self, venue: str, instrument: str, resolution: str,
        repaired_starts: list[int] | tuple[int, ...],
    ) -> dict:
        """Clear only gaps whose complete expected candle starts were restored.

        This method changes continuity state only; it never invents candles and
        never advances the live watermark.  A range is cleared only when the
        supplied starts exactly cover every missing bucket for that range.
        """
        window = TIMEFRAME_SECONDS.get(resolution)
        if window is None or window <= 0:
            return {"ranges_repaired": 0, "rejected": len(repaired_starts),
                    "reason": "unsupported resolution"}
        starts = sorted(set(int(value) for value in repaired_starts))
        key = (venue, instrument, resolution)
        with_gap = self._gaps.get(key, [])
        if not with_gap:
            return {"ranges_repaired": 0, "rejected": len(starts),
                    "reason": "no matching gap"}

        remaining = []
        repaired_ranges = 0
        consumed: set[int] = set()
        for gap in with_gap:
            expected = list(range(gap.after_start + window, gap.before_start, window))
            if starts and all(item in starts for item in expected):
                repaired_ranges += 1
                consumed.update(expected)
            else:
                remaining.append(gap)
        if remaining:
            self._gaps[key] = remaining
        else:
            self._gaps.pop(key, None)
        return {
            "ranges_repaired": repaired_ranges,
            "rejected": len([item for item in starts if item not in consumed]),
            "reason": "all matching gaps repaired" if repaired_ranges else "incomplete repair",
        }

    def status(self, venue: str, instrument: str, resolution: str) -> dict:
        gaps = self._gaps.get((venue, instrument, resolution), [])
        return {
            "status": "DATA_GAP_UNREPAIRED" if gaps else "CONTINUOUS",
            "gaps": list(gaps),
            "missing_candles": sum(gap.missing_candles for gap in gaps),
        }
