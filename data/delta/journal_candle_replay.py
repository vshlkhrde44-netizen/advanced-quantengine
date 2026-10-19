"""Build deterministic legacy replay candles from a normalized event journal.

The journal stores forming candle updates, not a trusted close flag. This
adapter applies the same finality and continuity rules as public ingress;
therefore replay never turns an end-of-file forming candle into a closed bar.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.history.candle import Candle
from data.delta.candle_continuity import CandleContinuityTracker
from data.delta.candle_finality import CandleFinalityTracker
from data.delta.event_journal import NormalizedEventJournal, decode_record
from data.delta.market_events import NormalizedCandle
from data.delta.public_event_stream import IngestResult
from data.delta.runtime_candle_bridge import to_runtime_candle


@dataclass(frozen=True, slots=True)
class JournalCandleReplayReport:
    records_seen: int
    candle_records_seen: int
    delivered: int
    suppressed: int


def replay_verified_candles(journal: NormalizedEventJournal, *, symbol: str, resolution: str) -> tuple[list[Candle], JournalCandleReplayReport]:
    """Replay only causally closed, continuous, traded-price volume candles."""
    finality = CandleFinalityTracker()
    continuity = CandleContinuityTracker()
    output: list[Candle] = []
    seen = candle_seen = suppressed = 0
    for record in journal.records():
        seen += 1
        event = decode_record(record)
        if not isinstance(event, NormalizedCandle):
            continue
        candle_seen += 1
        if event.provenance.instrument != symbol or event.resolution != resolution:
            continue
        continuity.ingest(event)
        closed = finality.ingest(event)
        status = continuity.status(event.provenance.venue, symbol, resolution)["status"]
        result = IngestResult(
            "APPLIED", "candlestick_" + resolution,
            closed_candle=closed,
            data_gap_status=status,
        )
        decision = to_runtime_candle(result, expected_resolution=resolution)
        if decision.candle is None:
            suppressed += 1
        else:
            output.append(decision.candle)
    return output, JournalCandleReplayReport(seen, candle_seen, len(output), suppressed)
