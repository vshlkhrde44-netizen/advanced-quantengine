"""Deterministic state reconstruction from a validated normalized-event journal.

Replay is deliberately state-only: it never invokes strategy callbacks and
never treats historical candles as newly closed runtime candles.  The journal
is an evidence source, not an execution trigger.
"""

from __future__ import annotations

from dataclasses import dataclass

from data.delta.event_journal import NormalizedEventJournal, decode_record
from data.delta.market_events import NormalizedCandle, NormalizedL1Book, NormalizedL2Book, NormalizedTrade
from data.delta.market_state_store import MarketStateStore


@dataclass(frozen=True, slots=True)
class ReplayReport:
    records_seen: int
    events_applied: int
    duplicate_or_old: int


def replay_journal(journal: NormalizedEventJournal, store: MarketStateStore | None = None) -> tuple[MarketStateStore, ReplayReport]:
    """Apply journal records in durable ordinal order into a fresh state store.

    Corruption is propagated from the journal reader/decoder.  A duplicate or
    stale event remains observable in the report; it is never fabricated into
    a state update.
    """
    target = store if store is not None else MarketStateStore()
    seen = applied = duplicates = 0
    for record in journal.records():
        seen += 1
        event = decode_record(record)
        if isinstance(event, NormalizedTrade):
            accepted = target.apply_trade(event)
        elif isinstance(event, NormalizedCandle):
            accepted = target.apply_candle(event)
        elif isinstance(event, (NormalizedL1Book, NormalizedL2Book)):
            accepted = target.apply_order_book(event)
        else:  # defensive: decode_record is closed over the union
            raise TypeError("unsupported normalized event")
        if accepted:
            applied += 1
        else:
            duplicates += 1
    return target, ReplayReport(seen, applied, duplicates)
