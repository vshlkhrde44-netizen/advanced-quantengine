"""Opt-in append-only durable journal for normalized public market events.

The journal persists only accepted, typed market events.  It contains no
credentials and does not infer missing market facts.  ``append`` returns only
after the complete JSONL record is flushed and fsynced; recovery refuses a
partial or unknown record rather than silently skipping it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import threading
from typing import Iterator

from data.delta.market_events import (
    BookLevel,
    EventProvenance,
    NormalizedCandle,
    NormalizedL1Book,
    NormalizedL2Book,
    NormalizedTrade,
)


MarketEvent = NormalizedTrade | NormalizedCandle | NormalizedL1Book | NormalizedL2Book
SCHEMA_VERSION = 1


class JournalCorruption(ValueError):
    """A persisted record cannot be trusted for deterministic recovery."""


@dataclass(frozen=True, slots=True)
class JournalRecord:
    schema_version: int
    ordinal: int
    event_type: str
    event: dict


def _event_type(event: MarketEvent) -> str:
    if isinstance(event, NormalizedTrade):
        return "TRADE"
    if isinstance(event, NormalizedCandle):
        return "CANDLE"
    if isinstance(event, NormalizedL1Book):
        return "L1_BOOK"
    if isinstance(event, NormalizedL2Book):
        return "L2_BOOK"
    raise TypeError("event must be a normalized market event")


class NormalizedEventJournal:
    """A process-safe JSONL journal with strict read validation."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._next_ordinal = self._last_ordinal() + 1

    def _last_ordinal(self) -> int:
        records = list(self.records())
        return records[-1].ordinal if records else 0

    def append(self, event: MarketEvent) -> JournalRecord:
        """Durably append one typed event and return its persisted record."""
        with self._lock:
            record = JournalRecord(
                schema_version=SCHEMA_VERSION,
                ordinal=self._next_ordinal,
                event_type=_event_type(event),
                event=asdict(event),
            )
            serialized = json.dumps(asdict(record), sort_keys=True, separators=(",", ":")) + "\n"
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(serialized)
                handle.flush()
                os.fsync(handle.fileno())
            self._next_ordinal += 1
            return record

    def records(self) -> Iterator[JournalRecord]:
        """Yield every record in order; reject corruption rather than omit it."""
        if not self.path.exists():
            return
        previous = 0
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.endswith("\n"):
                    raise JournalCorruption(f"partial record at line {line_number}")
                try:
                    value = json.loads(line)
                    record = JournalRecord(**value)
                except (TypeError, ValueError, json.JSONDecodeError) as error:
                    raise JournalCorruption(f"invalid record at line {line_number}") from error
                if (
                    record.schema_version != SCHEMA_VERSION
                    or record.event_type not in {"TRADE", "CANDLE", "L1_BOOK", "L2_BOOK"}
                    or not isinstance(record.event, dict)
                    or record.ordinal != previous + 1
                ):
                    raise JournalCorruption(f"untrusted record at line {line_number}")
                previous = record.ordinal
                yield record


def decode_record(record: JournalRecord) -> MarketEvent:
    """Reconstruct a typed normalized event from a validated journal record.

    This decoder intentionally accepts the current schema only.  A future
    migration must provide an explicit upgrader rather than reinterpreting
    historical JSON with a new constructor contract.
    """
    if record.schema_version != SCHEMA_VERSION:
        raise JournalCorruption("unsupported schema version")
    try:
        values = dict(record.event)
        provenance = EventProvenance(**values.pop("provenance"))
        if record.event_type == "TRADE":
            return NormalizedTrade(provenance=provenance, **values)
        if record.event_type == "CANDLE":
            return NormalizedCandle(provenance=provenance, **values)
        if record.event_type == "L1_BOOK":
            return NormalizedL1Book(provenance=provenance, **values)
        if record.event_type == "L2_BOOK":
            return NormalizedL2Book(
                provenance=provenance,
                bids=tuple(BookLevel(**level) for level in values["bids"]),
                asks=tuple(BookLevel(**level) for level in values["asks"]),
            )
    except (KeyError, TypeError, ValueError) as error:
        raise JournalCorruption("record does not match normalized event schema") from error
    raise JournalCorruption("unknown normalized event type")
