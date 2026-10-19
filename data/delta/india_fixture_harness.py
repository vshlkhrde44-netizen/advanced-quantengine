"""India advisory fixture harness (TEST AND FIXTURE INFRASTRUCTURE ONLY).

This module is deliberately NOT part of any production runtime.  It must
never be imported by ``main.py``, ``data/websocket_client.py``,
``data/delta/verified_socket_transport.py``, ``core/runtime``,
strategy, execution, or risk code.  It exists only to:

* deterministically redact bounded, credential-free India public captures
  into advisory fixtures (strip nondeterministic transport metadata only);
* deterministically serialize/hash those fixtures (byte-identical output);
* classify India evidence with a precise vocabulary
  (VERIFIED_SCHEMA / VERIFIED_BEHAVIOR / INSUFFICIENT / NOT_OBSERVED /
  UNAVAILABLE) without ever promoting unavailable evidence to verified;
* provide the test-only ``_fx_rollover_skip_delta`` capability, which means
  exactly: "skip the India unavailable-rollover assertion because India
  rollover evidence is currently unavailable in this bundle".  It never
  means "India rollover is verified" and never affects production, Global
  evidence validation, or readiness.

Evidence thresholds (advisory India track, Global bar unchanged):

* candlestick_1m schema: >= 1 genuine frame -> VERIFIED_SCHEMA.
* trades flow: >= 2 genuine frames -> VERIFIED_BEHAVIOR, else INSUFFICIENT.
* ticker schema: >= 1 genuine frame -> VERIFIED_SCHEMA, else UNAVAILABLE.
* duplicate candle: >= 1 exact duplicate update -> VERIFIED_BEHAVIOR,
  else NOT_OBSERVED.
* rollover: >= 2 distinct ``cst`` values exactly one resolution step apart
  (60s for 1m, in seconds or microseconds) with multiple updates sharing
  at least one ``cst`` -> VERIFIED_BEHAVIOR, else NOT_OBSERVED.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from typing import Any


INDIA_PUBLIC_SOCKET = "wss://public-socket.india.delta.exchange"
INDIA_SYMBOL = "BTCUSD"
CAPTURE_SCHEMA = "quantengine.delta-public-capture.v1"

RESOLUTION_STEP_SECONDS = {"1m": 60}

# Environment variables that enable the test-only rollover skip.  The
# canonical test capability name is ``_fx_rollover_skip_delta``; longer
# aliases exist only so CI can set the flag without a leading underscore.
SKIP_ENV_VARS = (
    "_fx_rollover_skip_delta",
    "FX_ROLLOVER_SKIP_DELTA",
    "QUANTENGINE_FX_ROLLOVER_SKIP_DELTA",
)

# Substrings that must never appear in a persisted fixture frame key or in a
# URL-like string value.  The capture is public and credential-free, so any
# such occurrence is treated as a defect, never persisted.
_SENSITIVE_KEY_SUBSTRINGS = (
    "api_key",
    "api_secret",
    "authorization",
    "bearer",
    "cookie",
    "private_key",
    "secret",
    "token",
)


def _contains_sensitive_key(obj: Any) -> str | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            lowered = str(key).lower()
            for marker in _SENSITIVE_KEY_SUBSTRINGS:
                if marker in lowered:
                    return str(key)
            found = _contains_sensitive_key(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _contains_sensitive_key(item)
            if found is not None:
                return found
    elif isinstance(obj, str):
        lowered = obj.lower()
        if "authorization" in lowered and "://" in lowered:
            return obj[:32]
    return None


def assert_no_secret_material(bundle: dict[str, Any]) -> None:
    """Fail closed if a bundle appears to contain credential-like material."""
    for record in bundle.get("frames", []):
        if not isinstance(record, dict):
            continue
        frame = record.get("frame")
        hit = _contains_sensitive_key(frame)
        if hit is not None:
            raise ValueError(
                "refusing to persist fixture with sensitive material: %r" % hit
            )
    endpoint = str(bundle.get("endpoint", ""))
    if "@" in endpoint or "api-key" in endpoint.lower():
        raise ValueError("refusing to persist fixture with sensitive endpoint")


def redact_capture_for_fixture(bundle: dict[str, Any]) -> dict[str, Any]:
    """Return a deterministic, redacted advisory copy of a raw capture.

    Preserved exactly: every ``frame`` mapping, ``classification``,
    ordering, exchange timestamps (``cst``/``ts``/``t``), symbol/product
    identifiers, OHLCV, trade fields, ticker fields, and control envelopes.
    Removed only: nondeterministic transport metadata (``receipt_unix_ns``,
    ``started_unix``).  ``receipt_ordinal`` is recomputed deterministically
    as 1..N in stored order.  No frames are added, removed, reordered, or
    synthesized; no rollover/trades/ticker frames are fabricated.
    """
    if not isinstance(bundle, dict):
        raise ValueError("capture bundle must be a mapping")
    if bundle.get("schema") != CAPTURE_SCHEMA:
        raise ValueError("unexpected capture schema: %r" % (bundle.get("schema"),))
    if bundle.get("endpoint") != INDIA_PUBLIC_SOCKET:
        raise ValueError("India harness requires %r" % INDIA_PUBLIC_SOCKET)
    assert_no_secret_material(bundle)
    records = bundle.get("frames")
    if not isinstance(records, list):
        raise ValueError("capture bundle has no frames list")
    redacted_records: list[dict[str, Any]] = []
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict) or not isinstance(record.get("frame"), dict):
            raise ValueError("frame record %d is not a mapping" % index)
        frame = copy.deepcopy(record["frame"])
        classification = record.get("classification")
        entry: dict[str, Any] = {
            "receipt_ordinal": index,
            "frame": frame,
        }
        if classification is not None:
            entry["classification"] = classification
        redacted_records.append(entry)
    redacted: dict[str, Any] = {
        "schema": CAPTURE_SCHEMA,
        "endpoint": bundle.get("endpoint"),
        "symbol": bundle.get("symbol"),
        "requested_channels": list(bundle.get("requested_channels", [])),
        "observed_channels": sorted(
            {str(r["frame"].get("type")) for r in redacted_records}
        ),
        "frames": redacted_records,
        "control_frames": sum(
            1 for r in redacted_records if r.get("classification") == "control_or_unrequested"
        ),
        "malformed_frames": int(bundle.get("malformed_frames", 0)),
        "max_frames": bundle.get("max_frames"),
        "timeout_seconds": bundle.get("timeout_seconds"),
        "capture_complete": bool(bundle.get("capture_complete", False)),
        "limitations": list(bundle.get("limitations", [])),
        "advisory_only": True,
        "production_wiring": "UNCHANGED",
        "provenance": {
            "source": "bounded credential-free capture of %s %s (public channels only, no auth, no orders)"
            % (INDIA_PUBLIC_SOCKET, bundle.get("symbol")),
            "redaction": "removed receipt_unix_ns and started_unix only; "
            "receipt_ordinal recomputed deterministically; "
            "frames byte-preserved; no frames synthesized",
            "redacted_by": "data.delta.india_fixture_harness.redact_capture_for_fixture",
        },
    }
    return redacted


def serialize_fixture(bundle: dict[str, Any]) -> str:
    """Serialize deterministically: sorted keys, 2-space indent, trailing NL."""
    return json.dumps(bundle, indent=2, sort_keys=True) + "\n"


def hash_fixture(bundle: dict[str, Any]) -> str:
    """Stable sha256 over the deterministic serialization."""
    return hashlib.sha256(serialize_fixture(bundle).encode("utf-8")).hexdigest()


def _frames(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    records = bundle.get("frames", [])
    out: list[dict[str, Any]] = []
    for record in records:
        if isinstance(record, dict) and isinstance(record.get("frame"), dict):
            out.append(record["frame"])
    return out


def candle_starts(candles: list[dict[str, Any]]) -> list[int]:
    starts: list[int] = []
    for frame in candles:
        cst = frame.get("cst")
        if isinstance(cst, bool):
            continue
        if isinstance(cst, (int, float)) and cst > 0:
            starts.append(int(cst))
    return starts


def has_india_rollover(bundle: dict[str, Any], resolution: str = "1m") -> bool:
    """True only for an actual observed cross-boundary rollover sequence."""
    step = RESOLUTION_STEP_SECONDS.get(resolution, 60)
    candles = [f for f in _frames(bundle) if f.get("type") == "candlestick_1m"]
    starts = candle_starts(candles)
    if len(candles) < 2 or len(starts) < 2:
        return False
    distinct = list(dict.fromkeys(starts))
    if len(distinct) < 2:
        return False
    # Require multiple updates sharing at least one cst (a real update
    # sequence, not two isolated frames) plus a consecutive-step transition.
    from collections import Counter as _Counter

    counts = _Counter(starts)
    shared = any(count >= 2 for count in counts.values())
    consecutive = any(
        distinct[i + 1] - distinct[i] == step * 1_000_000
        or distinct[i + 1] - distinct[i] == step
        for i in range(len(distinct) - 1)
    )
    return bool(shared and consecutive)


def has_duplicate_candle(bundle: dict[str, Any]) -> bool:
    candles = [f for f in _frames(bundle) if f.get("type") == "candlestick_1m"]
    serialized = [json.dumps(frame, sort_keys=True) for frame in candles]
    return len(serialized) > len(set(serialized)) and len(serialized) > 0


def classify_india_evidence(bundle: dict[str, Any]) -> dict[str, str]:
    """Classify advisory India evidence with the precise status vocabulary."""
    frames = _frames(bundle)
    candles = [f for f in frames if f.get("type") == "candlestick_1m"]
    trades = [f for f in frames if f.get("type") == "trades"]
    tickers = [f for f in frames if f.get("type") == "ticker"]
    out: dict[str, str] = {}
    out["candlestick_1m"] = "VERIFIED_SCHEMA" if len(candles) >= 1 else "INSUFFICIENT"
    if len(trades) >= 2:
        out["trades"] = "VERIFIED_BEHAVIOR"
    elif len(trades) == 1:
        out["trades"] = "INSUFFICIENT"
    else:
        out["trades"] = "UNAVAILABLE"
    out["ticker"] = "VERIFIED_SCHEMA" if len(tickers) >= 1 else "UNAVAILABLE"
    out["duplicate_candle"] = (
        "VERIFIED_BEHAVIOR" if has_duplicate_candle(bundle) else "NOT_OBSERVED"
    )
    out["rollover"] = (
        "VERIFIED_BEHAVIOR" if has_india_rollover(bundle) else "NOT_OBSERVED"
    )
    return out


def fx_rollover_skip_requested(explicit: bool | None = None) -> bool:
    """Return True only when the test-only India rollover skip is requested.

    ``explicit=True/False`` overrides the environment.  Otherwise any of
    ``SKIP_ENV_VARS`` set to ``1``/``true``/``True`` enables the skip.  This
    helper has no production effect: it is consulted only by India advisory
    fixture tests, never by transport, Global validation, strategy,
    execution, risk, or readiness code.
    """
    if explicit is not None:
        return bool(explicit)
    for name in SKIP_ENV_VARS:
        if os.getenv(name, "") in ("1", "true", "True"):
            return True
    return False


def require_india_rollover(
    bundle: dict[str, Any], *, skip: bool | None = None
) -> str:
    """Enforce the India rollover assertion with an explicit skip path.

    Returns ``"OBSERVED"`` when a genuine rollover sequence is present.
    Returns ``"NOT_OBSERVED_SKIPPED"`` when rollover is absent but the
    test-only ``_fx_rollover_skip_delta`` skip was explicitly requested.
    Raises ``AssertionError`` when rollover is absent and no skip was
    requested.  The skipped status must never be presented as verified.
    """
    if has_india_rollover(bundle):
        return "OBSERVED"
    if fx_rollover_skip_requested(skip):
        return "NOT_OBSERVED_SKIPPED"
    raise AssertionError(
        "INDIA_ROLLOVER_NOT_OBSERVED: no consecutive-cst rollover in bundle; "
        "pass skip=True (_fx_rollover_skip_delta) to record the skip explicitly"
    )
