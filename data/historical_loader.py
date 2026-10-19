"""
====================================================

QuantEngine

Build : 2.9
Module: Historical Loader

Downloads historical OHLC candles from Delta Exchange.

Delta Exchange's /v2/history/candles endpoint caps out at
roughly 2000 candles per single request (source: Delta's own
API docs/community support -- this varies slightly by API
version/region, so this module treats it as a conservative
limit rather than an exact contract). Requesting more than
that in one call to `candles()` will silently return fewer
candles than asked for, not an error -- which is why the
default single-call limit is enforced here, and why
`candles_paginated()` exists for anything larger.

====================================================
"""

import time

import httpx

from core.common.logging_config import get_logger

log = get_logger(__name__)

BASE_URL = "https://api.delta.exchange"

# Conservative single-request cap. Delta's documented limit is
# ~2000-4000 depending on source; 2000 is used here so a single
# `candles()` call never silently truncates without at least a
# warning.
MAX_CANDLES_PER_REQUEST = 2000

RESOLUTION_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "1d": 86400,
    "1w": 604800,
}


class HistoricalLoader:

    def __init__(self, base_url=None):

        # Environment separation: an explicit base_url (e.g. the
        # India Testnet catalog host) selects the venue; the
        # default preserves the legacy Global endpoint so every
        # existing research caller is behavior-neutral.
        self.base_url = (base_url or BASE_URL).rstrip("/")

        self.client = httpx.Client(timeout=15)

    def close(self):

        self.client.close()

    # -------------------------------------------------

    def candles(
        self,
        symbol: str,
        resolution: str = "1m",
        candles: int = 500,
    ):
        """
        Download the last N candles in a single request.

        N is silently clamped to MAX_CANDLES_PER_REQUEST if
        larger, since Delta's API will not return more than
        that from one call regardless of what's requested --
        without this clamp, asking for e.g. 10000 would
        silently come back with ~2000 and look like a bug
        rather than an API limit. Use candles_paginated() for
        anything beyond a single request's worth of history.
        """

        if resolution not in RESOLUTION_SECONDS:
            raise ValueError(f"Unsupported resolution: {resolution}")

        if candles > MAX_CANDLES_PER_REQUEST:

            log.warning(
                "Requested %d candles but a single request is "
                "capped at %d by Delta's API; use "
                "candles_paginated() for more history. "
                "Clamping to %d for this call.",
                candles, MAX_CANDLES_PER_REQUEST, MAX_CANDLES_PER_REQUEST,
            )

            candles = MAX_CANDLES_PER_REQUEST

        end = int(time.time())

        start = end - (candles * RESOLUTION_SECONDS[resolution])

        return self._fetch(symbol, resolution, start, end)

    # -------------------------------------------------

    def candles_paginated(
        self,
        symbol: str,
        resolution: str = "1m",
        total_candles: int = 5000,
        request_delay: float = 0.2,
        end_timestamp: int | None = None,
    ):
        """
        Download more candles than fit in a single request by
        paginating backward in time. Returns the same
        {"success": ..., "result": [...]} shape as candles(),
        with `result` sorted newest-first (matching Delta's own
        response ordering) and deduplicated by timestamp.

        request_delay is a small pause between requests to be a
        polite API citizen -- Delta enforces rate limits and a
        tight backward-paging loop with no delay risks 429s.

        ``end_timestamp`` is an optional deterministic acquisition anchor.
        When supplied, pagination is reproducible and does not consult the
        wall clock. Existing callers that omit it retain the historical
        live-window behavior.
        """

        if resolution not in RESOLUTION_SECONDS:
            raise ValueError(f"Unsupported resolution: {resolution}")

        step_seconds = RESOLUTION_SECONDS[resolution]

        if end_timestamp is None:
            end = int(time.time())
        else:
            if (isinstance(end_timestamp, bool) or
                    not isinstance(end_timestamp, int) or
                    end_timestamp <= 0):
                raise ValueError("end_timestamp must be a positive integer")
            end = int(end_timestamp)

        all_results = []
        seen_timestamps = set()

        remaining = total_candles

        while remaining > 0:

            batch_size = min(remaining, MAX_CANDLES_PER_REQUEST)

            start = end - (batch_size * step_seconds)

            log.debug(
                "Fetching batch: symbol=%s resolution=%s "
                "start=%d end=%d size=%d",
                symbol, resolution, start, end, batch_size,
            )

            data = self._fetch(symbol, resolution, start, end)

            batch = data.get("result", [])

            if not batch:
                # No more history available for this symbol --
                # stop paginating rather than looping forever.
                log.debug(
                    "No more candles returned; stopping "
                    "pagination with %d collected.",
                    len(all_results),
                )
                break

            new_count = 0

            for candle in batch:

                ts = candle.get("time")

                if ts is not None and ts not in seen_timestamps:

                    seen_timestamps.add(ts)
                    all_results.append(candle)
                    new_count += 1

            if new_count == 0:
                # Every candle in this batch was already seen --
                # the API is returning an overlapping/unmoving
                # window, so stop instead of looping forever.
                log.debug(
                    "Batch returned no new candles; stopping "
                    "pagination."
                )
                break

            # Phase 12 acquisition fix: decrement by NEW
            # candles, not by requested batch size. Sparse
            # history (a batch thinner than requested) used to
            # under-fetch: `remaining` shrank by the full batch
            # size while fewer rows arrived, so the caller
            # silently received less history than requested.
            remaining -= new_count
            end = start  # walk further back in time

            if remaining > 0:
                time.sleep(request_delay)

        all_results.sort(key=lambda c: c.get("time", 0), reverse=True)

        log.info(
            "candles_paginated: requested=%d collected=%d symbol=%s",
            total_candles, len(all_results), symbol,
        )

        return {"success": True, "result": all_results}

    # -------------------------------------------------

    def _fetch(self, symbol, resolution, start, end):

        params = {
            "symbol": symbol,
            "resolution": resolution,
            "start": start,
            "end": end,
        }

        response = self.client.get(
            self.base_url + "/v2/history/candles",
            params=params,
        )

        response.raise_for_status()

        return response.json()