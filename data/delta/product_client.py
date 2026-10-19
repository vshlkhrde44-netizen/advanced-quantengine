"""
====================================================

QuantEngine

Delta Product Discovery Client (Phase 6A)

Fetches the Delta /v2/products catalog through an
INJECTABLE transport so tests run against mocked Delta
responses with zero network.

    client = ProductClient(transport=MockTransport())
    products = client.fetch()          # raw /v2/products "result"
    products = client.fetch_cached()   # cached within TTL seconds

The default transport is the real DeltaREST client;
anything with .get(endpoint, params=...) works.
====================================================
"""

import time

from core.exchanges.delta.delta_rest import DeltaREST

PRODUCTS_ENDPOINT = "/v2/products"


class ProductClient:

    def __init__(self, transport=None, cache_ttl=0.0):
        self.rest = transport if transport is not None else DeltaREST()
        self._cache_ttl = cache_ttl
        self._cache = None
        self._cached_at = 0.0
        self._last_fetch_time = None

    # -------------------------------------------------

    def fetch(self):
        """Raw product list from the exchange (the `result` array
        of /v2/products). Raises on transport failure so a caller
        can never mistake a failed refresh for an empty catalog."""
        data = self.rest.get(PRODUCTS_ENDPOINT)
        if isinstance(data, dict):
            return list(data.get("result") or [])
        return list(data or [])

    def fetch_cached(self, max_age_seconds=None):
        """fetch() with a TTL cache -- a frequent refresh loop
        never hammers the exchange when products barely change."""
        ttl = (
            self._cache_ttl
            if max_age_seconds is None
            else max_age_seconds
        )
        now = time.time()
        if (
            self._cache is not None
            and ttl > 0
            and (now - self._cached_at) < ttl
        ):
            return self._cache
        products = self.fetch()
        self._cache = products
        self._cached_at = now
        return products

    def clear_cache(self):
        self._cache = None
        self._cached_at = 0.0