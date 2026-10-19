"""
====================================================

QuantEngine

Delta Product Discovery (Phase 6A)

The end-to-end discovery workflow:

    ProductClient.fetch()      -> raw /v2/products rows
    DeltaProductNormalizer     -> canonical Instrument records
    InstrumentRegistry.upsert  -> lifecycle state
    InstrumentRegistry.save()  -> persistent catalog

returning perf-run summary dicts:

    discovery.refresh(client, registry, normalizer)
      {"discovered": n, "newly_listed": [...], "delisted": [...]}

DISCOVER EVERYTHING -- no hardcoded symbol, no default
universe. The registry is the single source of truth; the
eligibility layer (Phase 6B) decides what may be TRADED.
====================================================
"""

from data.delta.product_client import ProductClient
from data.delta.product_normalizer import (
    DeltaProductNormalizer,
)

DEFAULT_PRODUCTS_ENDPOINT = "/v2/products"


def discover_products(
    client=None,
    normalizer=None,
):
    """
    Fetch + normalize the raw product catalog. Pure read --
    no persistence, no state. Returns the canonical
    Instrument list.
    """
    client = client if client is not None else ProductClient()
    normalizer = (
        normalizer
        if normalizer is not None
        else DeltaProductNormalizer()
    )
    raw = client.fetch()
    return normalizer.normalize_many(raw)


def refresh_instrument_registry(
    registry,
    client=None,
    normalizer=None,
):
    """
    The product-refresh heartbeat:

        1. discover all products (normalized instruments)
        2. upsert into the persistent registry
           (new FIRST sightings -> NEWLY_LISTED, repeats ->
           ACTIVE)
        3. mark_present() -> any instrument that vanished is
           DELISTED (graceful, still queryable)
        4. save() -> the new catalog persists

    Returns ("newly_listed",) / (summary,) for refresh logging.
    """
    instruments = discover_products(
        client=client,
        normalizer=normalizer,
    )

    newly_listed = []
    for instrument in instruments:
        stored, first = registry.upsert(instrument)
        if first:
            newly_listed.append(stored.symbol)

    registry.mark_present(
        i.key for i in instruments
    )
    if registry.path is not None:
        registry.save()

    return {
        "discovered": len(instruments),
        "newly_listed": newly_listed,
        "delisted": [
            i.symbol
            for i in registry.delisted()
        ],
        "total_catalog": len(registry),
    }