"""
====================================================

QuantEngine

Delta Product Normalizer (Phase 6A)

Maps raw Delta /v2/products JSON into the canonical
Instrument record -- the boundary where exchange-specific
shape ends and the engine-wide universe begins.

Real-payload defenses (mirroring delta_mapper.py):

- `contract_type` / `product_type` are lowercase; the
  normalizer uses the canonical product_type_from_delta()
  map (SPOT / PERPETUAL / FUTURE / CALL_OPTION /
  PUT_OPTION / UNKNOWN).
- `underlying_asset` and `quoting_asset` are asset OBJECTS;
  their `symbol` field is extracted.
- numeric contract fields are coerced safely (None /
  garbage -> 0.0).
- Delta disables products with is_delisted / is_disabled /
  is_unlisted flags; trading_enabled is derived from them,
  NOT from a free-form state string.

Unknown types are still normalized (product_type=UNKNOWN):
discovery keeps every product visible, the eligibility
layer decides.
====================================================
"""

from core.registry.instrument import Instrument
from core.registry.product_types import (
    PRODUCT_STATE_NEWLY_LISTED,
    product_type_from_delta,
)


def _num(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value, default=0):
    return int(_num(value, default))


def _symbol(value, default=""):
    if isinstance(value, dict):
        return value.get("symbol", "") or default
    if isinstance(value, str):
        return value
    return default


def _trading_enabled(product):
    disabled = any(
        bool(product.get(flag, False))
        for flag in ("is_delisted", "is_disabled", "is_unlisted")
    )
    state = (product.get("state") or "").lower()
    if not disabled and state in ("live", "active", ""):
        return True
    return False


class DeltaProductNormalizer:

    INTERNAL_EXCHANGE = "DELTA"

    def normalize(self, product):
        symbol = product.get("symbol", "")
        product_type = product_type_from_delta(
            product.get("contract_type")
            or product.get("product_type")
            or ""
        )

        # Delta does not emit an explicit option_type / expiry /
        # strike on every product; options carry them inside the
        # symbol (C- / P-) and `expiry_time` numeric fields when
        # present.
        option_type = (product.get("option_type") or "").lower()
        if not option_type:
            if product_type == "CALL_OPTION":
                option_type = "call"
            elif product_type == "PUT_OPTION":
                option_type = "put"

        return Instrument(
            instrument_id=str(
                product.get("id", 0) or 0
            ),
            symbol=symbol,
            exchange=self.INTERNAL_EXCHANGE,
            product_type=product_type,
            underlying_asset=_symbol(
                product.get("underlying_asset")
            ),
            quote_asset=_symbol(
                product.get("quoting_asset")
            ),
            expiry_time=_int(product.get("expiry_time", 0)) or None,
            strike_price=_num(product.get("strike_price", 0)) or None,
            option_type=option_type,
            tick_size=_num(product.get("tick_size", 0)),
            quantity_step=_num(
                product.get("contract_value", 1),
                default=1.0,
            ),
            minimum_quantity=_num(
                product.get("minimum_order_size", 0),
            ),
            trading_enabled=_trading_enabled(product),
            state=PRODUCT_STATE_NEWLY_LISTED,
            metadata={
                "raw_contract_type": product.get(
                    "contract_type", ""
                ),
                "price_precision": _int(
                    product.get("price_precision", 0),
                ),
                "quantity_precision": _int(
                    product.get("contract_value_precision", 0),
                ),
                "is_linear": bool(product.get("is_linear", False)),
                "state": product.get("state", ""),
                "maker_commission_rate": product.get("maker_commission_rate"),
                "taker_commission_rate": product.get("taker_commission_rate"),
                "funding_method": product.get("funding_method"),
                "annualized_funding": product.get("annualized_funding"),
                "funding_clamp_value": product.get("funding_clamp_value"),
            },
        )

    def normalize_many(self, products):
        return [self.normalize(p) for p in products or []]