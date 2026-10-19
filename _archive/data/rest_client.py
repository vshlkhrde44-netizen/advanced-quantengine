"""
====================================================

QuantEngine

Build : 2.7
Module: Delta REST Client

Purpose:
Provides a reusable interface to Delta Exchange REST API.

====================================================
"""

from __future__ import annotations

from typing import Any

import httpx

from config.settings import settings


class DeltaREST:
    """Delta Exchange REST client."""

    BASE_URL = "https://api.delta.exchange"

    def __init__(self) -> None:
        self.client = httpx.Client(
            base_url=self.BASE_URL,
            timeout=10.0,
        )

    def _get(self, endpoint: str, params: dict | None = None) -> Any:
        """Internal GET helper."""

        response = self.client.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------
    # Public Endpoints
    # -------------------------------------------------

    def ping(self) -> Any:
        return self._get("/v2/products")

    def products(self) -> Any:
        return self._get("/v2/products")

    def product(self, symbol: str) -> Any:
        products = self.products()["result"]

        for product in products:
            if product["symbol"] == symbol:
                return product

        return None

    def close(self) -> None:
        self.client.close()