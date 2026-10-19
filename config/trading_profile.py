"""
====================================================

Trading Profile — DEAD CONFIGURATION (forensic verdict 2026-09-22)

DO NOT USE for gating. The authoritative trading policy is the runtime policy manager
(``core.risk.trading_policy``): global 1:1..1:20 bounds with exact
instrument overrides. ``MIN_RISK_REWARD = 6.0`` remains research-only.
This legacy profile is retained for provenance and is not an executable
risk source.

====================================================
"""

PROFILE = "Balanced"

PROFILES = {

    "Conservative": {

        "risk_per_trade": 0.01,

        "allocation": 500,

        "min_rr": 3.0,

        "min_confidence": 90,

    },

    "Balanced": {

        "risk_per_trade": 0.015,

        "allocation": 500,

        "min_rr": 2.5,

        "min_confidence": 80,

    },

    "Aggressive": {

        "risk_per_trade": 0.02,

        "allocation": 500,

        "min_rr": 2.0,

        "min_confidence": 75,

    },

}