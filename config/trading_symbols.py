"""
====================================================

Trading Universe Configuration

====================================================
"""

# Load every perpetual product from Delta

AUTO_DISCOVER = True

# -------------------------------------------------

# Manual Trading Control

AUTO_SYMBOL_SELECTION = False

# -------------------------------------------------

# Markets allowed to trade

ENABLED_SYMBOLS = [

    "BTCUSDT",

    "ETHUSDT",

    "DOGEUSDT",

    "XRPUSDT",

    "AAPLXUSDT",

    "GOOGLXUSDT",

    "NVDAXUSDT",

]

# -------------------------------------------------

# Markets never trade

DISABLED_SYMBOLS = []

# -------------------------------------------------

# Future

MAX_ACTIVE_SYMBOLS = 20