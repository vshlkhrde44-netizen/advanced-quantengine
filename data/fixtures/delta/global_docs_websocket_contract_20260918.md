# Global Delta Public Contract — Documentation Fixture (not evidence)

Accessed: 2026-09-18. Source: official Delta Exchange API documentation
at <https://docs.delta.exchange/> (single-page docs; WebSocket Feed,
Subscribing to Channels, Public Channels sections) plus the repository's
configured endpoint constants.

This file records DOCUMENTATION ONLY.  It is not captured exchange traffic
and must never be fed to the normalizer, the verifier, or any test as if it
were observed frames.

## Endpoints (as documented / configured)

* Global public socket (repository production default):
  `wss://public-socket.delta.exchange`
* India public socket (documented India venue; separate fixture):
  `wss://public-socket.india.delta.exchange`
* Global REST base (per `DeltaREST.BASE_URL` consistency check):
  host contains `api.delta.exchange` (India REST uses
  `api.india.delta.exchange`; testnet uses `cdn-ind.testnet.deltaex.org`).

India and Global endpoints are NOT interchangeable.  Product universes
differ (India: `BTCUSD`-style symbols; Global: `BTCUSDT`-style symbols).

## Public channels (documentation section titles observed 2026-09-18)

`ticker`, `ob_l1`, `ob_l2`, `ob_updates`, `trades`, `mark_price`,
`candlesticks`, `spot_price`, `spot_30mtwap_price`, `funding_rate`,
`product_updates`, `system_status`.

Documented subscribe envelope shape (channel list under
`payload.channels` with `name` + `symbols`):

```json
{"type": "subscribe", "payload": {"channels": [{"name": "<channel>", "symbols": ["<SYMBOL>"]}]}}
```

## What documentation does NOT establish

* Exact live Global `candlestick_1m` / `trades` field sets for `BTCUSDT`
  (compact keys `o/h/l/c/v/res/sy/ts/cst`, role key `r`, etc. remain
  EXTERNAL VERIFICATION REQUIRED until a bounded Global capture lands).
* Any candle-close/finality flag or public sequence number.
* Subscription-acknowledgement schema or reconnect/resubscribe guarantees.
* Contract metadata (tick size, quantity step, multiplier, settlement),
  fees, funding, margin, or bracket semantics.

## Relationship to code

* Normalizers (`data/delta/market_events.py`) implement the compact shapes
  above as DOCUMENTED contracts with `sequence=None`, `closed=False`,
  missing volume as `None`, and maker/taker role preserved without
  aggressor-side inference.
* `is_observed_subscription_snapshot()` recognizes only the OBSERVED
  `{"type": "subscriptions", "channels": [...]}` echo shape retained in a
  capture with `--include-control-frames`; documentation alone never
  satisfies it.
* Global runtime wiring stays refused until
  `scripts/verify_delta_global_evidence.py` passes on a real Global bundle.
