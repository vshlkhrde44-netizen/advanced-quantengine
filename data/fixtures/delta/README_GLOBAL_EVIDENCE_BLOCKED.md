# Global Delta Evidence — PARTIAL (2026-09-18, genuine captures)

Status date: 2026-09-18 (UTC). All fixtures below are real bounded public
captures from `wss://public-socket.delta.exchange` for `BTCUSDT`, retained
under `data/fixtures/delta/observed_global_btcusdt_*_20260918.json`.

## Observed (TESTED against the normalized chain)

* Subscription acknowledgement: multi-channel `subscriptions` echo
  (`subscription_echo`, both reconnect sessions).
* Real `candlestick_1m` schema: `o/h/l/c/v/res/sy/ts/cst` with traded volume
  (4 burst captures, 2–3 updates each, OHLCV evolving within one `cst`).
* Natural duplicate frames in 3 captures (rejected as DUPLICATE_OR_OLD;
  exactly-once delivery preserved).
* Real `trades` schema: `p/s/r/sy/t/ts` (2 frames; `r` is maker/taker role,
  never aggressor side).
* Ticker reference shape (6 frames; mark `m`, top-level `sy`, embedded
  daily-stats `ohlc` array — NOT 1m OHLCV, never strategy candles).
* Sparse publication: roughly one burst per 5–6 minutes; silent minutes are
  normal.  Cross-capture `cst` spacings are 300–360s gaps, never a 60s
  rollover.  The continuity tracker therefore reports
  `DATA_GAP_UNREPAIRED` on real traffic — the fail-closed design is
  confirmed correct by real data.
* Reconnect: 2 client-initiated sessions, each re-echoing the subscription
  (NOT server-initiated disconnect evidence).

## Still BLOCKED (missing)

* Candle rollover: two CONSECUTIVE `cst` values 60s apart in one session —
  not observed in ~25 minutes of captures across 6 sessions.
* Production wiring of the verified transport (requires the rollover above
  plus a promotion decision; system stays fail-closed until then).
* Server-initiated disconnect/recovery behavior.

## Required evidence (Phase 1)

Real bounded public frames from the exact configured Global endpoint:

* endpoint: `wss://public-socket.delta.exchange`
* universe member: `BTCUSDT` (plus each additional configured symbol)
* channels: `candlestick_1m` + `trades`
* subscription acknowledgement (`subscriptions` control echo)
* multiple candle updates sharing one `cst`
* at least one rollover to a second `cst` exactly 60s later (1m)
* reconnect/resubscription observation (serial sessions minimum)

Verifier: `scripts/verify_delta_global_evidence.py`.

Transport gate: `data/delta/verified_socket_transport.py` refuses to connect
until a bundle passes that verifier for the requested symbols.

## Current state (superseded 2026-09-18 — see PARTIAL header above)

**~~BLOCKED — no Global `BTCUSDT` capture is retained in this repository.~~**
Genuine captures now exist (see header).  Rollover and production wiring
remain BLOCKED.

* `observed_india_btcusd_*_20260916.json` fixtures are India `BTCUSD`
  evidence only.  They must NOT be substituted for Global `BTCUSDT`.
* No frame in this directory was fabricated for Global.  An absent fixture
  fails closed; it never passes the verifier.

## How to capture (credential-free, public channels only)

Run from the repository root (credential-free, public channels only):

```powershell
python scripts/capture_delta_public_frames.py `
  --endpoint wss://public-socket.delta.exchange `
  --symbol BTCUSDT `
  --channels candlestick_1m,trades `
  --timeout 130 --max-frames 500 `
  --include-control-frames `
  --output data/fixtures/delta/observed_global_btcusdt_trades_candles_rollover_<YYYYMMDD>.json

python scripts/capture_delta_public_frames.py `
  --endpoint wss://public-socket.delta.exchange `
  --symbol BTCUSDT `
  --channels candlestick_1m,trades `
  --timeout 25 --max-frames 200 `
  --include-control-frames `
  --reconnect-sessions 2 `
  --output data/fixtures/delta/observed_global_btcusdt_client_reconnect_<YYYYMMDD>.json
```

The 130s window spans a minute boundary so a rollover can be observed.
In practice (2026-09-18) the endpoint publishes roughly one burst per
5–6 minutes, so longer windows (300–600s) or boundary-timed captures are
more likely to observe two bursts.  Verify before claiming anything:

```powershell
python scripts/verify_delta_global_evidence.py --bundle <bundle> --symbols BTCUSDT
```

## Rules

* Do NOT copy/rename an India fixture as Global evidence.
* Do NOT hand-write candle/trade frames.
* Do NOT treat a subscription echo as finality, sequence, or continuity.
* Client serial sessions do NOT prove server-initiated disconnect recovery;
  every reconnect must still pass `CandleSessionGate`.
