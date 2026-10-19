"""
====================================================

QuantEngine

Live WebSocket Engine -- LEGACY / DEPRECATED PATH.

FAIL-CLOSED (Phase 2): this feed maintains the single-symbol
mark-price REFERENCE ONLY (MarketState.mark_price / best_bid /
best_ask / spread / timestamp / connected).  It MUST NOT synthesize
strategy candles from ticker marks: ticker marks are neither traded
OHLCV nor an evidenced candle-finality source, and every candle built
here has zero reported volume.

Strategy candle publication is DISABLED by default
(``publish_candles=False``).  The canonical production feed is the
verified traded-candle transport
(data/delta/verified_socket_transport.py) driven through the 6D
real-time subscription manager.  This module is retained for operator
mark-price reference and explicit legacy tests only; no new strategy
consumers may be added.

To re-enable the historical ticker->CandleBuilder->CandleEvent path for
an explicit offline test, pass ``publish_candles=True`` (or set
``ALLOW_LEGACY_TICKER_CANDLES=1``).  Production LIVE/ANALYZE entry
points must NOT do this: they run reference-only and deliver zero
strategy candles until the verified transport is evidenced.

====================================================
"""

import asyncio
import json

import websockets

from config.settings import settings
from core.common.timestamp import normalize_timestamp
from core.events.event_bus import EventBus
from core.events.event_types import EventType
from core.events.candle_event import CandleEvent
from core.market_state import MarketState
from data.candle_builder import CandleBuilder
from data.realtime.reconnect_policy import ReconnectPolicy

WS_URL = "wss://public-socket.delta.exchange"

async def start(

    event_bus: EventBus,

    market: MarketState,

    max_reconnect_attempts: int = None,

    publish_candles: bool = False,

):

    import os

    if os.getenv("ALLOW_LEGACY_TICKER_CANDLES", "") in ("1", "true", "True"):
        publish_candles = True

    builder = CandleBuilder(timeframe=settings.TIMEFRAME)
    policy = ReconnectPolicy(base_delay=1.0, factor=2.0, max_delay=60.0, max_attempts=max_reconnect_attempts)
    attempt = 0

    # Phase 11C operational counters for this legacy
    # single-symbol feed: malformed frames are rejected and
    # counted, never fatal to the connection.
    malformed_frames = 0
    frames_received = 0

    while True:
        try:
            print("Connecting...")
            async with websockets.connect(WS_URL) as ws:

                print("Connected!")
                attempt = 0  # reset on successful connect

                subscribe = {

                    "type": "subscribe",

                    "payload": {

                        "channels": [

                            {

                                "name": "ticker",

                                "symbols": [

                                    settings.SYMBOLS[0]

                                ],

                            }

                        ]

                    },

                }

                await ws.send(json.dumps(subscribe))

                print(

                    f"Subscribed to {settings.SYMBOLS[0]}\n"

                )

                while True:

                    message = await ws.recv()

                    # Phase 11C: per-frame isolation. A malformed
                    # / unexpected frame is rejected and counted;
                    # only transport-level failures drive the
                    # reconnect policy below.
                    try:
                        data = json.loads(message)
                    except (ValueError, TypeError):
                        malformed_frames += 1
                        continue

                    if not isinstance(data, dict):
                        malformed_frames += 1
                        continue

                    if data.get("type") != "ticker":

                        continue

                    try:
                        ticks = data["d"]
                        ticker = (
                            ticks[0]
                            if isinstance(ticks, list)
                            else None
                        )
                        if not isinstance(ticker, dict):
                            raise KeyError("d[0]")
                        symbol = ticker["s"]
                        mark = float(ticker["m"])
                        bid = float(ticker["q"][2])
                        ask = float(ticker["q"][0])
                        if (
                            not isinstance(symbol, str)
                            or not symbol
                            or mark <= 0
                            or mark != mark
                            or bid <= 0
                            or ask <= 0
                        ):
                            raise ValueError("ticker fields")
                    except (KeyError, TypeError, ValueError, IndexError):
                        malformed_frames += 1
                        continue

                    frames_received += 1

                    # Canonical timestamp: normalize exchange ts at adapter boundary
                    try:
                        canonical_ts = normalize_timestamp(data["ts"])
                    except (ValueError, TypeError, KeyError):
                        # Invalid timestamp → skip tick (fail-soft)
                        malformed_frames += 1
                        continue

                    with market.lock:

                        market.symbol = symbol

                        market.mark_price = mark

                        market.best_bid = bid

                        market.best_ask = ask

                        market.spread = ask - bid

                        market.timestamp = canonical_ts

                        market.connected = True

                    # Phase 2 fail-closed: ticker marks update the
                    # mark-price reference above and NOTHING ELSE by
                    # default.  Strategy candles from marks are only
                    # emitted on the explicit legacy opt-in path.
                    if not publish_candles:

                        continue

                    finished = builder.update(

                        canonical_ts,

                        market.mark_price,

                    )

                    if finished:

                        event_bus.publish(

                            CandleEvent(

                                candle=finished,

                                symbol=market.symbol,

                                timeframe=getattr(market, "timeframe", settings.TIMEFRAME),

                            )

                        )
                        event_bus.dispatch()
        except (OSError, websockets.exceptions.WebSocketException, asyncio.TimeoutError) as exc:
            attempt += 1
            if policy.should_give_up(attempt):
                print(f"Reconnect exhausted after {attempt} attempts: {exc}")
                raise
            delay = policy.next_delay(attempt)
            print(f"WebSocket disconnected ({exc}), reconnecting in {delay:.1f}s (attempt {attempt})...")
            with market.lock:
                market.connected = False
            await asyncio.sleep(delay)
            continue
        except Exception:
            raise
        # Normal exit (should not happen for infinite feed, but break to avoid busy loop)
        break