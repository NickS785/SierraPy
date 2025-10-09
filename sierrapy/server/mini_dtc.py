#!/usr/bin/env python3
"""
Lightweight DTC server (JSON encoding) to stream realtime data to Sierra Chart.

• Implements: ENCODING_RESPONSE, LOGON_RESPONSE, HEARTBEAT, MARKET_DATA_* messages (subset)
• Encoding: JSON (each message is a JSON object terminated by a single null byte "\x00")
• Transport: TCP (asyncio)

Tested against Sierra Chart DTC Protocol Server/Client using JSON encoding. Adjust ports
in the __main__ section. To switch to binary later, replace JsonWire with BinaryWire and add packers.

References (DTC official docs):
- Message format/encoding and JSON null terminator: DTC Protocol docs
- Message sets & fields: Authentication/Logon, Market Data

TIP: In Sierra Chart, set: Global Settings → Sierra Chart Server Settings → DTC Protocol Server →
      connect as a DTC Client (or File → Connect to Data Feed → DTC). Choose JSON encoding,
      disable authentication for this sample, or send Username/Password in LOGON_REQUEST.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple, Any, Callable

# -------------------------------
# Wire codec (JSON with null-term)
# -------------------------------
class JsonWire:
    terminator = b"\x00"

    @staticmethod
    def encode(obj: dict) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8") + JsonWire.terminator

    @staticmethod
    async def read_obj(reader: asyncio.StreamReader) -> dict:
        # Read until null terminator; accumulate across partial TCP frames
        buf = bytearray()
        while True:
            chunk = await reader.read(1)
            if not chunk:
                # Connection closed
                if not buf:
                    raise asyncio.IncompleteReadError(partial=b"", expected=1)
                # Last message without terminator — try parse
                break
            if chunk == JsonWire.terminator:
                break
            buf.extend(chunk)
        if not buf:
            return {}
        return json.loads(buf.decode("utf-8"))

# -------------------------------
# DTC helpers / constants (names)
# -------------------------------

# We use string Type names for JSON encoding per DTC docs/examples.
# Minimal set required for realtime streaming.
TYPE_ENCODING_RESPONSE = "ENCODING_RESPONSE"
TYPE_LOGON_REQUEST = "LOGON_REQUEST"
TYPE_LOGON_RESPONSE = "LOGON_RESPONSE"
TYPE_HEARTBEAT = "HEARTBEAT"
TYPE_MARKET_DATA_REQUEST = "MARKET_DATA_REQUEST"
TYPE_MARKET_DATA_REJECT = "MARKET_DATA_REJECT"
TYPE_MARKET_DATA_SNAPSHOT = "MARKET_DATA_SNAPSHOT"
TYPE_MARKET_DATA_UPDATE_TRADE = "MARKET_DATA_UPDATE_TRADE"
TYPE_MARKET_DATA_UPDATE_BID_ASK = "MARKET_DATA_UPDATE_BID_ASK"
TYPE_MARKET_DATA_FEED_STATUS = "MARKET_DATA_FEED_STATUS"

# Simple enums/flags as strings/ints to keep JSON readable
LOGON_SUCCESS = "LOGON_SUCCESS"
MARKET_DATA_FEED_AVAILABLE = "MARKET_DATA_FEED_AVAILABLE"
SUBSCRIBE = "SUBSCRIBE"
UNSUBSCRIBE = "UNSUBSCRIBE"
SNAPSHOT = "SNAPSHOT"

CURRENT_DTC_VERSION = 7  # As of DTCProtocol.3.proto

@dataclass
class Subscription:
    symbol: str
    symbol_id: int
    exchange: str = ""

# -------------------------------
# Core connection handler
# -------------------------------
class DTCConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: "DTCServer"):
        self.r = reader
        self.w = writer
        self.srv = server
        self.remote = writer.get_extra_info("peername")
        self.heartbeat_interval = 15
        self.last_rx = time.time()
        self.subs: Dict[int, Subscription] = {}
        self._hb_task: Optional[asyncio.Task] = None
        self._rx_task: Optional[asyncio.Task] = None
        self.alive = True

    # --- send convenience
    def send(self, obj: dict) -> None:
        if not self.alive:
            return
        payload = JsonWire.encode(obj)
        self.w.write(payload)

    async def drain(self) -> None:
        try:
            await self.w.drain()
        except ConnectionResetError:
            self.alive = False

    # --- protocol steps
    def send_encoding_response(self):
        self.send({
            "Type": TYPE_ENCODING_RESPONSE,
            "ProtocolVersion": CURRENT_DTC_VERSION,
            "Encoding": "JSON",
            "ProtocolType": "DTC",
        })

    def send_logon_response(self, result: str = LOGON_SUCCESS, text: str = ""):
        self.send({
            "Type": TYPE_LOGON_RESPONSE,
            "ProtocolVersion": CURRENT_DTC_VERSION,
            "Result": result,
            "ResultText": text,
            "ServerName": self.srv.server_name,
            "MarketDepthUpdatesBestBidAndAsk": 0,
            "TradingIsSupported": 0,
            "SecurityDefinitionsSupported": 0,
            "HistoricalPriceDataSupported": 0,
            "ResubscribeWhenMarketDataFeedAvailable": 1,
            "SymbolExchangeDelimiter": "-",
        })

    def send_feed_status(self, status: str = MARKET_DATA_FEED_AVAILABLE):
        self.send({
            "Type": TYPE_MARKET_DATA_FEED_STATUS,
            "Status": status,
        })

    def send_snapshot(self, sub: Subscription, snapshot: dict):
        msg = {
            "Type": TYPE_MARKET_DATA_SNAPSHOT,
            "SymbolID": sub.symbol_id,
            # common fields; only include available ones
        }
        msg.update({k: v for k, v in snapshot.items() if v is not None})
        self.send(msg)

    def send_trade(self, sub: Subscription, price: float, volume: float, dt_micros: int | None = None):
        msg = {
            "Type": TYPE_MARKET_DATA_UPDATE_TRADE,
            "SymbolID": sub.symbol_id,
            "Price": price,
            "Volume": volume,
        }
        if dt_micros is not None:
            msg["DateTimeMicroseconds"] = dt_micros
        self.send(msg)

    def send_bid_ask(self, sub: Subscription, bid_price: float, bid_qty: float, ask_price: float, ask_qty: float, dt_micros: int | None = None):
        msg = {
            "Type": TYPE_MARKET_DATA_UPDATE_BID_ASK,
            "SymbolID": sub.symbol_id,
            "BidPrice": bid_price,
            "BidQuantity": bid_qty,
            "AskPrice": ask_price,
            "AskQuantity": ask_qty,
        }
        if dt_micros is not None:
            msg["DateTimeMicroseconds"] = dt_micros
        self.send(msg)

    async def _heartbeat_loop(self):
        try:
            while self.alive:
                await asyncio.sleep(self.heartbeat_interval)
                self.send({"Type": TYPE_HEARTBEAT})
                await self.drain()
        except asyncio.CancelledError:
            pass

    async def run(self):
        self.send_encoding_response()
        await self.drain()
        self._hb_task = asyncio.create_task(self._heartbeat_loop())
        try:
            while self.alive:
                msg = await JsonWire.read_obj(self.r)
                if not msg:
                    # Ignore keepalives/blank
                    continue
                self.last_rx = time.time()
                t = msg.get("Type")
                if t == TYPE_LOGON_REQUEST:
                    # Accept all logons; optional heartbeat override
                    self.heartbeat_interval = int(msg.get("HeartbeatIntervalInSeconds", self.heartbeat_interval))
                    self.send_logon_response()
                    self.send_feed_status(MARKET_DATA_FEED_AVAILABLE)
                    await self.drain()
                elif t == TYPE_HEARTBEAT:
                    # Client heartbeat; no response needed
                    pass
                elif t == TYPE_MARKET_DATA_REQUEST:
                    await self._handle_market_data_request(msg)
                else:
                    # Unknown types are ignored in JSON mode
                    pass
        except (asyncio.IncompleteReadError, ConnectionResetError, json.JSONDecodeError):
            self.alive = False
        finally:
            if self._hb_task:
                self._hb_task.cancel()
            self.w.close()
            try:
                await self.w.wait_closed()
            except Exception:
                pass
            # Remove subs from server
            for sub in list(self.subs.values()):
                self.srv.unsubscribe(self, sub.symbol_id)

    async def _handle_market_data_request(self, msg: dict):
        action = msg.get("RequestAction")
        symbol_id = int(msg.get("SymbolID", 0))
        symbol = msg.get("Symbol", "")
        exchange = msg.get("Exchange", "")

        if action == SUBSCRIBE:
            if not symbol or symbol_id == 0:
                self.send({
                    "Type": TYPE_MARKET_DATA_REJECT,
                    "SymbolID": symbol_id,
                    "RejectText": "Symbol and non-zero SymbolID required"
                })
                return
            sub = Subscription(symbol=symbol, symbol_id=symbol_id, exchange=exchange)
            # Track locally and on server
            self.subs[symbol_id] = sub
            self.srv.subscribe(self, sub)
            # Provide initial snapshot if server has one
            snap = await self.srv.snapshot_provider(symbol)
            if snap:
                self.send_snapshot(sub, snap)
            await self.drain()
        elif action == UNSUBSCRIBE:
            sub = self.subs.pop(symbol_id, None)
            if sub:
                self.srv.unsubscribe(self, symbol_id)
        elif action == SNAPSHOT:
            sub = Subscription(symbol=symbol, symbol_id=symbol_id, exchange=exchange)
            snap = await self.srv.snapshot_provider(symbol)
            if snap:
                self.send_snapshot(sub, snap)
                await self.drain()
        else:
            self.send({
                "Type": TYPE_MARKET_DATA_REJECT,
                "SymbolID": symbol_id,
                "RejectText": f"Unsupported RequestAction: {action}"
            })

# -------------------------------
# Server and data routing
# -------------------------------
class DTCServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 11099, server_name: str = "MiniDTC"):
        self.host = host
        self.port = port
        self.server_name = server_name
        self._server: Optional[asyncio.base_events.Server] = None
        # symbol_id -> set of DTCConnection
        self._routes: Dict[int, Set[DTCConnection]] = {}
        # symbol -> latest snapshot
        self._snapshots: Dict[str, dict] = {}
        # pluggable snapshot provider
        self.snapshot_provider: Callable[[str], Any] = self.default_snapshot_provider

    async def default_snapshot_provider(self, symbol: str) -> dict:
        return self._snapshots.get(symbol, {})

    def subscribe(self, conn: DTCConnection, sub: Subscription):
        self._routes.setdefault(sub.symbol_id, set()).add(conn)

    def unsubscribe(self, conn: DTCConnection, symbol_id: int):
        conns = self._routes.get(symbol_id)
        if conns and conn in conns:
            conns.remove(conn)
            if not conns:
                self._routes.pop(symbol_id, None)

    async def start(self):
        self._server = await asyncio.start_server(self._on_client, self.host, self.port)
        print(f"MiniDTC listening on {self.host}:{self.port}")

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        conn = DTCConnection(reader, writer, self)
        await conn.run()

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # -------------
    # Data injection
    # -------------
    def update_snapshot(self, symbol: str, **fields):
        # store partial snapshot fields
        snap = self._snapshots.setdefault(symbol, {})
        snap.update(fields)

    def publish_trade(self, symbol_id: int, price: float, volume: float, dt_micros: Optional[int] = None):
        conns = self._routes.get(symbol_id, ())
        for conn in list(conns):
            sub = conn.subs.get(symbol_id)
            if not sub:
                continue
            conn.send_trade(sub, price, volume, dt_micros)

    def publish_bid_ask(self, symbol_id: int, bid_price: float, bid_qty: float, ask_price: float, ask_qty: float, dt_micros: Optional[int] = None):
        conns = self._routes.get(symbol_id, ())
        for conn in list(conns):
            sub = conn.subs.get(symbol_id)
            if not sub:
                continue
            conn.send_bid_ask(sub, bid_price, bid_qty, ask_price, ask_qty, dt_micros)

# -------------------------------
# Demo: synthetic price streamer
# -------------------------------
async def demo_price_loop(server: DTCServer, symbol_id: int, symbol: str):
    import math
    t0 = time.time()
    price0 = 100.0
    vol = 1.0
    # Prepare an initial snapshot for the symbol
    server.update_snapshot(symbol, LastTradePrice=price0, SessionOpenPrice=price0, BidPrice=price0-0.5, AskPrice=price0+0.5,
                           BidQuantity=1.0, AskQuantity=1.0, SessionVolume=0.0)
    while True:
        await asyncio.sleep(0.25)
        t = time.time() - t0
        price = price0 + math.sin(t) * 0.5
        vol += 1
        dt_micros = int(time.time_ns() // 1000)
        server.publish_trade(symbol_id, round(price, 2), volume=1.0, dt_micros=dt_micros)
        server.publish_bid_ask(symbol_id, bid_price=round(price-0.25, 2), bid_qty=2.0,
                               ask_price=round(price+0.25, 2), ask_qty=2.0, dt_micros=dt_micros)

# -------------------------------
# Main
# -------------------------------
async def main():
    server = DTCServer(host="127.0.0.1", port=11099, server_name="MiniDTC")
    await server.start()

    # Start a demo publisher for a single symbol (e.g., ESZ5) with SymbolID 1001.
    asyncio.create_task(demo_price_loop(server, symbol_id=1001, symbol="ESZ5"))

    try:
        # Run forever
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())
