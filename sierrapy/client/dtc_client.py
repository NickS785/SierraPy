
# dtc_client.py
"""
Minimal Python wrapper for Sierra Chart's DTC protocol focusing on:
- Authentication & Monitoring (Encoding, Logon, Heartbeat, Logoff)
- Historical Price Data (request + streaming responses: bars or ticks)

Binary encoding only. Uses asyncio.
"""

from __future__ import annotations
import asyncio
import struct
import time
from dataclasses import dataclass
from typing import Optional, Callable, Awaitable, Dict, Any, List, Tuple, Union
from datetime import datetime, timezone

# -----------------------------------------------------------------------------
# DTC constants (subset we need)
# -----------------------------------------------------------------------------

CURRENT_VERSION = 8  # DTC CURRENT_VERSION

# Core message types
LOGON_REQUEST      = 1
LOGON_RESPONSE     = 2
HEARTBEAT          = 3
LOGOFF             = 5
ENCODING_REQUEST   = 6
ENCODING_RESPONSE  = 7

# Historical price data
HISTORICAL_PRICE_DATA_REQUEST               = 800
HISTORICAL_PRICE_DATA_RESPONSE_HEADER       = 801
HISTORICAL_PRICE_DATA_REJECT                = 802
HISTORICAL_PRICE_DATA_RECORD_RESPONSE       = 803   # aggregated bars (OHLCV...)
HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE  = 804   # tick-by-tick
HISTORICAL_PRICE_DATA_RESPONSE_TRAILER      = 807

# Enums (subset)
BINARY_ENCODING                         = 0
BINARY_WITH_VARIABLE_LENGTH_STRINGS     = 1
JSON_ENCODING                           = 2
JSON_COMPACT_ENCODING                   = 3
PROTOCOL_BUFFERS                        = 4

# HistoricalDataIntervalEnum (values are seconds; 0 means TICK)
INTERVAL_TICK        = 0
INTERVAL_1_SECOND    = 1
INTERVAL_1_MINUTE    = 60
INTERVAL_5_MINUTE    = 300
INTERVAL_15_MINUTE   = 900
INTERVAL_30_MINUTE   = 1800
INTERVAL_1_HOUR      = 3600
INTERVAL_1_DAY       = 86400

# AtBidOrAskEnum (tick responses)
BID_ASK_UNSET = 0
AT_BID = 1
AT_ASK = 2

# Fixed-length string sizes (binary encoding)
USERNAME_PASSWORD_LENGTH = 32
SYMBOL_EXCHANGE_DELIMITER_LENGTH = 4
SYMBOL_LENGTH = 64
EXCHANGE_LENGTH = 16
UNDERLYING_SYMBOL_LENGTH = 32
SYMBOL_DESCRIPTION_LENGTH = 64
EXCHANGE_DESCRIPTION_LENGTH = 48
ORDER_ID_LENGTH = 32
TRADE_ACCOUNT_LENGTH = 32
TEXT_DESCRIPTION_LENGTH = 96
TEXT_MESSAGE_LENGTH = 256
ORDER_FREE_FORM_TEXT_LENGTH = 48
CLIENT_SERVER_NAME_LENGTH = 48
GENERAL_IDENTIFIER_LENGTH = 64
CURRENCY_CODE_LENGTH = 8
ORDER_FILL_EXECUTION_LENGTH = 64
PRICE_STRING_LENGTH = 16

# LogonStatusEnum
LOGON_SUCCESS               = 1
LOGON_ERROR                 = 2
LOGON_ERROR_NO_RECONNECT    = 3
LOGON_RECONNECT_NEW_ADDRESS = 4

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _pack_cstr(text: Optional[str], length: int) -> bytes:
    if text is None:
        text = ''
    data = text.encode('utf-8', errors='ignore')
    if len(data) >= length:
        data = data[:length-1]  # leave room for NUL
    data += b'\x00'
    if len(data) < length:
        data += b'\x00' * (length - len(data))
    return data[:length]

def _unpack_cstr(buf: bytes) -> str:
    nul = buf.find(b'\x00')
    if nul != -1:
        buf = buf[:nul]
    return buf.decode('utf-8', errors='ignore')

def _unix_seconds(dt: Optional[Union[int, float, datetime]]) -> int:
    """Convert dt to UNIX seconds (int). If None, returns 0."""
    if dt is None:
        return 0
    if isinstance(dt, (int, float)):
        return int(dt)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    raise TypeError(f"Unsupported datetime type: {type(dt)!r}")

def _ensure_ms(dt: Optional[Union[int, float, datetime]]) -> int:
    """UNIX time in milliseconds (int)."""
    if dt is None:
        return 0
    if isinstance(dt, int):
        # assume already ms if large; otherwise assume sec
        return dt if dt > 10_000_000_000 else dt * 1000
    if isinstance(dt, float):
        return int(dt * 1000.0)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    raise TypeError(f"Unsupported datetime type: {type(dt)!r}")

# -----------------------------------------------------------------------------
# Message builders
# -----------------------------------------------------------------------------

def build_encoding_request(encoding: int = BINARY_ENCODING, protocol_version: int = CURRENT_VERSION) -> bytes:
    body = struct.pack('<ii', protocol_version, encoding) + _pack_cstr('DTC', 4)
    size = 4 + len(body)
    return struct.pack('<HH', size, ENCODING_REQUEST) + body

def build_logon_request(*,
                        username: str = '',
                        password: str = '',
                        general_text_data: str = '',
                        integer_1: int = 0,
                        integer_2: int = 0,
                        heartbeat_interval_sec: int = 10,
                        trade_account: str = '',
                        hardware_identifier: str = '',
                        client_name: str = 'PythonDTC',
                        market_data_tx_interval_ms: int = 0,
                        protocol_version: int = CURRENT_VERSION) -> bytes:
    parts = [
        struct.pack('<i', protocol_version),
        _pack_cstr(username, USERNAME_PASSWORD_LENGTH),
        _pack_cstr(password, USERNAME_PASSWORD_LENGTH),
        _pack_cstr(general_text_data, GENERAL_IDENTIFIER_LENGTH),
        struct.pack('<i', integer_1),
        struct.pack('<i', integer_2),
        struct.pack('<i', int(heartbeat_interval_sec)),
        struct.pack('<i', 0),  # Unused1
        _pack_cstr(trade_account, TRADE_ACCOUNT_LENGTH),
        _pack_cstr(hardware_identifier, GENERAL_IDENTIFIER_LENGTH),
        _pack_cstr(client_name, 32),
        struct.pack('<i', int(market_data_tx_interval_ms)),
    ]
    body = b''.join(parts)
    size = 4 + len(body)
    return struct.pack('<HH', size, LOGON_REQUEST) + body

def build_heartbeat(num_dropped: int = 0, current_datetime_utc: int | None = None) -> bytes:
    if current_datetime_utc is None:
        # The protocol allows zeros here
        num_dropped = 0
        current_datetime_utc = 0
    body = struct.pack('<IQ', int(num_dropped), int(current_datetime_utc))
    size = 4 + len(body)
    return struct.pack('<HH', size, HEARTBEAT) + body

def build_logoff(reason: str = '', do_not_reconnect: bool = False) -> bytes:
    body = _pack_cstr(reason, TEXT_DESCRIPTION_LENGTH) + struct.pack('<B', 1 if do_not_reconnect else 0)
    size = 4 + len(body)
    return struct.pack('<HH', size, LOGOFF) + body

# ---- Historical Price Data --------------------------------------------------

def build_historical_price_data_request(*,
                                        request_id: int,
                                        symbol: str,
                                        exchange: str = '',
                                        record_interval: int = INTERVAL_TICK,
                                        start_datetime: Optional[Union[int, float, datetime]] = None,
                                        end_datetime: Optional[Union[int, float, datetime]] = None,
                                        max_days_to_return: int = 0,
                                        use_zlib_compression: bool = False,
                                        request_dividend_adjusted_stock_data: bool = False,
                                        integer_1: int = 0) -> bytes:
    """Build s_HistoricalPriceDataRequest (message 800)."""
    body = b''.join([
        struct.pack('<i', int(request_id)),
        _pack_cstr(symbol, SYMBOL_LENGTH),
        _pack_cstr(exchange, EXCHANGE_LENGTH),
        struct.pack('<i', int(record_interval)),
        struct.pack('<q', _unix_seconds(start_datetime)),  # t_DateTime (int64 seconds)
        struct.pack('<q', _unix_seconds(end_datetime)),    # t_DateTime
        struct.pack('<I', int(max_days_to_return)),
        struct.pack('<B', 1 if use_zlib_compression else 0),
        struct.pack('<B', 1 if request_dividend_adjusted_stock_data else 0),
        struct.pack('<H', int(integer_1)),
    ])
    size = 4 + len(body)
    return struct.pack('<HH', size, HISTORICAL_PRICE_DATA_REQUEST) + body

# -----------------------------------------------------------------------------
# Parsers
# -----------------------------------------------------------------------------

def parse_header(header_bytes: bytes) -> dict:
    size, msg_type = struct.unpack('<HH', header_bytes)
    return {'size': size, 'type': msg_type}

def parse_encoding_response(payload: bytes) -> dict:
    proto_ver, encoding = struct.unpack('<ii', payload[:8])
    proto_type = _unpack_cstr(payload[8:12])
    return {'ProtocolVersion': proto_ver, 'Encoding': encoding, 'ProtocolType': proto_type}

def parse_logon_response(payload: bytes) -> dict:
    offset = 0
    (proto_ver, result) = struct.unpack('<ii', payload[offset:offset+8]); offset += 8
    result_text = _unpack_cstr(payload[offset:offset+TEXT_DESCRIPTION_LENGTH]); offset += TEXT_DESCRIPTION_LENGTH
    reconnect_addr = _unpack_cstr(payload[offset:offset+64]); offset += 64
    (integer_1,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    server_name = _unpack_cstr(payload[offset:offset+60]); offset += 60
    (md_best_bid_ask, trading_supported, oco_supported, cancel_replace_supported) = struct.unpack('<BBBB', payload[offset:offset+4]); offset += 4
    symbol_exchange_delim = _unpack_cstr(payload[offset:offset+SYMBOL_EXCHANGE_DELIMITER_LENGTH]); offset += SYMBOL_EXCHANGE_DELIMITER_LENGTH
    flags = struct.unpack('<BBBBBBBBB', payload[offset:offset+9]); offset += 9
    (security_defs_supported,
     hist_price_supported,
     resub_when_feed_available,
     md_depth_supported,
     one_hist_req_per_conn,
     bracket_orders_supported,
     unused_1,
     multiple_positions_per_symbol_acct,
     market_data_supported) = flags
    return {
        'ProtocolVersion': proto_ver,
        'Result': result,
        'ResultText': result_text,
        'ReconnectAddress': reconnect_addr,
        'Integer_1': integer_1,
        'ServerName': server_name,
        'MarketDepthUpdatesBestBidAndAsk': md_best_bid_ask,
        'TradingIsSupported': trading_supported,
        'OCOOrdersSupported': oco_supported,
        'OrderCancelReplaceSupported': cancel_replace_supported,
        'SymbolExchangeDelimiter': symbol_exchange_delim,
        'SecurityDefinitionsSupported': security_defs_supported,
        'HistoricalPriceDataSupported': hist_price_supported,
        'ResubscribeWhenMarketDataFeedAvailable': resub_when_feed_available,
        'MarketDepthIsSupported': md_depth_supported,
        'OneHistoricalPriceDataRequestPerConnection': one_hist_req_per_conn,
        'BracketOrdersSupported': bracket_orders_supported,
        'UsesMultiplePositionsPerSymbolAndTradeAccount': multiple_positions_per_symbol_acct,
        'MarketDataSupported': market_data_supported,
    }

def parse_heartbeat(payload: bytes) -> dict:
    num_dropped, current_dt = struct.unpack('<IQ', payload[:12])
    return {'NumDroppedMessages': int(num_dropped), 'CurrentDateTime': int(current_dt)}

# ---- Historical price data parsers ------------------------------------------

def parse_hist_resp_header(payload: bytes) -> dict:
    # s_HistoricalPriceDataResponseHeader
    offset = 0
    (request_id,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    (record_interval,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    (use_zlib,) = struct.unpack('<B', payload[offset:offset+1]); offset += 1
    (no_records,) = struct.unpack('<B', payload[offset:offset+1]); offset += 1
    (int_to_float_divisor,) = struct.unpack('<f', payload[offset:offset+4]); offset += 4
    return {
        'RequestID': request_id,
        'RecordInterval': record_interval,
        'UseZLibCompression': use_zlib,
        'NoRecordsToReturn': no_records,
        'IntToFloatPriceDivisor': int_to_float_divisor,
    }

def parse_hist_reject(payload: bytes) -> dict:

    offset = 0
    # Defensive slicing to avoid struct.error if payload is short
    def take(fmt, size):
        nonlocal offset
        if len(payload) < offset + size:
            # return zero/default if missing
            offset = len(payload)
            return (0,)
        out = struct.unpack(fmt, payload[offset:offset+size])
        offset += size
        return out

    (request_id,) = take('<i', 4)
    # RejectText is fixed-length; clamp to available bytes
    end = min(len(payload), offset + TEXT_DESCRIPTION_LENGTH)
    reject_text = _unpack_cstr(payload[offset:end])
    offset = end
    # Reason code: int16
    (reason_code,) = take('<h', 2)
    # Retry seconds: uint16
    (retry_time_sec,) = take('<H', 2)

    return {
        'RequestID': request_id,
        'RejectText': reject_text,
        'RejectReasonCode': int(reason_code),
        'RetryTimeInSeconds': int(retry_time_sec),
    }

def parse_hist_bar_record(payload: bytes) -> dict:
    # s_HistoricalPriceDataRecordResponse (bars)
    offset = 0
    (request_id,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    (start_dt_us,) = struct.unpack('<q', payload[offset:offset+8]); offset += 8  # microseconds
    (open_p, high_p, low_p, last_p, volume) = struct.unpack('<ddddd', payload[offset:offset+8*5]); offset += 8*5
    (open_interest_or_numtrades,) = struct.unpack('<I', payload[offset:offset+4]); offset += 4
    (bid_vol, ask_vol) = struct.unpack('<dd', payload[offset:offset+16]); offset += 16
    (is_final,) = struct.unpack('<B', payload[offset:offset+1]); offset += 1
    return {
        'RequestID': request_id,
        'StartDateTimeMicroseconds': start_dt_us,
        'Open': open_p, 'High': high_p, 'Low': low_p, 'Last': last_p,
        'Volume': volume,
        'OpenInterestOrNumTrades': open_interest_or_numtrades,
        'BidVolume': bid_vol, 'AskVolume': ask_vol,
        'IsFinalRecord': is_final,
    }

def parse_hist_tick_record(payload: bytes) -> dict:
    # s_HistoricalPriceDataTickRecordResponse
    offset = 0
    (request_id,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    (dt_ms,) = struct.unpack('<q', payload[offset:offset+8]); offset += 8
    (at_bid_or_ask,) = struct.unpack('<H', payload[offset:offset+2]); offset += 2
    (price, volume) = struct.unpack('<dd', payload[offset:offset+16]); offset += 16
    (is_final,) = struct.unpack('<B', payload[offset:offset+1]); offset += 1
    return {
        'RequestID': request_id,
        'DateTimeMilliseconds': dt_ms,
        'AtBidOrAsk': at_bid_or_ask,
        'Price': price, 'Volume': volume,
        'IsFinalRecord': is_final,
    }

def parse_hist_trailer(payload: bytes) -> dict:
    offset = 0
    (request_id,) = struct.unpack('<i', payload[offset:offset+4]); offset += 4
    (final_last_dt_us,) = struct.unpack('<q', payload[offset:offset+8]); offset += 8
    return {'RequestID': request_id, 'FinalRecordLastDateTimeMicroseconds': final_last_dt_us}

# -----------------------------------------------------------------------------
# High-level asyncio client
# -----------------------------------------------------------------------------

OnMessageCallback = Callable[[int, Dict[str, Any]], Awaitable[None]]

@dataclass
class DTCClientConfig:
    host: str
    port: int
    username: str = ''
    password: str = ''
    general_text_data: str = ''
    trade_account: str = ''
    hardware_identifier: str = ''
    client_name: str = 'PythonDTC'
    heartbeat_interval_sec: int = 10
    market_data_tx_interval_ms: int = 0
    send_encoding_request: bool = False  # set True to explicitly request binary/JSON
    encoding: int = BINARY_ENCODING       # used if send_encoding_request=True

class DTCClient:
    def __init__(self, cfg: DTCClientConfig):
        self.cfg = cfg
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._on_message: OnMessageCallback | None = None
        # pending historical requests keyed by RequestID -> asyncio.Queue
        self._pending_hist: Dict[int, asyncio.Queue] = {}

    # ---- Public API ---------------------------------------------------------

    async def connect_and_logon(self, on_message: Optional[OnMessageCallback] = None) -> None:
        """Connect TCP, (optional) ENCODING_REQUEST, then LOGON_REQUEST, start heartbeat."""
        self._on_message = on_message
        self._reader, self._writer = await asyncio.open_connection(self.cfg.host, self.cfg.port)
        self._read_task = asyncio.create_task(self._read_loop())
        if self.cfg.send_encoding_request:
            await self._send_bytes(build_encoding_request(self.cfg.encoding))
        await self._send_bytes(build_logon_request(
            username=self.cfg.username,
            password=self.cfg.password,
            general_text_data=self.cfg.general_text_data,
            heartbeat_interval_sec=self.cfg.heartbeat_interval_sec,
            trade_account=self.cfg.trade_account,
            hardware_identifier=self.cfg.hardware_identifier,
            client_name=self.cfg.client_name,
            market_data_tx_interval_ms=self.cfg.market_data_tx_interval_ms,
        ))
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def close(self, reason: str = '', do_not_reconnect: bool = False) -> None:
        if self._writer and not self._writer.is_closing():
            try:
                await self._send_bytes(build_logoff(reason, do_not_reconnect))
            except Exception:
                pass
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        if self._read_task:
            self._read_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ---- Historical Price Data ---------------------------------------------

    async def request_historical_prices(self, *,
                                        request_id: int,
                                        symbol: str,
                                        exchange: str = '',
                                        record_interval: int = INTERVAL_TICK,
                                        start_datetime: Optional[Union[int, float, datetime]] = None,
                                        end_datetime: Optional[Union[int, float, datetime]] = None,
                                        max_days_to_return: int = 0,
                                        use_zlib_compression: bool = False,
                                        request_dividend_adjusted_stock_data: bool = False) -> Dict[str, Any]:
        """Send HISTORICAL_PRICE_DATA_REQUEST and collect all messages until trailer.

        Returns a dict: {
            'Header': { ... },
            'Bars': [ ... ],             # if interval != TICK
            'Ticks': [ ... ],            # if interval == TICK
            'Reject': { ... } or None,
            'Trailer': { ... }
        }
        """
        if self._writer is None:
            raise RuntimeError("Not connected")
        # Create queue for this RequestID
        q: asyncio.Queue = asyncio.Queue()
        self._pending_hist[request_id] = q

        # Send request
        req = build_historical_price_data_request(
            request_id=request_id,
            symbol=symbol,
            exchange=exchange,
            record_interval=record_interval,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            max_days_to_return=max_days_to_return,
            use_zlib_compression=use_zlib_compression,
            request_dividend_adjusted_stock_data=request_dividend_adjusted_stock_data,
            integer_1=0,
        )
        await self._send_bytes(req)

        # Collect responses until trailer or reject
        header = None
        bars: List[dict] = []
        ticks: List[dict] = []
        reject = None
        trailer = None

        while True:
            msg_type, payload = await q.get()
            if msg_type == HISTORICAL_PRICE_DATA_RESPONSE_HEADER:
                header = parse_hist_resp_header(payload)
            elif msg_type == HISTORICAL_PRICE_DATA_RECORD_RESPONSE:
                rec = parse_hist_bar_record(payload)
                bars.append(rec)
                if rec.get('IsFinalRecord'):
                    # continue waiting for trailer
                    pass
            elif msg_type == HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE:
                rec = parse_hist_tick_record(payload)
                ticks.append(rec)
                if rec.get('IsFinalRecord'):
                    pass
            elif msg_type == HISTORICAL_PRICE_DATA_REJECT:
                reject = parse_hist_reject(payload)
                # End immediately on reject
                break
            elif msg_type == HISTORICAL_PRICE_DATA_RESPONSE_TRAILER:
                trailer = parse_hist_trailer(payload)
                break
            else:
                # ignore unknown for this request
                pass

        # Cleanup
        self._pending_hist.pop(request_id, None)
        return {'Header': header, 'Bars': bars, 'Ticks': ticks, 'Reject': reject, 'Trailer': trailer}

    # ---- Internals ----------------------------------------------------------

    async def _send_bytes(self, data: bytes) -> None:
        assert self._writer is not None
        self._writer.write(data)
        await self._writer.drain()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1, int(self.cfg.heartbeat_interval_sec)))
                await self._send_bytes(build_heartbeat())  # zeros are valid
        except asyncio.CancelledError:
            return

    async def _read_loop(self) -> None:
        assert self._reader is not None
        reader = self._reader
        try:
            while True:
                header = await reader.readexactly(4)
                size, msg_type = struct.unpack('<HH', header)
                remaining = size - 4
                payload = await reader.readexactly(remaining) if remaining > 0 else b''

                # If it's historical data and contains RequestID, try to route to queue
                routed = False
                if msg_type in (HISTORICAL_PRICE_DATA_RESPONSE_HEADER,
                                HISTORICAL_PRICE_DATA_RECORD_RESPONSE,
                                HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE,
                                HISTORICAL_PRICE_DATA_REJECT,
                                HISTORICAL_PRICE_DATA_RESPONSE_TRAILER):
                    # Peek RequestID (first 4 bytes of payload)
                    if len(payload) >= 4:
                        req_id = struct.unpack('<i', payload[:4])[0]
                        q = self._pending_hist.get(req_id)
                        if q is not None:
                            await q.put((msg_type, payload))
                            routed = True

                if not routed:
                    # deliver to generic on_message callback (parsed if known)
                    parsed = None
                    if msg_type == ENCODING_RESPONSE:
                        parsed = parse_encoding_response(payload)
                    elif msg_type == LOGON_RESPONSE:
                        parsed = parse_logon_response(payload)
                    elif msg_type == HEARTBEAT:
                        parsed = parse_heartbeat(payload)
                    elif msg_type == LOGOFF:
                        reason = _unpack_cstr(payload[:-1]) if len(payload) >= 1 else ''
                        do_not_reconnect = bool(payload[-1]) if len(payload) >= 1 else False
                        parsed = {'Reason': reason, 'DoNotReconnect': do_not_reconnect}
                    else:
                        parsed = {'raw': payload}

                    if self._on_message is not None:
                        await self._on_message(msg_type, parsed)

        except asyncio.CancelledError:
            pass
        except asyncio.IncompleteReadError:
            # Connection closed by peer
            pass

# -----------------------------------------------------------------------------
# Convenience demo runner
# -----------------------------------------------------------------------------

async def demo_hist(host: str, port: int, symbol: str, *, username: str = '', password: str = '',
                    start: Optional[datetime] = None, end: Optional[datetime] = None,
                    interval: int = INTERVAL_1_MINUTE) -> None:
    cfg = DTCClientConfig(host=host, port=port, username=username, password=password, client_name='PythonDTC-Demo')
    client = DTCClient(cfg)

    async def on_msg(msg_type: int, data: Dict[str, Any]):
        print(f"<- type={msg_type} data={data}")

    await client.connect_and_logon(on_message=on_msg)
    try:
        result = await client.request_historical_prices(
            request_id=1,
            symbol=symbol,
            record_interval=interval,
            start_datetime=start or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            end_datetime=end or datetime.now(timezone.utc),
            use_zlib_compression=False,
        )
        header = result['Header']
        bars = result['Bars']
        ticks = result['Ticks']
        reject = result['Reject']
        trailer = result['Trailer']

        print("Header:", header)
        print("Reject:", reject)
        print("Trailer:", trailer)
        print(f"Records: bars={len(bars)} ticks={len(ticks)}")
        if bars[:3]:
            print("Sample bars:", bars[:3])
        if ticks[:3]:
            print("Sample ticks:", ticks[:3])
    finally:
        await client.close("Done", do_not_reconnect=True)
