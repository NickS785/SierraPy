# fast_scid_reader.py
"""
Fast, zero-copy Python reader for Sierra Chart Intraday (.scid) files.

Key features
------------
- Memory-mapped I/O (mmap) for O(1) open on multi‑GB files
- Auto-detects common fixed-record variants (40B v10, 44B v8)
- Vectorized decoding with NumPy structured dtypes
- Binary search by time; fast slicing to [start,end]
- Optional chunked iteration for low‑RAM hosts
- Clean API: records(), to_numpy(), to_pandas(), iter_chunks(), export_csv()

Assumptions & notes
-------------------
- .scid files may have a header (typically 56 bytes). Auto-detects and skips header.
- 40‑byte variant uses 32‑bit **UNIX seconds** (fits in int32). 44‑byte variant uses
  **SCDateTime days** (Excel-like days since 1899‑12‑30) in float64.
- Little‑endian layout (Windows default). If you have big‑endian data, set `force_variant`
  and adjust dtypes accordingly.

Dependencies: numpy (required), pandas (optional for to_pandas).
"""
from __future__ import annotations

import io
import logging
import mmap
import os
import struct
from dataclasses import dataclass
from enum import Enum
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Mapping, Optional, Sequence, Tuple

import numpy as np

try:  # optional
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:  # optional
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pa = None  # type: ignore
    pq = None  # type: ignore

Variant = Literal["V10_40B", "V8_44B"]

_EPOCH_DAYS_OFFSET = 25569.0  # 1970-01-01 relative to 1899-12-30
_SECONDS_PER_DAY = 86400.0
_MAX_REASONABLE_UNIX = 4102444800  # 2100-01-01
_MIN_REASONABLE_UNIX = 315532800   # 1980-01-01

# Sierra Chart uses this sentinel value for invalid/empty OHLC data
_SCID_INVALID_FLOAT = -1.9990019654456028e+37  # SC's "empty" marker for float32

# NumPy structured dtypes (little-endian) - Compatible with Sierra Chart SCID format
DTYPE_V10_40B = np.dtype([
    ("DateTime", "<i8"),      # int64 microseconds since 1899-12-30 (modern format)
    ("Open", "<f4"),
    ("High", "<f4"),
    ("Low", "<f4"),
    ("Close", "<f4"),
    ("NumTrades", "<u4"),
    ("TotalVolume", "<u4"),
    ("BidVolume", "<u4"),
    ("AskVolume", "<u4"),
])  # 40 bytes

DTYPE_V8_44B = np.dtype([
    ("DateTime", "<f8"),      # float64 days since 1899-12-30 (legacy format)
    ("Open", "<f4"),
    ("High", "<f4"),
    ("Low", "<f4"),
    ("Close", "<f4"),
    ("NumTrades", "<u4"),
    ("TotalVolume", "<u4"),
    ("BidVolume", "<u4"),
    ("AskVolume", "<u4"),
])  # 40 bytes (not 44 as originally thought)


@dataclass(frozen=True)
class Schema:
    variant: Variant
    dtype: np.dtype
    record_size: int
    has_unix_seconds: bool  # True for V10_40B


SCHEMAS = {
    "V10_40B": Schema("V10_40B", DTYPE_V10_40B, 40, False),  # Uses int64 microseconds, not UNIX seconds
    "V8_44B": Schema("V8_44B", DTYPE_V8_44B, 40, False),    # Legacy format, also 40 bytes
}


def _sc_microseconds_to_epoch_ms(microseconds: np.ndarray | int) -> np.ndarray | int:
    """Convert Sierra Chart int64 microseconds (since 1899-12-30) to UNIX epoch milliseconds."""
    # Sierra Chart epoch: 1899-12-30 00:00:00 UTC
    # Unix epoch: 1970-01-01 00:00:00 UTC
    # Difference: 25569 days * 86400 seconds * 1000000 microseconds
    sc_epoch_offset_us = 25569 * 86400 * 1000000

    if isinstance(microseconds, np.ndarray):
        # Convert to epoch microseconds, then to milliseconds
        epoch_us = microseconds - sc_epoch_offset_us
        return (epoch_us // 1000).astype(np.int64)
    else:
        # Single value
        epoch_us = microseconds - sc_epoch_offset_us
        return epoch_us // 1000

def _sc_days_to_epoch_ms(days: np.ndarray | float) -> np.ndarray | int:
    """Convert SCDateTime days (since 1899-12-30) to UNIX epoch milliseconds."""
    if isinstance(days, np.ndarray):
        return ((days - _EPOCH_DAYS_OFFSET) * _SECONDS_PER_DAY * 1000.0).astype(np.int64)
    return int(round((days - _EPOCH_DAYS_OFFSET) * _SECONDS_PER_DAY * 1000.0))


def _validate_unix_seconds(x: int) -> bool:
    return _MIN_REASONABLE_UNIX <= x <= _MAX_REASONABLE_UNIX

def _validate_sc_microseconds(x: int) -> bool:
    # Validate int64 microseconds since 1899-12-30
    # Modern data: ~3e15 to 6e15 microseconds (roughly 1980-2100)
    return 10**14 <= x <= 10**17

def _validate_days(x: float) -> bool:
    # Accept roughly 1930..2100 to be generous
    # 1930-01-01 in SC days ~ 10957, 2100-01-01 ~ 73050
    return 9000.0 <= x <= 80000.0


def _clean_invalid_floats(arr: np.ndarray) -> np.ndarray:
    """Replace Sierra Chart's invalid float sentinel values with NaN.

    Sierra Chart uses approximately -1.999e37 to mark invalid/empty OHLC values.
    This function replaces those sentinel values with proper NaN.
    """
    # Use a threshold since float32 precision makes exact comparison unreliable
    # Any value less than -1e37 is considered invalid
    cleaned = arr.copy()
    invalid_mask = cleaned < -1e37
    if invalid_mask.any():
        cleaned[invalid_mask] = np.nan
    return cleaned


def _default_ohlcv_aggregation(columns: Sequence[str]) -> Dict[str, str]:
    agg: Dict[str, str] = {}
    if "Open" in columns:
        agg["Open"] = "first"
    if "High" in columns:
        agg["High"] = "max"
    if "Low" in columns:
        agg["Low"] = "min"
    if "Close" in columns:
        agg["Close"] = "last"

    sum_candidates = {"NumTrades", "TotalVolume", "BidVolume", "AskVolume"}
    for name in columns:
        if name in sum_candidates:
            agg[name] = "sum"

    for name in columns:
        if name not in agg:
            agg[name] = "last"

    return agg


def _resample_ohlcv(
    frame: "pd.DataFrame",
    rule: str,
    *,
    resample_kwargs: Optional[Mapping[str, Any]] = None,
) -> "pd.DataFrame":
    if frame.empty:
        return frame

    kwargs = dict(resample_kwargs or {})
    agg = kwargs.pop("agg", None)

    resampled = frame.resample(rule, **kwargs)
    if agg is None:
        agg = _default_ohlcv_aggregation(frame.columns)

    open_from_close = None
    if "Close" in frame.columns:
        open_from_close = resampled["Close"].first()

    result = resampled.agg(agg)
    result.index.name = frame.index.name
    if open_from_close is not None:
        result["Open"] = open_from_close
    return result.dropna(how="all")


def _bucket_by_volume(
    index: "pd.DatetimeIndex",
    data: Dict[str, np.ndarray],
    *,
    volume_per_bar: int,
    volume_column: str,
) -> Tuple["pd.DatetimeIndex", Dict[str, np.ndarray]]:
    if index.empty:
        return index, {name: arr[:0] for name, arr in data.items()}

    ordered_columns = list(data.keys())
    volume = np.asarray(data[volume_column], dtype=np.int64)
    cumsum = np.cumsum(volume, dtype=np.int64)
    bucket_ids = np.floor_divide(np.maximum(cumsum - 1, 0), volume_per_bar)

    change = np.empty_like(bucket_ids, dtype=bool)
    change[0] = True
    if change.size > 1:
        change[1:] = bucket_ids[1:] != bucket_ids[:-1]
    starts = np.flatnonzero(change)
    ends = np.empty_like(starts)
    ends[:-1] = starts[1:]
    ends[-1] = bucket_ids.size
    last_idx = ends - 1

    idx_values = index.asi8
    new_index = pd.DatetimeIndex(idx_values[last_idx], tz=index.tz)
    new_index.name = index.name

    result: Dict[str, np.ndarray] = {}
    handled: set[str] = set()

    def _assign(name: str, values: np.ndarray) -> None:
        result[name] = values
        handled.add(name)

    def _sum_reduce(arr: np.ndarray) -> np.ndarray:
        if arr.size == 0:
            return arr
        if arr.dtype.kind in "iu":
            return np.add.reduceat(arr.astype(np.int64, copy=False), starts)
        return np.add.reduceat(arr, starts)

    if "Open" in data:
        _assign("Open", np.asarray(data["Open"])[starts])
    if "High" in data:
        _assign("High", np.maximum.reduceat(np.asarray(data["High"]), starts))
    if "Low" in data:
        _assign("Low", np.minimum.reduceat(np.asarray(data["Low"]), starts))
    if "Close" in data:
        _assign("Close", np.asarray(data["Close"])[last_idx])

    sum_columns = {"NumTrades", "TotalVolume", "BidVolume", "AskVolume", volume_column}
    for name in sum_columns:
        if name in data and name not in handled:
            _assign(name, _sum_reduce(np.asarray(data[name])))

    for name in ordered_columns:
        if name not in handled:
            arr = np.asarray(data[name])
            _assign(name, arr[last_idx])

    return new_index, result


class FastScidReader:
    """
    High-throughput reader for Sierra Chart .scid files using memory-mapped I/O.

    Parameters
    ----------
    path : str
        Path to .scid file.
    force_variant : Optional[Variant]
        If provided, skip auto-detection and use this schema.
    read_only : bool
        Map the file as read-only (recommended). If False, uses ACCESS_COPY on
        platforms that support it.

    Example
    -------
    >>> rdr = FastScidReader("/path/Data/ESU25-CME.scid").open()
    >>> len(rdr)  # number of records
    12345678
    >>> # Slice by time
    >>> df = rdr.to_pandas(start_ms=1726000000000, end_ms=1726086400000)
    """

    def __init__(self, path: str, *, force_variant: Optional[Variant] = None, read_only: bool = True):
        self.path = os.fspath(path)
        self.force_variant: Optional[Variant] = force_variant
        self.read_only = read_only

        self._fh: Optional[io.BufferedReader] = None
        self._mm: Optional[mmap.mmap] = None
        self._schema: Optional[Schema] = None
        self._np_view: Optional[np.ndarray] = None  # structured array view
        self._timestamps_view: Optional[np.ndarray] = None
        self._header_size: int = 0  # Size of header to skip

    # ------------------------------ context manager ------------------------------
    def __enter__(self) -> "FastScidReader":
        return self.open()

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --------------------------------- core API ---------------------------------
    def open(self) -> "FastScidReader":
        """Memory-map the file and prepare a zero-copy NumPy view."""
        if self._mm is not None:
            return self

        size = os.path.getsize(self.path)
        if size == 0:
            raise ValueError("Empty file")

        self._fh = open(self.path, "rb", buffering=0)
        access = mmap.ACCESS_READ if self.read_only else mmap.ACCESS_COPY
        self._mm = mmap.mmap(self._fh.fileno(), length=0, access=access)

        # Detect header and schema
        self._header_size = self._detect_header_size()
        data_size = size - self._header_size

        schema = self._detect_schema(data_size) if self.force_variant is None else SCHEMAS[self.force_variant]
        self._schema = schema

        if data_size % schema.record_size != 0:
            raise ValueError(f"Data size {data_size} (after {self._header_size}-byte header) is not a multiple of record size {schema.record_size}")

        # Zero-copy structured array view over the data portion (skip header)
        self._np_view = np.frombuffer(self._mm, dtype=schema.dtype, offset=self._header_size)
        if "DateTime" not in (self._np_view.dtype.names or ()):  # pragma: no cover - defensive
            raise RuntimeError("Timestamp field 'DateTime' missing from schema dtype")
        self._timestamps_view = self._np_view["DateTime"]
        return self

    def close(self) -> None:
        # Clear references to numpy arrays that might hold buffer references
        if hasattr(self, '_np_view'):
            self._np_view = None
        if hasattr(self, '_timestamps_view'):
            self._timestamps_view = None

        # Close memory map with enhanced BufferError handling
        if self._mm is not None:
            max_retries = 5
            try:
                for attempt in range(max_retries):
                    try:
                        self._mm.close()
                        break  # Success
                    except BufferError as e:
                        if attempt < max_retries - 1:
                            # Force garbage collection and wait
                            import gc
                            gc.collect()
                            import time
                            time.sleep(0.01 * (attempt + 1))  # Progressive delay
                        else:
                            # Last attempt failed - log and force close file descriptor
                            logging.warning(f"FastScidReader: BufferError persists after {max_retries} attempts: {e}")
                            # Try to force close the underlying file descriptor
                            try:
                                if hasattr(self._mm, 'fileno'):
                                    import os
                                    os.close(self._mm.fileno())
                            except Exception:
                                pass  # Best effort
                    except Exception as e:
                        logging.warning(f"FastScidReader: Unexpected error during mmap close: {e}")
                        break
            finally:
                self._mm = None

        # Close file handle
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception as e:
                logging.warning(f"FastScidReader: Error closing file handle: {e}")
            finally:
                self._fh = None

        # Clear other references
        self._schema = None
        self._header_size = 0

    # --------------------------------- helpers ----------------------------------
    def _detect_header_size(self) -> int:
        """Detect if file has a SCID header and return its size."""
        if not self._fh:
            return 0

        # Check for SCID header signature
        self._fh.seek(0)
        first_4_bytes = self._fh.read(4)

        if first_4_bytes == b'SCID':
            # Read header size from offset 4 (little-endian uint32)
            header_size_bytes = self._fh.read(4)
            if len(header_size_bytes) == 4:
                header_size = struct.unpack('<I', header_size_bytes)[0]
                # Validate header size is reasonable (typically 56 bytes)
                if 40 <= header_size <= 4096 and header_size % 4 == 0:
                    return header_size
            # Fallback to typical header size
            return 56

        # No header detected
        return 0

    def _detect_schema(self, data_size: int) -> Schema:
        """Heuristically choose between int64 microseconds (modern) and float64 days (legacy)."""
        # Both formats use 40-byte records
        if data_size % 40 != 0:
            raise ValueError(f"Data size {data_size} is not a multiple of 40-byte records")

        # Try int64 microseconds (modern format)
        microseconds_plausible = False
        microseconds_first = None
        microseconds_last = None

        # Read first and last int64 timestamps (accounting for header offset)
        first_bytes = self._peek(self._header_size, 8)
        last_bytes = self._peek(self._header_size + data_size - 40, 8)
        if first_bytes and last_bytes:
            microseconds_first = struct.unpack("<q", first_bytes)[0]  # int64
            microseconds_last = struct.unpack("<q", last_bytes)[0]
            microseconds_plausible = (_validate_sc_microseconds(microseconds_first) and
                                    _validate_sc_microseconds(microseconds_last) and
                                    microseconds_last >= microseconds_first)

        # Try float64 days (legacy format)
        days_plausible = False
        days_first = None
        days_last = None

        if first_bytes and last_bytes:
            days_first = struct.unpack("<d", first_bytes)[0]  # float64
            days_last = struct.unpack("<d", last_bytes)[0]
            days_plausible = (_validate_days(days_first) and
                            _validate_days(days_last) and
                            days_last >= days_first)

        # Decide between formats
        if microseconds_plausible and not days_plausible:
            return SCHEMAS["V10_40B"]  # Modern int64 microseconds
        if days_plausible and not microseconds_plausible:
            return SCHEMAS["V8_44B"]   # Legacy float64 days
        if microseconds_plausible and days_plausible:
            # Both look plausible - prefer modern format
            return SCHEMAS["V10_40B"]

        # Neither format looks plausible - default to modern
        return SCHEMAS["V10_40B"]

    def _peek(self, offset: int, n: int) -> bytes:
        assert self._fh is not None
        self._fh.seek(offset)
        return self._fh.read(n)

    # -------------------------------- query API ---------------------------------
    @property
    def schema(self) -> Schema:
        if self._schema is None:
            raise RuntimeError("Reader not opened")
        return self._schema

    def __len__(self) -> int:
        if self._schema is None:
            return 0
        size = os.path.getsize(self.path)
        data_size = size - self._header_size
        return data_size // self._schema.record_size

    @property
    def count(self) -> int:
        """Number of records available in the mapped SCID file."""
        return len(self)

    @property
    def view(self) -> np.ndarray:
        """Structured array view over the file (zero-copy)."""
        if self._np_view is None:
            raise RuntimeError("Reader not opened")
        return self._np_view

    def columns(self) -> Tuple[str, ...]:
        return tuple(self.view.dtype.names or ())

    # ------------------------------ time handling -------------------------------
    def times_epoch_ms(self) -> np.ndarray:
        """Return vector of timestamps in UNIX epoch milliseconds (NumPy int64).
        This allocates once (derived from the on-disk column).
        """
        v = self.view
        if self.schema.variant == "V10_40B":
            # Modern format: int64 microseconds since 1899-12-30
            microseconds = v["DateTime"].astype(np.int64, copy=True)  # Force copy
            return _sc_microseconds_to_epoch_ms(microseconds)
        else:
            # Legacy format: float64 days since 1899-12-30
            days = v["DateTime"].astype(np.float64, copy=True)  # Force copy
            return _sc_days_to_epoch_ms(days)  # int64 array

    def _convert_raw_timestamp(self, raw: Any) -> int:
        if self._schema is None or self._timestamps_view is None:
            raise RuntimeError("Reader not opened")
        if self._schema.variant == "V10_40B":
            return int(_sc_microseconds_to_epoch_ms(int(raw)))
        return int(_sc_days_to_epoch_ms(float(raw)))

    def read_timestamp(self, i: int) -> int:
        """Return the timestamp at index ``i`` in epoch milliseconds (UTC).

        This operation performs O(1) I/O, allocates no additional memory, and
        raises :class:`IndexError` if the requested index is out of range.  A
        negative index is interpreted relative to the end of the file, matching
        Python sequence semantics.
        """

        if self._timestamps_view is None:
            raise RuntimeError("Reader not opened")

        n = self.count
        if n == 0:
            raise IndexError("empty scid")

        if i < 0:
            i += n

        if not 0 <= i < n:
            raise IndexError(i)

        raw_value = self._timestamps_view[i]
        return self._convert_raw_timestamp(raw_value)

    def peek_range(self) -> tuple[int, Optional[int], Optional[int]]:
        """Return ``(count, start_ms, end_ms)`` for the mapped SCID file.

        The timestamps are UNIX epoch milliseconds (UTC).  When the file has no
        records this returns ``(0, None, None)``.  The operation is O(1), zero
        allocation, and safe to call repeatedly.
        """

        n = self.count
        if n == 0:
            return 0, None, None

        if self._timestamps_view is None:
            raise RuntimeError("Reader not opened")

        start_raw = self._timestamps_view[0]
        end_raw = self._timestamps_view[n - 1]
        return n, self._convert_raw_timestamp(start_raw), self._convert_raw_timestamp(end_raw)

    def searchsorted(self, ts_ms: int, *, side: Literal["left", "right"] = "left") -> int:
        """Binary search index for a timestamp in epoch milliseconds.
        Assumes data are non-decreasing in time (typical for .scid).
        """
        t = self.times_epoch_ms()
        return int(np.searchsorted(t, ts_ms, side=side))

    def slice_by_time(self, *, start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> slice:
        t0 = 0 if start_ms is None else self.searchsorted(start_ms, side="left")
        t1 = len(self) if end_ms is None else self.searchsorted(end_ms, side="right")
        return slice(t0, t1)

    # ------------------------------ data extractors -----------------------------
    def to_numpy(
        self,
        *,
        columns: Optional[Sequence[str]] = None,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        copy: bool = True,  # Default to True for safety
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Return (data, times_ms) where `data` is a 2D float64 array (n x k) for requested columns.
        If `columns` is None, defaults to OHLCV (Open, High, Low, Close, TotalVolume).
        Set `copy=False` to attempt zero-copy (views) when possible for numeric types.
        WARNING: copy=False may cause BufferError on file close if references are held.
        """
        if columns is None:
            columns = ("Open", "High", "Low", "Close", "TotalVolume")
        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        v = self.view[sl]
        # Build a 2D array efficiently
        mats = []
        for c in columns:
            col = v[c]
            mats.append(col.astype(np.float64, copy=copy))
        data = np.stack(mats, axis=1) if mats else np.empty((len(v), 0), dtype=np.float64)
        times = self.times_epoch_ms()[sl]
        return data, times

    def to_pandas(
        self,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        tz: Optional[str] = None,
        resample_rule: Optional[str] = None,
        resample_kwargs: Optional[Mapping[str, Any]] = None,
        volume_per_bar: Optional[int] = None,
        volume_column: str = "TotalVolume",
        drop_invalid_rows: bool = False,
    ):
        if pd is None:
            raise RuntimeError("pandas is not installed; run `pip install pandas`")
        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        v = self.view[sl]
        t = self.times_epoch_ms()[sl]
        idx = pd.to_datetime(t, unit="ms", utc=True)
        if tz:
            idx = idx.tz_convert(tz)

        # Force copies to avoid holding references to memory-mapped buffer
        data_dict: Dict[str, np.ndarray] = {}
        ohlc_columns = {"Open", "High", "Low", "Close"}
        for name in (
            columns
            or [
                "Open",
                "High",
                "Low",
                "Close",
                "NumTrades",
                "TotalVolume",
                "BidVolume",
                "AskVolume",
            ]
        ):
            if name in v.dtype.names:
                arr = v[name].copy()  # Force copy
                # Clean invalid sentinel values in OHLC columns
                if name in ohlc_columns:
                    arr = _clean_invalid_floats(arr)
                data_dict[name] = arr

        if volume_per_bar is not None:
            if volume_per_bar <= 0:
                raise ValueError("volume_per_bar must be a positive integer")
            if volume_column not in data_dict:
                raise ValueError(
                    f"Column '{volume_column}' is required for volume bucketing"
                )
            idx, data_dict = _bucket_by_volume(
                idx,
                data_dict,
                volume_per_bar=volume_per_bar,
                volume_column=volume_column,
            )

        frame = pd.DataFrame(data_dict, index=idx)
        frame.index.name = "DateTime"

        # Optionally drop rows with invalid OHLC data
        if drop_invalid_rows:
            price_cols = [c for c in ["Open", "High", "Low", "Close"] if c in frame.columns]
            if price_cols:
                # Drop rows where any price column is NaN or <= 0
                valid_mask = frame[price_cols].notna().all(axis=1) & (frame[price_cols] > 0).all(axis=1)
                frame = frame[valid_mask]

        if resample_rule:
            frame = _resample_ohlcv(
                frame,
                resample_rule,
                resample_kwargs=resample_kwargs,
            )

        return frame

    # ------------------------------ iteration utils -----------------------------
    def iter_chunks(self, *, chunk_records: int = 1_000_000, copy: bool = False) -> Iterator[np.ndarray]:
        """Yield structured-array chunks (zero-copy slices) of at most `chunk_records` length.

        Args:
            chunk_records: Maximum records per chunk
            copy: If True, yield copies instead of views (safer for concurrent usage)
        """
        n = len(self)
        v = self.view
        for start in range(0, n, chunk_records):
            end = min(start + chunk_records, n)
            chunk = v[start:end]
            yield chunk.copy() if copy else chunk

    # ---------------------------- SCID export helpers ---------------------------
    def _make_scid_header(self, record_count: int) -> bytes:
        """Return a SCID header for ``record_count`` records.

        When the source file contains a header, this clones the bytes and updates
        the record count where possible. Otherwise a minimal 56-byte header is
        synthesised. This keeps the output compatible with Sierra Chart readers
        that expect the ``SCID`` signature.
        """

        header_size = self._header_size or 56
        if self._mm is not None and self._header_size:
            header = bytearray(self._mm[:self._header_size])
        else:
            header = bytearray(header_size)

        if len(header) >= 4:
            struct.pack_into("<4s", header, 0, b"SCID")
        if len(header) >= 8:
            struct.pack_into("<I", header, 4, len(header))
        if len(header) >= 16:
            struct.pack_into("<I", header, 12, self.schema.record_size)
        if len(header) >= 20:
            struct.pack_into("<I", header, 16, int(record_count))
        if len(header) >= 24:
            # Duplicate record count where legacy headers expect both start/end
            struct.pack_into("<I", header, 20, int(record_count))

        return bytes(header)

    def _pack_tick_record(self, record: np.void) -> bytes:
        """Pack a structured NumPy tick ``record`` into SCID binary layout."""

        if self.schema.variant == "V10_40B":
            fmt = "<qffffIIII"
        else:
            fmt = "<dffffIIII"

        dt_value = record["DateTime"]
        open_ = float(record["Open"])
        high = float(record["High"])
        low = float(record["Low"])
        close = float(record["Close"])
        num_trades = int(record["NumTrades"])
        total_volume = int(record["TotalVolume"])
        bid_volume = int(record["BidVolume"])
        ask_volume = int(record["AskVolume"])

        return struct.pack(
            fmt,
            dt_value,
            open_,
            high,
            low,
            close,
            num_trades,
            total_volume,
            bid_volume,
            ask_volume,
        )

    def _write_filtered_ticks(
        self,
        output_path: str,
        ticks: Sequence[np.void] | np.ndarray,
        *,
        rebase_level: float | None = None,
        preserve_tick_ratio: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Internal writer shared by specialised SCID exporters."""

        import os

        if os.path.exists(output_path) and not overwrite:
            raise FileExistsError(f"{output_path} already exists")

        if isinstance(ticks, np.ndarray):
            if ticks.size == 0:
                raise ValueError("No ticks selected for export")
            data = ticks.copy()
        else:
            tick_list = list(ticks)
            if not tick_list:
                raise ValueError("No ticks selected for export")
            data = np.array(tick_list, dtype=self.view.dtype)

        if rebase_level is not None:
            price_field = "Close" if "Close" in data.dtype.names else None
            if price_field is None:
                raise ValueError("Cannot rebase ticks without a Close column")

            base_price = float(data[price_field][0])
            if base_price == 0:
                raise ValueError("Cannot rebase when base price is zero")

            scale = float(rebase_level) / base_price
            for field in ["Open", "High", "Low", "Close"]:
                if field in data.dtype.names:
                    data[field] = data[field] * scale

            if preserve_tick_ratio and hasattr(self, "header") and hasattr(self.header, "tick_size"):
                self.header.tick_size *= scale  # type: ignore[attr-defined]

        with open(output_path, "wb") as fh:
            fh.write(self._make_scid_header(int(data.size)))
            for record in data:
                fh.write(self._pack_tick_record(record))
            fh.flush()

    # ------------------------------- export utils -------------------------------
    def export_csv(
        self,
        out_path: str,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_columns: Sequence[str] = ("Open", "High", "Low", "Close", "NumTrades", "TotalVolume", "BidVolume", "AskVolume"),
        include_time: bool = True,
        chunk_records: int = 5_000_000,
    ) -> None:
        """High-speed CSV export using chunked writes. Suitable for multi-GB files.
        Writes header. Time column is ISO-8601 in UTC.
        """
        if pd is None:
            # pandas-free path: format with NumPy and Python I/O
            self._export_csv_numpy(out_path, start_ms=start_ms, end_ms=end_ms,
                                   include_columns=include_columns, include_time=include_time,
                                   chunk_records=chunk_records)
            return
        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        cols = list(include_columns)
        with open(out_path, "w", newline="") as f:
            # header
            header = (["DateTime"] if include_time else []) + cols
            f.write(",".join(header) + "\n")
            # chunks
            start = sl.start or 0
            stop = sl.stop or len(self)
            for chunk_start in range(start, stop, chunk_records):
                chunk_end = min(chunk_start + chunk_records, stop)
                c = self.view[chunk_start:chunk_end]
                if c.size == 0:
                    continue
                t_ms = (self.times_epoch_ms())[chunk_start:chunk_end]
                df = pd.DataFrame({name: c[name] for name in cols})
                if include_time:
                    dt = pd.to_datetime(t_ms, unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S.%fZ")
                    df.insert(0, "DateTime", dt.values)
                df.to_csv(f, header=False, index=False, lineterminator="\n")

    def _export_csv_numpy(
        self,
        out_path: str,
        *,
        start_ms: Optional[int],
        end_ms: Optional[int],
        include_columns: Sequence[str],
        include_time: bool,
        chunk_records: int,
    ) -> None:
        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        cols = list(include_columns)
        with open(out_path, "w", newline="") as f:
            header = (["DateTime"] if include_time else []) + cols
            f.write(",".join(header) + "\n")
            n = len(self)
            start = sl.start or 0
            stop = sl.stop or n
            for chunk_start in range(start, stop, chunk_records):
                chunk_end = min(chunk_start + chunk_records, stop)
                block = self.view[chunk_start:chunk_end]
                if block.size == 0:
                    continue
                arrs = [block[name] for name in cols]
                mat = np.column_stack(arrs)
                if include_time:
                    t_ms = self.times_epoch_ms()[chunk_start:chunk_end]
                    # Use numpy datetime64 for speed then format as ISO Z
                    dt64 = t_ms.astype("datetime64[ms]")
                    # numpy prints ISO without 'Z'; append 'Z'
                    date_str = np.array([str(x).replace(" ", "T") + "Z" for x in dt64], dtype=object)
                    out = np.column_stack((date_str, mat))
                else:
                    out = mat
                for row in out:
                    f.write(",".join(map(str, row)))
                    f.write("\n")

    def export_to_parquet(
        self,
        out_path: str,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_columns: Sequence[str] = ("Open", "High", "Low", "Close", "NumTrades", "TotalVolume", "BidVolume", "AskVolume"),
        chunk_records: int = 1_000_000,
        compression: str = "snappy",
        include_time: bool = True,
    ) -> None:
        """
        Export data to Parquet format using chunked processing for memory efficiency.

        Suitable for multi-GB files without loading entire dataset into memory.
        Uses PyArrow for efficient columnar storage and compression.

        Args:
            out_path: Output Parquet file path
            start_ms: Start timestamp in epoch milliseconds (None = from beginning)
            end_ms: End timestamp in epoch milliseconds (None = to end)
            include_columns: Columns to include in export
            chunk_records: Records per chunk (controls memory usage)
            compression: Parquet compression ('snappy', 'gzip', 'lz4', 'brotli', 'zstd')
            include_time: Include DateTime column

        Raises:
            RuntimeError: If PyArrow is not installed
        """
        if pa is None or pq is None:
            raise RuntimeError("PyArrow is required for Parquet export. Install with: pip install pyarrow")

        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        cols = list(include_columns)

        # Build Arrow schema
        schema_fields = []
        if include_time:
            schema_fields.append(pa.field("DateTime", pa.timestamp("ms", tz="UTC")))

        for col in cols:
            if col in ["Open", "High", "Low", "Close"]:
                schema_fields.append(pa.field(col, pa.float32()))
            elif col in ["NumTrades", "TotalVolume", "BidVolume", "AskVolume"]:
                schema_fields.append(pa.field(col, pa.uint32()))
            else:
                # Default to float32 for unknown columns
                schema_fields.append(pa.field(col, pa.float32()))

        schema = pa.schema(schema_fields)

        # Initialize Parquet writer
        start = sl.start or 0
        stop = sl.stop or len(self)

        with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
            # Process in chunks to control memory usage
            for chunk_start in range(start, stop, chunk_records):
                chunk_end = min(chunk_start + chunk_records, stop)
                chunk_view = self.view[chunk_start:chunk_end]

                if chunk_view.size == 0:
                    continue

                # Build Arrow arrays for this chunk
                arrays = []

                if include_time:
                    # Get timestamps for this chunk
                    chunk_times = self.times_epoch_ms()[chunk_start:chunk_end]
                    # Convert to Arrow timestamp array
                    time_array = pa.array(chunk_times, type=pa.timestamp("ms", tz="UTC"))
                    arrays.append(time_array)

                # Add data columns
                for col in cols:
                    if col in chunk_view.dtype.names:
                        col_data = chunk_view[col]

                        # Convert to appropriate Arrow type
                        if col in ["Open", "High", "Low", "Close"]:
                            arrow_array = pa.array(col_data.astype(np.float32))
                        elif col in ["NumTrades", "TotalVolume", "BidVolume", "AskVolume"]:
                            arrow_array = pa.array(col_data.astype(np.uint32))
                        else:
                            arrow_array = pa.array(col_data.astype(np.float32))

                        arrays.append(arrow_array)
                    else:
                        # Column not found, create null array
                        null_array = pa.nulls(len(chunk_view), type=schema.field(col).type)
                        arrays.append(null_array)

                # Create record batch and write to file
                batch = pa.record_batch(arrays, schema=schema)
                writer.write_batch(batch)

    def export_to_parquet_optimized(
        self,
        out_path: str,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_columns: Sequence[str] = ("Open", "High", "Low", "Close", "NumTrades", "TotalVolume", "BidVolume", "AskVolume"),
        chunk_records: int = 2_000_000,
        compression: str = "zstd",
        include_time: bool = True,
        use_dictionary: bool = False,
    ) -> Dict[str, Any]:
        """
        Optimized Parquet export with advanced features and statistics.

        Returns metadata about the export process including compression ratios,
        processing time, and file statistics.

        Args:
            out_path: Output Parquet file path
            start_ms: Start timestamp in epoch milliseconds
            end_ms: End timestamp in epoch milliseconds
            include_columns: Columns to include
            chunk_records: Records per chunk (larger = better compression, more memory)
            compression: Compression algorithm ('zstd', 'snappy', 'gzip', 'lz4', 'brotli')
            include_time: Include DateTime column
            use_dictionary: Use dictionary encoding for string-like data

        Returns:
            Dictionary with export statistics
        """
        if pa is None or pq is None:
            raise RuntimeError("PyArrow is required for Parquet export. Install with: pip install pyarrow")

        import time
        start_time = time.perf_counter()

        sl = self.slice_by_time(start_ms=start_ms, end_ms=end_ms)
        cols = list(include_columns)

        # Build optimized Arrow schema with metadata
        schema_fields = []
        if include_time:
            schema_fields.append(pa.field("DateTime", pa.timestamp("us", tz="UTC")))  # Microsecond precision

        for col in cols:
            if col in ["Open", "High", "Low", "Close"]:
                field_type = pa.float32()
            elif col in ["NumTrades", "TotalVolume", "BidVolume", "AskVolume"]:
                field_type = pa.uint32()
            else:
                field_type = pa.float32()

            # Add dictionary encoding if requested
            if use_dictionary and col in ["NumTrades"]:  # Example: encode trade counts
                field_type = pa.dictionary(pa.int16(), field_type)

            schema_fields.append(pa.field(col, field_type))

        # Add metadata to schema
        metadata = {
            "source": "FastScidReader",
            "variant": self.schema.variant,
            "export_time": str(datetime.now()),
            "compression": compression
        }
        schema = pa.schema(schema_fields, metadata=metadata)

        # Export statistics
        stats = {
            "total_records": 0,
            "chunks_processed": 0,
            "bytes_processed": 0,
            "compression_ratio": 0.0,
            "export_time": 0.0
        }

        start_idx = sl.start or 0
        stop_idx = sl.stop or len(self)
        total_records = stop_idx - start_idx

        # Parquet writer with optimized settings
        writer_kwargs = {
            "compression": compression,
            "use_dictionary": use_dictionary,
            "row_group_size": chunk_records,  # Optimize row group size
            "data_page_size": 1024 * 1024,   # 1MB pages
        }

        with pq.ParquetWriter(out_path, schema, **writer_kwargs) as writer:
            for chunk_start in range(start_idx, stop_idx, chunk_records):
                chunk_end = min(chunk_start + chunk_records, stop_idx)
                chunk_view = self.view[chunk_start:chunk_end]

                if chunk_view.size == 0:
                    continue

                arrays = []

                if include_time:
                    # High-precision timestamps
                    chunk_times = self.times_epoch_ms()[chunk_start:chunk_end]
                    # Convert to microseconds for higher precision
                    time_us = chunk_times * 1000
                    time_array = pa.array(time_us, type=pa.timestamp("us", tz="UTC"))
                    arrays.append(time_array)

                # Process data columns
                for col in cols:
                    if col in chunk_view.dtype.names:
                        col_data = chunk_view[col]

                        if col in ["Open", "High", "Low", "Close"]:
                            arrow_array = pa.array(col_data.astype(np.float32))
                        elif col in ["NumTrades", "TotalVolume", "BidVolume", "AskVolume"]:
                            arrow_array = pa.array(col_data.astype(np.uint32))
                        else:
                            arrow_array = pa.array(col_data.astype(np.float32))

                        arrays.append(arrow_array)

                # Write batch
                batch = pa.record_batch(arrays, schema=schema)
                writer.write_batch(batch)

                # Update statistics
                stats["chunks_processed"] += 1
                stats["total_records"] += len(chunk_view)
                stats["bytes_processed"] += chunk_view.nbytes

        # Calculate final statistics
        export_time = time.perf_counter() - start_time
        stats["export_time"] = export_time

        # Get file size for compression ratio
        output_size = os.path.getsize(out_path)
        if stats["bytes_processed"] > 0:
            stats["compression_ratio"] = stats["bytes_processed"] / output_size

        stats["output_size_mb"] = output_size / (1024 * 1024)
        stats["records_per_second"] = total_records / export_time if export_time > 0 else 0

        return stats


    # --------------------------- filtered SCID writers --------------------------
    def write_scid_filtered_rebased(
        self,
        output_path: str,
        dates: Sequence[date],
        *,
        rebase_level: float | None = None,
        preserve_tick_ratio: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Export ticks whose trading date is in ``dates`` with optional rebasing.

        Parameters
        ----------
        output_path:
            Destination ``.scid`` path.
        dates:
            Iterable of :class:`datetime.date` values to retain.
        rebase_level:
            Optional price level to rebase the output series to.
        preserve_tick_ratio:
            When rebasing, also scale ``tick_size`` metadata if available.
        overwrite:
            Allow overwriting an existing file.
        """

        if not dates:
            raise ValueError("dates must contain at least one entry")

        day_targets = {np.datetime64(d) for d in dates}
        day_array = self.times_epoch_ms().astype("datetime64[ms]").astype("datetime64[D]")
        mask = np.isin(day_array, np.array(sorted(day_targets), dtype="datetime64[D]"))
        selected = self.view[mask]

        self._write_filtered_ticks(
            output_path,
            selected,
            rebase_level=rebase_level,
            preserve_tick_ratio=preserve_tick_ratio,
            overwrite=overwrite,
        )

    def write_scid_by_date_range(
        self,
        output_path: str,
        start_date: date,
        end_date: date,
        *,
        rebase_level: float | None = None,
        preserve_tick_ratio: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Export ticks that fall within ``start_date`` and ``end_date`` inclusive."""

        if start_date > end_date:
            raise ValueError("start_date must be on or before end_date")

        days = self.times_epoch_ms().astype("datetime64[ms]").astype("datetime64[D]")
        start = np.datetime64(start_date)
        end = np.datetime64(end_date)
        mask = (days >= start) & (days <= end)
        selected = self.view[mask]

        self._write_filtered_ticks(
            output_path,
            selected,
            rebase_level=rebase_level,
            preserve_tick_ratio=preserve_tick_ratio,
            overwrite=overwrite,
        )

    def write_scid_by_calendar_filter(
        self,
        output_path: str,
        *,
        days_of_week: Optional[set[int]] = None,
        months: Optional[set[int]] = None,
        weeks_of_year: Optional[set[int]] = None,
        rebase_level: float | None = None,
        preserve_tick_ratio: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Export ticks filtered by calendar fields.

        Parameters
        ----------
        output_path:
            Destination ``.scid`` path.
        days_of_week:
            Integers using Python's convention ``0=Monday`` ... ``6=Sunday``.
        months:
            Integers ``1`` through ``12`` representing calendar months.
        weeks_of_year:
            ISO week numbers ``1`` through ``53``.
        rebase_level:
            Optional price level to rebase the output series to.
        preserve_tick_ratio:
            When rebasing, also scale ``tick_size`` metadata if available.
        overwrite:
            Allow overwriting an existing file.
        """

        if not any([days_of_week, months, weeks_of_year]):
            raise ValueError("At least one calendar filter must be provided")

        times_ms = self.times_epoch_ms()
        dt_ms = times_ms.astype("datetime64[ms]")
        dt_days = dt_ms.astype("datetime64[D]")

        mask = np.ones(times_ms.shape, dtype=bool)

        if days_of_week:
            invalid = [d for d in days_of_week if d < 0 or d > 6]
            if invalid:
                raise ValueError(f"Invalid weekday values: {invalid}")
            dow = (dt_days.astype("int64") + 3) % 7
            mask &= np.isin(dow, list(days_of_week))

        if months:
            invalid = [m for m in months if m < 1 or m > 12]
            if invalid:
                raise ValueError(f"Invalid month values: {invalid}")
            month_numbers = (dt_ms.astype("datetime64[M]").astype(int) % 12) + 1
            mask &= np.isin(month_numbers, list(months))

        if weeks_of_year:
            invalid = [w for w in weeks_of_year if w < 1 or w > 53]
            if invalid:
                raise ValueError(f"Invalid ISO week values: {invalid}")
            weeks = np.fromiter(
                (datetime.utcfromtimestamp(int(ms) / 1000).isocalendar()[1] for ms in times_ms),
                dtype=np.int16,
                count=times_ms.size,
            )
            mask &= np.isin(weeks, list(weeks_of_year))

        selected = self.view[mask]

        self._write_filtered_ticks(
            output_path,
            selected,
            rebase_level=rebase_level,
            preserve_tick_ratio=preserve_tick_ratio,
            overwrite=overwrite,
        )


# ---------------------------- asynchronous helper -----------------------------


class RollConvention(str, Enum):
    """Available conventions for defining roll windows."""

    NEXT_ROLL = "next_roll"
    OWN_ROLL = "own_roll"


@dataclass(frozen=True)
class ScidContractInfo:
    """Contract information parsed from SCID filename."""

    ticker: str
    month: str
    year: int
    exchange: str
    file_path: Path

    @property
    def contract_id(self) -> str:
        return f"{self.month}{str(self.year)[-2:]}"


@dataclass(frozen=True)
class RollPeriod:
    """Represents the active window for a specific futures contract."""

    contract: ScidContractInfo
    start: "pd.Timestamp"
    end: "pd.Timestamp"
    roll_date: "pd.Timestamp"
    expiry: "pd.Timestamp"

    @property
    def contract_id(self) -> str:
        return self.contract.contract_id


class ScidTickerFileManager:
    """
    Manages SCID files for each ticker with format {ticker}{month_code}{2digit-year}-{exchange}.scid
    Identifies the "front" month (closest large file to next calendar month) for current data.
    """

    def __init__(self, folder: str):
        self.folder = Path(folder)
        self._ticker_files: Dict[str, List[Path]] = {}
        self._ticker_contracts: Dict[str, List[ScidContractInfo]] = {}
        self._discover_files()

    def _discover_files(self) -> None:
        """Discover and categorize all .scid files in the folder."""
        if not self.folder.is_dir():
            return

        for file_path in self.folder.glob("*.scid"):
            info = self._parse_scid_filename(file_path)
            if info is None:
                continue

            ticker = info.ticker
            self._ticker_files.setdefault(ticker, []).append(file_path)
            self._ticker_contracts.setdefault(ticker, []).append(info)

        # Sort files by contract expiry for each ticker
        for ticker in self._ticker_files:
            contracts_with_paths = list(zip(self._ticker_contracts[ticker], self._ticker_files[ticker]))
            contracts_with_paths.sort(key=lambda x: self._calculate_contract_expiry(x[0]))

            self._ticker_contracts[ticker] = [c[0] for c in contracts_with_paths]
            self._ticker_files[ticker] = [c[1] for c in contracts_with_paths]

    def _parse_scid_filename(self, file_path: Path) -> Optional[ScidContractInfo]:
        """Parse SCID filenames like CLH25-NYM.scid."""
        import re

        MONTH_CODE_MAP = {
            "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
            "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12
        }

        m = re.match(r"([A-Za-z]+)([FGHJKMNQUVXZ])(\d{2})-([A-Za-z]+)\.scid", file_path.name, re.IGNORECASE)
        if not m:
            return None

        ticker, mcode, yy, exch = m.groups()
        yy = int(yy)
        year = 2000 + yy if yy <= 70 else 1900 + yy  # Pivot year similar to dly_parse

        return ScidContractInfo(
            ticker=ticker.upper(),
            month=mcode.upper(),
            year=year,
            exchange=exch.upper(),
            file_path=file_path
        )

    def _calculate_contract_expiry(self, contract: ScidContractInfo) -> pd.Timestamp:
        """Calculate contract expiry date."""
        month_code_map = {
            "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
            "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12
        }

        month_num = month_code_map.get(contract.month.upper())
        if month_num is None:
            raise ValueError(f"Invalid month code: {contract.month}")

        # Energy contracts typically expire the month before delivery
        if contract.ticker.upper() in ['CL', 'HO', 'RB', 'NG']:
            if month_num == 1:  # January delivery -> December expiry of previous year
                expiry_year = contract.year - 1
                expiry_month = 12
            else:
                expiry_year = contract.year
                expiry_month = month_num - 1
        else:
            # Most other contracts expire in the delivery month
            expiry_year = contract.year
            expiry_month = month_num

        return pd.Timestamp(year=expiry_year, month=expiry_month, day=20)

    def calculate_contract_expiry(self, contract: ScidContractInfo) -> pd.Timestamp:
        """Public helper for retrieving the calculated expiry of a contract."""

        return self._calculate_contract_expiry(contract)

    def generate_roll_schedule(
        self,
        ticker: str,
        *,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        roll_offset: Optional[pd.DateOffset] = None,
        roll_convention: RollConvention = RollConvention.NEXT_ROLL,
    ) -> List[RollPeriod]:
        """
        Build a roll schedule for ``ticker`` using "roll one month before expiry" logic.

        Parameters
        ----------
        ticker:
            Futures symbol to build the schedule for.
        start, end:
            Optional start/end boundaries for the schedule. When omitted, the
            schedule defaults to one roll offset before the first contract and
            the final contract's expiry respectively.
        roll_offset:
            Offset applied backwards from the expiry to determine the roll date.
            Defaults to one calendar month.

        Returns
        -------
        list[RollPeriod]
            Chronologically ordered list of contract windows.
        """

        if pd is None:
            raise RuntimeError("pandas is required to generate a roll schedule")

        contracts = self.get_contracts_for_ticker(ticker)
        if not contracts:
            return []

        if isinstance(roll_convention, str):
            try:
                roll_convention = RollConvention(roll_convention.lower())
            except ValueError as exc:
                raise ValueError(
                    f"Unknown roll convention: {roll_convention!r}"
                ) from exc

        resolved_offset = roll_offset or pd.DateOffset(months=1)

        entries: List[Tuple[ScidContractInfo, pd.Timestamp, pd.Timestamp]] = []
        for contract in contracts:
            expiry = self._calculate_contract_expiry(contract)
            roll_date = expiry - resolved_offset
            entries.append((contract, roll_date, expiry))

        entries.sort(key=lambda item: item[1])

        schedule_start = start if start is not None else entries[0][1] - resolved_offset
        schedule_end = end if end is not None else entries[-1][2]

        periods: List[RollPeriod] = []
        cursor = schedule_start

        for idx, (contract, roll_date, expiry) in enumerate(entries):
            if cursor >= schedule_end:
                break

            if roll_convention is RollConvention.NEXT_ROLL:
                if idx == 0:
                    window_start = max(cursor, schedule_start)
                else:
                    window_start = max(cursor, roll_date)

                if idx + 1 < len(entries):
                    next_boundary = entries[idx + 1][1]
                else:
                    next_boundary = schedule_end

                window_end = min(schedule_end, next_boundary)
                if idx == len(entries) - 1:
                    window_end = min(window_end, expiry)
            else:  # RollConvention.OWN_ROLL
                window_start = max(cursor, schedule_start)
                window_end = min(schedule_end, roll_date)

            if window_start >= window_end:
                cursor = max(cursor, window_end)
                continue

            periods.append(
                RollPeriod(
                    contract=contract,
                    start=window_start,
                    end=window_end,
                    roll_date=roll_date,
                    expiry=expiry,
                )
            )

            cursor = window_end

        return periods

    def get_tickers(self) -> List[str]:
        """Return list of all available tickers."""
        return list(self._ticker_files.keys())

    def get_files_for_ticker(self, ticker: str) -> List[Path]:
        """Return all files for a specific ticker, sorted by expiry date."""
        return self._ticker_files.get(ticker.upper(), [])

    def get_contracts_for_ticker(self, ticker: str) -> List[ScidContractInfo]:
        """Return all contract info for a specific ticker, sorted by expiry date."""
        return self._ticker_contracts.get(ticker.upper(), [])

    def get_front_month_file(self, ticker: str, reference_date: Optional[pd.Timestamp] = None) -> Optional[Path]:
        """
        Get the "front" month file (closest large file to next calendar month).

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        reference_date : pd.Timestamp, optional
            Reference date for determining front month. Defaults to current date.

        Returns:
        --------
        Path or None
            Path to front month file, or None if no suitable file found
        """
        if reference_date is None:
            reference_date = pd.Timestamp.now()

        contracts = self.get_contracts_for_ticker(ticker)
        files = self.get_files_for_ticker(ticker)

        if not contracts:
            return None

        # Find contracts that haven't expired yet
        active_contracts = []
        for i, contract in enumerate(contracts):
            expiry = self._calculate_contract_expiry(contract)
            if expiry >= reference_date:
                file_size = files[i].stat().st_size if files[i].exists() else 0
                active_contracts.append((contract, files[i], file_size, expiry))

        if not active_contracts:
            return None

        # Sort by expiry date, then by file size (largest first for same expiry)
        active_contracts.sort(key=lambda x: (x[3], -x[2]))

        # Return the file with the earliest expiry (and largest size if tied)
        return active_contracts[0][1]

    def get_front_month_reader(self, ticker: str, reference_date: Optional[pd.Timestamp] = None) -> Optional[FastScidReader]:
        """
        Get a FastScidReader for the front month file.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        reference_date : pd.Timestamp, optional
            Reference date for determining front month

        Returns:
        --------
        FastScidReader or None
            Reader for front month file, or None if no suitable file found
        """
        front_file = self.get_front_month_file(ticker, reference_date)
        if front_file is None:
            return None

        return FastScidReader(str(front_file))

    def get_front_month_data(self, ticker: str, reference_date: Optional[pd.Timestamp] = None,
                           start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Load data from the front month file for a ticker.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        reference_date : pd.Timestamp, optional
            Reference date for determining front month
        start_ms : int, optional
            Start timestamp in epoch milliseconds
        end_ms : int, optional
            End timestamp in epoch milliseconds

        Returns:
        --------
        pd.DataFrame or None
            Front month data, or None if no suitable file found
        """
        reader = self.get_front_month_reader(ticker, reference_date)
        if reader is None:
            return None

        with reader.open() as r:
            return r.to_pandas(start_ms=start_ms, end_ms=end_ms)

    def get_file_statistics(self, ticker: str) -> Dict[str, Any]:
        """
        Get statistics about files for a ticker.

        Parameters:
        -----------
        ticker : str
            Ticker symbol

        Returns:
        --------
        Dict[str, Any]
            Statistics including file count, total size, date ranges, etc.
        """
        files = self.get_files_for_ticker(ticker)
        contracts = self.get_contracts_for_ticker(ticker)

        if not files:
            return {"ticker": ticker, "file_count": 0, "total_size": 0}

        total_size = sum(f.stat().st_size for f in files if f.exists())
        file_count = len(files)

        # Get date ranges from the first and last files
        earliest_expiry = None
        latest_expiry = None
        if contracts:
            earliest_expiry = self._calculate_contract_expiry(contracts[0])
            latest_expiry = self._calculate_contract_expiry(contracts[-1])

        return {
            "ticker": ticker,
            "file_count": file_count,
            "total_size": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "earliest_expiry": earliest_expiry,
            "latest_expiry": latest_expiry,
            "contracts": [c.contract_id for c in contracts]
        }

    def get_all_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all tickers."""
        return {ticker: self.get_file_statistics(ticker) for ticker in self.get_tickers()}


def main():
    """Console script entry point for sierrapy-scid command."""
    import argparse

    ap = argparse.ArgumentParser(description="Fast .scid reader")
    ap.add_argument("path", help="Path to .scid file")
    ap.add_argument("--info", action="store_true", help="Print detected schema and record count")
    ap.add_argument("--export", metavar="CSV", help="Export to CSV path")
    ap.add_argument("--start", type=str, default=None, help="Start ISO (UTC) e.g. 2025-09-19T00:00:00Z")
    ap.add_argument("--end", type=str, default=None, help="End ISO (UTC)")
    args = ap.parse_args()

    def parse_iso_to_ms(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        s = s.rstrip("Z")
        ts = np.datetime64(s)
        return int(ts.astype("datetime64[ms]").astype(np.int64))

    rdr = FastScidReader(args.path).open()
    if args.info:
        print(f"Variant: {rdr.schema.variant}; records: {len(rdr)}; columns: {rdr.columns()}")

    if args.export:
        start_ms = parse_iso_to_ms(args.start)
        end_ms = parse_iso_to_ms(args.end)
        rdr.export_csv(args.export, start_ms=start_ms, end_ms=end_ms)
        print(f"Exported to {args.export}")


# ------------------------------ small CLI helper ------------------------------
if __name__ == "__main__":
    main()
