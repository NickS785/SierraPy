from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np

from sierrapy.parser.scid_parse import DTYPE_V10_40B

_SC_EPOCH_OFFSET_US = 25569 * 86400 * 1_000_000


def write_scid_file(directory: Path, name: str, timestamps_ms: Sequence[int]) -> Path:
    path = directory / name
    arr = np.zeros(len(timestamps_ms), dtype=DTYPE_V10_40B)
    if timestamps_ms:
        arr["DateTime"] = np.asarray(timestamps_ms, dtype=np.int64) * 1000 + _SC_EPOCH_OFFSET_US
        arr["Open"] = np.linspace(1.0, 1.0 + len(timestamps_ms) - 1, len(timestamps_ms), dtype=np.float32)
        arr["High"] = arr["Open"]
        arr["Low"] = arr["Open"]
        arr["Close"] = arr["Open"]
        arr["NumTrades"] = np.arange(1, len(timestamps_ms) + 1, dtype=np.uint32)
        arr["TotalVolume"] = np.arange(10, 10 + 10 * len(timestamps_ms), 10, dtype=np.uint32)
        arr["BidVolume"] = arr["TotalVolume"]
        arr["AskVolume"] = arr["TotalVolume"]

    path.write_bytes(arr.tobytes())
    return path
