from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from sierrapy.parser import async_scid_reader as asc
from sierrapy.parser.async_scid_reader import AsyncScidReader
from sierrapy.parser.scid_parse import RollPeriod, ScidContractInfo


_created_readers: list["_DummyFastReader"] = []


class _DummyFastReader:
    def __init__(self, path: str) -> None:
        self.path = path
        self.start_ms: int | None = None
        self.end_ms: int | None = None
        _created_readers.append(self)

    def open(self) -> "_DummyFastReader":
        return self

    def __enter__(self) -> "_DummyFastReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def to_pandas(self, *, start_ms=None, end_ms=None, **kwargs):
        self.start_ms = start_ms
        self.end_ms = end_ms

        idx = pd.to_datetime(
            [
                "2025-09-19T23:00:00Z",
                "2025-09-20T00:00:00Z",
                "2025-09-20T01:00:00Z",
                "2025-10-19T23:00:00Z",
                "2025-10-20T00:00:00Z",
            ]
        )

        data = {
            "Open": [1, 2, 3, 4, 5],
            "High": [1, 2, 3, 4, 5],
            "Low": [1, 2, 3, 4, 5],
            "Close": [1, 2, 3, 4, 5],
            "TotalVolume": [10, 20, 30, 40, 50],
        }

        frame = pd.DataFrame(data, index=idx)
        frame.index.name = "DateTime"
        return frame


def test_read_period_limits_contract_window(monkeypatch):
    _created_readers.clear()

    reader = AsyncScidReader("/tmp")

    monkeypatch.setattr(asc, "FastScidReader", _DummyFastReader)

    async def run_sync(func):
        return func()

    monkeypatch.setattr(reader, "_run_in_executor", run_sync)

    contract = ScidContractInfo(
        ticker="NG",
        month="V",
        year=2025,
        exchange="NYM",
        file_path=Path("/fake/path")
    )

    start = pd.Timestamp("2025-09-20T00:00:00Z")
    end = pd.Timestamp("2025-10-20T00:00:00Z")

    period = RollPeriod(
        contract=contract,
        start=start,
        end=end,
        roll_date=start,
        expiry=end,
    )

    df = asyncio.run(
        reader._read_period(
            period,
            columns=None,
            include_metadata=True,
            volume_per_bar=None,
            volume_column="TotalVolume",
            resample_rule=None,
            resample_kwargs=None,
            drop_volume_column=False,
            drop_invalid_rows=False,
        )
    )

    dummy_reader = _created_readers[-1]

    # ``FastScidReader`` should be asked only for the window we need.
    expected_start_ms = int(start.value // 1_000_000)
    expected_end_ms = int(end.value // 1_000_000) - 1

    assert dummy_reader.start_ms == expected_start_ms
    assert dummy_reader.end_ms == expected_end_ms

    assert (df.index >= start).all()
    assert (df.index < end).all()

    # Metadata is preserved and only one contract is present for the window.
    assert set(df["Contract"].unique()) == {contract.contract_id}
