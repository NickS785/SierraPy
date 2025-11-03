from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pytest

from sierrapy.parser import async_scid_reader as asc
from sierrapy.parser.async_scid_reader import AsyncScidReader
from sierrapy.parser.scid_parse import RollPeriod, ScidContractInfo


_TO_PANDAS_CALLS: List[str] = []
_PEEK_RANGES: Dict[str, tuple[int, int, int]] = {}


def _epoch_ms(ts: str) -> int:
    return int(pd.Timestamp(ts, tz="UTC").value // 1_000_000)


class _PeekFastReader:
    def __init__(self, path: str, **_: object) -> None:
        self.path = path

    def open(self) -> "_PeekFastReader":
        return self

    def __enter__(self) -> "_PeekFastReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def peek_range(self) -> tuple[int, int | None, int | None]:
        return _PEEK_RANGES[self.path]

    def to_pandas(self, *, start_ms=None, end_ms=None, **_: object) -> "pd.DataFrame":
        _TO_PANDAS_CALLS.append(self.path)
        index = pd.to_datetime(
            ["2025-01-01T12:00:00Z", "2025-01-01T18:00:00Z"], utc=True
        )
        data = {"Close": [1.0, 1.5]}
        frame = pd.DataFrame(data, index=index)
        frame.index.name = "DateTime"
        return frame


@pytest.fixture(autouse=True)
def _reset_globals() -> None:
    _TO_PANDAS_CALLS.clear()
    _PEEK_RANGES.clear()


@pytest.fixture
def _schedule(tmp_path: Path) -> List[RollPeriod]:
    contract_a = ScidContractInfo("NG", "Z", 2025, "NYM", tmp_path / "a.scid")
    contract_b = ScidContractInfo("NG", "F", 2026, "NYM", tmp_path / "b.scid")

    period_a = RollPeriod(
        contract=contract_a,
        start=pd.Timestamp("2025-01-01T00:00:00Z"),
        end=pd.Timestamp("2025-02-01T00:00:00Z"),
        roll_date=pd.Timestamp("2025-01-15T00:00:00Z"),
        expiry=pd.Timestamp("2025-02-01T00:00:00Z"),
    )
    period_b = RollPeriod(
        contract=contract_b,
        start=pd.Timestamp("2025-03-01T00:00:00Z"),
        end=pd.Timestamp("2025-04-01T00:00:00Z"),
        roll_date=pd.Timestamp("2025-03-15T00:00:00Z"),
        expiry=pd.Timestamp("2025-04-01T00:00:00Z"),
    )

    _PEEK_RANGES[str(contract_a.file_path)] = (
        10,
        _epoch_ms("2025-01-01T00:00:00Z"),
        _epoch_ms("2025-01-02T00:00:00Z"),
    )
    _PEEK_RANGES[str(contract_b.file_path)] = (
        5,
        _epoch_ms("2025-01-01T00:00:00Z"),
        _epoch_ms("2025-01-10T00:00:00Z"),
    )

    return [period_a, period_b]


def test_preflight_skips_non_overlapping(monkeypatch, tmp_path: Path, _schedule: List[RollPeriod]) -> None:
    reader = AsyncScidReader(tmp_path)

    monkeypatch.setattr(asc, "FastScidReader", _PeekFastReader)

    async def run_sync(func):
        return func()

    monkeypatch.setattr(reader, "_run_in_executor", run_sync)

    contract_c = ScidContractInfo("NG", "G", 2026, "NYM", tmp_path / "c.scid")
    period_c = RollPeriod(
        contract=contract_c,
        start=pd.Timestamp("2025-04-01T00:00:00Z"),
        end=pd.Timestamp("2025-05-01T00:00:00Z"),
        roll_date=pd.Timestamp("2025-04-15T00:00:00Z"),
        expiry=pd.Timestamp("2025-05-01T00:00:00Z"),
    )

    _PEEK_RANGES[str(contract_c.file_path)] = (
        8,
        _epoch_ms("2025-03-15T00:00:00Z"),
        _epoch_ms("2025-04-20T00:00:00Z"),
    )

    schedule = _schedule + [period_c]

    monkeypatch.setattr(reader._manager, "generate_roll_schedule", lambda *_, **__: schedule)

    baseline = asyncio.run(
        reader.load_front_month_series(
            "NG",
            include_metadata=False,
            preflight_peek=False,
        )
    )
    baseline_calls = list(_TO_PANDAS_CALLS)

    _TO_PANDAS_CALLS.clear()

    optimized = asyncio.run(
        reader.load_front_month_series(
            "NG",
            include_metadata=False,
            preflight_peek=True,
        )
    )
    optimized_calls = list(_TO_PANDAS_CALLS)

    pd.testing.assert_frame_equal(baseline, optimized)

    assert len(baseline_calls) == 3
    assert len(optimized_calls) == 2
    assert optimized_calls[0] == str(schedule[0].contract.file_path)
    assert optimized_calls[1] == str(schedule[-1].contract.file_path)


def test_preflight_keeps_truncated_final_period(monkeypatch, tmp_path: Path, _schedule: List[RollPeriod]) -> None:
    reader = AsyncScidReader(tmp_path)

    monkeypatch.setattr(asc, "FastScidReader", _PeekFastReader)

    async def run_sync(func):
        return func()

    monkeypatch.setattr(reader, "_run_in_executor", run_sync)
    monkeypatch.setattr(reader._manager, "generate_roll_schedule", lambda *_, **__: _schedule)

    final_path = str(_schedule[-1].contract.file_path)
    _PEEK_RANGES[final_path] = (
        2,
        _epoch_ms("2025-02-01T00:00:00Z"),
        _epoch_ms("2025-02-15T00:00:00Z"),
    )

    baseline = asyncio.run(
        reader.load_front_month_series(
            "NG",
            include_metadata=False,
            preflight_peek=False,
        )
    )
    baseline_calls = list(_TO_PANDAS_CALLS)

    _TO_PANDAS_CALLS.clear()

    optimized = asyncio.run(
        reader.load_front_month_series(
            "NG",
            include_metadata=False,
            preflight_peek=True,
        )
    )
    optimized_calls = list(_TO_PANDAS_CALLS)

    pd.testing.assert_frame_equal(baseline, optimized)

    assert len(baseline_calls) == 2
    assert len(optimized_calls) == 2
    assert optimized_calls[-1] == final_path
