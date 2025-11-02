from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd
import pytest

from sierrapy.parser import async_scid_reader as asc
from sierrapy.parser.async_scid_reader import AsyncScidReader
from sierrapy.parser.scid_parse import RollPeriod, ScidContractInfo, normalize_resample_params


_created_readers: list["_DummyFastReader"] = []
_parquet_exports: list[tuple[str, str, dict]] = []


class _DummyFastReader:
    def __init__(self, path: str) -> None:
        self.path = path
        self.start_ms: int | None = None
        self.end_ms: int | None = None
        self.kwargs: dict | None = None
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
        self.kwargs = kwargs

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
            "BidVolume": [5, 5, 5, 5, 5],
        }

        frame = pd.DataFrame(data, index=idx)
        frame.index.name = "DateTime"
        return frame


class _DummyParquetFastReader:
    def __init__(self, path: str) -> None:
        self.path = path
        Path(path).touch()

    def open(self) -> "_DummyParquetFastReader":
        return self

    def __enter__(self) -> "_DummyParquetFastReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def export_to_parquet_optimized(self, out_path: str, **kwargs):
        record = (self.path, out_path, kwargs)
        _parquet_exports.append(record)
        return {
            "source": self.path,
            "output": out_path,
            "kwargs": kwargs,
        }


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

    roll_date = start + pd.Timedelta(hours=1)

    period = RollPeriod(
        contract=contract,
        start=start,
        end=end,
        roll_date=roll_date,
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
    assert dummy_reader.kwargs == {
        "columns": None,
        "volume_per_bar": None,
        "volume_column": "TotalVolume",
        "resample_rule": None,
        "resample_kwargs": None,
        "drop_invalid_rows": False,
    }

    assert (df.index >= start).all()
    assert (df.index < end).all()

    # Metadata is preserved and only one contract is present for the window.
    assert set(df["Contract"].unique()) == {contract.contract_id}


def test_load_front_month_series_volume_rule(monkeypatch):
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
        file_path=Path("/fake/path"),
    )

    start = pd.Timestamp("2025-09-20T00:00:00Z")
    end = pd.Timestamp("2025-10-20T00:00:00Z")

    roll_date = start + pd.Timedelta(hours=1)

    period = RollPeriod(
        contract=contract,
        start=start,
        end=end,
        roll_date=roll_date,
        expiry=end,
    )

    monkeypatch.setattr(reader.manager, "generate_roll_schedule", lambda *args, **kwargs: [period])

    df = asyncio.run(
        reader.load_front_month_series(
            "NG",
            start=start,
            end=end,
            columns=["Open"],
            include_metadata=False,
            resample_rule="volume:30",
        )
    )

    dummy_reader = _created_readers[-1]

    assert dummy_reader.kwargs["volume_per_bar"] == 30
    assert dummy_reader.kwargs["volume_column"] == "TotalVolume"
    assert dummy_reader.kwargs["resample_rule"] is None
    assert dummy_reader.kwargs["resample_kwargs"] is None

    assert "TotalVolume" not in df.columns
    assert not df.empty


def test_export_scid_files_to_parquet(monkeypatch, tmp_path):
    _parquet_exports.clear()

    monkeypatch.setattr(asc, "FastScidReader", _DummyParquetFastReader)

    reader = AsyncScidReader(tmp_path)

    async def run_sync(func):
        return func()

    monkeypatch.setattr(reader, "_run_in_executor", run_sync)

    source_a = tmp_path / "a.scid"
    source_b = tmp_path / "b.scid"
    # Ensure files exist so export proceeds
    source_a.touch()
    source_b.touch()

    target_a = tmp_path / "out" / "a.parquet"
    target_b = tmp_path / "nested" / "dir" / "b.parquet"

    include_columns = ["Open", "Close"]

    stats = asyncio.run(
        reader.export_scid_files_to_parquet(
            [(source_a, target_a), (source_b, target_b)],
            start_ms=1,
            end_ms=2,
            include_columns=include_columns,
            chunk_records=123,
            compression="snappy",
            include_time=False,
            use_dictionary=True,
            resample_rule="volume:500:BidVolume",
            resample_kwargs={"label": "right"},
            volume_per_bar=None,
            volume_column="BidVolume",
        )
    )

    assert target_a.parent.exists()
    assert target_b.parent.exists()

    assert len(_parquet_exports) == 2

    export_map = {Path(src): (src, dst, kwargs) for src, dst, kwargs in _parquet_exports}

    call_a = export_map[source_a]
    assert call_a[1] == str(target_a)
    assert call_a[2]["start_ms"] == 1
    assert call_a[2]["end_ms"] == 2
    assert call_a[2]["include_columns"] == include_columns
    assert call_a[2]["chunk_records"] == 123
    assert call_a[2]["compression"] == "snappy"
    assert call_a[2]["include_time"] is False
    assert call_a[2]["use_dictionary"] is True
    assert call_a[2]["resample_rule"] is None
    assert call_a[2]["resample_kwargs"] is None
    assert call_a[2]["volume_per_bar"] == 500
    assert call_a[2]["volume_column"] == "BidVolume"

    call_b = export_map[source_b]
    assert call_b[1] == str(target_b)

    assert stats[str(source_a)]["output"] == str(target_a)
    assert stats[str(source_b)]["output"] == str(target_b)


def test_normalize_resample_params_time_rule():
    rule, volume, column = normalize_resample_params("5T", None, "TotalVolume")

    assert rule == "5T"
    assert volume is None
    assert column == "TotalVolume"


def test_normalize_resample_params_volume_rule():
    rule, volume, column = normalize_resample_params("volume:250:BidVolume", None, "TotalVolume")

    assert rule is None
    assert volume == 250
    assert column == "BidVolume"


def test_normalize_resample_params_conflicts():
    with pytest.raises(ValueError):
        normalize_resample_params("volume:250", 100, "TotalVolume")

    with pytest.raises(ValueError):
        normalize_resample_params("volume:250:BidVolume", None, "AskVolume")
