from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

from sierrapy.parser.scid_parse import FastScidReader

from tests.utils_scid import write_scid_file


@pytest.fixture
def scid_path(tmp_path: Path) -> Path:
    timestamps = [1_700_000_000_000, 1_700_000_001_000, 1_700_000_002_000]
    return write_scid_file(tmp_path, "sample.scid", timestamps)


def test_peek_real_file(tmp_path: Path) -> None:
    timestamps = [1_700_000_000_000, 1_700_000_050_000, 1_700_000_100_000]
    path = write_scid_file(tmp_path, "peek.scid", timestamps)

    with FastScidReader(str(path), read_only=True).open() as reader:
        count, start, end = reader.peek_range()

        assert count == len(timestamps)
        assert start == timestamps[0]
        assert end == timestamps[-1]
        assert reader.read_timestamp(0) == start
        assert reader.read_timestamp(-1) == end

        assert reader._timestamps_view is not None  # sanity: attribute populated
        dt_field = reader.view["DateTime"]
        assert np.shares_memory(reader._timestamps_view, dt_field)


def test_read_timestamp_bounds(scid_path: Path) -> None:
    with FastScidReader(str(scid_path), read_only=True).open() as reader:
        with pytest.raises(IndexError):
            reader.read_timestamp(3)
        with pytest.raises(IndexError):
            reader.read_timestamp(-4)


def test_peek_empty_file(tmp_path: Path) -> None:
    empty = tmp_path / "empty.scid"
    empty.write_bytes(b"")
    reader = FastScidReader(str(empty), read_only=True)

    with pytest.raises(ValueError):
        reader.open()


def test_peek_missing_file(tmp_path: Path) -> None:
    reader = FastScidReader(str(tmp_path / "missing.scid"), read_only=True)
    with pytest.raises(FileNotFoundError):
        reader.open()


@pytest.mark.skipif("SCID_PATH" not in os.environ, reason="SCID_PATH not configured")
def test_peek_range_integration() -> None:
    path = Path(os.environ["SCID_PATH"])
    with FastScidReader(str(path), read_only=True).open() as reader:
        count, start, end = reader.peek_range()

        assert count >= 0
        if count == 0:
            assert start is None and end is None
        else:
            assert start is not None and end is not None and start <= end
            assert reader.read_timestamp(0) == start
            assert reader.read_timestamp(-1) == end
