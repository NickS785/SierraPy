from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from sierrapy.parser.async_scid_reader import AsyncScidReader
from sierrapy.parser.scid_parse import ScidTickerFileManager


def _expected_year_from_digit(digit: int) -> int:
    current_year = datetime.now(timezone.utc).year
    decade_base = (current_year // 10) * 10
    year = decade_base + digit
    if year > current_year + 5:
        year -= 10
    elif year < current_year - 5:
        year += 10
    return year


def test_manager_discovers_rithmic_files(tmp_path: Path) -> None:
    (tmp_path / "CLM4.NYM.scid").touch()

    manager = ScidTickerFileManager(str(tmp_path), service="rithmic")
    contracts = manager.get_contracts_for_ticker("CL")

    assert len(contracts) == 1
    contract = contracts[0]
    assert contract.month == "M"
    assert contract.service == "rithmic"
    assert contract.year == _expected_year_from_digit(4)


def test_generate_schedule_bridges_missing_contracts(tmp_path: Path) -> None:
    for name in ["CLM4.NYM.scid", "CLZ4.NYM.scid"]:
        (tmp_path / name).touch()

    manager = ScidTickerFileManager(str(tmp_path), service="rithmic")
    periods = manager.generate_roll_schedule("CL")

    assert len(periods) == 2
    assert periods[0].end == periods[1].start


def test_async_reader_service_flag(monkeypatch, tmp_path: Path) -> None:
    reader = AsyncScidReader(tmp_path)

    class _StubManager:
        def generate_roll_schedule(self, *_, **__):
            return []

    requested: list[str] = []

    async def _run_sync(func):
        return func()

    monkeypatch.setattr(reader, "_run_in_executor", _run_sync)

    def _fake_get_manager(service: str) -> _StubManager:
        requested.append(service)
        return _StubManager()

    monkeypatch.setattr(reader, "_get_manager", _fake_get_manager)

    result = asyncio.run(
        reader.load_front_month_continuous("CL", service="rithmic")
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert requested == ["rithmic"]
