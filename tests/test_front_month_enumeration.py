from __future__ import annotations

from pathlib import Path

import pandas as pd

from sierrapy.parser.async_scid_reader import AsyncScidReader
from sierrapy.parser.scid_parse import RollPeriod, ScidContractInfo


def _period(contract_id: str, start: str, end: str, path: Path) -> RollPeriod:
    ticker = "NG"
    month = contract_id[0]
    year = 2000 + int(contract_id[1:])
    contract = ScidContractInfo(
        ticker=ticker,
        month=month,
        year=year,
        exchange="NYM",
        file_path=path,
    )
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    return RollPeriod(
        contract=contract,
        start=start_ts,
        end=end_ts,
        roll_date=start_ts + pd.Timedelta(days=1),
        expiry=end_ts,
    )


def test_enumerate_front_month_contracts(monkeypatch, tmp_path: Path) -> None:
    reader = AsyncScidReader(tmp_path)
    periods = [
        _period("Z25", "2025-10-01T00:00:00Z", "2025-11-01T00:00:00Z", tmp_path / "one.scid"),
        _period("F26", "2025-11-01T00:00:00Z", "2025-12-01T00:00:00Z", tmp_path / "two.scid"),
    ]

    monkeypatch.setattr(reader, "generate_roll_schedule", lambda *_, **__: periods)

    result = reader.enumerate_front_month_contracts("NG")
    assert len(result) == 2

    for idx, row in enumerate(result):
        period = periods[idx]
        assert row["contract"] == period.contract.contract_id
        assert row["file"] == str(period.contract.file_path)
        assert row["start"] == period.start.tz_convert("UTC")
        assert row["end"] == period.end.tz_convert("UTC")
        assert row["start"] < row["end"]
