from __future__ import annotations

from pathlib import Path

import pandas as pd

from sierrapy.parser.async_scid_reader import _stitch_with_tail


def _load_fixture(name: str, contract: str) -> pd.DataFrame:
    df = pd.read_csv(Path("tests/fixtures") / name, parse_dates=["DateTime"])
    df["DateTime"] = pd.to_datetime(df["DateTime"], utc=True)
    df = df.set_index("DateTime")
    df.index.name = "DateTime"
    df["contract"] = contract
    df["SourceFile"] = df.get("SourceFile", contract + ".scid")
    return df


def test_tail_stitching_fills_gap_with_latest_contract() -> None:
    gcv = _load_fixture("example_tail.csv", "GCV2025")
    gcz = _load_fixture("example_gc.csv", "GCZ2025")

    schedule = [
        (
            "GCV2025",
            pd.Timestamp("2025-08-01T00:00:00Z"),
            pd.Timestamp("2025-09-01T00:00:00Z") - pd.Timedelta(nanoseconds=1),
        ),
        (
            "GCZ2025",
            pd.Timestamp("2025-09-01T00:00:00Z"),
            pd.Timestamp("2026-01-01T00:00:00Z") - pd.Timedelta(nanoseconds=1),
        ),
    ]

    stitched = _stitch_with_tail({"GCV2025": gcv, "GCZ2025": gcz}, schedule)

    assert not stitched.empty
    assert stitched.index.is_monotonic_increasing
    assert not stitched.index.duplicated().any()
    assert {"GCV2025", "GCZ2025"} <= set(stitched["contract"].unique())

    gcv_last = gcv.index.max()
    after_gap = stitched.loc[stitched.index > gcv_last]
    assert not after_gap.empty

    first_tail_timestamp = after_gap.index.min()
    assert first_tail_timestamp == gcv_last + pd.Timedelta(hours=1)
    assert after_gap.loc[first_tail_timestamp, "contract"] == "GCZ2025"

    assert len(stitched) > 112
    assert stitched.iloc[112]["contract"] == "GCZ2025"


def test_contract_expiry_capped_to_last_bar() -> None:
    index = pd.to_datetime(
        ["2025-03-01T00:00:00Z", "2025-03-02T00:00:00Z", "2025-03-03T00:00:00Z"],
        utc=True,
    )
    df = pd.DataFrame(
        {
            "Close": [1.0, 2.0, 3.0],
            "ContractExpiry": pd.Timestamp("2025-03-15T00:00:00Z"),
            "SourceFile": "TEST2025.scid",
        },
        index=index,
    )
    df.index.name = "DateTime"

    schedule = [
        (
            "TEST2025",
            pd.Timestamp("2025-03-01T00:00:00Z"),
            pd.Timestamp("2025-03-31T00:00:00Z") - pd.Timedelta(nanoseconds=1),
        )
    ]

    stitched = _stitch_with_tail({"TEST2025": df}, schedule)
    assert not stitched.empty

    effective_end = stitched.index.max()
    expiry_values = stitched["ContractExpiry"].unique()
    assert len(expiry_values) == 1
    assert expiry_values[0] == effective_end


def test_tail_priority_prefers_latest_contract_and_handles_missing_source() -> None:
    base_index = pd.to_datetime(
        ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"],
        utc=True,
    )
    base_df = pd.DataFrame(
        {
            "Close": [10.0, 11.0, 12.0],
            "SourceFile": "ABCZ2024.scid",
        },
        index=base_index,
    )
    base_df.index.name = "DateTime"

    newer_index = pd.to_datetime(
        ["2024-01-04T00:00:00Z", "2024-01-05T00:00:00Z"],
        utc=True,
    )
    newer_df = pd.DataFrame({"Close": [13.0, 14.0]}, index=newer_index)
    newer_df.index.name = "DateTime"

    older_index = pd.to_datetime(
        ["2023-12-31T00:00:00Z", "2024-01-04T00:00:00Z"],
        utc=True,
    )
    older_df = pd.DataFrame(
        {
            "Close": [9.5, 9.75],
            "SourceFile": "ABCZ2023.scid",
        },
        index=older_index,
    )
    older_df.index.name = "DateTime"

    schedule = [
        (
            "ABCZ2024",
            pd.Timestamp("2024-01-01T00:00:00Z"),
            pd.Timestamp("2024-01-05T00:00:00Z") - pd.Timedelta(nanoseconds=1),
        ),
        (
            "ABCZ2025",
            pd.Timestamp("2024-01-05T00:00:00Z"),
            pd.Timestamp("2024-02-01T00:00:00Z") - pd.Timedelta(nanoseconds=1),
        ),
    ]

    parts = {
        "ABCZ2024": base_df,
        "ABCZ2025": newer_df,
        "ABCZ2023": older_df,
    }

    stitched = _stitch_with_tail(parts, schedule)

    gap_rows = stitched.loc[
        (stitched.index > base_index.max())
        & (stitched.index <= pd.Timestamp("2024-01-05T00:00:00Z") - pd.Timedelta(nanoseconds=1))
    ]
    assert not gap_rows.empty
    assert (gap_rows["contract"] == "ABCZ2025").all()

    # SourceFile should exist even when it was missing on input (filled with empty string).
    assert "SourceFile" in stitched.columns
    tail_source_values = gap_rows["SourceFile"].unique()
    assert len(tail_source_values) == 1
    assert tail_source_values[0] == ""
