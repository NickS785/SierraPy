"""Asynchronous helpers for working with Sierra Chart SCID files."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, TypeVar, Union

from .scid_parse import (
    FastScidReader,
    RollConvention,
    RollPeriod,
    ScidTickerFileManager,
)

try:  # Optional dependency (aligned with FastScidReader)
    import pandas as pd
    import numpy as np
except Exception:  # pragma: no cover - handled at runtime for optional dependency
    pd = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]

T = TypeVar("T")


def _ensure_pandas() -> "pd":
    if pd is None:  # pragma: no cover - exercised when pandas unavailable
        raise RuntimeError("pandas is required for asynchronous SCID reading")
    return pd


def _coerce_timestamp(value: Optional[Union["pd.Timestamp", datetime, str]]) -> Optional["pd.Timestamp"]:
    if value is None:
        return None

    frame_pd = _ensure_pandas()
    if isinstance(value, frame_pd.Timestamp):
        return value
    return frame_pd.Timestamp(value)


def _ensure_utc(ts: "pd.Timestamp") -> "pd.Timestamp":
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _timestamp_to_epoch_ms(ts: Optional["pd.Timestamp"]) -> Optional[int]:
    if ts is None:
        return None
    utc_ts = _ensure_utc(ts)
    return int(utc_ts.value // 1_000_000)


def _normalize_roll_convention(
    value: Union[RollConvention, str, None]
) -> RollConvention:
    if isinstance(value, RollConvention):
        return value
    if value is None:
        return RollConvention.NEXT_ROLL
    try:
        return RollConvention(value.lower())
    except ValueError as exc:
        valid = ", ".join(member.value for member in RollConvention)
        raise ValueError(
            f"Unknown roll convention {value!r}. Expected one of: {valid}"
        ) from exc


class AsyncScidReader:
    """Asynchronously load front-month SCID data across contract rolls."""

    def __init__(
        self,
        directory: Union[str, Path],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_concurrency: Optional[int] = None,
    ) -> None:
        self.reader = FastScidReader
        self._manager = ScidTickerFileManager(str(directory))
        self._loop = loop
        self._semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(max_concurrency) if max_concurrency else None
        )
        self._logger = logging.getLogger(__name__)

    @property
    def manager(self) -> ScidTickerFileManager:
        return self._manager

    def generate_roll_schedule(
        self,
        ticker: str,
        *,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
    ) -> List[RollPeriod]:
        start_ts = _coerce_timestamp(start)
        end_ts = _coerce_timestamp(end)
        return self._manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
            roll_convention=_normalize_roll_convention(roll_convention),
        )

    async def load_front_month_series(
        self,
        ticker: str,
        *,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        columns: Optional[Sequence[str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
        include_metadata: bool = True,
        volume_per_bar: Optional[int] = None,
        volume_column: str = "TotalVolume",
        resample_rule: Optional[str] = None,
        resample_kwargs: Optional[Dict[str, Any]] = None,
        drop_invalid_rows: bool = False,

    ) -> "pd.DataFrame":
        frame_pd = _ensure_pandas()

        start_ts = _coerce_timestamp(start)
        end_ts = _coerce_timestamp(end)

        effective_columns: Optional[List[str]] = None
        drop_volume_column = False
        if columns is not None:
            effective_columns = list(columns)

        if volume_per_bar is not None and effective_columns is not None:
            if volume_column not in effective_columns:
                effective_columns.append(volume_column)
                drop_volume_column = True

        periods = self._manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
            roll_convention=_normalize_roll_convention(roll_convention),
        )

        if not periods:
            return frame_pd.DataFrame()

        tasks = [
            self._read_period(
                period,
                columns=effective_columns or columns,
                include_metadata=include_metadata,
                volume_per_bar=volume_per_bar,
                volume_column=volume_column,
                resample_rule=resample_rule,
                resample_kwargs=resample_kwargs,
                drop_volume_column=drop_volume_column,
                drop_invalid_rows=drop_invalid_rows,
            )

            for period in periods
        ]

        frames = await asyncio.gather(*tasks)
        if not frames:
            return frame_pd.DataFrame()

        combined = frame_pd.concat(frames, axis=0)
        combined.sort_index(inplace=True)

        # Remove any duplicate timestamps (shouldn't occur with strict roll cutoff, but safety check)
        combined = combined.loc[~combined.index.duplicated(keep="last")]

        if start_ts is not None:
            combined = combined.loc[combined.index >= _ensure_utc(start_ts)]
        if end_ts is not None:
            combined = combined.loc[combined.index < _ensure_utc(end_ts)]

        combined.index.name = "DateTime"
        return combined

    async def load_scid_files(
        self,
        file_paths: Sequence[Union[str, Path]],
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        include_path_column: bool = True,
        volume_per_bar: Optional[int] = None,
        volume_column: str = "TotalVolume",
        resample_rule: Optional[str] = None,
        resample_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, "pd.DataFrame"]:
        _ensure_pandas()

        effective_columns: Optional[List[str]] = None
        drop_volume_column = False
        if columns is not None:
            effective_columns = list(columns)

        if volume_per_bar is not None and effective_columns is not None:
            if volume_column not in effective_columns:
                effective_columns.append(volume_column)
                drop_volume_column = True

        normalized: List[Path] = [Path(path) for path in file_paths]
        results: Dict[str, "pd.DataFrame"] = {}

        async def _read_and_store(path: Path) -> None:
            df = await self._read_file(
                path,
                start_ms=start_ms,
                end_ms=end_ms,
                columns=effective_columns or columns,
                volume_per_bar=volume_per_bar,
                volume_column=volume_column,
                resample_rule=resample_rule,
                resample_kwargs=resample_kwargs,
                drop_volume_column=drop_volume_column,
            )
            if include_path_column and not df.empty:
                df = df.copy()
                df["SourceFile"] = str(path)
            results[str(path)] = df

        await asyncio.gather(*(_read_and_store(path) for path in normalized))
        return results

    async def export_scid_files_to_parquet(
        self,
        exports: Sequence[tuple[Union[str, Path], Union[str, Path]]],
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_columns: Sequence[str] = (
            "Open",
            "High",
            "Low",
            "Close",
            "NumTrades",
            "TotalVolume",
            "BidVolume",
            "AskVolume",
        ),
        chunk_records: int = 2_000_000,
        compression: str = "zstd",
        include_time: bool = True,
        use_dictionary: bool = False,
        create_parent_dirs: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Export multiple SCID files to Parquet concurrently.

        Parameters
        ----------
        exports:
            Sequence of ``(scid_path, parquet_path)`` tuples to export.
        start_ms, end_ms, include_columns, chunk_records, compression,
        include_time, use_dictionary:
            Forwarded to :meth:`FastScidReader.export_to_parquet_optimized`.
        create_parent_dirs:
            When ``True`` (default), ensure the destination directory exists.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            Mapping of source SCID path to the statistics returned by
            :meth:`FastScidReader.export_to_parquet_optimized`.
        """

        normalized = [(Path(src), Path(dst)) for src, dst in exports]
        results: Dict[str, Dict[str, Any]] = {}

        async def _export(src: Path, dst: Path) -> None:
            stats = await self._export_file_to_parquet(
                src,
                dst,
                start_ms=start_ms,
                end_ms=end_ms,
                include_columns=include_columns,
                chunk_records=chunk_records,
                compression=compression,
                include_time=include_time,
                use_dictionary=use_dictionary,
                create_parent_dirs=create_parent_dirs,
            )
            results[str(src)] = stats

        await asyncio.gather(*(_export(src, dst) for src, dst in normalized))
        return results

    async def _read_period(
        self,
        period: RollPeriod,
        *,
        columns: Optional[Sequence[str]],
        include_metadata: bool,
        volume_per_bar: Optional[int],
        volume_column: str,
        resample_rule: Optional[str],
        resample_kwargs: Optional[Dict[str, Any]],
        drop_volume_column: bool,
        drop_invalid_rows: bool,

    ) -> "pd.DataFrame":
        frame_pd = _ensure_pandas()

        start_bound = _ensure_utc(period.start)
        end_bound = _ensure_utc(period.end)
        start_ms = _timestamp_to_epoch_ms(period.start)
        end_exclusive_ms = _timestamp_to_epoch_ms(period.end)

        if start_ms is not None and end_exclusive_ms is not None and start_ms >= end_exclusive_ms:
            return frame_pd.DataFrame()

        end_ms: Optional[int]
        if end_exclusive_ms is None:
            end_ms = None
        else:
            # ``FastScidReader.to_pandas`` treats ``end_ms`` as inclusive.
            # Subtract one millisecond so the read window is effectively
            # ``[start, end)`` which prevents pulling data from the next
            # contract's roll window.
            end_ms = end_exclusive_ms - 1

            if start_ms is not None and end_ms < start_ms:
                return frame_pd.DataFrame()

        def _load() -> "pd.DataFrame":
            with FastScidReader(str(period.contract.file_path)).open() as reader:
                df = reader.to_pandas(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    columns=columns,
                    volume_per_bar=volume_per_bar,
                    volume_column=volume_column,
                    resample_rule=resample_rule,
                    resample_kwargs=resample_kwargs,
                    drop_invalid_rows=drop_invalid_rows,
                )

            # Apply period bounds
            if not df.empty:
                mask = (df.index >= start_bound) & (df.index < end_bound)
                df = df.loc[mask]

            if include_metadata and not df.empty:
                df = df.copy()
                df["Contract"] = period.contract.contract_id
                df["Ticker"] = period.contract.ticker
                df["RollDate"] = _ensure_utc(period.roll_date)
                df["ContractExpiry"] = _ensure_utc(period.expiry)
                df["SourceFile"] = str(period.contract.file_path)

            if drop_volume_column and volume_column in df.columns:
                df = df.drop(columns=[volume_column])

            return df

        df = await self._run_in_executor(_load)
        return df

    async def _read_file(
        self,
        path: Path,
        *,
        start_ms: Optional[int],
        end_ms: Optional[int],
        columns: Optional[Sequence[str]],
        volume_per_bar: Optional[int],
        volume_column: str,
        resample_rule: Optional[str],
        resample_kwargs: Optional[Dict[str, Any]],
        drop_volume_column: bool,

    ) -> "pd.DataFrame":
        frame_pd = _ensure_pandas()

        def _load() -> "pd.DataFrame":
            if not path.exists():
                self._logger.warning("SCID file not found: %s", path)
                return frame_pd.DataFrame()

            with FastScidReader(str(path)).open() as reader:
                return reader.to_pandas(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    columns=columns,
                    volume_per_bar=volume_per_bar,
                    volume_column=volume_column,
                    resample_rule=resample_rule,
                    resample_kwargs=resample_kwargs,
                )

        df = await self._run_in_executor(_load)
        if drop_volume_column and volume_column in df.columns:
            df = df.drop(columns=[volume_column])
        return df

    async def _export_file_to_parquet(
        self,
        src: Path,
        dst: Path,
        *,
        start_ms: Optional[int],
        end_ms: Optional[int],
        include_columns: Sequence[str],
        chunk_records: int,
        compression: str,
        include_time: bool,
        use_dictionary: bool,
        create_parent_dirs: bool,
    ) -> Dict[str, Any]:
        def _export() -> Dict[str, Any]:
            if not src.exists():
                self._logger.warning("SCID file not found: %s", src)
                return {}

            if create_parent_dirs:
                dst.parent.mkdir(parents=True, exist_ok=True)

            reader = FastScidReader(str(src))
            return reader.export_to_parquet_optimized(
                str(dst),
                start_ms=start_ms,
                end_ms=end_ms,
                include_columns=include_columns,
                chunk_records=chunk_records,
                compression=compression,
                include_time=include_time,
                use_dictionary=use_dictionary,
            )

        return await self._run_in_executor(_export)


    async def _run_in_executor(self, func: Callable[[], T]) -> T:
        if self._semaphore is None:
            return await self._submit(func)
        async with self._semaphore:
            return await self._submit(func)

    async def _submit(self, func: Callable[[], T]) -> T:
        try:
            to_thread = asyncio.to_thread
        except AttributeError:  # pragma: no cover - Python < 3.9 fallback
            loop = self._loop or asyncio.get_running_loop()
            return await loop.run_in_executor(None, func)
        else:
            return await to_thread(func)


class ScidReader:
    """Synchronous wrapper around AsyncScidReader that handles asyncio automatically."""

    def __init__(
        self,
        directory: Union[str, Path],
        *,
        max_concurrency: Optional[int] = None,
    ) -> None:
        self._async_reader = AsyncScidReader(
            directory,
            max_concurrency=max_concurrency,
        )


    @property
    def manager(self) -> ScidTickerFileManager:
        return self._async_reader.manager

    def generate_roll_schedule(
        self,
        ticker: str,
        *,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
    ) -> List[RollPeriod]:
        return self._async_reader.generate_roll_schedule(
            ticker,
            start=start,
            end=end,
            roll_offset=roll_offset,
            roll_convention=roll_convention,
        )

    def load_front_month_series(
        self,
        ticker: str,
        *,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        columns: Optional[Sequence[str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
        include_metadata: bool = True,
        volume_per_bar: Optional[int] = None,
        volume_column: str = "TotalVolume",
        resample_rule: Optional[str] = None,
        resample_kwargs: Optional[Dict[str, Any]] = None,
        drop_invalid_rows: bool = False,
    ) -> "pd.DataFrame":
        """Load front-month series synchronously (handles asyncio internally)."""
        return asyncio.run(
            self._async_reader.load_front_month_series(
                ticker,
                start=start,
                end=end,
                columns=columns,
                roll_offset=roll_offset,
                roll_convention=roll_convention,
                include_metadata=include_metadata,
                volume_per_bar=volume_per_bar,
                volume_column=volume_column,
                resample_rule=resample_rule,
                resample_kwargs=resample_kwargs,
                drop_invalid_rows=drop_invalid_rows,
            )
        )

    def load_scid_files(
        self,
        file_paths: Sequence[Union[str, Path]],
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        include_path_column: bool = True,
        volume_per_bar: Optional[int] = None,
        volume_column: str = "TotalVolume",
        resample_rule: Optional[str] = None,
        resample_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, "pd.DataFrame"]:
        """Load SCID files synchronously (handles asyncio internally)."""
        return asyncio.run(
            self._async_reader.load_scid_files(
                file_paths,
                start_ms=start_ms,
                end_ms=end_ms,
                columns=columns,
                include_path_column=include_path_column,
                volume_per_bar=volume_per_bar,
                volume_column=volume_column,
                resample_rule=resample_rule,
                resample_kwargs=resample_kwargs,
            )
        )

    def export_scid_files_to_parquet(
        self,
        exports: Sequence[tuple[Union[str, Path], Union[str, Path]]],
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_columns: Sequence[str] = (
            "Open",
            "High",
            "Low",
            "Close",
            "NumTrades",
            "TotalVolume",
            "BidVolume",
            "AskVolume",
        ),
        chunk_records: int = 2_000_000,
        compression: str = "zstd",
        include_time: bool = True,
        use_dictionary: bool = False,
        create_parent_dirs: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Synchronous wrapper around asynchronous Parquet export."""

        return asyncio.run(
            self._async_reader.export_scid_files_to_parquet(
                exports,
                start_ms=start_ms,
                end_ms=end_ms,
                include_columns=include_columns,
                chunk_records=chunk_records,
                compression=compression,
                include_time=include_time,
                use_dictionary=use_dictionary,
                create_parent_dirs=create_parent_dirs,
            )
        )


__all__ = [
    "AsyncScidReader",
    "ScidReader",
]

