"""Asynchronous helpers for working with Sierra Chart SCID files."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, TypeVar, Union

from .scid_parse import FastScidReader, RollPeriod, ScidTickerFileManager

try:  # Optional dependency (aligned with FastScidReader)
    import pandas as pd
except Exception:  # pragma: no cover - handled at runtime for optional dependency
    pd = None  # type: ignore[assignment]

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


class AsyncFrontMonthScidReader:
    """Asynchronously load front-month SCID data across contract rolls."""

    def __init__(
        self,
        directory: Union[str, Path],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_concurrency: Optional[int] = None,
    ) -> None:
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
    ) -> List[RollPeriod]:
        start_ts = _coerce_timestamp(start)
        end_ts = _coerce_timestamp(end)
        return self._manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
        )

    async def load_front_month_series(
        self,
        ticker: str,
        *,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        columns: Optional[Sequence[str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        include_metadata: bool = True,
    ) -> "pd.DataFrame":
        frame_pd = _ensure_pandas()

        start_ts = _coerce_timestamp(start)
        end_ts = _coerce_timestamp(end)

        periods = self._manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
        )

        if not periods:
            return frame_pd.DataFrame()

        tasks = [
            self._read_period(period, columns=columns, include_metadata=include_metadata)
            for period in periods
        ]

        frames = await asyncio.gather(*tasks)
        if not frames:
            return frame_pd.DataFrame()

        combined = frame_pd.concat(frames, axis=0)
        combined.sort_index(inplace=True)
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
    ) -> Dict[str, "pd.DataFrame"]:
        _ensure_pandas()

        normalized: List[Path] = [Path(path) for path in file_paths]
        results: Dict[str, "pd.DataFrame"] = {}

        async def _read_and_store(path: Path) -> None:
            df = await self._read_file(path, start_ms=start_ms, end_ms=end_ms, columns=columns)
            if include_path_column and not df.empty:
                df = df.copy()
                df["SourceFile"] = str(path)
            results[str(path)] = df

        await asyncio.gather(*(_read_and_store(path) for path in normalized))
        return results

    async def _read_period(
        self,
        period: RollPeriod,
        *,
        columns: Optional[Sequence[str]],
        include_metadata: bool,
    ) -> "pd.DataFrame":
        start_bound = _ensure_utc(period.start)
        end_bound = _ensure_utc(period.end)
        start_ms = _timestamp_to_epoch_ms(period.start)
        end_ms = _timestamp_to_epoch_ms(period.end)

        def _load() -> "pd.DataFrame":
            with FastScidReader(str(period.contract.file_path)).open() as reader:
                df = reader.to_pandas(start_ms=start_ms, end_ms=end_ms, columns=columns)
            if include_metadata:
                df = df.copy()
                df["Contract"] = period.contract.contract_id
                df["Ticker"] = period.contract.ticker
                df["RollDate"] = _ensure_utc(period.roll_date)
                df["ContractExpiry"] = _ensure_utc(period.expiry)
                df["SourceFile"] = str(period.contract.file_path)
            return df

        df = await self._run_in_executor(_load)
        if df.empty:
            return df

        mask = (df.index >= start_bound) & (df.index < end_bound)
        return df.loc[mask]

    async def _read_file(
        self,
        path: Path,
        *,
        start_ms: Optional[int],
        end_ms: Optional[int],
        columns: Optional[Sequence[str]],
    ) -> "pd.DataFrame":
        frame_pd = _ensure_pandas()

        def _load() -> "pd.DataFrame":
            if not path.exists():
                self._logger.warning("SCID file not found: %s", path)
                return frame_pd.DataFrame()

            with FastScidReader(str(path)).open() as reader:
                return reader.to_pandas(start_ms=start_ms, end_ms=end_ms, columns=columns)

        return await self._run_in_executor(_load)

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


__all__ = [
    "AsyncFrontMonthScidReader",
]

