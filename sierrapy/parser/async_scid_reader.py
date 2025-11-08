"""Asynchronous helpers for working with Sierra Chart SCID files."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

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


_MONTH_CODE: Dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}


_CONTRACT_PATTERN = re.compile(
    r"(?P<root>[A-Za-z]+)(?P<month>[FGHJKMNQUVXZ])(?P<year>\d{4}|\d{2})",
    re.IGNORECASE,
)


def _parse_contract_from_sourcefile(path: str) -> Optional[str]:
    if not path:
        return None

    name = Path(path).name
    match = _CONTRACT_PATTERN.search(name)
    if not match:
        return None

    root = match.group("root").upper()
    month_code = match.group("month").upper()
    year_token = match.group("year")

    try:
        year_value = int(year_token)
    except (TypeError, ValueError):
        return None

    if len(year_token) == 2:
        year_value = year_value + 2000 if year_value < 70 else year_value + 1900

    return f"{root}{month_code}{year_value}"


def _contract_sort_key(contract: str) -> Tuple[int, int]:
    if not contract:
        return (0, 0)

    match = _CONTRACT_PATTERN.match(contract.strip())
    if not match:
        return (0, 0)

    month_code = match.group("month").upper()
    year_token = match.group("year")

    try:
        year_value = int(year_token)
    except (TypeError, ValueError):
        return (0, _MONTH_CODE.get(month_code, 0))

    if len(year_token) == 2:
        year_value = year_value + 2000 if year_value < 70 else year_value + 1900

    return (year_value, _MONTH_CODE.get(month_code, 0))


def _limit_to_active_window(
    df: "pd.DataFrame",
    start_ts: Optional["pd.Timestamp"],
    end_ts: Optional["pd.Timestamp"],
) -> "pd.DataFrame":
    if df.empty:
        result = df.copy()
        result.index.name = "DateTime"
        return result

    result = df
    if start_ts is not None:
        result = result.loc[result.index >= _ensure_utc(start_ts)]
    if end_ts is not None:
        result = result.loc[result.index <= _ensure_utc(end_ts)]

    result = result.copy()
    result.index.name = "DateTime"
    return result


def _stitch_with_tail(
    parts: Dict[str, "pd.DataFrame"],
    schedule: List[Tuple[str, "pd.Timestamp", "pd.Timestamp"]],
    *,
    allow_tail: bool = True,
) -> "pd.DataFrame":
    frame_pd = _ensure_pandas()
    logger = logging.getLogger(__name__)

    if not schedule:
        return frame_pd.DataFrame()

    all_contracts = {contract for contract, _, _ in schedule} | set(parts.keys())
    normalized_parts: Dict[str, "pd.DataFrame"] = {}

    for contract in all_contracts:
        df = parts.get(contract)
        if df is None:
            normalized = frame_pd.DataFrame()
        else:
            normalized = df.copy()

        if not normalized.empty:
            if not isinstance(normalized.index, frame_pd.DatetimeIndex):
                if "DateTime" in normalized.columns:
                    idx = frame_pd.to_datetime(
                        normalized["DateTime"], errors="coerce", utc=True
                    )
                    normalized = normalized.copy()
                    normalized.index = idx
                else:
                    idx = frame_pd.to_datetime(normalized.index, errors="coerce", utc=True)
                    normalized.index = idx
            else:
                normalized.index = frame_pd.to_datetime(
                    normalized.index, errors="coerce", utc=True
                )

            normalized = normalized.loc[~normalized.index.isna()]
            normalized.sort_index(inplace=True)
        else:
            normalized = normalized.copy()

        normalized.index.name = "DateTime"

        contract_value: Optional[str] = None
        if "contract" in normalized.columns:
            candidates = normalized["contract"].dropna().astype(str)
            if not candidates.empty:
                contract_value = candidates.iloc[0]
        if contract_value is None and "Contract" in normalized.columns:
            candidates = normalized["Contract"].dropna().astype(str)
            if not candidates.empty:
                contract_value = candidates.iloc[0]
        if contract_value is None and "SourceFile" in normalized.columns:
            for path in normalized["SourceFile"].dropna().astype(str):
                parsed = _parse_contract_from_sourcefile(path)
                if parsed:
                    contract_value = parsed
                    break
        if contract_value is None:
            contract_value = contract

        if "SourceFile" not in normalized.columns:
            normalized["SourceFile"] = ""
        else:
            normalized["SourceFile"] = normalized["SourceFile"].fillna("")

        normalized["contract"] = contract_value

        normalized_parts[contract] = normalized

    tail_priority = sorted(normalized_parts.keys(), key=_contract_sort_key, reverse=True)
    priority_map = {name: idx + 1 for idx, name in enumerate(tail_priority)}

    stitched_segments: List["pd.DataFrame"] = []

    for contract, start_ts, end_ts in schedule:
        start_bound = _ensure_utc(start_ts)
        end_bound = _ensure_utc(end_ts)

        base_df = normalized_parts.get(contract, frame_pd.DataFrame())
        window_frames: List["pd.DataFrame"] = []
        base_last: Optional["pd.Timestamp"] = None

        if not base_df.empty:
            base_slice = _limit_to_active_window(base_df, start_bound, end_bound)
            if not base_slice.empty:
                base_last = base_slice.index.max()
                if base_last is not None and base_last > end_bound:
                    base_last = end_bound
                base_slice = _limit_to_active_window(base_slice, start_bound, base_last)
                base_slice = base_slice.copy()
                base_slice["_stitch_priority"] = 0
                if base_last is not None and "ContractExpiry" in base_slice.columns:
                    base_slice["ContractExpiry"] = _ensure_utc(base_last)
                window_frames.append(base_slice)

        cursor = (
            base_last
            if base_last is not None
            else start_bound - frame_pd.Timedelta(nanoseconds=1)
        )

        if allow_tail and (base_last is None or base_last < end_bound):
            for tail_contract in tail_priority:
                if tail_contract == contract:
                    continue

                tail_df = normalized_parts.get(tail_contract)
                if tail_df is None or tail_df.empty:
                    continue

                tail_slice = _limit_to_active_window(tail_df, start_bound, end_bound)
                if cursor is not None:
                    tail_slice = tail_slice.loc[tail_slice.index > cursor]

                if tail_slice.empty:
                    continue

                tail_slice = tail_slice.copy()
                tail_slice["_stitch_priority"] = priority_map.get(tail_contract, len(priority_map) + 1)

                tail_end = tail_slice.index.max()
                if tail_end is not None and "ContractExpiry" in tail_slice.columns:
                    effective_tail_end = tail_end if tail_end <= end_bound else end_bound
                    tail_slice["ContractExpiry"] = _ensure_utc(effective_tail_end)

                logger.debug(
                    "Tail stitching %s into %s window [%s, %s]",
                    tail_contract,
                    contract,
                    start_bound,
                    tail_slice.index.max(),
                )
                window_frames.append(tail_slice)
                cursor = tail_slice.index.max()
                if cursor is not None and cursor >= end_bound:
                    break

        if not window_frames:
            continue

        window_df = frame_pd.concat(window_frames, axis=0, sort=False)
        if window_df.empty:
            continue

        window_df = window_df.copy()
        window_df["_DateTime"] = window_df.index
        window_df.sort_values(
            by=["_DateTime", "_stitch_priority", "contract"],
            inplace=True,
            kind="stable",
        )
        window_df = window_df.loc[~window_df["_DateTime"].duplicated(keep="first")]
        window_df = window_df.drop(columns=["_stitch_priority"], errors="ignore")
        window_df = window_df.set_index("_DateTime")
        window_df.index.name = "DateTime"
        stitched_segments.append(window_df)

    if not stitched_segments:
        return frame_pd.DataFrame()

    combined = frame_pd.concat(stitched_segments, axis=0, sort=False)
    if combined.empty:
        combined.index.name = "DateTime"
        return combined

    combined.sort_index(inplace=True)
    combined = combined.loc[~combined.index.duplicated(keep="first")]
    combined.index.name = "DateTime"

    if "contract" in combined.columns:
        combined["contract"] = combined["contract"].astype(str)
    if "Contract" in combined.columns:
        combined["Contract"] = combined["Contract"].astype(str)

    return combined


class AsyncScidReader:
    """Asynchronously load front-month SCID data across contract rolls."""

    def __init__(
        self,
        directory: Union[str, Path],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_concurrency: Optional[int] = None,
        default_service: str = "sierra",
    ) -> None:
        self.reader = FastScidReader
        self._directory = Path(directory)
        self._manager_cache: Dict[str, ScidTickerFileManager] = {}
        self._default_service = default_service.lower()
        self._manager = self._get_manager(self._default_service)
        self._loop = loop
        self._semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(max_concurrency) if max_concurrency else None
        )
        self._logger = logging.getLogger(__name__)

    @property
    def manager(self) -> ScidTickerFileManager:
        return self._manager

    def _get_manager(self, service: str) -> ScidTickerFileManager:
        key = service.lower()
        try:
            return self._manager_cache[key]
        except KeyError:
            manager = ScidTickerFileManager(str(self._directory), service=key)
            self._manager_cache[key] = manager
            return manager

    def generate_roll_schedule(
        self,
        ticker: str,
        *,
        service: Optional[str] = None,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
    ) -> List[RollPeriod]:
        start_ts = _coerce_timestamp(start)
        end_ts = _coerce_timestamp(end)
        manager = self._get_manager(service or self._default_service)
        return manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
            roll_convention=_normalize_roll_convention(roll_convention),
        )

    async def load_front_month_continuous(
        self,
        ticker: str,
        *,
        service: str = "sierra",
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
        preflight_peek: bool = False,
        allow_tail: bool = True,

    ) -> "pd.DataFrame":
        """Load a continuous front-month series with optional tail stitching.

        Parameters
        ----------
        ticker:
            The futures ticker symbol to load.
        service:
            Service name that determines filename conventions.
        start, end:
            Optional UTC boundaries to constrain the schedule and output.
        columns:
            Columns to request from :class:`FastScidReader`.
        roll_offset, roll_convention:
            Arguments forwarded to :meth:`ScidTickerFileManager.generate_roll_schedule`.
        include_metadata:
            When ``True`` (default) attach contract metadata columns.
        volume_per_bar, volume_column, resample_rule, resample_kwargs:
            Additional parameters forwarded to :meth:`FastScidReader.to_pandas`.
        drop_invalid_rows:
            Drop rows flagged as invalid by the reader when ``True``.
        preflight_peek:
            Skip contracts with no data when ``True`` by peeking at file ranges.
        allow_tail:
            When ``True`` (default), fill gaps at the end of a contract's scheduled
            window using the latest subsequent contract with available data. Tail
            stitching also caps the effective expiry of each contract at its final
            observed bar.
        """
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

        manager = self._get_manager(service)

        periods = manager.generate_roll_schedule(
            ticker,
            start=start_ts,
            end=end_ts,
            roll_offset=roll_offset,
            roll_convention=_normalize_roll_convention(roll_convention),
        )

        if not periods:
            return frame_pd.DataFrame()

        if preflight_peek:
            filtered_periods: List[RollPeriod] = []
            for period in periods:
                try:
                    with FastScidReader(str(period.contract.file_path), read_only=True).open() as reader:
                        try:
                            count, first_ms, last_ms = reader.peek_range()
                        except AttributeError:
                            times = reader.times_epoch_ms()
                            count = len(times)
                            if count == 0:
                                first_ms = last_ms = None
                            else:
                                first_ms = int(times[0])
                                last_ms = int(times[-1])
                except Exception:
                    filtered_periods.append(period)
                    continue

                if count == 0 or first_ms is None or last_ms is None:
                    continue

                period_start_ms = _timestamp_to_epoch_ms(period.start)
                period_end_ms_exclusive = _timestamp_to_epoch_ms(period.end)

                if period_end_ms_exclusive is not None and first_ms >= period_end_ms_exclusive:
                    continue
                if period_start_ms is not None and last_ms < period_start_ms:
                    continue

                filtered_periods.append(period)
            periods = filtered_periods

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

        inclusive_offset = frame_pd.Timedelta(nanoseconds=1)
        schedule: List[Tuple[str, "pd.Timestamp", "pd.Timestamp"]] = []
        contract_frames: Dict[str, "pd.DataFrame"] = {}

        for period, df in zip(periods, frames):
            contract_label = (
                f"{period.contract.ticker.upper()}"
                f"{period.contract.month.upper()}"
                f"{period.contract.year}"
            )
            window_start = _ensure_utc(period.start)
            window_end_exclusive = _ensure_utc(period.end)

            if frame_pd.isna(window_end_exclusive) or window_end_exclusive <= window_start:
                window_end = window_start
            else:
                window_end = window_end_exclusive - inclusive_offset
                if window_end < window_start:
                    window_end = window_start

            schedule.append((contract_label, window_start, window_end))

            current_df = df if df is not None else frame_pd.DataFrame()
            if "SourceFile" not in current_df.columns:
                current_df = current_df.copy()
                current_df["SourceFile"] = str(period.contract.file_path) if not current_df.empty else ""

            current_df = current_df.copy()
            current_df["contract"] = contract_label

            existing = contract_frames.get(contract_label)
            if existing is not None and not existing.empty:
                current_df = frame_pd.concat([existing, current_df], axis=0, sort=False)

            contract_frames[contract_label] = current_df

        combined = _stitch_with_tail(contract_frames, schedule, allow_tail=allow_tail)

        if combined.empty:
            return combined

        if start_ts is not None:
            combined = combined.loc[combined.index >= _ensure_utc(start_ts)]
        if end_ts is not None:
            combined = combined.loc[combined.index < _ensure_utc(end_ts)]

        combined.index.name = "DateTime"
        return combined

    async def load_front_month_series(
        self,
        ticker: str,
        *,
        service: str = "sierra",
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
        preflight_peek: bool = False,
        allow_tail: bool = True,
    ) -> "pd.DataFrame":
        """Backward compatible alias for :meth:`load_front_month_continuous`."""

        return await self.load_front_month_continuous(
            ticker,
            service=service,
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
            preflight_peek=preflight_peek,
            allow_tail=allow_tail,
        )

    def enumerate_front_month_contracts(
        self,
        ticker: str,
        *,
        service: str = "sierra",
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
    ) -> List[Dict[str, Any]]:
        """Return metadata describing the generated front-month schedule."""

        periods = self.generate_roll_schedule(
            ticker,
            service=service,
            start=start,
            end=end,
            roll_offset=roll_offset,
            roll_convention=roll_convention,
        )

        results: List[Dict[str, Any]] = []
        for period in periods:
            results.append(
                {
                    "contract": period.contract.contract_id,
                    "file": str(period.contract.file_path),
                    "start": _ensure_utc(period.start),
                    "end": _ensure_utc(period.end),
                    "roll_date": _ensure_utc(period.roll_date),
                    "expiry": _ensure_utc(period.expiry),
                }
            )

        return results

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
        default_service: str = "sierra",
    ) -> None:
        self._async_reader = AsyncScidReader(
            directory,
            max_concurrency=max_concurrency,
            default_service=default_service,
        )


    @property
    def manager(self) -> ScidTickerFileManager:
        return self._async_reader.manager

    def generate_roll_schedule(
        self,
        ticker: str,
        *,
        service: Optional[str] = None,
        start: Optional[Union["pd.Timestamp", datetime, str]] = None,
        end: Optional[Union["pd.Timestamp", datetime, str]] = None,
        roll_offset: Optional["pd.DateOffset"] = None,
        roll_convention: Union[RollConvention, str, None] = None,
    ) -> List[RollPeriod]:
        return self._async_reader.generate_roll_schedule(
            ticker,
            service=service,
            start=start,
            end=end,
            roll_offset=roll_offset,
            roll_convention=roll_convention,
        )

    def load_front_month_continuous(
        self,
        ticker: str,
        *,
        service: str = "sierra",
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
        preflight_peek: bool = False,
    ) -> "pd.DataFrame":
        """Load front-month series synchronously (handles asyncio internally)."""
        return asyncio.run(
            self._async_reader.load_front_month_continuous(
                ticker,
                service=service,
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
                preflight_peek=preflight_peek,
            )
        )

    def load_front_month_series(
        self,
        ticker: str,
        *,
        service: str = "sierra",
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
        preflight_peek: bool = False,
    ) -> "pd.DataFrame":
        """Backward compatible alias for :meth:`load_front_month_continuous`."""

        return asyncio.run(
            self._async_reader.load_front_month_series(
                ticker,
                service=service,
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
                preflight_peek=preflight_peek,
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

