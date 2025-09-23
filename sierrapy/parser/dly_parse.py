import os
import re
import fnmatch
import time
import threading
import concurrent.futures
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Sequence

import numpy as np
import pandas as pd



# Default location for DLY files


MONTH_CODE_MAP = {
    "F": 1,  # Jan
    "G": 2,  # Feb
    "H": 3,  # Mar

    "J": 4,  # Apr
    "K": 5,  # May
    "M": 6,  # Jun
    "N": 7,  # Jul
    "Q": 8,  # Aug
    "U": 9,  # Sep
    "V": 10, # Oct
    "X": 11, # Nov
    "Z": 12, # Dec
}


@dataclass(frozen=True)
class ContractInfo:
    ticker: str
    month: str
    year: int
    exchange: str

    @property
    def contract_id(self) -> str:
        return f"{self.month}{str(self.year)[-2:]}"


def calculate_contract_expiry(month_code: str, year: int, ticker: Optional[str] = None) -> pd.Timestamp:
    """
    Calculate the actual contract expiry date based on month code and year.

    For most futures contracts, expiry patterns vary by contract type:
    - Energy (CL, NG): Generally 3rd Friday of month before delivery month
    - Financials (ES, NQ): 3rd Friday of expiry month
    - Agricultural: Varies, but typically mid-month

    This uses a conservative approach of the 20th of the month before the
    delivery month for energy contracts, and 20th of expiry month for others.

    Parameters:
    -----------
    month_code : str
        Single letter month code (F, G, H, J, K, M, N, Q, U, V, X, Z)
    year : int
        4-digit year
    ticker : str, optional
        Contract ticker (e.g., 'CL', 'NG') for contract-specific logic

    Returns:
    --------
    pd.Timestamp
        Contract expiry date
    """
    month_num = MONTH_CODE_MAP.get(month_code.upper())
    if month_num is None:
        raise ValueError(f"Invalid month code: {month_code}")

    # Energy contracts (CL, NG, etc.) typically expire the month before delivery
    if ticker and ticker.upper() in ['CL', 'HO', 'RB', 'NG']:
        # Expire in the month before delivery month
        if month_num == 1:  # January delivery -> December expiry of previous year
            expiry_year = year - 1
            expiry_month = 12
        else:
            expiry_year = year
            expiry_month = month_num - 1
    else:
        # Most other contracts expire in the delivery month
        expiry_year = year
        expiry_month = month_num

    # Use the 20th as a reasonable expiry date approximation
    # This avoids weekend issues and is close to typical futures expiry patterns
    return pd.Timestamp(year=expiry_year, month=expiry_month, day=20)


def parse_contract_filename(fn: str, pivot: int = 1970) -> Optional[ContractInfo]:
    """Parse filenames like CLH25-NYM.dly."""
    m = re.match(r"([A-Za-z]+)([FGHJKMNQUVXZ])(\d{2})-([A-Za-z]+)\.dly", fn, re.IGNORECASE)
    if not m:
        return None
    ticker, mcode, yy, exch = m.groups()
    yy = int(yy)
    year = 2000 + yy if yy <= pivot % 100 else 1900 + yy
    return ContractInfo(ticker.upper(), mcode.upper(), year, exch.upper())


class TickerFileManager:
    """
    Manages files for each ticker with format {ticker}{month_code}{2digit-year}-{exchange}.dly
    Identifies the "front" month (closest large file to next calendar month) for current data.
    """

    def __init__(self, folder: str):
        self.folder = Path(folder)
        self._ticker_files: Dict[str, List[Path]] = {}
        self._ticker_contracts: Dict[str, List[ContractInfo]] = {}
        self._discover_files()

    def _discover_files(self) -> None:
        """Discover and categorize all .dly files in the folder."""
        if not self.folder.is_dir():
            return

        for file_path in self.folder.glob("*.dly"):
            info = parse_contract_filename(file_path.name)
            if info is None:
                continue

            ticker = info.ticker
            self._ticker_files.setdefault(ticker, []).append(file_path)
            self._ticker_contracts.setdefault(ticker, []).append(info)

        # Sort files by contract expiry for each ticker
        for ticker in self._ticker_files:
            contracts_with_paths = list(zip(self._ticker_contracts[ticker], self._ticker_files[ticker]))
            contracts_with_paths.sort(key=lambda x: calculate_contract_expiry(x[0].month, x[0].year, ticker))

            self._ticker_contracts[ticker] = [c[0] for c in contracts_with_paths]
            self._ticker_files[ticker] = [c[1] for c in contracts_with_paths]

    def get_tickers(self) -> List[str]:
        """Return list of all available tickers."""
        return list(self._ticker_files.keys())

    def get_files_for_ticker(self, ticker: str) -> List[Path]:
        """Return all files for a specific ticker, sorted by expiry date."""
        return self._ticker_files.get(ticker.upper(), [])

    def get_contracts_for_ticker(self, ticker: str) -> List[ContractInfo]:
        """Return all contract info for a specific ticker, sorted by expiry date."""
        return self._ticker_contracts.get(ticker.upper(), [])

    def get_front_month_file(self, ticker: str, reference_date: Optional[pd.Timestamp] = None) -> Optional[Path]:
        """
        Get the "front" month file (closest large file to next calendar month).

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        reference_date : pd.Timestamp, optional
            Reference date for determining front month. Defaults to current date.

        Returns:
        --------
        Path or None
            Path to front month file, or None if no suitable file found
        """
        if reference_date is None:
            reference_date = pd.Timestamp.now()

        contracts = self.get_contracts_for_ticker(ticker)
        files = self.get_files_for_ticker(ticker)

        if not contracts:
            return None

        # Find contracts that haven't expired yet
        active_contracts = []
        for i, contract in enumerate(contracts):
            expiry = calculate_contract_expiry(contract.month, contract.year, ticker)
            if expiry >= reference_date:
                file_size = files[i].stat().st_size if files[i].exists() else 0
                active_contracts.append((contract, files[i], file_size, expiry))

        if not active_contracts:
            return None

        # Sort by expiry date, then by file size (largest first for same expiry)
        active_contracts.sort(key=lambda x: (x[3], -x[2]))

        # Return the file with the earliest expiry (and largest size if tied)
        return active_contracts[0][1]

    def get_front_month_data(self, ticker: str, reference_date: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
        """
        Load data from the front month file for a ticker.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        reference_date : pd.Timestamp, optional
            Reference date for determining front month

        Returns:
        --------
        pd.DataFrame or None
            Front month data, or None if no suitable file found
        """
        front_file = self.get_front_month_file(ticker, reference_date)
        if front_file is None:
            return None

        return _read_dly(str(front_file))

    def get_continuous_series(self, ticker: str, start_date: Optional[pd.Timestamp] = None,
                            end_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        Create a continuous price series by rolling between contracts.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        start_date : pd.Timestamp, optional
            Start date for the series
        end_date : pd.Timestamp, optional
            End date for the series

        Returns:
        --------
        pd.DataFrame
            Continuous price series
        """
        contracts = self.get_contracts_for_ticker(ticker)
        files = self.get_files_for_ticker(ticker)

        if not contracts:
            return pd.DataFrame()

        all_data = []
        for i, (contract, file_path) in enumerate(zip(contracts, files)):
            if not file_path.exists():
                continue

            df = _read_dly(str(file_path))
            if df.empty:
                continue

            # Add contract metadata
            df = df.copy()
            df['ticker'] = ticker
            df['contract'] = f"{contract.month}{contract.year}"
            df['expiry'] = calculate_contract_expiry(contract.month, contract.year, ticker)

            all_data.append(df)

        if not all_data:
            return pd.DataFrame()

        # Combine all data
        combined = pd.concat(all_data, axis=0).sort_index()

        # Filter by date range if specified
        if start_date is not None:
            combined = combined[combined.index >= start_date]
        if end_date is not None:
            combined = combined[combined.index <= end_date]

        return combined


def discover_tickers(folder: str) -> Dict[str, List[str]]:
    """
    Return mapping: ticker -> [file paths] for all .dly files that parse.

    DEPRECATED: Use TickerFileManager instead for better functionality.
    """
    out: Dict[str, List[str]] = {}
    folder_str = str(folder)  # Ensure we have a string path

    if not os.path.isdir(folder_str):
        return out

    for fn in os.listdir(folder_str):
        if not fn.endswith(".dly"):
            continue
        info = parse_contract_filename(fn)
        if info is None:
            continue
        out.setdefault(info.ticker, []).append(os.path.join(folder_str, fn))

    for t, files in out.items():
        files.sort()
    return out


def _read_dly(path: str) -> pd.DataFrame:
    """Read a .dly file with columns date, close and optionally volume/OI."""
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path, delim_whitespace=True, header=None,
                         names=["date", "close", "volume", "open_interest"],
                         parse_dates=[0])

    # Strip spaces from column names first
    df.columns = [str(c).strip() for c in df.columns]

    # Find date column with case-insensitive search
    date_col = None
    for col in df.columns:
        if str(col).lower().strip() in ['date', 'datetime', 'timestamp']:
            date_col = col
            break

    if date_col is None:
        # If no date column found, assume first column is date
        date_col = df.columns[0]

    # Ensure we have the expected column name
    if date_col != 'date':
        df = df.rename(columns={date_col: 'date'})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date").sort_index()
    return df

