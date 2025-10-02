"""
SierraPy - A Python library for Sierra Chart data parsing and analysis.

This package provides efficient tools for reading and analyzing Sierra Chart data files:
- Fast SCID (intraday) file reading with memory-mapped I/O
- DLY (daily) file parsing and management
- Ticker file management with front month identification
- Data export capabilities (CSV, Parquet)
- DTC client for real-time data
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Import main classes and functions for easy access
from .client.dtc_client import DTCClient, DTCClientConfig
from .parser.scid_parse import (
    FastScidReader,
    RollPeriod,
    ScidTickerFileManager,
    ScidContractInfo,
    Schema,
)
from .parser.async_scid_reader import AsyncScidReader
from .parser.dly_parse import (
    TickerFileManager,
    ContractInfo,
    calculate_contract_expiry,
    parse_contract_filename,
)

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",

    # DTC Client
    "DTCClient",
    "DTCClientConfig",

    # DLY parsing
    "TickerFileManager",
    "ContractInfo",
    "calculate_contract_expiry",
    "parse_contract_filename",

    # SCID parsing
    "FastScidReader",
    "AsyncScidReader",
    "ScidTickerFileManager",
    "ScidContractInfo",
    "RollPeriod",
    "Schema",
]
