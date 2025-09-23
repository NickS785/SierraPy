"""
Parser module for Sierra Chart data files.

This module provides parsers for both SCID (intraday) and DLY (daily) file formats.
"""

from .scid_parse import (
    FastScidReader,
    ScidTickerFileManager,
    ScidContractInfo,
    Schema,
)
from .dly_parse import (
    TickerFileManager,
    ContractInfo,
    calculate_contract_expiry,
    parse_contract_filename,
)

__all__ = [
    # SCID parsing
    "FastScidReader",
    "ScidTickerFileManager",
    "ScidContractInfo",
    "Schema",

    # DLY parsing
    "TickerFileManager",
    "ContractInfo",
    "calculate_contract_expiry",
    "parse_contract_filename",
]