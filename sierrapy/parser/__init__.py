"""
Parser module for Sierra Chart data files.

This module provides parsers for both SCID (intraday) and DLY (daily) file formats.
"""

from .scid_parse import (
    FastScidReader,
    RollPeriod,
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
from .async_scid_reader import AsyncScidReader, ScidReader

resample_logic = {
                "Open":"first",
                "High":"max",
                "Low":"min",
                "Close":"last",
                "Volume":"sum",
                "NumberOfTrades":"sum",
                "BidVolume":"sum",
                "AskVolume":"sum"
                }
__all__ = [
    # SCID parsing
    "FastScidReader",
    "AsyncScidReader",
    "ScidReader",
    "ScidTickerFileManager",
    "ScidContractInfo",
    "RollPeriod",
    "Schema",

    # DLY parsing
    "TickerFileManager",
    "ContractInfo",
    "calculate_contract_expiry",
    "parse_contract_filename",

    #Utilities
    "resample_logic"
]