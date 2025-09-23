"""
Client module for Sierra Chart real-time data connections.

This module provides DTC (Data and Trading Communications) protocol clients
for connecting to Sierra Chart servers and receiving real-time market data.
"""

from .dtc_client import DTCClient, DTCClientConfig

__all__ = [
    "DTCClient",
    "DTCClientConfig",
]