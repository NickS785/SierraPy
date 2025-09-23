#!/usr/bin/env python3
"""
Basic usage examples for SierraPy package.

This example demonstrates how to use the main features of SierraPy for
reading Sierra Chart data files and managing ticker files.
"""

import sierrapy
import pandas as pd
from pathlib import Path


def scid_example():
    """Example of reading SCID files."""
    print("=== SCID File Reading Example ===")

    # Replace with your actual SCID file path
    scid_path = "/path/to/your/ESU25-CME.scid"

    if not Path(scid_path).exists():
        print(f"SCID file not found: {scid_path}")
        print("Please update the path to point to an actual SCID file.")
        return

    # Open and read SCID file
    reader = sierrapy.FastScidReader(scid_path)

    with reader.open() as r:
        print(f"File: {scid_path}")
        print(f"Format: {r.schema.variant}")
        print(f"Records: {len(r):,}")
        print(f"Columns: {r.columns()}")

        # Read first 1000 records as DataFrame
        df = r.to_pandas()
        if len(df) > 1000:
            df = df.head(1000)

        print(f"\nFirst few records:")
        print(df.head())

        print(f"\nData info:")
        print(df.info())


def scid_ticker_management_example():
    """Example of managing multiple SCID files by ticker."""
    print("\n=== SCID Ticker Management Example ===")

    # Replace with your SCID files directory
    scid_dir = "/path/to/your/scid/files"

    if not Path(scid_dir).exists():
        print(f"SCID directory not found: {scid_dir}")
        print("Please update the path to point to a directory with SCID files.")
        return

    # Create ticker manager
    manager = sierrapy.ScidTickerFileManager(scid_dir)

    # Get available tickers
    tickers = manager.get_tickers()
    print(f"Available tickers: {tickers}")

    if not tickers:
        print("No tickers found in directory")
        return

    # Work with first ticker
    ticker = tickers[0]
    print(f"\nWorking with ticker: {ticker}")

    # Get file statistics
    stats = manager.get_file_statistics(ticker)
    print(f"Files: {stats['file_count']}")
    print(f"Total size: {stats['total_size_mb']:.1f} MB")
    print(f"Contracts: {stats['contracts']}")

    # Get front month file
    front_file = manager.get_front_month_file(ticker)
    if front_file:
        print(f"Front month file: {front_file.name}")

        # Load front month data
        df = manager.get_front_month_data(ticker)
        if df is not None and not df.empty:
            print(f"Front month data shape: {df.shape}")
            print(df.head())


def dly_ticker_management_example():
    """Example of managing DLY files by ticker."""
    print("\n=== DLY Ticker Management Example ===")

    # Replace with your DLY files directory
    dly_dir = "/path/to/your/dly/files"

    if not Path(dly_dir).exists():
        print(f"DLY directory not found: {dly_dir}")
        print("Please update the path to point to a directory with DLY files.")
        return

    # Create ticker manager
    manager = sierrapy.TickerFileManager(dly_dir)

    # Get available tickers
    tickers = manager.get_tickers()
    print(f"Available tickers: {tickers}")

    if not tickers:
        print("No tickers found in directory")
        return

    # Work with first ticker
    ticker = tickers[0]
    print(f"\nWorking with ticker: {ticker}")

    # Get all contracts for this ticker
    contracts = manager.get_contracts_for_ticker(ticker)
    print(f"Available contracts: {[c.contract_id for c in contracts]}")

    # Get front month data
    front_data = manager.get_front_month_data(ticker)
    if front_data is not None and not front_data.empty:
        print(f"Front month data shape: {front_data.shape}")
        print(front_data.head())

        # Get continuous series
        print(f"\nGetting continuous series for {ticker}...")
        continuous = manager.get_continuous_series(
            ticker,
            start_date=pd.Timestamp("2024-01-01"),
            end_date=pd.Timestamp("2024-12-31")
        )

        if not continuous.empty:
            print(f"Continuous series shape: {continuous.shape}")
            print(f"Date range: {continuous.index.min()} to {continuous.index.max()}")
            print(continuous.head())


def export_example():
    """Example of exporting data."""
    print("\n=== Data Export Example ===")

    # Replace with your actual SCID file path
    scid_path = "/path/to/your/ESU25-CME.scid"

    if not Path(scid_path).exists():
        print(f"SCID file not found: {scid_path}")
        print("Please update the path to point to an actual SCID file.")
        return

    reader = sierrapy.FastScidReader(scid_path)

    with reader.open() as r:
        # Export to CSV
        csv_path = "example_export.csv"
        print(f"Exporting to CSV: {csv_path}")
        r.export_csv(csv_path, chunk_records=100_000)  # Use smaller chunks for demo

        if Path(csv_path).exists():
            print(f"CSV export successful: {Path(csv_path).stat().st_size / 1024:.1f} KB")

        # Export to Parquet (if pyarrow is available)
        try:
            parquet_path = "example_export.parquet"
            print(f"Exporting to Parquet: {parquet_path}")
            stats = r.export_to_parquet_optimized(
                parquet_path,
                compression="snappy",
                chunk_records=100_000
            )

            print(f"Parquet export stats:")
            print(f"  Records: {stats['total_records']:,}")
            print(f"  Output size: {stats['output_size_mb']:.1f} MB")
            print(f"  Compression ratio: {stats['compression_ratio']:.2f}")
            print(f"  Export time: {stats['export_time']:.2f} seconds")

        except ImportError:
            print("PyArrow not available - skipping Parquet export")
        except Exception as e:
            print(f"Parquet export failed: {e}")


def main():
    """Run all examples."""
    print("SierraPy Basic Usage Examples")
    print("=" * 40)

    # Update these paths to point to your actual data files
    print("NOTE: Please update the file paths in this script to point to your actual data files.")
    print()

    try:
        scid_example()
        scid_ticker_management_example()
        dly_ticker_management_example()
        export_example()

    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure to update the file paths to point to your actual data files.")


if __name__ == "__main__":
    main()