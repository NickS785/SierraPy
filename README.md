# SierraPy

A Python library for Sierra Chart data parsing and analysis, providing efficient tools for reading and processing financial market data.

## Features

- **Fast SCID Reading**: Memory-mapped I/O for high-performance reading of Sierra Chart intraday (.scid) files
- **DLY File Support**: Parse Sierra Chart daily (.dly) files with automatic format detection
- **Ticker Management**: Smart file management with front month identification for futures contracts
- **Data Export**: Export to CSV and Parquet formats with optimized performance
- **Real-time Data**: DTC client for connecting to Sierra Chart servers (optional)

## Installation

### From Source

```bash
git clone https://github.com/NickS785/sierrapy.git
cd sierrapy
pip install -e .
```

### Dependencies

- Python >= 3.8
- numpy >= 1.20.0
- pandas >= 1.3.0
- pyarrow >= 5.0.0 (optional, for Parquet export)

## Quick Start

### Reading SCID Files

```python
import sierrapy

# Open a SCID file with automatic format detection
reader = sierrapy.FastScidReader("/path/to/ESU25-CME.scid")

with reader.open() as r:
    # Get basic info
    print(f"Records: {len(r)}")
    print(f"Columns: {r.columns()}")

    # Read as pandas DataFrame
    df = r.to_pandas()
    print(df.head())

    # Read specific time range (timestamps in epoch milliseconds)
    start_ms = 1726000000000  # 2024-09-10 22:40:00 UTC
    end_ms = 1726086400000    # 2024-09-12 00:00:00 UTC
    df_filtered = r.to_pandas(start_ms=start_ms, end_ms=end_ms)
```

### Managing Multiple SCID Files by Ticker

```python
# Manage all SCID files in a directory
manager = sierrapy.ScidTickerFileManager("/path/to/scid/files")

# Get available tickers
tickers = manager.get_tickers()
print(f"Available tickers: {tickers}")

# Get front month data for ES
front_data = manager.get_front_month_data("ES")
print(front_data.head())

# Get file statistics
stats = manager.get_file_statistics("ES")
print(f"ES files: {stats['file_count']}, Total size: {stats['total_size_mb']:.1f} MB")
```

### Reading DLY Files

```python
# Manage daily files
dly_manager = sierrapy.TickerFileManager("/path/to/dly/files")

# Get front month daily data
daily_data = dly_manager.get_front_month_data("CL")
print(daily_data.head())

# Get continuous series across all contracts
continuous = dly_manager.get_continuous_series("CL",
                                             start_date=pd.Timestamp("2024-01-01"),
                                             end_date=pd.Timestamp("2024-12-31"))
```

### Data Export

```python
# Export SCID data to CSV
with sierrapy.FastScidReader("/path/to/data.scid").open() as reader:
    reader.export_csv("/path/to/output.csv")

# Export to Parquet with compression
    stats = reader.export_to_parquet_optimized(
        "/path/to/output.parquet",
        compression="zstd",
        chunk_records=2_000_000
    )
    print(f"Compression ratio: {stats['compression_ratio']:.2f}")
```

### Front Month Logic

The "front month" is determined as the closest contract to expiry that:
1. Has not yet expired relative to the reference date
2. Has the largest file size (most data) if multiple contracts have the same expiry

This logic works well for:
- **Energy contracts** (CL, NG, etc.): Expire month before delivery
- **Financial contracts** (ES, NQ, etc.): Expire in delivery month
- **Other contracts**: Default to delivery month expiry

### Asynchronous Front-Month Reader

For workflows that require stitching contracts over long horizons, the
`AsyncFrontMonthScidReader` orchestrates roll schedules (rolling one month before
expiry) and loads multiple `.scid` files concurrently:

```python
import asyncio
import sierrapy

async def load_continuous_series():
    reader = sierrapy.AsyncFrontMonthScidReader("/path/to/scid/folder")

    # Build a roll schedule (one month before expiry) and load front-month data
    df = await reader.load_front_month_series(
        "CL",
        start="2024-01-01",
        end="2024-12-31",
        volume_per_bar=50_000,      # bucket trades into ~50k volume bars while loading
        resample_rule="15T",        # optionally resample the stitched series to 15-minute bars
    )

    # Load multiple raw files concurrently
    raw = await reader.load_scid_files([
        "/path/to/CLU24-NYM.scid",
        "/path/to/CLZ24-NYM.scid",
    ], volume_per_bar=25_000)

    return df, raw

continuous_df, raw_files = asyncio.run(load_continuous_series())
print(continuous_df.head())
```

## File Format Support

### SCID Files
- **V10_40B**: Modern format with int64 microseconds (40-byte records)
- **V8_44B**: Legacy format with float64 days (40-byte records)
- Automatic format detection
- Header support (typically 56 bytes)

### DLY Files
- CSV format with automatic delimiter detection
- Flexible column naming (date, datetime, timestamp)
- Support for OHLC + volume/open interest data

## CLI Tools

### SCID File Inspector

```bash
# Get file info
sierrapy-scid /path/to/file.scid --info

# Export to CSV with time filter
sierrapy-scid /path/to/file.scid --export output.csv --start 2024-09-10T00:00:00Z --end 2024-09-11T00:00:00Z
```

## Performance

- **Memory-mapped I/O**: O(1) open time for multi-GB files
- **Zero-copy reads**: Minimal memory allocation for large datasets
- **Vectorized operations**: NumPy-based processing for speed
- **Chunked processing**: Handle files larger than available RAM
- **Binary search**: Fast time-based filtering
- **In-flight aggregation**: Bucket to volume bars or resample to new timeframes while reading

## Examples

See the `/examples` directory for more detailed usage examples:

- `basic_scid_reading.py` - Simple SCID file reading
- `ticker_management.py` - Working with multiple contracts
- `data_export.py` - Export workflows
- `performance_comparison.py` - Performance benchmarks

## Development

### Setup Development Environment

```bash
git clone https://github.com/yourusername/sierrapy.git
cd sierrapy
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/ tests/
flake8 src/ tests/
mypy src/
```

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Changelog

### v0.1.0 (Initial Release)
- FastScidReader with memory-mapped I/O
- TickerFileManager for DLY files
- ScidTickerFileManager for SCID files
- Front month identification logic
- CSV and Parquet export capabilities
- Basic DTC client support
