# MIDAS Processor

A high-performance Rust tool for converting UK Met Office MIDAS weather datasets from BADC-CSV format to optimized Parquet files for efficient analysis.

## Overview

MIDAS Processor is part of a climate research toolkit designed to process historical UK weather data from the [CEDA Archive](https://catalogue.ceda.ac.uk/). It transforms the original BADC-CSV format into modern, optimized Parquet files with significant performance improvements for analytical workloads.

### The MIDAS Ecosystem

This tool works as part of a complete climate data processing pipeline:

1. **[midas-fetcher](https://github.com/rjl-climate/midas_fetcher)**: Downloads MIDAS datasets from CEDA
2. **midas-processor** (this tool): Converts BADC-CSV to optimized Parquet
3. **Analysis tools**: Python/R analysis of the resulting Parquet files

## What is MIDAS?

MIDAS (Met Office Integrated Data Archive System) contains historical weather observations from 1000+ UK land-based weather stations, spanning from the late 19th century to present day. The datasets include:

- **Daily rainfall observations** (161k+ files, ~5GB)
- **Daily temperature observations** (41k+ files, ~2GB)
- **Wind observations** (12k+ files, ~9GB)
- **Solar radiation observations** (3k+ files, ~2GB)

## Key Features

### 🚀 **Performance Optimizations**
- **Large row groups** (500K rows)
- **Smart compression** (Snappy, ZSTD, LZ4 options)
- **Column statistics** for query pruning
- **Memory-efficient streaming** for large datasets

### 🔍 **Intelligent Processing**
- **Automatic dataset discovery** from midas-fetcher cache
- **Schema detection** and validation

### 💻 **User-Friendly Interface**
- **Interactive dataset selection** when run without arguments
- **Simple command-line interface** with sensible defaults
- **Comprehensive progress reporting** with file counts and timing
- **Verbose mode** for debugging and optimization insights

## Installation

### Prerequisites
- Rust 1.70+ (uses Rust 2024 edition features)
- 8GB+ RAM recommended for large datasets

### From Source
```bash
git clone https://github.com/your-org/midas-processor
cd midas-processor
cargo install --path .
```

### From Crates.io
```bash
cargo install midas-processor
```

## Usage

### Quick Start

1. **Download datasets** using [midas-fetcher](https://github.com/rjl-climate/midas_fetcher)
2. **Convert with auto-discovery**:
   ```bash
   midas-processor
   ```
   This will show available datasets and let you select one interactively.

### Common Usage Patterns

```bash
# Interactive dataset selection
midas-processor

# Process specific dataset
midas-processor /path/to/uk-daily-rain-obs-202407

# Custom output location
midas-processor --output-path ./analysis/rain_data.parquet

# High compression for archival
midas-processor --compression zstd

# Schema analysis only (no conversion)
midas-processor --discovery-only --verbose

# Combine options
midas-processor /path/to/dataset --compression lz4 --verbose
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `DATASET_PATH` | Path to MIDAS dataset directory (optional) | Auto-discover |
| `--output-path` | Custom output location | `../parquet/{dataset}.parquet` |
| `--compression` | Compression algorithm (snappy/zstd/lz4/none) | `snappy` |
| `--discovery-only` | Analyze schema without converting | `false` |
| `--verbose` | Enable detailed logging | `false` |


## Technical Details

### Data Structure Optimizations

1. **Station-Timestamp Sorting**: Data is sorted by `station_id` then `ob_end_time` for optimal query performance
2. **Large Row Groups**: 500K rows per group for better compression and fewer metadata operations
3. **Column Statistics**: Enabled for all columns to allow query engines to skip irrelevant data
4. **Memory Streaming**: Processes datasets larger than available RAM through streaming execution


### Quality Control

- **Header validation**: Ensures BADC-CSV headers are correctly parsed
- **Schema consistency**: Validates column structures across files
- **Error reporting**: Detailed error messages with file locations
- **Missing data handling**: Graceful handling of incomplete or corrupted files

## Integration with Analysis Tools

### Python (Polars)
```python
import polars as pl

# Fast station-based query
df = pl.scan_parquet("rain_data.parquet") \
       .filter(pl.col("station_id") == "00009") \
       .collect()

# Time range analysis
monthly_avg = pl.scan_parquet("temperature_data.parquet") \
                .filter(pl.col("ob_end_time").dt.year() == 2023) \
                .group_by(["station_id", pl.col("ob_end_time").dt.month()]) \
                .agg(pl.col("air_temperature").mean()) \
                .collect()
```

### Python (Pandas)
```python
import pandas as pd

# Read with automatic optimization
df = pd.read_parquet("rain_data.parquet")

# Station-specific analysis
station_data = df[df['station_id'] == '00009']
```

### R
```r
library(arrow)

# Lazy evaluation with Arrow
rain_data <- open_dataset("rain_data.parquet")

# Efficient aggregation
monthly_totals <- rain_data %>%
  filter(year(ob_end_time) == 2023) %>%
  group_by(station_id, month = month(ob_end_time)) %>%
  summarise(total_rain = sum(prcp_amt, na.rm = TRUE)) %>%
  collect()
```

## Troubleshooting

### Common Issues

**Memory Issues**
```bash
# For very large datasets, ensure sufficient RAM or use streaming
midas --verbose  # Monitor memory usage
```

**Performance Issues**
```bash
# Check if storage is the bottleneck
midas --verbose  # Shows processing rates
```

**Cache Directory Not Found**
```bash
# Ensure midas-fetcher has been run first
ls ~/Library/Application\ Support/midas-fetcher/cache/  # macOS
ls ~/.config/midas-fetcher/cache/  # Linux
```

### Error Messages

- **"No MIDAS datasets found in cache"**: Run midas-fetcher first to download datasets
- **"Failed to parse header"**: BADC-CSV file may be corrupted, check source data
- **"Configuration file not found"**: Dataset type not recognized, check file structure

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for detailed version history and release notes.

## Contributing

We welcome contributions! Please see our [contributing guidelines](CONTRIBUTING.md) for details.

### Development Setup

```bash
git clone https://github.com/your-org/midas-processor
cd midas-processor
cargo build
cargo test
```

### Code Style
- Use `cargo fmt` for formatting
- Ensure `cargo clippy` passes without warnings
- Add tests for new functionality
- Update documentation for API changes

## Citation

If you use this tool in your research, please cite:

```bibtex
@software{midas_processor,
  title = {MIDAS Processor: High-Performance Climate Data Processing},
  author = {Richard Lyon},
  year = {2025},
  url = {https://github.com/rjl-climate/midas_processor}
}
```

## Support

- **Documentation**: See [docs/](docs/) directory
- **Issues**: Report bugs via [GitHub Issues](https://github.com/your-org/midas-processor/issues)
- **Discussions**: Ask questions in [GitHub Discussions](https://github.com/your-org/midas-processor/discussions)

## Acknowledgments

- **UK Met Office**: For providing the MIDAS datasets
- **CEDA**: For hosting and maintaining the climate data archive
- **BADC**: For developing the CSV format standards
- **Polars Project**: For the high-performance DataFrame library enabling fast processing
