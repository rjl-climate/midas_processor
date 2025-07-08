# MIDAS Processor

[![Rust](https://img.shields.io/badge/rust-1.80%2B-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)

High-performance Rust CLI tool that converts UK Met Office MIDAS weather data from CSV to optimized Parquet format for fast Python analysis.

## Overview

**MIDAS** is the UK Met Office's archive of land-based weather observations spanning 1000+ stations from the late 19th century onwards. Working with this data requires a three-step pipeline:

1. **[midas_fetcher](https://github.com/rjl-climate/midas_fetcher)** - Downloads raw CSV data from CEDA archive with hierarchical caching
2. **midas_processor** (this tool) - Converts cached CSV to optimized Parquet with processing flags and station evolution tracking  
3. **Python analysis** - Fast data access in pandas/polars (10x+ faster than CSV)

## Installation

**Prerequisites:**
- Rust 1.80+ toolchain
- [midas_fetcher](https://github.com/rjl-climate/midas_fetcher) to download source data

**Install from source:**
```bash
git clone <repository-url>
cd midas_processor
cargo install --path .
```

**Install from crates.io:**
```bash
cargo install midas_processor
```

## Usage Examples

**Basic processing after data fetch:**
```bash
# First, download data with midas_fetcher
midas_fetcher download --dataset uk-daily-temperature-obs

# Then convert to Parquet
midas-processor process --datasets uk-daily-temperature-obs
```

**Process multiple datasets:**
```bash
midas-processor process --datasets uk-daily-temperature-obs,uk-daily-rain-obs
```

**Generate station registry report:**
```bash
midas-processor stations --detailed --format json
```

**Verify Parquet output:**
```bash
python tests/verify_parquet_output.py output/uk-daily-temperature-obs_202501.parquet
```

## Philosophy

### High-Performance Python Access

MIDAS Processor optimizes for scientific data analysis workflows:

- **10x+ faster loading** than CSV in pandas/polars
- **Snappy compression** for fast decompression
- **Columnar storage** optimized for temporal queries
- **Row group sizing** (1M+ rows) for sequential reads
- **Dictionary encoding** for categorical data (station IDs, counties)

### Station ID Evolution Preservation

Climate analysis often requires multi-decade datasets where station IDs change over time. MIDAS Processor preserves this evolution:

- **Complete station history** linked to observations
- **Seamless temporal analysis** across ID changes
- **Research-grade metadata** for station moves and equipment changes
- **Quality control context** for long-term trend analysis

### Processing Flags for Data Quality

Every field includes processing flags (`pf_*` columns) indicating:
- **Parse status** - successful/failed data extraction
- **Station lookup** - metadata enrichment success
- **Quality assessment** - data reliability indicators
- **Processing steps** - complete audit trail

## Contributing

Contributions welcome! This project uses:
- **Rust 2024 edition** with async closures and improved scoping
- **Conventional commits** for clear change tracking
- **Comprehensive testing** with 331+ test cases
- **Documentation** via rustdoc and architectural diagrams

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Changelog

### v0.1.0 (Unreleased)
- Initial release with complete MIDAS processing pipeline
- Dynamic schema generation with processing flags
- Station ID evolution tracking
- High-performance Parquet output optimized for Python
- Comprehensive CLI with progress reporting and validation