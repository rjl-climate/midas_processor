# MIDAS Processor Planning Document

## Project Overview

The MIDAS Processor is a Rust command-line tool that converts UK Met Office MIDAS weather observation data from CSV format into optimized Apache Parquet files. The tool is designed to process data downloaded by the midas-fetcher tool, creating single Parquet files per dataset optimized for fast loading in Python data analysis workflows using Pandas or Polars.

## Goals

1. **Transform MIDAS CSV data to Parquet format** with proper data typing and compression
2. **Combine station metadata with observations** for self-contained analysis files
3. **Handle quality control information** according to Met Office specifications
4. **Optimize for Python read performance** over file size
5. **Provide single-file-per-dataset output** for maximum convenience

## Technical Architecture

### Core Components

1. **Station Registry**
   - Loads and indexes all capability files
   - Provides O(1) lookup of station metadata by `src_id`
   - Handles record status filtering (`rec_st_ind = 9`)

2. **CSV Parser**
   - Reads MIDAS CSV files with proper handling of metadata headers
   - Parses observation records with quality control fields
   - Handles missing values (`NA`) appropriately

3. **Record Processor**
   - Deduplicates records based on `rec_st_ind` (1 supersedes 9)
   - Enriches observations with station metadata
   - Applies configurable quality control filters

4. **Parquet Writer**
   - Writes optimized Parquet files with Snappy compression
   - Configures large row groups for sequential read performance
   - Uses appropriate data types for Python compatibility

## Data Model

### Station Metadata
```rust
struct Station {
    src_id: i32,                    // Primary key
    src_name: String,               // Human-readable name
    high_prcn_lat: f64,            // WGS84 latitude
    high_prcn_lon: f64,            // WGS84 longitude
    east_grid_ref: Option<i32>,     // UK grid reference
    north_grid_ref: Option<i32>,
    grid_ref_type: Option<String>,  // Grid system
    src_bgn_date: DateTime,         // Station start date
    src_end_date: DateTime,         // Station end date
    authority: String,              // Operating authority
    historic_county: String,        // County name
    height_meters: f32,            // Elevation
}
```

### Observation Record
```rust
struct Observation {
    // Temporal
    ob_end_time: DateTime,
    ob_hour_count: i32,

    // Station reference
    id: i32,                        // Maps to src_id
    id_type: String,                // Usually "SRCE" or "DCNN"

    // Record metadata
    met_domain_name: String,        // Data type identifier
    rec_st_ind: i32,               // Record status
    version_num: i32,               // QC version

    // Measurements (varies by dataset)
    // Quality flags (0=valid, 1=suspect, 2=erroneous)
    // Descriptor codes

    // Processing metadata
    meto_stmp_time: DateTime,
    midas_stmp_etime: i32,
}
```

### Quality Control Handling
```rust
enum QualityFlag {
    Valid = 0,        // Passed all QC checks
    Suspect = 1,      // Failed at least one check
    Erroneous = 2,    // Considered incorrect
    NotChecked = 3,   // No QC applied
    Missing = 9,      // No data available
}
```

## Implementation Plan

### Phase 1: Core Infrastructure
- [ ] Set up Rust project with dependencies (arrow-rs, parquet, serde, chrono)
- [ ] Implement MIDAS CSV parser with header handling
- [ ] Create data structures for stations and observations
- [ ] Build station registry loader

### Phase 2: Data Processing
- [ ] Implement observation file parser for each dataset type
- [ ] Add record deduplication logic
- [ ] Create metadata enrichment pipeline
- [ ] Implement quality control filtering

### Phase 3: Parquet Output
- [ ] Configure Parquet writer with optimized settings
- [ ] Implement single-file-per-dataset writer
- [ ] Add progress reporting and logging
- [ ] Create metadata summary files

### Phase 4: Polish and Optimization
- [ ] Add command-line interface with clap
- [ ] Implement parallel processing for large datasets
- [ ] Add comprehensive error handling
- [ ] Write documentation and examples

## Key Design Decisions

### 1. Single File Output
Each dataset produces one Parquet file for simplicity:
- `uk-daily-temperature-obs.parquet` (~750 MB)
- `uk-daily-rain-obs.parquet` (~4.7 GB)
- etc.

### 2. Denormalized Schema
Station metadata is repeated in each observation row to avoid joins in Python.

### 3. Performance Optimizations
- Snappy compression for fast decompression
- Large row groups (1M+ rows) for sequential reads
- Plain encoding for numeric columns
- Dictionary encoding only for low-cardinality strings

### 4. Data Type Choices
- `i64` timestamps for pandas datetime64[ns] compatibility
- `f32` for measurements (sufficient precision)
- `i8` for quality flags (space efficient)

## File Organization

### Input Structure
```
midas-fetcher/cache/
├── uk-daily-temperature-obs/
│   ├── capability/
│   │   └── {county}/{station}/
│   └── qcv-1/
│       └── {county}/{station}/{year}.csv
└── uk-daily-rain-obs/
    └── ...
```

### Output Structure
```
output/
├── uk-daily-temperature-obs.parquet
├── uk-daily-weather-obs.parquet
├── uk-daily-rain-obs.parquet
├── uk-hourly-weather-obs.parquet
├── uk-hourly-rain-obs.parquet
├── uk-mean-wind-obs.parquet
├── uk-radiation-obs.parquet
├── uk-soil-temperature-obs.parquet
├── stations.parquet
└── metadata/
    ├── {dataset}.metadata.json
    └── processing_log.json
```

## Configuration

```toml
[processing]
input_path = "/path/to/midas-fetcher/cache"
output_path = "/path/to/parquet/output"
datasets = ["uk-daily-temperature-obs", "uk-daily-rain-obs"]

[quality_control]
include_suspect = false
include_unchecked = false
min_quality_version = 1

[parquet]
compression = "snappy"
row_group_size = 1_000_000
page_size_mb = 1

[performance]
parallel_workers = 8
memory_limit_gb = 16
```

## Error Handling Strategy

1. **Graceful Degradation**: Skip malformed files with warnings
2. **Data Validation**: Log QC failures and suspicious values
3. **Recovery**: Support resuming interrupted processing
4. **Reporting**: Generate detailed processing report with statistics

## Testing Strategy

1. **Unit Tests**: Parser, deduplication, QC logic
2. **Integration Tests**: End-to-end processing of sample data
3. **Validation**: Compare output with Python-loaded CSV data
4. **Performance**: Benchmark loading times vs CSV

## Success Metrics

1. **Correctness**: Output matches CSV data when loaded in Python
2. **Performance**: 10x+ faster loading than CSV in Pandas/Polars
3. **Size**: Parquet files 70-80% smaller than CSV
4. **Usability**: Single command processes all datasets

## Future Enhancements

1. **Incremental Updates**: Process only new data since last run
2. **Regional Splits**: Optional geographic partitioning
3. **Custom Schemas**: User-defined column selection
4. **Cloud Storage**: Direct write to S3/Azure/GCS
5. **Data Validation**: Automated quality reports

## Dependencies

```toml
[dependencies]
arrow = "50.0"
parquet = "50.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
chrono = "0.4"
clap = { version = "4.0", features = ["derive"] }
rayon = "1.7"
csv = "1.3"
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
indicatif = "0.17"
```

## Command Line Interface

```bash
# Process all datasets
midas-processor --input /path/to/cache --output /path/to/output

# Process specific datasets
midas-processor --input /path/to/cache --output /path/to/output \
  --datasets uk-daily-temperature-obs,uk-daily-rain-obs

# With quality control options
midas-processor --input /path/to/cache --output /path/to/output \
  --include-suspect --qc-version 1

# Dry run to see what would be processed
midas-processor --input /path/to/cache --output /path/to/output --dry-run
```

## Further reading:

1. **MIDAS Fetcher**: The code that created the cache files https://github.com/rjl-climate/midas_fetcher
2. **MIDAS OPEN User Guide**: Official guidance https://help.ceda.ac.uk/article/4982-midas-open-user-guide
3. **MIDAS Data User Guide**: (PDF) Detailed description of data https://zenodo.org/records/7357335/files/MIDAS_User_Guide_for_UK_Land_Observations.pdf?download=1
4. **BADC-CSV**: Explains the format https://help.ceda.ac.uk/article/105-badc-csv


---

This planning document serves as the authoritative guide for implementing the MIDAS Processor. It should be updated as design decisions evolve during development.
