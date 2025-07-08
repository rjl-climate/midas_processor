# Weather Data Parquet Writer - Design Specification

## Overview

Build a Rust module for converting daily weather records into optimized Parquet files for time series analysis using Python pandas. The module processes weather station data organized by county and monitoring station, and optimizes for temporal queries while maintaining efficient geographic filtering capabilities. Use the record_processor and station_registry modules for the data to be encoded.

Weather data is organised into datasets (e.g. "uk-daily-temperature-obs"). This module will create one Parquet file per dataset and file it in directory "parquet_files" in the app's storage root directory.

Provide meaningful "progress bar" information is given to the user and a summary at the end of processing of any issues that arose during processing.

Use the "## Module Architecture" section below as a guidance but improve as your research knowledge improves.

THIS IS MANDATORY. You MUST read the provided Core technical documentation in the next section by scraping the websites and following improtant links as requried to develop a good undersdtanding. THIS IS MANDATORY.

Task steps:

CREATE src/app/services/parquet_writer.rs:
  - CONFIGURE ArrowWriter with optimal settings
  - IMPLEMENT batch writing with memory management
  - ADD progress reporting
  - HANDLE large datasets efficiently


## Essential Reading List

```yaml
# MUST READ - Core technical documentation
- url: https://docs.rs/parquet/latest/parquet/arrow/index.html
  why: Arrow-rs Parquet integration API and schema conversion patterns
  critical: ARROW_SCHEMA_META_KEY metadata hints for pandas compatibility and type recovery

- url: https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html
  why: Complete ArrowWriter API including memory management and lifecycle
  critical: Mandatory writer.close() call and memory monitoring via memory_size()/in_progress_size()

- url: https://parquet.apache.org/docs/file-format/
  why: Official Parquet format specification and file structure
  critical: Single-pass writing design and metadata-after-data layout for sequential readers

- url: https://docs.rs/parquet/latest/parquet/file/properties/index.html
  why: WriterProperties configuration and optimization settings
  critical: Dictionary encoding via set_dictionary_enabled() and compression codec selection

- url: https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
  why: Arrow type system and schema definition patterns
  critical: TimestampNanosecond for pandas compatibility and Dictionary encoding for categorical data

- url: https://github.com/apache/arrow-rs/blob/main/parquet/examples/write_parquet.rs
  why: Real-world Parquet writing patterns and memory management
  critical: Row group sizing (50K-1M rows) and system memory monitoring techniques

- url: https://docs.rs/tokio/latest/tokio/fs/struct.File.html
  why: Async file I/O patterns and performance considerations
  critical: spawn_blocking behavior and batched operation requirements for performance

- url: https://docs.rs/rayon/latest/rayon/iter/trait.ParallelIterator.html
  why: Parallel processing patterns for large weather datasets
  critical: Work-stealing scheduler and par_iter() conversion patterns

- url: https://docs.pola.rs/user-guide/io/parquet/
  why: Polars Parquet optimization strategies and best practices
  critical: Row group statistics requirements and sorted data for predicate pushdown

- url: https://arrow.apache.org/docs/python/parquet.html
  why: PyArrow compatibility patterns and pandas integration
  critical: Schema metadata preservation and partition column handling
```

## Known Gotchas and Library Quirks

### ArrowWriter Critical Behaviors
```rust
// GOTCHA: Writer MUST be manually closed - no automatic Drop implementation
// Unlike std::fs::File, ArrowWriter requires explicit close() to write footer
let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
writer.write(&batch).unwrap();
writer.close().unwrap(); // MANDATORY - file will be corrupted without this

// GOTCHA: Memory monitoring is essential for large datasets
// Writer buffers entire row groups in memory before flushing
if writer.in_progress_size() > 100_000_000 { // 100MB threshold
    writer.flush().unwrap();
}

// GOTCHA: RecordBatch schema must exactly match writer schema
// Any schema mismatch will panic at runtime, not compile time
```

### WriterProperties Configuration Pitfalls
```rust
// GOTCHA: Dictionary encoding via dedicated method, not set_encoding()
// set_encoding() for dictionary will panic - use set_dictionary_enabled() instead
let props = WriterProperties::builder()
    .set_dictionary_enabled(ColumnPath::from("station_id"), true) // ✓ Correct
    // .set_encoding(ColumnPath::from("station_id"), Encoding::RLE_DICTIONARY) // ✗ PANICS!
    .build();

// GOTCHA: Data page size is "best effort" based on write_batch_size
// Actual page sizes may vary significantly from set_data_page_size_limit()
.set_data_page_size_limit(1024 * 1024) // Approximate, not guaranteed

// GOTCHA: Row group size affects both memory usage and query performance
// Too small = metadata overhead, too large = memory pressure
.set_max_row_group_size(100_000) // Balance between memory and metadata
```

### Arrow Schema and Type Quirks
```rust
// GOTCHA: TimestampNanosecond required for full pandas compatibility
// Other timestamp units may cause precision loss in pandas
Field::new("date", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
// Not: TimeUnit::Millisecond - pandas expects nanoseconds

// GOTCHA: Dictionary encoding key type affects file size and performance
// Use smallest possible key type for your data cardinality
DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
// Not: UInt32 for <65k unique values - wastes space

// GOTCHA: Schema metadata preserved via ARROW_SCHEMA_META_KEY
// Critical for pandas to recover original Arrow types from Parquet
```

### Tokio Async File Performance Traps
```rust
// GOTCHA: Tokio file operations use spawn_blocking internally
// Many small writes are extremely inefficient - batch operations
// BAD: Multiple small writes
for batch in batches {
    writer.write(&batch).await?; // Each write = separate spawn_blocking
}

// GOOD: Collect and write in larger batches
let large_batch = concatenate_batches(&batches)?;
writer.write(&large_batch).await?; // Single spawn_blocking call

// GOTCHA: File::flush() is critical for write completion
// Unlike std::fs, tokio files may not flush on drop
file.flush().await?; // Required before dropping
```

### Rayon Parallel Processing Gotchas
```rust
// GOTCHA: Rayon requires specific trait imports
use rayon::prelude::*; // Essential - missing this causes compile errors

// GOTCHA: Parallel iteration order is non-deterministic
// Results may vary between runs for non-commutative operations
.par_iter().enumerate() // Order not guaranteed across threads

// GOTCHA: Overhead dominates for small datasets
// Only use parallel processing for substantial workloads
if records.len() > 10_000 { // Threshold depends on operation complexity
    records.par_iter()
} else {
    records.iter()
}
```

### Parquet Format Edge Cases
```rust
// GOTCHA: Parquet metadata written after data requires seekable streams
// Cannot write to append-only streams or pipes
let file = File::create("output.parquet")?; // ✓ Seekable
// let stdout = std::io::stdout(); // ✗ Not seekable

// GOTCHA: Column statistics generation affects write performance
// Disable for write-heavy workloads where statistics aren't needed
.set_statistics_enabled(ColumnPath::from("large_text_column"), EnabledStatistics::None)

// GOTCHA: Bloom filters position affects compatibility
// AfterRowGroup (default) provides better compatibility than BeforeRowGroup
.set_bloom_filter_position(BloomFilterPosition::AfterRowGroup)
```

### Weather Data Specific Considerations
```rust
// GOTCHA: Missing weather data (NA values) must be handled consistently
// Use Option<f32> rather than sentinel values for proper null handling
Field::new("temperature", DataType::Float32, true) // nullable=true

// GOTCHA: Station ID dictionary encoding highly beneficial
// Weather stations have limited cardinality - excellent dictionary candidates
DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))

// GOTCHA: Date sorting critical for time series optimization
// Sort by date before writing to enable row group pruning
weather_data.sort_by_key(|record| record.date);

// GOTCHA: Year partitioning enables efficient temporal queries
// Primary partitioning by year, secondary by county for time series analysis
write_to_dataset(table, "weather_data/", &["year", "county"])
```

### Memory Management and Performance
```rust
// GOTCHA: Large dictionary sizes can cause memory pressure
// Monitor dictionary growth and disable for high-cardinality columns
if unique_values > 100_000 {
    props.set_dictionary_enabled(column_path, false);
}

// GOTCHA: Arrow arrays are reference-counted
// Clone operations are cheap but watch for memory retention
let batch_clone = batch.clone(); // Cheap Arc clone, not data copy

// GOTCHA: String data dominates memory usage in weather records
// Use string interning or categorical encoding for repeated strings
let station_array = DictionaryArray::from_iter(station_ids.iter());
```

## Module Architecture

### Core Components

```rust
pub mod weather_parquet {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use chrono::{DateTime, Utc};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub struct WeatherRecord {
        pub station_id: String,
        pub county: String,
        pub date: DateTime<Utc>,
        pub metric_type: MetricType,
        pub value: Option<f32>,
        pub quality_flag: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum MetricType {
        MaxTemp,
        MinTemp,
        AvgTemp,
        Precipitation,
        Humidity,
        WindSpeed,
        WindDirection,
    }

    pub struct StationMetadata {
        pub station_id: String,
        pub station_name: String,
        pub county: String,
        pub latitude: f64,
        pub longitude: f64,
        pub altitude: Option<f32>,
        pub installation_date: DateTime<Utc>,
        pub decommission_date: Option<DateTime<Utc>>,
    }

    pub struct WeatherParquetWriter<W: Write + Send> {
        writer: ArrowWriter<W>,
        schema: Arc<Schema>,
        record_batch_builder: RecordBatchBuilder,
        batch_size: usize,
        written_rows: usize,
    }

    pub struct WriterConfig {
        pub row_group_size: usize,
        pub compression: Compression,
        pub enable_statistics: bool,
        pub enable_dictionary: bool,
        pub data_page_size: usize,
    }
}
```

### Implementation Strategy

The module will implement a streaming writer that:

1. **Optimizes for time series queries** by partitioning primarily by year, secondarily by county
2. **Leverages dictionary encoding** for categorical data like station IDs and metric types
3. **Uses proper timestamp types** (TimestampNanosecond) for pandas compatibility
4. **Implements memory monitoring** to prevent OOM conditions during large dataset processing
5. **Provides async support** via Tokio for non-blocking I/O operations
6. **Enables parallel processing** using Rayon for data transformation pipelines

### Key Design Decisions

- **Schema-first approach**: Define Arrow schema upfront and validate all data against it
- **Batch processing**: Collect records into optimally-sized batches before writing
- **Error propagation**: Use Result types throughout with proper error context
- **Memory efficiency**: Stream processing with configurable batch sizes and memory limits
- **Compatibility focus**: Ensure pandas can read files with optimal query performance
- **Partitioning strategy**: Year-first partitioning for temporal analysis optimization

This specification provides a comprehensive foundation for implementing a production-ready weather data Parquet writer optimized for time series analysis workflows.

You MUST read the provided Core technical documentation in the next section by scraping the websites and following improtant links as requried to develop a good undersdtanding. THIS IS MANDATORY.

### Integration Points
```yaml
FILESYSTEM:
  - pattern: Use walkdir for recursive directory traversal
  - path: Cache structure from midas_fetcher
  - validation: Check file existence and permissions

CONFIGURATION:
  - pattern: Layered config (file -> env -> args)
  - location: Config file at ~/.config/midas-processor/config.toml
  - validation: Validate all paths and numeric ranges

LOGGING:
  - pattern: Structured logging with tracing
  - output: stderr for human-readable, stdout for machine-readable
  - levels: info for progress, warn for recoverable errors, error for failures

PROGRESS:
  - pattern: indicatif progress bars
  - metrics: Files processed, records written, errors encountered
  - estimation: Based on file count and size
```

## Validation Loop

### Level 1: Syntax & Style
```bash
# Run these FIRST - fix any errors before proceeding
cargo fmt --all                     # Format code
cargo clippy --all-targets -- -D warnings  # Lint with warnings as errors
cargo check --all-targets          # Compile check

# Expected: No errors. If errors, READ the error and fix.
```

### Level 2: Unit Tests
```rust
// CREATE tests for each module following these patterns:
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_station_registry_load() {
        // GIVEN: Sample capability files
        let temp_dir = TempDir::new().unwrap();
        create_test_capability_files(&temp_dir);

        // WHEN: Loading registry
        let registry = StationRegistry::load_from_cache(temp_dir.path()).unwrap();

        // THEN: Stations are indexed correctly
        assert_eq!(registry.stations.len(), 3);
        assert!(registry.get_station(12345).is_some());
    }

    #[test]
    fn test_csv_parser_badc_format() {
        // GIVEN: BADC-CSV file with header and data sections
        let csv_content = create_test_badc_csv();

        // WHEN: Parsing file
        let observations = BadcCsvParser::parse_string(&csv_content).unwrap();

        // THEN: Observations parsed correctly
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].measurements.len(), 3);
    }

    #[test]
    fn test_record_deduplication() {
        // GIVEN: Records with different rec_st_ind values
        let records = vec![
            create_observation_with_rec_st_ind(9),
            create_observation_with_rec_st_ind(1), // Should supersede
        ];

        // WHEN: Processing records
        let deduplicated = RecordProcessor::deduplicate(records);

        // THEN: Only rec_st_ind=1 remains
        assert_eq!(deduplicated.len(), 1);
        assert_eq!(deduplicated[0].rec_st_ind, 1);
    }
}
```

```bash
# Run and iterate until passing:
cargo test --all-targets
# If failing: Read error, understand root cause, fix code, re-run
```

### Level 3: Integration Test
```bash
# Create sample data
mkdir -p test_cache/uk-daily-temperature-obs/qcv-1/devon/exeter/
echo "header content" > test_cache/uk-daily-temperature-obs/qcv-1/devon/exeter/2023.csv

# Run the application
cargo run -- --input test_cache --output test_output --dry-run

# Expected: Success with processing summary
# Check: test_output directory created with expected files
```

## Final Validation Checklist
- [ ] All tests pass: `cargo test --all-targets`
- [ ] No linting errors: `cargo clippy --all-targets -- -D warnings`
- [ ] No compilation errors: `cargo check --all-targets`
- [ ] Integration test successful: processes sample MIDAS data
- [ ] Performance benchmark: 10x+ faster than CSV loading
- [ ] Memory usage: Stays within configured limits
- [ ] Error recovery: Handles malformed files gracefully
- [ ] CLI help: Comprehensive usage information

---

## Anti-Patterns to Avoid
- ❌ Don't use unwrap() in library code - always propagate errors
- ❌ Don't assume all stations have complete data - handle gaps gracefully
- ❌ Don't hardcode dataset names - make them configurable
- ❌ Don't load entire datasets into memory - use streaming processing
- ❌ Don't use default Parquet settings - optimize for scientific data access
