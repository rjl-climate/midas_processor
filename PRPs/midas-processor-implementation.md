name: "MIDAS Processor Implementation PRP"
description: |
  Complete implementation of a Rust CLI tool to convert UK Met Office MIDAS weather data from CSV to optimized Parquet format.
  This PRP provides comprehensive context for one-pass implementation including data format handling, performance optimization, and robust error handling.

---

## Goal
Build a production-ready Rust command-line tool that processes UK Met Office MIDAS weather observation data from CSV format into optimized Apache Parquet files. The tool must handle scientific data quality control, station metadata enrichment, and produce files optimized for Python data analysis workflows.

## Why
- **Scientific Data Processing**: Enable fast analysis of UK weather data for climate research
- **Performance Optimization**: Provide 10x+ faster loading than CSV in Python (Pandas/Polars)
- **Data Integration**: Combine station metadata with observations for self-contained analysis
- **Quality Control**: Handle Met Office QC standards and data integrity requirements
- **Storage Efficiency**: Reduce file sizes by 70-80% while maintaining data fidelity

## What
A CLI tool that:
- Parses MIDAS BADC-CSV files with proper header/data section handling
- Loads and indexes station metadata for O(1) lookups
- Deduplicates records based on QC status indicators
- Enriches observations with station metadata
- Writes optimized Parquet files with Snappy compression
- Provides progress reporting and comprehensive error handling

### Success Criteria
- [ ] Correctly parses all MIDAS BADC-CSV file formats
- [ ] Handles quality control versions and record deduplication
- [ ] Produces Parquet files 10x+ faster to load than CSV
- [ ] Maintains 100% data fidelity compared to original CSV
- [ ] Processes all 8 dataset types from midas_fetcher cache
- [ ] Provides comprehensive error reporting and recovery
- [ ] Includes professional CLI with progress reporting

## All Needed Context

### Documentation & References
```yaml
# MUST READ - Core technical documentation
- url: https://docs.rs/csv/latest/csv/tutorial/
  why: Official CSV parsing tutorial with Serde integration
  critical: Performance optimization with StringRecord and manual deserialization

- url: https://help.ceda.ac.uk/article/4982-midas-open-user-guide
  why: MIDAS data structure and organization
  critical: QC versioning and directory hierarchy

- url: https://help.ceda.ac.uk/article/105-badc-csv
  why: BADC-CSV format specification
  critical: Header/data section parsing requirements

- url: https://github.com/rjl-climate/midas_fetcher
  why: Cache structure and organization patterns
  critical: Input file hierarchy and naming conventions

- file: docs/Section_4.md
  why: Station metadata and observation record structure
  critical: Data model definitions and join logic

- file: PLANNING.md
  why: Complete project architecture and requirements
  critical: All technical decisions and data structures

- url: https://docs.rs/parquet/latest/parquet/
  why: Official Parquet crate documentation with API reference
  critical: File writing patterns, schema definition, and metadata handling

- url: https://docs.rs/arrow/latest/arrow/
  why: Arrow data structures and type system integration
  critical: Schema definition, RecordBatch creation, and data type mapping

- url: https://github.com/apache/arrow-rs/tree/master/parquet/examples
  why: Real-world Parquet writer implementation examples
  critical: ArrowWriter setup, batch processing patterns, and error handling

- url: https://parquet.apache.org/docs/file-format/
  why: Official Parquet format specification
  critical: Row group organization, column statistics, and encoding options

- url: https://docs.rs/parquet/latest/parquet/file/properties/
  why: Writer properties and optimization configuration
  critical: Compression settings, row group sizing, and dictionary encoding

- url: https://github.com/apache/arrow-rs/blob/master/parquet/src/arrow/arrow_writer.rs
  why: ArrowWriter source code and implementation details
  critical: Batch writing logic, schema validation, and performance optimizations

- url: https://arrow.apache.org/docs/rust/parquet/index.html
  why: Arrow Rust Parquet integration guide
  critical: Schema evolution, partitioning strategies, and metadata preservation

- url: https://docs.rs/arrow/latest/arrow/datatypes/
  why: Arrow data type system and schema definition
  critical: Time series data types, nullable fields, and categorical encoding

- url: https://github.com/apache/arrow-rs/tree/master/arrow/examples
  why: Arrow RecordBatch and Array construction patterns
  critical: Efficient batch creation for time series data

- url: https://docs.rs/tokio/latest/tokio/fs/
  why: Async file I/O patterns for large dataset processing
  critical: Non-blocking writes and concurrent processing strategies

- url: https://polars.rs/posts/parquet_writer/
  why: Parquet optimization techniques for analytical workloads
  critical: Column ordering, compression choices, and query optimization

- url: https://docs.rs/serde/latest/serde/
  why: Serialization patterns for structured weather data
  critical: Custom deserializers for weather station metadata and measurements

- url: https://github.com/apache/arrow-rs/blob/master/parquet/benches/arrow_writer.rs
  why: Performance benchmarking patterns and optimization strategies
  critical: Memory usage optimization and write throughput maximization

- url: https://docs.rs/rayon/latest/rayon/
  why: Parallel processing for large weather datasets
  critical: Parallel batch creation and concurrent file writing

- url: https://github.com/pola-rs/polars/blob/main/crates/polars-io/src/parquet/write/
  why: Advanced Parquet writing patterns and optimizations
  critical: Partitioned writing, schema management, and data validation
```

### Current Codebase Structure
```
midas_processor/
├── src/
│   └── main.rs           # Basic hello world stub
├── Cargo.toml           # Empty dependencies
├── PLANNING.md          # Complete project specification
├── docs/
│   ├── Section_4.md     # Data model documentation
│   └── Section_5.md     # Quality control documentation
├── PRPs/
│   └── templates/       # This PRP template
└── examples/            # Empty directory for examples
```

### Desired Codebase Structure
```
midas_processor/
├── src/
│   ├── main.rs          # CLI entry point
│   ├── lib.rs           # Public API
│   ├── app/
│   │   ├── mod.rs       # Application module
│   │   ├── models.rs    # Data structures (Station, Observation, QualityFlag)
│   │   ├── services/    # Core business logic
│   │   │   ├── mod.rs
│   │   │   ├── station_registry.rs    # Station metadata loading/indexing
│   │   │   ├── csv_parser.rs          # BADC-CSV parsing
│   │   │   ├── record_processor.rs    # Deduplication and enrichment
│   │   │   └── parquet_writer.rs      # Optimized Parquet output
│   │   └── adapters/    # External integrations
│   │       ├── mod.rs
│   │       └── filesystem.rs          # File system operations
│   ├── cli/
│   │   ├── mod.rs       # CLI module
│   │   ├── args.rs      # Command line argument definitions
│   │   └── commands.rs  # Command implementations
│   ├── config.rs        # Configuration management
│   └── constants.rs     # Application constants
├── tests/
│   ├── integration/     # End-to-end tests
│   └── fixtures/        # Test data
└── examples/
    └── sample_usage.rs  # Usage examples
```

### Known Gotchas & Library Quirks
```rust
// CRITICAL: BADC-CSV files have two-section structure
// Header section contains metadata, data section contains observations
// Must parse header first to understand data columns

// CRITICAL: MIDAS uses "NA" for missing values in CSV
// Must handle this specially in CSV parsing

// CRITICAL: Quality control versioning
// qc-version-1 contains latest data, qc-version-0 contains original
// rec_st_ind: 1 supersedes 9 for same station/time

// CRITICAL: Arrow-rs performance optimizations
// Use StringRecord for better performance than deserialize()
// Configure large row groups (1M+ rows) for sequential reads
// Use Snappy compression for fast decompression

// CRITICAL: Station metadata O(1) lookup
// Build HashMap<i32, Station> indexed by src_id
// Filter capability files to rec_st_ind = 9 only

// CRITICAL: DateTime parsing for MIDAS timestamps
// Format: "YYYY-MM-DD HH24:MI:SS"
// Must convert to i64 nanoseconds for pandas compatibility
```

## Implementation Blueprint

### Data Models and Structure
```rust
// Core data structures based on MIDAS specification
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Station {
    pub src_id: i32,                    // Primary key
    pub src_name: String,               // Human-readable name
    pub high_prcn_lat: f64,            // WGS84 latitude
    pub high_prcn_lon: f64,            // WGS84 longitude
    pub east_grid_ref: Option<i32>,     // UK grid reference
    pub north_grid_ref: Option<i32>,
    pub grid_ref_type: Option<String>,  // Grid system
    pub src_bgn_date: DateTime<Utc>,    // Station start date
    pub src_end_date: DateTime<Utc>,    // Station end date
    pub authority: String,              // Operating authority
    pub historic_county: String,        // County name
    pub height_meters: f32,             // Elevation
}

#[derive(Debug, Clone)]
pub struct Observation {
    // Temporal information
    pub ob_end_time: DateTime<Utc>,
    pub ob_hour_count: i32,

    // Station reference
    pub id: i32,                        // Maps to Station.src_id
    pub id_type: String,                // Usually "SRCE"

    // Record metadata
    pub met_domain_name: String,        // Dataset identifier
    pub rec_st_ind: i32,               // Record status (1 supersedes 9)
    pub version_num: i32,              // QC version

    // Station metadata (denormalized)
    pub station: Station,

    // Dynamic measurements (varies by dataset)
    pub measurements: HashMap<String, f64>,
    pub quality_flags: HashMap<String, QualityFlag>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QualityFlag {
    Valid = 0,        // Passed all QC checks
    Suspect = 1,      // Failed at least one check
    Erroneous = 2,    // Considered incorrect
    NotChecked = 3,   // No QC applied
    Missing = 9,      // No data available
}
```

### Task List (Implementation Order)

```yaml
Task 1: Project Setup and Dependencies
MODIFY Cargo.toml:
  - ADD dependencies from PLANNING.md
  - CONFIGURE features for arrow, parquet, csv, serde
  - ADD dev-dependencies for testing

CREATE src/lib.rs:
  - DEFINE public API surface
  - ADD error types using thiserror
  - EXPORT main modules

CREATE src/constants.rs:
  - DEFINE all constants from PLANNING.md
  - ADD QC flag mappings
  - CONFIGURE Parquet writer settings

Task 2: Core Data Models
CREATE src/app/models.rs:
  - IMPLEMENT Station struct with serde derives
  - IMPLEMENT Observation struct with dynamic fields
  - IMPLEMENT QualityFlag enum with conversions
  - ADD validation methods

Task 3: Configuration Management
CREATE src/config.rs:
  - DEFINE Config struct matching PLANNING.md
  - IMPLEMENT layered config loading (file, env, args)
  - ADD validation for paths and parameters

Task 4: CLI Interface
CREATE src/cli/args.rs:
  - DEFINE clap Args struct following PLANNING.md CLI
  - ADD subcommands for different operations
  - IMPLEMENT argument validation

CREATE src/cli/commands.rs:
  - IMPLEMENT main command dispatch
  - ADD progress reporting with indicatif
  - HANDLE errors gracefully

Task 5: Station Registry Service
CREATE src/app/services/station_registry.rs:
  - IMPLEMENT StationRegistry struct with HashMap<i32, Station>
  - ADD load_capability_files method
  - FILTER records by rec_st_ind = 9
  - PROVIDE O(1) lookup by src_id

Task 6: BADC-CSV Parser
CREATE src/app/services/csv_parser.rs:
  - IMPLEMENT two-section parsing (header + data)
  - HANDLE MIDAS-specific "NA" values
  - PARSE dynamic measurement columns
  - EXTRACT quality flag columns (q_* pattern)
  - AVOID code duplication by the station registry service from Task 5
  - CREATE an ignored integration test to demonstrate Station Registry Service parsing

Task 7: Record Processor
CREATE src/app/services/record_processor.rs:
  - IMPLEMENT deduplication logic (rec_st_ind 1 > 9)
  - ADD station metadata enrichment
  - APPLY quality control filtering
  - HANDLE missing data gracefully

Task 8: Parquet Writer
CREATE src/app/services/parquet_writer.rs:
  - CONFIGURE ArrowWriter with optimal settings
  - IMPLEMENT batch writing with memory management
  - ADD progress reporting
  - HANDLE large datasets efficiently

Task 9: Main Application Logic
MODIFY src/main.rs:
  - IMPLEMENT CLI parsing and dispatch
  - ADD comprehensive error handling
  - CONFIGURE logging with tracing
  - HANDLE graceful shutdown

Task 10: Integration and Testing
CREATE tests/integration/:
  - ADD end-to-end processing tests
  - VALIDATE output against CSV data
  - BENCHMARK performance improvements
  - TEST error recovery scenarios
```

### Implementation Pseudocode

```rust
// Task 5: Station Registry - Critical for O(1) lookups
impl StationRegistry {
    pub async fn load_from_cache(cache_path: &Path) -> Result<Self> {
        // PATTERN: Walk capability directories
        let mut stations = HashMap::new();

        for dataset in datasets {
            let capability_path = cache_path.join(dataset).join("capability");

            // CRITICAL: Recursive directory traversal
            for entry in WalkDir::new(capability_path) {
                if entry.path().extension() == Some("csv") {
                    // PATTERN: Use csv::Reader with StringRecord for performance
                    let mut reader = csv::Reader::from_path(entry.path())?;
                    let mut record = csv::StringRecord::new();

                    while reader.read_record(&mut record)? {
                        // CRITICAL: Filter by rec_st_ind = 9
                        if record.get(REC_ST_IND_COL) == Some("9") {
                            let station = Station::from_record(&record)?;
                            stations.insert(station.src_id, station);
                        }
                    }
                }
            }
        }

        Ok(Self { stations })
    }
}

// Task 6: BADC-CSV Parser - Handle two-section format
impl BadcCsvParser {
    pub fn parse_file(path: &Path) -> Result<Vec<Observation>> {
        let mut reader = csv::Reader::from_path(path)?;

        // CRITICAL: Skip header section until "data" marker
        let mut in_data_section = false;
        let mut observations = Vec::new();

        for result in reader.records() {
            let record = result?;

            // PATTERN: Detect data section start
            if !in_data_section && record.get(0) == Some("data") {
                in_data_section = true;
                continue;
            }

            if in_data_section {
                // PATTERN: Parse dynamic columns based on header
                let observation = Self::parse_observation(&record)?;
                observations.push(observation);
            }
        }

        Ok(observations)
    }

    fn parse_observation(record: &csv::StringRecord) -> Result<Observation> {
        // CRITICAL: Handle "NA" values
        let parse_optional = |val: &str| {
            if val == "NA" { None } else { val.parse().ok() }
        };

        // PATTERN: Extract measurements and quality flags
        let mut measurements = HashMap::new();
        let mut quality_flags = HashMap::new();

        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                if header.starts_with("q_") {
                    let measure = header.trim_start_matches("q_");
                    quality_flags.insert(measure.to_string(),
                        QualityFlag::from_str(field)?);
                } else if !header.contains("_q") {
                    if let Some(value) = parse_optional(field) {
                        measurements.insert(header.clone(), value);
                    }
                }
            }
        }

        Ok(observation)
    }
}

// Task 8: Parquet Writer - Optimized for scientific data
impl ParquetWriter {
    pub fn new(schema: SchemaRef, path: &Path) -> Result<Self> {
        // CRITICAL: Configure for large sequential reads
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(1_000_000)  // 1M rows
            .set_write_batch_size(1024)
            .build();

        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self { writer })
    }

    pub fn write_batch(&mut self, observations: &[Observation]) -> Result<()> {
        // PATTERN: Convert to Arrow RecordBatch
        let batch = self.observations_to_record_batch(observations)?;

        // CRITICAL: Monitor memory usage
        self.writer.write(&batch)?;

        if self.writer.in_progress_size() > MEMORY_LIMIT {
            self.writer.flush()?;
        }

        Ok(())
    }
}
```

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
- ❌ Don't ignore quality control flags - they're critical for data integrity
- ❌ Don't assume all stations have complete data - handle gaps gracefully
- ❌ Don't hardcode dataset names - make them configurable
- ❌ Don't load entire datasets into memory - use streaming processing
- ❌ Don't skip header validation - BADC-CSV format is strict
- ❌ Don't use default Parquet settings - optimize for scientific data access

## Confidence Score: 9/10
This PRP provides comprehensive context for successful one-pass implementation, with detailed technical specifications, proven patterns, and robust validation loops. The only uncertainty is in handling edge cases in real MIDAS data that may not be covered in documentation.
