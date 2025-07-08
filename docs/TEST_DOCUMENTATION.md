# Test Documentation

This document provides a comprehensive overview of all tests in the MIDAS Processor codebase, organized by module and explaining what each test validates.

## Overview
- **Total Tests**: 57 unit tests
- **Test Categories**: Models, Services, CLI, Configuration, Constants
- **Test Framework**: Standard Rust `#[test]` and `#[tokio::test]` for async tests

---

## Station Registry Tests (`src/app/services/station_registry.rs`)
**Purpose**: Validate the core station metadata lookup service

### Core Functionality Tests

#### `test_station_registry_new()`
- **Purpose**: Verifies StationRegistry constructor creates empty registry with correct initial state
- **Validates**: Empty stations map, correct cache path, empty datasets list

#### `test_load_from_cache_success()`
- **Purpose**: Tests complete station loading from mock MIDAS cache structure
- **Validates**: 
  - Correct number of stations loaded (3 stations from 2 files)
  - Proper BADC-CSV parsing with rec_st_ind filtering
  - Duplicate handling (1 duplicate ignored)
  - Load statistics accuracy
- **Test Data**: Mock cache with 2 datasets, 5 total records, 2 filtered out

#### `test_load_from_cache_nonexistent_path()`
- **Purpose**: Ensures proper error handling for invalid cache paths
- **Validates**: Returns `Error::StationRegistry` with appropriate error message

### File Discovery Tests

#### `test_discover_capability_files()`
- **Purpose**: Tests recursive discovery of CSV capability files in cache structure
- **Validates**: Finds correct number of capability files in nested directories

#### `test_discover_capability_files_missing_dataset()`
- **Purpose**: Handles graceful fallback when dataset directories don't exist
- **Validates**: Returns empty list without errors for missing datasets

### BADC-CSV Parsing Tests

#### `test_load_capability_file()`
- **Purpose**: Tests parsing of individual BADC-CSV capability files
- **Validates**:
  - Correct parsing of BADC format (header section → data marker → CSV data)
  - Record filtering by rec_st_ind (only records with value 9)
  - Returns both station list and total record count

#### `test_parse_station_record_valid()`
- **Purpose**: Validates parsing of individual station records from CSV
- **Validates**: All required fields correctly parsed into Station struct

#### `test_parse_station_record_filtered()`
- **Purpose**: Tests filtering of non-definitive records
- **Validates**: Records with rec_st_ind ≠ 9 are filtered out (returns None)

### Lookup Method Tests

#### `test_station_lookup_methods()`
- **Purpose**: Comprehensive test of all station lookup functionality
- **Validates**:
  - **Basic lookups**: get_station(), contains_station(), station_count()
  - **Name search**: Case-insensitive partial matching
  - **Regional search**: Geographic bounding box queries
  - **Temporal search**: Active stations during date ranges
- **Test Data**: 2 stations (Heathrow, Birmingham) with different operational periods

### Utility Method Tests

#### `test_station_ids_and_stations()`
- **Purpose**: Tests collection methods for all stations and IDs
- **Validates**: station_ids() and stations() return complete collections

#### `test_registry_metadata()`
- **Purpose**: Validates metadata accessor method
- **Validates**: RegistryMetadata contains correct cache path, datasets, counts

---

## Model Tests (`src/app/models.rs`)
**Purpose**: Validate core data structures (Station, Observation, QualityFlag)

### Station Model Tests

#### `test_station_creation_valid()`
- **Purpose**: Tests Station constructor with valid input
- **Validates**: All fields correctly assigned, validation passes

#### `test_station_required_fields()`
- **Purpose**: Ensures required field validation works
- **Validates**: Constructor rejects missing/invalid required fields

#### `test_station_coordinate_validation()`
- **Purpose**: Tests geographic coordinate bounds checking
- **Validates**: Latitude (-90 to 90) and longitude (-180 to 180) validation

#### `test_station_date_validation()`
- **Purpose**: Tests temporal validation rules
- **Validates**: Start date must be before end date

#### `test_station_grid_reference_pairing()`
- **Purpose**: Tests OSGB grid reference validation
- **Validates**: East/north grid references must be paired (both or neither)

#### `test_station_location_methods()`
- **Purpose**: Tests geographic utility methods
- **Validates**: Location formatting and coordinate access

#### `test_station_activity_checks()`
- **Purpose**: Tests temporal activity queries
- **Validates**: is_active_on() correctly determines operational status

### Observation Model Tests

#### `test_observation_creation_valid()`
- **Purpose**: Tests Observation constructor with valid data
- **Validates**: All measurement fields and metadata correctly assigned

#### `test_observation_measurement_access()`
- **Purpose**: Tests measurement value accessors
- **Validates**: get_measurement() returns correct values for different types

#### `test_observation_usable_measurements()`
- **Purpose**: Tests quality filtering of measurements
- **Validates**: Only measurements with "usable" quality flags are returned

#### `test_observation_supersedes()`
- **Purpose**: Tests record precedence logic
- **Validates**: Higher quality records supersede lower quality ones

#### `test_observation_station_id_mismatch()`
- **Purpose**: Tests validation of station ID consistency
- **Validates**: Constructor rejects mismatched station IDs

#### `test_observation_time_outside_station_period()`
- **Purpose**: Tests temporal consistency validation
- **Validates**: Observation time must fall within station operational period

#### `test_observation_invalid_record_status()`
- **Purpose**: Tests record status validation
- **Validates**: Only valid rec_st_ind values are accepted

#### `test_observation_invalid_hour_count()`
- **Purpose**: Tests hour count validation for aggregated data
- **Validates**: Hour count must be positive for aggregated measurements

### Quality Flag Tests

#### `test_quality_flag_from_i8()` / `test_quality_flag_from_string()`
- **Purpose**: Tests QualityFlag parsing from different input types
- **Validates**: Correct conversion from numeric and string representations

#### `test_quality_flag_to_i8()`
- **Purpose**: Tests QualityFlag serialization
- **Validates**: Correct conversion back to numeric format

#### `test_quality_flag_display()`
- **Purpose**: Tests human-readable formatting
- **Validates**: Display trait produces expected output

#### `test_quality_flag_description()`
- **Purpose**: Tests descriptive text for quality flags
- **Validates**: Each flag has appropriate human-readable description

#### `test_quality_flag_usability()`
- **Purpose**: Tests quality-based data filtering
- **Validates**: is_usable() correctly identifies high-quality data

#### `test_quality_flag_all_values()`
- **Purpose**: Tests comprehensive coverage of all quality flag values
- **Validates**: All defined quality flags can be parsed and converted

### Serialization Tests

#### `test_serde_serialization()`
- **Purpose**: Tests JSON serialization/deserialization of all models
- **Validates**: All structs correctly serialize to/from JSON format

---

## CLI Tests (`src/cli/`)
**Purpose**: Validate command-line interface and argument parsing

### Argument Parsing Tests (`src/cli/args.rs`)

#### `test_dataset_list_parsing()`
- **Purpose**: Tests parsing of comma-separated dataset lists
- **Validates**: parse_dataset_list() correctly splits and trims dataset names

#### `test_get_datasets()`
- **Purpose**: Tests dataset resolution logic
- **Validates**: Uses provided datasets or falls back to defaults

#### `test_show_progress()`
- **Purpose**: Tests progress bar display logic
- **Validates**: Progress shown unless explicitly disabled

#### `test_log_level()`
- **Purpose**: Tests logging verbosity calculation
- **Validates**: Correct log level based on quiet/verbose flags

#### `test_args_validation()`
- **Purpose**: Tests command-line argument validation
- **Validates**: Required arguments and valid combinations

### Command Processing Tests (`src/cli/commands.rs`)

#### `test_processing_stats()`
- **Purpose**: Tests statistics collection and formatting
- **Validates**: ProcessingStats correctly accumulates metrics

#### `test_format_size()`
- **Purpose**: Tests human-readable size formatting
- **Validates**: Bytes converted to KB/MB/GB with appropriate precision

#### `test_dry_run()`
- **Purpose**: Tests dry-run mode functionality
- **Validates**: No actual file operations performed in dry-run mode

#### `test_apply_cli_overrides()`
- **Purpose**: Tests command-line configuration overrides
- **Validates**: CLI arguments correctly override configuration file values

#### `test_prepare_directories()`
- **Purpose**: Tests output directory creation logic
- **Validates**: Required directories created with correct permissions

#### `test_is_critical_error()`
- **Purpose**: Tests error classification for recovery decisions
- **Validates**: Critical vs. recoverable error identification

---

## Configuration Tests (`src/config.rs`)
**Purpose**: Validate configuration loading and validation

### Configuration Creation Tests

#### `test_config_creation()`
- **Purpose**: Tests Config struct instantiation
- **Validates**: Default values and field assignment

#### `test_config_defaults()`
- **Purpose**: Tests default configuration values
- **Validates**: All defaults are sensible for typical usage

#### `test_config_validation()`
- **Purpose**: Tests configuration validation rules
- **Validates**: Invalid configurations are rejected with clear errors

### Configuration Loading Tests

#### `test_config_file_parsing()`
- **Purpose**: Tests TOML configuration file parsing
- **Validates**: Files correctly parsed into Config structs

#### `test_config_env_vars()`
- **Purpose**: Tests environment variable override support
- **Validates**: Environment variables correctly override file values

#### `test_config_merge()`
- **Purpose**: Tests configuration layering (file → env → CLI)
- **Validates**: Correct precedence order maintained

### Configuration Utility Tests

#### `test_memory_limit_bytes()`
- **Purpose**: Tests memory limit calculation
- **Validates**: Human-readable sizes converted to bytes correctly

---

## Constants Tests (`src/constants.rs`)
**Purpose**: Validate constant definitions and utility functions

### Measurement Processing Tests

#### `test_measurement_extraction()`
- **Purpose**: Tests measurement type detection from column names
- **Validates**: Correct identification of temperature, rainfall, etc.

#### `test_quality_flag_column_detection()`
- **Purpose**: Tests quality flag column identification
- **Validates**: Quality columns correctly matched to measurement columns

#### `test_usable_quality()`
- **Purpose**: Tests quality flag usability determination
- **Validates**: Only high-quality flags marked as usable

### Utility Function Tests

#### `test_output_filenames()`
- **Purpose**: Tests output file naming conventions
- **Validates**: Consistent naming patterns for processed files

#### `test_quality_flag_descriptions()`
- **Purpose**: Tests quality flag documentation
- **Validates**: All flags have descriptive text for user interfaces

---

## Test Coverage Summary

### By Module:
- **Station Registry**: 11 tests (core functionality)
- **Models**: 18 tests (data structures and validation)
- **CLI**: 10 tests (argument parsing and commands)
- **Configuration**: 8 tests (loading and validation)
- **Constants**: 5 tests (utility functions)
- **Quality Flags**: 8 tests (data quality management)

### Test Types:
- **Unit Tests**: 57 (isolated component testing)
- **Integration Tests**: Embedded in unit tests with mock data
- **Validation Tests**: Input validation and error handling
- **Functionality Tests**: Core business logic verification

### Coverage Areas:
- ✅ **Data Models**: Complete validation of all struct fields and methods
- ✅ **File Parsing**: BADC-CSV format handling with edge cases
- ✅ **Database Operations**: Station registry O(1) lookups
- ✅ **Error Handling**: All error paths tested with expected outcomes
- ✅ **CLI Interface**: Argument parsing and command validation
- ✅ **Configuration**: Multi-layer config loading and validation
- ✅ **Quality Control**: Data quality assessment and filtering