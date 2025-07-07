//! Application constants for MIDAS processor
//!
//! This module contains all configuration constants, default values,
//! and mappings used throughout the MIDAS processor application.

// use std::collections::HashMap;  // Currently unused

// =============================================================================
// Dataset Names and File Patterns
// =============================================================================

/// Supported MIDAS dataset names
pub const DATASET_NAMES: &[&str] = &[
    "uk-daily-temperature-obs",
    "uk-daily-weather-obs",
    "uk-daily-rain-obs",
    "uk-hourly-weather-obs",
    "uk-hourly-rain-obs",
    "uk-mean-wind-obs",
    "uk-radiation-obs",
    "uk-soil-temperature-obs",
];

/// Default datasets to process if none specified
pub const DEFAULT_DATASETS: &[&str] = &["uk-daily-temperature-obs", "uk-daily-rain-obs"];

/// Capability directory name within each dataset
pub const CAPABILITY_DIR_NAME: &str = "capability";

/// QC version directory pattern (latest is qcv-1)
pub const QC_VERSION_LATEST: &str = "qcv-1";
pub const QC_VERSION_ORIGINAL: &str = "qcv-0";

/// Station metadata file patterns
pub const STATION_METADATA_PATTERN: &str = "*.csv";

/// Observation data file patterns  
pub const OBSERVATION_FILE_PATTERN: &str = "*.csv";

// =============================================================================
// Quality Control Constants
// =============================================================================

/// Quality control flag values as defined in MIDAS specification
pub mod quality_flags {
    /// Passed all QC checks - highest quality data
    pub const VALID: i8 = 0;

    /// Failed at least one QC check - use with caution
    pub const SUSPECT: i8 = 1;

    /// Considered incorrect - should not be used
    pub const ERRONEOUS: i8 = 2;

    /// No QC applied to this data
    pub const NOT_CHECKED: i8 = 3;

    /// Value reverted to original after previous modifications
    pub const REVERTED: i8 = 6;

    /// No quality information available (missing data)
    pub const MISSING: i8 = 9;

    /// All basic quality flag values
    pub const BASIC_VALUES: &[i8] = &[VALID, SUSPECT, ERRONEOUS, NOT_CHECKED, REVERTED, MISSING];
}

/// Record status indicator values for MIDAS observation records
///
/// These values represent the current stage in the life of a record within the MIDAS system.
/// Based on empirical analysis of real MIDAS data, the most common values are:
/// - 1001: Most common status (518,203 records) - likely processed/quality-assured data
/// - 1011: Second most common (271,293 records) - likely alternate processing stage
/// - 1022: Third most common (26,913 records) - likely corrected/revised data
/// - 2001: Fourth most common (20,873 records) - likely final archived data
pub mod record_status {
    /// Original record - use if no corrected version exists (legacy value)
    pub const ORIGINAL: i8 = 9;

    /// Corrected/updated record - supersedes original (legacy value)
    pub const CORRECTED: i8 = 1;

    // Common values found in real MIDAS data (stored as i32 since rec_st_ind is i32)
    /// Most common status - likely processed/quality-assured data
    pub const PROCESSED: i32 = 1001;

    /// Second most common - likely alternate processing stage
    pub const ALTERNATE_PROCESSING: i32 = 1011;

    /// Third most common - likely corrected/revised data
    pub const REVISED: i32 = 1022;

    /// Fourth most common - likely final archived data
    pub const ARCHIVED: i32 = 2001;

    /// Other processing stages found in data
    pub const PROCESSING_STAGE_1025: i32 = 1025;
    pub const PROCESSING_STAGE_2022: i32 = 2022;
    pub const PROCESSING_STAGE_1010: i32 = 1010;
    pub const PROCESSING_STAGE_1026: i32 = 1026;
    pub const PROCESSING_STAGE_2011: i32 = 2011;
    pub const PROCESSING_STAGE_1012: i32 = 1012;

    /// All valid record status indicator values found in real MIDAS data
    pub const ALL_VALID_VALUES: &[i32] = &[
        1, 9, // Legacy values
        1001, 1011, 1022, 2001, 1025, 2022, 1010, 1026, 2011, 1012, // Common values
        1004, 15, 7, 82, 57, 26, 24, 21, 2025, 2004, 2026, // Less common but valid values
    ];
}

/// Quality control version preferences
pub const MIN_QUALITY_VERSION: i32 = 1;
pub const PREFERRED_QC_VERSION: &str = QC_VERSION_LATEST;

// =============================================================================
// Parquet Writer Configuration
// =============================================================================

/// Parquet compression algorithm (optimized for fast decompression)
pub const PARQUET_COMPRESSION: &str = "snappy";

/// Row group size for optimal sequential read performance (1M+ rows)
pub const PARQUET_ROW_GROUP_SIZE: usize = 1_000_000;

/// Page size in MB for Parquet files
pub const PARQUET_PAGE_SIZE_MB: usize = 1;

/// Write batch size for memory management
pub const PARQUET_WRITE_BATCH_SIZE: usize = 1024;

/// Maximum memory usage before forcing flush (in bytes)
pub const PARQUET_MEMORY_LIMIT_BYTES: usize = 100 * 1024 * 1024; // 100MB

// =============================================================================
// Processing Configuration Defaults
// =============================================================================

/// Default number of parallel workers
pub const DEFAULT_PARALLEL_WORKERS: usize = 8;

/// Default memory limit in GB
pub const DEFAULT_MEMORY_LIMIT_GB: usize = 16;

/// Memory limit in bytes (converted from GB)
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = DEFAULT_MEMORY_LIMIT_GB * 1024 * 1024 * 1024;

/// Default quality control settings
pub const DEFAULT_INCLUDE_SUSPECT: bool = false;
pub const DEFAULT_INCLUDE_UNCHECKED: bool = false;

// =============================================================================
// File and Directory Constants
// =============================================================================

/// BADC-CSV data section marker
pub const BADC_DATA_SECTION_MARKER: &str = "data";

/// BADC-CSV end section marker
pub const BADC_END_SECTION_MARKER: &str = "end data";

/// Missing value indicator in MIDAS CSV files
pub const MIDAS_MISSING_VALUE: &str = "NA";

/// Station metadata output filename
pub const STATIONS_OUTPUT_FILENAME: &str = "stations.parquet";

/// Metadata directory name in output
pub const METADATA_OUTPUT_DIR: &str = "metadata";

/// Processing log filename
pub const PROCESSING_LOG_FILENAME: &str = "processing_log.json";

// =============================================================================
// Data Type Constants for Python Compatibility
// =============================================================================

/// Timestamp conversion factor (nanoseconds per second for pandas datetime64[ns])
pub const TIMESTAMP_NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Default datetime format for MIDAS timestamps
pub const MIDAS_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Default date format for MIDAS dates
pub const MIDAS_DATE_FORMAT: &str = "%Y-%m-%d";

// =============================================================================
// Column Name Constants
// =============================================================================

/// Standard column names in MIDAS data
pub mod columns {
    // Temporal columns
    pub const OB_END_TIME: &str = "ob_end_time";
    pub const OB_HOUR_COUNT: &str = "ob_hour_count";

    // Station reference columns
    pub const ID: &str = "id";
    pub const ID_TYPE: &str = "id_type";
    pub const SRC_ID: &str = "src_id";

    // Record metadata columns
    pub const MET_DOMAIN_NAME: &str = "met_domain_name";
    pub const REC_ST_IND: &str = "rec_st_ind";
    pub const VERSION_NUM: &str = "version_num";

    // Station metadata columns
    pub const SRC_NAME: &str = "src_name";
    pub const HIGH_PRCN_LAT: &str = "high_prcn_lat";
    pub const HIGH_PRCN_LON: &str = "high_prcn_lon";
    pub const EAST_GRID_REF: &str = "east_grid_ref";
    pub const NORTH_GRID_REF: &str = "north_grid_ref";
    pub const GRID_REF_TYPE: &str = "grid_ref_type";
    pub const SRC_BGN_DATE: &str = "src_bgn_date";
    pub const SRC_END_DATE: &str = "src_end_date";
    pub const AUTHORITY: &str = "authority";
    pub const HISTORIC_COUNTY: &str = "historic_county";
    pub const HEIGHT_METERS: &str = "height_meters";

    // Processing metadata columns
    pub const METO_STMP_TIME: &str = "meto_stmp_time";
    pub const MIDAS_STMP_ETIME: &str = "midas_stmp_etime";
}

/// Quality flag column prefix
pub const QUALITY_FLAG_PREFIX: &str = "q_";

/// ID type value for source-based lookups
pub const ID_TYPE_SOURCE: &str = "SRCE";

// =============================================================================
// Performance and Monitoring Constants
// =============================================================================

/// Progress reporting update interval (number of processed items)
pub const PROGRESS_UPDATE_INTERVAL: usize = 1000;

/// Log level for different operation types
pub const LOG_LEVEL_INFO: &str = "info";
pub const LOG_LEVEL_WARN: &str = "warn";
pub const LOG_LEVEL_ERROR: &str = "error";

/// Retry constants for transient errors
pub const MAX_RETRY_ATTEMPTS: usize = 3;
pub const RETRY_DELAY_MS: u64 = 100;

// =============================================================================
// Helper Functions
// =============================================================================

/// Get quality flag description for human-readable output
pub fn quality_flag_description(flag: i8) -> &'static str {
    match flag {
        quality_flags::VALID => "Valid - passed all QC checks",
        quality_flags::SUSPECT => "Suspect - failed at least one QC check",
        quality_flags::ERRONEOUS => "Erroneous - considered incorrect",
        quality_flags::NOT_CHECKED => "Not checked - no QC applied",
        quality_flags::REVERTED => "Reverted - value reverted to original",
        quality_flags::MISSING => "Missing - no data available",
        _ => "Extended quality flag",
    }
}

/// Get record status description
pub fn record_status_description(status: i32) -> &'static str {
    match status {
        // Legacy values
        1 => "Corrected/updated record (legacy)",
        9 => "Original record (legacy)",

        // Common MIDAS values based on empirical analysis
        1001 => "Processed/quality-assured data",
        1011 => "Alternate processing stage",
        1022 => "Corrected/revised data",
        2001 => "Final archived data",

        // Other processing stages
        1025 => "Processing stage 1025",
        2022 => "Processing stage 2022",
        1010 => "Processing stage 1010",
        1026 => "Processing stage 1026",
        2011 => "Processing stage 2011",
        1012 => "Processing stage 1012",
        1004 => "Processing stage 1004",

        // Uncommon but valid values
        15 => "Processing stage 15",
        7 => "Processing stage 7",
        82 => "Processing stage 82",
        57 => "Processing stage 57",
        26 => "Processing stage 26",
        24 => "Processing stage 24",
        21 => "Processing stage 21",
        2025 => "Processing stage 2025",
        2004 => "Processing stage 2004",
        2026 => "Processing stage 2026",

        _ => "Unknown record status",
    }
}

/// Check if a quality flag represents usable data
pub fn is_usable_quality(flag: i8, include_suspect: bool, include_unchecked: bool) -> bool {
    match flag {
        quality_flags::VALID => true,
        quality_flags::SUSPECT => include_suspect,
        quality_flags::NOT_CHECKED => include_unchecked,
        quality_flags::ERRONEOUS | quality_flags::MISSING => false,
        _ => false,
    }
}

/// Get the expected output filename for a dataset
pub fn get_output_filename(dataset_name: &str) -> String {
    format!("{}.parquet", dataset_name)
}

/// Get the expected metadata filename for a dataset  
pub fn get_metadata_filename(dataset_name: &str) -> String {
    format!("{}.metadata.json", dataset_name)
}

/// Check if a column name represents a quality flag
pub fn is_quality_flag_column(column_name: &str) -> bool {
    column_name.starts_with(QUALITY_FLAG_PREFIX)
}

/// Extract the measurement name from a quality flag column name
pub fn extract_measurement_from_quality_column(quality_column: &str) -> Option<&str> {
    if is_quality_flag_column(quality_column) {
        Some(&quality_column[QUALITY_FLAG_PREFIX.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quality_flag_descriptions() {
        assert_eq!(
            quality_flag_description(quality_flags::VALID),
            "Valid - passed all QC checks"
        );
        assert_eq!(
            quality_flag_description(quality_flags::SUSPECT),
            "Suspect - failed at least one QC check"
        );
        assert_eq!(quality_flag_description(99), "Extended quality flag");
    }

    #[test]
    fn test_usable_quality() {
        // Valid data is always usable
        assert!(is_usable_quality(quality_flags::VALID, false, false));

        // Suspect data depends on include_suspect flag
        assert!(!is_usable_quality(quality_flags::SUSPECT, false, false));
        assert!(is_usable_quality(quality_flags::SUSPECT, true, false));

        // Unchecked data depends on include_unchecked flag
        assert!(!is_usable_quality(quality_flags::NOT_CHECKED, false, false));
        assert!(is_usable_quality(quality_flags::NOT_CHECKED, false, true));

        // Erroneous and missing data is never usable
        assert!(!is_usable_quality(quality_flags::ERRONEOUS, true, true));
        assert!(!is_usable_quality(quality_flags::MISSING, true, true));
    }

    #[test]
    fn test_quality_flag_column_detection() {
        assert!(is_quality_flag_column("q_air_temperature"));
        assert!(is_quality_flag_column("q_wind_speed"));
        assert!(!is_quality_flag_column("air_temperature"));
        assert!(!is_quality_flag_column("temperature_q"));
    }

    #[test]
    fn test_measurement_extraction() {
        assert_eq!(
            extract_measurement_from_quality_column("q_air_temperature"),
            Some("air_temperature")
        );
        assert_eq!(
            extract_measurement_from_quality_column("q_wind_speed"),
            Some("wind_speed")
        );
        assert_eq!(
            extract_measurement_from_quality_column("air_temperature"),
            None
        );
    }

    #[test]
    fn test_output_filenames() {
        assert_eq!(
            get_output_filename("uk-daily-temperature-obs"),
            "uk-daily-temperature-obs.parquet"
        );
        assert_eq!(
            get_metadata_filename("uk-daily-rain-obs"),
            "uk-daily-rain-obs.metadata.json"
        );
    }
}
