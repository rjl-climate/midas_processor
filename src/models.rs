//! Core data structures and types for MIDAS processing.
//!
//! Defines dataset types, metadata structures, processing statistics,
//! and configuration objects used throughout the library.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Dataset types supported by MIDAS
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatasetType {
    Rain,
    Temperature,
    Wind,
    Radiation,
}

impl DatasetType {
    /// Detect dataset type from filename pattern
    pub fn from_path(path: &Path) -> Option<Self> {
        let path_str = path.to_string_lossy().to_lowercase();

        if path_str.contains("rain") {
            Some(DatasetType::Rain)
        } else if path_str.contains("temperature") {
            Some(DatasetType::Temperature)
        } else if path_str.contains("wind") {
            Some(DatasetType::Wind)
        } else if path_str.contains("radiation") {
            Some(DatasetType::Radiation)
        } else {
            None
        }
    }

    /// Get the expected column count for this dataset type
    pub fn expected_columns(&self) -> usize {
        match self {
            DatasetType::Rain => 15,
            DatasetType::Temperature => 22,
            DatasetType::Wind => 24,
            DatasetType::Radiation => 12, // Estimated based on pattern
        }
    }

    /// Get the primary time column name for this dataset type
    /// This is the column used for temporal sorting and indexing
    pub fn primary_time_column(&self) -> &'static str {
        match self {
            DatasetType::Rain => "ob_end_ctime",
            DatasetType::Temperature => "ob_end_time",
            DatasetType::Wind => "ob_end_time",
            DatasetType::Radiation => "ob_end_time", // To be verified
        }
    }
}

/// Metadata extracted from BADC-CSV headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationMetadata {
    pub station_name: String,
    pub station_id: String,
    pub county: String,
    pub latitude: f64,
    pub longitude: f64,
    pub height: f64,
    pub height_units: String,
}

/// File processing boundaries
#[derive(Debug, Clone)]
pub struct DataBoundaries {
    pub skip_rows: usize,
    pub data_rows: Option<usize>,
    pub total_lines: usize,
}

/// Processing statistics
#[derive(Debug, Default)]
pub struct ProcessingStats {
    pub files_processed: usize,
    pub files_failed: usize,
    pub total_rows: usize,
    pub output_path: PathBuf,
    pub processing_time_ms: u128,
}

/// Configuration for a specific dataset type
#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub dataset_type: DatasetType,
    pub schema: polars::prelude::Schema,
    pub empty_columns: Vec<String>,
    pub common_patterns: CommonPatterns,
}

/// Common patterns for column elimination
#[derive(Debug, Clone)]
pub struct CommonPatterns {
    pub always_na_columns: Vec<String>,
    pub descriptor_columns: Vec<String>,
    pub timestamp_columns: Vec<String>,
}

impl DatasetConfig {
    /// Create a new DatasetConfig with provided schema and analysis results
    pub fn new(
        dataset_type: DatasetType,
        schema: polars::prelude::Schema,
        empty_columns: Vec<String>,
        common_patterns: CommonPatterns,
    ) -> Self {
        Self {
            dataset_type,
            schema,
            empty_columns,
            common_patterns,
        }
    }
}
