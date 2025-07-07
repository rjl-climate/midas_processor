//! MIDAS Processor Library
//!
//! A Rust library for converting UK Met Office MIDAS weather observation data
//! from CSV format into optimized Apache Parquet files.
//!
//! This library provides tools for:
//! - Parsing MIDAS BADC-CSV files with proper header/data section handling
//! - Loading and indexing station metadata for O(1) lookups
//! - Deduplicating records based on quality control status indicators
//! - Enriching observations with station metadata
//! - Writing optimized Parquet files with Snappy compression
//! - Comprehensive error handling and recovery

pub mod config;
pub mod constants;

// Core application modules
pub mod app {
    pub mod models;
    pub mod services {
        pub mod badc_csv_parser;
        pub mod parquet_writer;
        pub mod record_processor;
        pub mod station_registry;
    }
    pub mod adapters {
        pub mod filesystem;
    }
}

// CLI modules
pub mod cli {
    pub mod args;
    pub mod commands;
}

// Re-export commonly used types
pub use app::models::{Observation, QualityFlag, Station};
pub use config::Config;

/// Result type alias for the MIDAS processor
pub type Result<T> = std::result::Result<T, Error>;

/// Comprehensive error types for MIDAS processing operations
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O operation failed
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    /// CSV parsing error
    #[error("CSV parsing error in file '{file}': {message}")]
    CsvParsing {
        file: String,
        message: String,
        #[source]
        source: Option<csv::Error>,
    },

    /// BADC-CSV format error
    #[error("BADC-CSV format error in file '{file}': {message}")]
    BadcFormat { file: String, message: String },

    /// Parquet writing error
    #[error("Parquet writing error: {message}")]
    ParquetWriting {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Arrow data processing error (temporarily disabled)
    #[error("Arrow data processing error: {message}")]
    ArrowProcessing {
        message: String,
        // #[source]
        // source: arrow::error::ArrowError,
    },

    /// Configuration error
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Station registry error
    #[error("Station registry error: {message}")]
    StationRegistry { message: String },

    /// Station not found
    #[error("Station not found: src_id = {src_id}")]
    StationNotFound { src_id: i32 },

    /// Data validation error
    #[error("Data validation error: {message}")]
    DataValidation { message: String },

    /// Quality control error
    #[error("Quality control error: {message}")]
    QualityControl { message: String },

    /// Date/time parsing error
    #[error("Date/time parsing error: {message}")]
    DateTimeParsing {
        message: String,
        #[source]
        source: chrono::ParseError,
    },

    /// File not found
    #[error("File not found: {path}")]
    FileNotFound { path: String },

    /// Directory traversal error
    #[error("Directory traversal error: {message}")]
    DirectoryTraversal {
        message: String,
        #[source]
        source: walkdir::Error,
    },

    /// Memory limit exceeded
    #[error("Memory limit exceeded: current usage {current_mb}MB exceeds limit {limit_mb}MB")]
    MemoryLimitExceeded { current_mb: usize, limit_mb: usize },

    /// Processing interrupted
    #[error("Processing interrupted: {reason}")]
    ProcessingInterrupted { reason: String },

    /// Unknown dataset type
    #[error("Unknown dataset type: {dataset_name}")]
    UnknownDataset { dataset_name: String },
}

impl Error {
    /// Create an I/O error with context
    pub fn io(message: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            message: message.into(),
            source,
        }
    }

    /// Create a CSV parsing error with context
    pub fn csv_parsing(
        file: impl Into<String>,
        message: impl Into<String>,
        source: Option<csv::Error>,
    ) -> Self {
        Self::CsvParsing {
            file: file.into(),
            message: message.into(),
            source,
        }
    }

    /// Create a BADC format error
    pub fn badc_format(file: impl Into<String>, message: impl Into<String>) -> Self {
        Self::BadcFormat {
            file: file.into(),
            message: message.into(),
        }
    }

    /// Create a Parquet writing error
    pub fn parquet_writing(
        message: impl Into<String>,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::ParquetWriting {
            message: message.into(),
            source,
        }
    }

    /// Create an Arrow processing error (temporarily disabled)
    pub fn arrow_processing(message: impl Into<String>) -> Self {
        Self::ArrowProcessing {
            message: message.into(),
            // source,
        }
    }

    /// Create a configuration error
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a station registry error
    pub fn station_registry(message: impl Into<String>) -> Self {
        Self::StationRegistry {
            message: message.into(),
        }
    }

    /// Create a station not found error
    pub fn station_not_found(src_id: i32) -> Self {
        Self::StationNotFound { src_id }
    }

    /// Create a data validation error
    pub fn data_validation(message: impl Into<String>) -> Self {
        Self::DataValidation {
            message: message.into(),
        }
    }

    /// Create a quality control error
    pub fn quality_control(message: impl Into<String>) -> Self {
        Self::QualityControl {
            message: message.into(),
        }
    }

    /// Create a date/time parsing error
    pub fn datetime_parsing(message: impl Into<String>, source: chrono::ParseError) -> Self {
        Self::DateTimeParsing {
            message: message.into(),
            source,
        }
    }

    /// Create a file not found error
    pub fn file_not_found(path: impl Into<String>) -> Self {
        Self::FileNotFound { path: path.into() }
    }

    /// Create a directory traversal error
    pub fn directory_traversal(message: impl Into<String>, source: walkdir::Error) -> Self {
        Self::DirectoryTraversal {
            message: message.into(),
            source,
        }
    }

    /// Create a memory limit exceeded error
    pub fn memory_limit_exceeded(current_mb: usize, limit_mb: usize) -> Self {
        Self::MemoryLimitExceeded {
            current_mb,
            limit_mb,
        }
    }

    /// Create a processing interrupted error
    pub fn processing_interrupted(reason: impl Into<String>) -> Self {
        Self::ProcessingInterrupted {
            reason: reason.into(),
        }
    }

    /// Create an unknown dataset error
    pub fn unknown_dataset(dataset_name: impl Into<String>) -> Self {
        Self::UnknownDataset {
            dataset_name: dataset_name.into(),
        }
    }

    /// Create an I/O error with a simple message
    pub fn io_error(message: impl Into<String>) -> Self {
        let message_str = message.into();
        Self::Io {
            message: message_str.clone(),
            source: std::io::Error::new(std::io::ErrorKind::Other, message_str),
        }
    }

    /// Create a file format error
    pub fn file_format(message: impl Into<String>) -> Self {
        Self::BadcFormat {
            file: "unknown".to_string(),
            message: message.into(),
        }
    }
}

// Automatic conversions from common error types
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            message: "I/O operation failed".to_string(),
            source: error,
        }
    }
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Self {
        Self::CsvParsing {
            file: "unknown".to_string(),
            message: "CSV parsing failed".to_string(),
            source: Some(error),
        }
    }
}

// Temporarily disabled due to arrow version conflict
// impl From<arrow::error::ArrowError> for Error {
//     fn from(error: arrow::error::ArrowError) -> Self {
//         Self::ArrowProcessing {
//             message: "Arrow processing failed".to_string(),
//             source: error,
//         }
//     }
// }

impl From<chrono::ParseError> for Error {
    fn from(error: chrono::ParseError) -> Self {
        Self::DateTimeParsing {
            message: "Date/time parsing failed".to_string(),
            source: error,
        }
    }
}

impl From<walkdir::Error> for Error {
    fn from(error: walkdir::Error) -> Self {
        Self::DirectoryTraversal {
            message: "Directory traversal failed".to_string(),
            source: error,
        }
    }
}
