//! Error handling for MIDAS processing operations.
//!
//! Provides comprehensive error types with context for file processing,
//! schema validation, and data conversion failures.

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MidasError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("Dataset not found at path: {path}")]
    DatasetNotFound { path: PathBuf },

    #[error("Invalid BADC-CSV format in file: {path} - {reason}")]
    InvalidFormat { path: PathBuf, reason: String },

    #[error(
        "Schema mismatch in dataset {dataset_type}: expected {expected} columns, found {found}"
    )]
    SchemaMismatch {
        dataset_type: String,
        expected: usize,
        found: usize,
    },

    #[error("Header parsing failed for file: {path} - {reason}")]
    HeaderParsingFailed { path: PathBuf, reason: String },

    #[error("No data marker found in file: {path}")]
    NoDataMarker { path: PathBuf },

    #[error("Processing failed for file: {path} - {reason}")]
    ProcessingFailed { path: PathBuf, reason: String },

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Schema union failed: {reason}. Files: {file_count}, Schemas: {schema_diff}")]
    SchemaUnionFailed {
        reason: String,
        file_count: usize,
        schema_diff: String,
    },

    #[error("Incompatible schemas in dataset {dataset_type}: {details}")]
    IncompatibleSchemas {
        dataset_type: String,
        details: String,
    },
}

pub type Result<T> = std::result::Result<T, MidasError>;
