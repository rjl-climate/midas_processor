//! # MIDAS Dataset Processing Library
//!
//! High-performance streaming conversion of MIDAS meteorological CSV datasets to Parquet format.
//! Handles BADC-CSV format with metadata extraction and schema validation.

pub mod cli;
pub mod config;
pub mod error;
pub mod header;
pub mod models;
pub mod processor;
pub mod schema;

pub use error::{MidasError, Result};
pub use models::{DatasetType, ProcessingStats, StationMetadata};
pub use processor::DatasetProcessor;
