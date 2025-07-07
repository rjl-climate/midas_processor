//! BADC-CSV parser for MIDAS observation data files
//!
//! This module provides a streamlined parser for BADC-CSV format files focused on
//! robust numerical data extraction and conversion to Parquet format. The design
//! eliminates unnecessary complexity while preserving all essential functionality.
//!
//! ## Architecture
//!
//! The parser is organized into logical components:
//! - [`parser`] - Core parsing orchestration and file handling
//! - [`header`] - BADC-CSV header metadata extraction  
//! - [`column_mapping`] - Dynamic column analysis and categorization
//! - [`record_parser`] - Individual CSV record processing
//! - [`field_parsers`] - Utility functions for field parsing and validation
//! - [`stats`] - Parsing statistics and result structures
//!
//! ## Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use midas_processor::app::services::badc_csv_parser::BadcCsvParser;
//! use midas_processor::app::services::station_registry::StationRegistry;
//!
//! # async fn example(station_registry: Arc<StationRegistry>) -> midas_processor::Result<()> {
//! let parser = BadcCsvParser::new(station_registry);
//! let result = parser.parse_file(std::path::Path::new("data.csv")).await?;
//!
//! println!("Parsed {} observations from {} records",
//!          result.stats.observations_parsed,
//!          result.stats.total_records);
//! # Ok(())
//! # }
//! ```

pub mod column_mapping;
pub mod field_parsers;
pub mod header;
pub mod parser;
pub mod record_parser;
pub mod stats;

#[cfg(test)]
pub mod tests;

// Re-export main types for easy access
pub use column_mapping::ColumnMapping;
pub use header::SimpleHeader;
pub use parser::BadcCsvParser;
pub use stats::{ParseResult, ParseStats};
