//! Record processing module for MIDAS observations
//!
//! This module provides a complete pipeline for processing MIDAS observations after CSV parsing.
//! It handles station metadata re-enrichment, deduplication, and quality control filtering
//! while maintaining a clear separation between original MIDAS QC flags and processing QC tracking.
//!
//! # Architecture
//!
//! The module is organized into logical components:
//! - [`processor`] - Main RecordProcessor struct and pipeline orchestration
//! - [`enrichment`] - Station metadata re-enrichment logic
//! - [`deduplication`] - Record deduplication and priority rules
//! - [`quality_filter`] - Quality control filtering (without interpreting original QC flags)
//! - [`stats`] - Processing statistics and result structures
//!
//! # Processing Pipeline
//!
//! The standard processing pipeline consists of three main stages:
//!
//! 1. **Station Re-enrichment**: Fix observations with missing or placeholder station metadata
//! 2. **Deduplication**: Apply MIDAS record status rules and handle duplicate measurements
//! 3. **Quality Filtering**: Apply simple configuration-based filtering
//!
//! # Quality Control Philosophy
//!
//! This module implements a dual quality control system:
//!
//! - **Original MIDAS QC Flags**: Passed through exactly as-is without interpretation
//! - **Processing QC Flags**: Simple binary tracking of processing operations
//!
//! This design avoids the complexity explosion that comes from attempting to interpret
//! often inconsistent BADC QC codes while still providing visibility into processing operations.
//!
//! # Example Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use midas_processor::app::services::record_processor::RecordProcessor;
//! use midas_processor::app::services::station_registry::StationRegistry;
//! use midas_processor::config::QualityControlConfig;
//!
//! # async fn example(observations: Vec<midas_processor::app::models::Observation>) -> midas_processor::Result<()> {
//! // Set up dependencies
//! let registry = Arc::new(StationRegistry::new(std::path::PathBuf::from("/cache")));
//! let qc_config = QualityControlConfig::default();
//!
//! // Create processor
//! let processor = RecordProcessor::new(registry, qc_config);
//!
//! // Process observations
//! let result = processor.process_observations(observations).await?;
//!
//! // Check results
//! println!("Processing summary: {}", result.summary());
//! println!("Processed {} observations", result.observation_count());
//! # Ok(())
//! # }
//! ```

pub mod deduplication;
pub mod detailed_stats;
pub mod enrichment;
pub mod processor;
pub mod quality_filter;
pub mod stats;

#[cfg(test)]
pub mod tests;

// Re-export main types for easy access
pub use detailed_stats::{
    DetailedProcessingStats, FileProcessingStats, ProcessingIssue, ProcessingIssueBuilder,
};
pub use processor::RecordProcessor;
pub use stats::{ProcessingResult, ProcessingStats};

// Re-export utility functions that might be useful externally
pub use deduplication::{analyze_duplicate_patterns, are_duplicates, get_deduplication_metrics};
pub use enrichment::{analyze_enrichment_needs, needs_station_enrichment};
pub use quality_filter::{
    get_processing_filter_stats, get_processing_quality_summary, has_analysis_quality,
};
