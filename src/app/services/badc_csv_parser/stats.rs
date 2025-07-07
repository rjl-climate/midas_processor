//! Parsing statistics and result structures for BADC-CSV processing
//!
//! This module provides types for tracking parsing performance, success rates,
//! and organizing parsed results for downstream processing.

use crate::app::models::Observation;

/// Parsing result with observations and basic statistics
#[derive(Debug, Clone)]
pub struct ParseResult {
    /// Successfully parsed observation records
    pub observations: Vec<Observation>,

    /// Basic parsing statistics
    pub stats: ParseStats,
}

/// Simple parsing statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParseStats {
    /// Total number of data records encountered
    pub total_records: usize,

    /// Number of observations successfully parsed
    pub observations_parsed: usize,

    /// Number of records skipped due to errors
    pub records_skipped: usize,

    /// List of parsing errors for debugging
    pub errors: Vec<String>,
}

impl ParseStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self {
            total_records: 0,
            observations_parsed: 0,
            records_skipped: 0,
            errors: Vec::new(),
        }
    }

    /// Calculate success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_records == 0 {
            0.0
        } else {
            (self.observations_parsed as f64 / self.total_records as f64) * 100.0
        }
    }

    /// Check if parsing was mostly successful (>90% success rate)
    pub fn is_successful(&self) -> bool {
        self.success_rate() > 90.0
    }
}

impl Default for ParseStats {
    fn default() -> Self {
        Self::new()
    }
}
