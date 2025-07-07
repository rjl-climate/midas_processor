//! Processing statistics and result structures for record processing pipeline
//!
//! This module provides types for tracking processing performance, success rates,
//! and organizing processed results for downstream operations.

use crate::app::models::Observation;

/// Statistics for record processing operations
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingStats {
    /// Total number of input observations
    pub total_input: usize,
    /// Number of observations successfully re-enriched with station metadata
    pub enriched: usize,
    /// Number of observations after deduplication
    pub deduplicated: usize,
    /// Number of observations after quality filtering
    pub quality_filtered: usize,
    /// Final number of output observations
    pub final_output: usize,
    /// Number of errors encountered during processing
    pub errors: usize,
    /// List of specific error messages for debugging
    pub error_messages: Vec<String>,
}

impl ProcessingStats {
    /// Create new empty processing statistics
    pub fn new() -> Self {
        Self {
            total_input: 0,
            enriched: 0,
            deduplicated: 0,
            quality_filtered: 0,
            final_output: 0,
            errors: 0,
            error_messages: Vec::new(),
        }
    }

    /// Add an error to the statistics
    pub fn add_error(&mut self, message: String) {
        self.errors += 1;
        self.error_messages.push(message);
    }

    /// Calculate success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_input == 0 {
            100.0
        } else {
            (self.final_output as f64 / self.total_input as f64) * 100.0
        }
    }

    /// Check if processing was mostly successful (>90% success rate)
    pub fn is_successful(&self) -> bool {
        self.success_rate() > 90.0
    }

    /// Get enrichment rate as a percentage
    pub fn enrichment_rate(&self) -> f64 {
        if self.total_input == 0 {
            0.0
        } else {
            (self.enriched as f64 / self.total_input as f64) * 100.0
        }
    }

    /// Get deduplication effectiveness (percentage of records that were unique)
    pub fn deduplication_effectiveness(&self) -> f64 {
        if self.enriched == 0 {
            0.0
        } else {
            (self.deduplicated as f64 / self.enriched as f64) * 100.0
        }
    }

    /// Get quality filtering rate (percentage that passed QC)
    pub fn quality_pass_rate(&self) -> f64 {
        if self.deduplicated == 0 {
            0.0
        } else {
            (self.quality_filtered as f64 / self.deduplicated as f64) * 100.0
        }
    }

    /// Get summary of processing pipeline statistics
    pub fn summary(&self) -> String {
        format!(
            "Processing Summary: {} -> {} observations ({:.1}% success) | \
             Enriched: {:.1}% | Deduplicated: {:.1}% effective | \
             Quality passed: {:.1}% | Errors: {}",
            self.total_input,
            self.final_output,
            self.success_rate(),
            self.enrichment_rate(),
            self.deduplication_effectiveness(),
            self.quality_pass_rate(),
            self.errors
        )
    }
}

impl Default for ProcessingStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of record processing operations
#[derive(Debug, Clone)]
pub struct ProcessingResult {
    /// Successfully processed observations
    pub observations: Vec<Observation>,
    /// Processing statistics and error information
    pub stats: ProcessingStats,
}

impl ProcessingResult {
    /// Create a new processing result
    pub fn new(observations: Vec<Observation>, stats: ProcessingStats) -> Self {
        Self {
            observations,
            stats,
        }
    }

    /// Get the number of processed observations
    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }

    /// Check if processing was successful based on statistics
    pub fn is_successful(&self) -> bool {
        self.stats.is_successful()
    }

    /// Get processing success rate
    pub fn success_rate(&self) -> f64 {
        self.stats.success_rate()
    }

    /// Get summary string for logging
    pub fn summary(&self) -> String {
        self.stats.summary()
    }
}
