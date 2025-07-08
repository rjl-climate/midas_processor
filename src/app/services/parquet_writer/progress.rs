//! Progress reporting and statistics tracking for Parquet writing operations
//!
//! This module provides progress bar management and real-time statistics tracking
//! during Parquet file generation with user-friendly feedback.

use crate::app::services::parquet_writer::config::WritingStats;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::debug;

/// Progress reporter for Parquet writing operations
pub struct ProgressReporter {
    progress_bar: Option<ProgressBar>,
    total_observations: usize,
}

impl ProgressReporter {
    /// Create a new progress reporter
    pub fn new() -> Self {
        Self {
            progress_bar: None,
            total_observations: 0,
        }
    }

    /// Set up progress reporting for the writing operation
    ///
    /// This creates a progress bar with appropriate styling and estimates
    /// based on the expected number of observations to process.
    pub fn setup_progress(&mut self, total_observations: usize) {
        self.total_observations = total_observations;

        let pb = ProgressBar::new(total_observations as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} observations ({percent}%) | {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Writing Parquet file");

        debug!(
            "Progress bar initialized for {} observations",
            total_observations
        );
        self.progress_bar = Some(pb);
    }

    /// Update progress with the number of observations processed
    pub fn increment(&self, observations_processed: usize) {
        if let Some(ref pb) = self.progress_bar {
            pb.inc(observations_processed as u64);
        }
    }

    /// Set a custom message for the progress bar
    pub fn set_message(&self, message: &str) {
        if let Some(ref pb) = self.progress_bar {
            pb.set_message(message.to_string());
        }
    }

    /// Update progress bar with current statistics
    pub fn update_with_stats(&self, stats: &WritingStats) {
        if let Some(ref pb) = self.progress_bar {
            let message = format!(
                "Writing... {} batches, {} flushes, {}",
                stats.batches_written,
                stats.memory_flushes,
                WritingStats::format_bytes(stats.peak_memory_usage_bytes)
            );
            pb.set_message(message);
        }
    }

    /// Finish progress reporting with a completion message
    pub fn finish(&self, stats: &WritingStats) {
        if let Some(ref pb) = self.progress_bar {
            let completion_message = format!(
                "Completed: {} observations, {} batches, {}",
                stats.observations_written,
                stats.batches_written,
                WritingStats::format_bytes(stats.bytes_written)
            );
            pb.finish_with_message(completion_message.clone());
            debug!("Progress reporting completed: {}", completion_message);
        }
    }

    /// Finish progress reporting with an error message
    pub fn finish_with_error(&self, error_message: &str) {
        if let Some(ref pb) = self.progress_bar {
            let error_msg = format!("Failed: {}", error_message);
            pb.finish_with_message(error_msg);
            debug!("Progress reporting finished with error: {}", error_message);
        }
    }

    /// Check if progress reporting is enabled
    pub fn is_enabled(&self) -> bool {
        self.progress_bar.is_some()
    }

    /// Get the total number of observations being tracked
    pub fn total_observations(&self) -> usize {
        self.total_observations
    }

    /// Get current position from progress bar
    pub fn current_position(&self) -> u64 {
        if let Some(ref pb) = self.progress_bar {
            pb.position()
        } else {
            0
        }
    }

    /// Calculate completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_observations == 0 {
            0.0
        } else {
            (self.current_position() as f64 / self.total_observations as f64) * 100.0
        }
    }

    /// Suspend progress bar to allow clean console output
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Some(ref pb) = self.progress_bar {
            pb.suspend(f)
        } else {
            f()
        }
    }
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        // Ensure progress bar is finished when reporter is dropped
        if let Some(ref pb) = self.progress_bar {
            if !pb.is_finished() {
                pb.finish_and_clear();
            }
        }
    }
}

/// Create a simple spinner progress bar for indeterminate operations
pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

/// Create a custom progress bar with specified total and styling
pub fn create_custom_progress_bar(total: u64, template: &str, progress_chars: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(template)
            .unwrap()
            .progress_chars(progress_chars),
    );
    pb
}

/// Progress tracking for batch operations
pub struct BatchProgress {
    current_batch: usize,
    total_batches: usize,
    observations_per_batch: usize,
    progress_bar: Option<ProgressBar>,
}

impl BatchProgress {
    /// Create a new batch progress tracker
    pub fn new(total_observations: usize, batch_size: usize) -> Self {
        let total_batches = total_observations.div_ceil(batch_size); // Ceiling division

        Self {
            current_batch: 0,
            total_batches,
            observations_per_batch: batch_size,
            progress_bar: None,
        }
    }

    /// Enable progress bar for batch tracking
    pub fn enable_progress_bar(&mut self) {
        let pb = ProgressBar::new(self.total_batches as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} batches | {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Processing batches");
        self.progress_bar = Some(pb);
    }

    /// Mark a batch as completed
    pub fn complete_batch(&mut self, observations_in_batch: usize) {
        self.current_batch += 1;

        if let Some(ref pb) = self.progress_bar {
            pb.inc(1);
            let message = format!(
                "Batch {}/{} ({} observations)",
                self.current_batch, self.total_batches, observations_in_batch
            );
            pb.set_message(message);
        }
    }

    /// Finish batch progress tracking
    pub fn finish(&self) {
        if let Some(ref pb) = self.progress_bar {
            pb.finish_with_message(format!("Completed {} batches", self.total_batches));
        }
    }

    /// Get current batch number
    pub fn current_batch(&self) -> usize {
        self.current_batch
    }

    /// Get total number of batches
    pub fn total_batches(&self) -> usize {
        self.total_batches
    }

    /// Get the number of observations per batch
    pub fn observations_per_batch(&self) -> usize {
        self.observations_per_batch
    }

    /// Check if all batches are complete
    pub fn is_complete(&self) -> bool {
        self.current_batch >= self.total_batches
    }

    /// Get completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_batches == 0 {
            100.0
        } else {
            (self.current_batch as f64 / self.total_batches as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test progress reporter initialization starts in disabled state
    /// Ensures safe default behavior before progress tracking is configured
    #[test]
    fn test_progress_reporter_creation() {
        let reporter = ProgressReporter::new();
        assert!(!reporter.is_enabled());
        assert_eq!(reporter.total_observations(), 0);
        assert_eq!(reporter.current_position(), 0);
        assert_eq!(reporter.completion_percentage(), 0.0);
    }

    /// Test progress bar setup enables tracking with styled indicators
    /// Validates configuration of user-friendly progress visualization
    #[test]
    fn test_progress_reporter_setup() {
        let mut reporter = ProgressReporter::new();
        reporter.setup_progress(1000);

        assert!(reporter.is_enabled());
        assert_eq!(reporter.total_observations(), 1000);
    }

    /// Test progress increments accurately track completion percentage
    /// Ensures real-time feedback during long-running Parquet generation
    #[test]
    fn test_progress_reporter_increment() {
        let mut reporter = ProgressReporter::new();
        reporter.setup_progress(1000);

        reporter.increment(100);
        assert_eq!(reporter.current_position(), 100);
        assert_eq!(reporter.completion_percentage(), 10.0);

        reporter.increment(400);
        assert_eq!(reporter.current_position(), 500);
        assert_eq!(reporter.completion_percentage(), 50.0);
    }

    /// Test progress operations without setup prevent runtime panics
    /// Ensures graceful degradation when progress tracking is not configured
    #[test]
    fn test_progress_reporter_without_setup() {
        let reporter = ProgressReporter::new();

        // Should not panic when called without setup
        reporter.increment(100);
        reporter.set_message("test");

        let stats = WritingStats::new();
        reporter.update_with_stats(&stats);
        reporter.finish(&stats);
        reporter.finish_with_error("test error");
    }

    /// Test edge cases handle zero observations and extreme values safely
    /// Prevents division-by-zero and overflow issues in progress calculations
    #[test]
    fn test_progress_reporter_edge_cases() {
        let mut reporter = ProgressReporter::new();
        reporter.setup_progress(0); // Zero observations

        assert_eq!(reporter.completion_percentage(), 0.0);

        // Test with very large number
        reporter.setup_progress(usize::MAX);
        assert_eq!(reporter.total_observations(), usize::MAX);
    }

    /// Test batch progress tracker calculates total batches correctly
    /// Ensures accurate batch counting for memory-efficient processing
    #[test]
    fn test_batch_progress_creation() {
        let batch_progress = BatchProgress::new(1000, 100);

        assert_eq!(batch_progress.total_batches(), 10);
        assert_eq!(batch_progress.current_batch(), 0);
        assert!(!batch_progress.is_complete());
        assert_eq!(batch_progress.completion_percentage(), 0.0);
    }

    /// Test ceiling division handles non-divisible observation counts properly
    /// Critical for ensuring all observations are processed across batches
    #[test]
    fn test_batch_progress_ceiling_division() {
        // Test that batch count properly handles non-divisible numbers
        let batch_progress = BatchProgress::new(1050, 100);
        assert_eq!(batch_progress.total_batches(), 11); // Should round up

        let batch_progress = BatchProgress::new(1000, 100);
        assert_eq!(batch_progress.total_batches(), 10); // Exact division

        let batch_progress = BatchProgress::new(99, 100);
        assert_eq!(batch_progress.total_batches(), 1); // Less than one batch
    }

    /// Test batch completion tracking provides accurate progress percentages
    /// Validates step-by-step processing feedback for large datasets
    #[test]
    fn test_batch_progress_completion() {
        let mut batch_progress = BatchProgress::new(250, 100);
        assert_eq!(batch_progress.total_batches(), 3);

        batch_progress.complete_batch(100);
        assert_eq!(batch_progress.current_batch(), 1);
        assert!((batch_progress.completion_percentage() - 100.0 / 3.0).abs() < 0.01);
        assert!(!batch_progress.is_complete());

        batch_progress.complete_batch(100);
        assert_eq!(batch_progress.current_batch(), 2);
        assert!(!batch_progress.is_complete());

        batch_progress.complete_batch(50);
        assert_eq!(batch_progress.current_batch(), 3);
        assert!(batch_progress.is_complete());
        assert_eq!(batch_progress.completion_percentage(), 100.0);
    }

    /// Test batch progress edge cases handle empty and single-item datasets
    /// Ensures robust behavior across all possible data size scenarios
    #[test]
    fn test_batch_progress_edge_cases() {
        // Zero observations
        let batch_progress = BatchProgress::new(0, 100);
        assert_eq!(batch_progress.total_batches(), 0);
        assert!(batch_progress.is_complete()); // Technically complete
        assert_eq!(batch_progress.completion_percentage(), 100.0);

        // Single observation
        let batch_progress = BatchProgress::new(1, 100);
        assert_eq!(batch_progress.total_batches(), 1);
    }

    /// Test spinner creation provides indeterminate progress indication
    /// Validates visual feedback for operations without known duration
    #[test]
    fn test_create_spinner() {
        let spinner = create_spinner("Testing...");
        // Can't easily test the visual aspects, but ensure it doesn't panic
        assert!(!spinner.is_finished());
        spinner.finish_and_clear();
    }

    /// Test custom progress bar allows flexible styling and templates
    /// Enables specialized progress visualization for different use cases
    #[test]
    fn test_create_custom_progress_bar() {
        let pb = create_custom_progress_bar(100, "{bar:40} {pos}/{len}", "##-");
        assert_eq!(pb.length(), Some(100));
        pb.finish_and_clear();
    }
}
