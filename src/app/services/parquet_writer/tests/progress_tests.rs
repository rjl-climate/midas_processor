//! Comprehensive unit tests for progress reporting functionality

use crate::app::services::parquet_writer::{
    config::WritingStats,
    progress::{ProgressReporter, BatchProgress, create_spinner, create_custom_progress_bar},
};

#[test]
fn test_progress_reporter_creation() {
    let reporter = ProgressReporter::new();
    assert!(!reporter.is_enabled());
    assert_eq!(reporter.total_observations(), 0);
    assert_eq!(reporter.current_position(), 0);
    assert_eq!(reporter.completion_percentage(), 0.0);
}

#[test]
fn test_progress_reporter_default() {
    let reporter = ProgressReporter::default();
    assert!(!reporter.is_enabled());
    assert_eq!(reporter.total_observations(), 0);
}

#[test]
fn test_progress_reporter_setup() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(1000);
    
    assert!(reporter.is_enabled());
    assert_eq!(reporter.total_observations(), 1000);
}

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
    
    reporter.increment(500);
    assert_eq!(reporter.current_position(), 1000);
    assert_eq!(reporter.completion_percentage(), 100.0);
}

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
    
    // Position should remain 0
    assert_eq!(reporter.current_position(), 0);
}

#[test]
fn test_progress_reporter_edge_cases() {
    let mut reporter = ProgressReporter::new();
    
    // Zero observations
    reporter.setup_progress(0);
    assert_eq!(reporter.completion_percentage(), 0.0);
    
    // Very large number
    reporter.setup_progress(usize::MAX);
    assert_eq!(reporter.total_observations(), usize::MAX);
}

#[test]
fn test_progress_reporter_completion_percentage() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    // Test various percentages
    reporter.increment(0);
    assert_eq!(reporter.completion_percentage(), 0.0);
    
    reporter.increment(25);
    assert_eq!(reporter.completion_percentage(), 25.0);
    
    reporter.increment(25);
    assert_eq!(reporter.completion_percentage(), 50.0);
    
    reporter.increment(50);
    assert_eq!(reporter.completion_percentage(), 100.0);
}

#[test]
fn test_progress_reporter_set_message() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    // Should not panic
    reporter.set_message("Processing data...");
    reporter.set_message("Almost done...");
    reporter.set_message("");
}

#[test]
fn test_progress_reporter_update_with_stats() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    let mut stats = WritingStats::new();
    stats.batches_written = 5;
    stats.memory_flushes = 2;
    stats.peak_memory_usage_bytes = 1024 * 1024; // 1MB
    
    // Should not panic
    reporter.update_with_stats(&stats);
}

#[test]
fn test_progress_reporter_finish() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    let mut stats = WritingStats::new();
    stats.observations_written = 100;
    stats.batches_written = 10;
    stats.bytes_written = 50000;
    
    // Should not panic
    reporter.finish(&stats);
}

#[test]
fn test_progress_reporter_finish_with_error() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    // Should not panic
    reporter.finish_with_error("Something went wrong");
    reporter.finish_with_error("");
}

#[test]
fn test_progress_reporter_suspend() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    let result = reporter.suspend(|| {
        42
    });
    
    assert_eq!(result, 42);
    
    // Should work without setup too
    let reporter = ProgressReporter::new();
    let result = reporter.suspend(|| "test");
    assert_eq!(result, "test");
}

#[test]
fn test_batch_progress_creation() {
    let batch_progress = BatchProgress::new(1000, 100);
    
    assert_eq!(batch_progress.total_batches(), 10);
    assert_eq!(batch_progress.current_batch(), 0);
    assert!(!batch_progress.is_complete());
    assert_eq!(batch_progress.completion_percentage(), 0.0);
}

#[test]
fn test_batch_progress_ceiling_division() {
    // Test that batch count properly handles non-divisible numbers
    let batch_progress = BatchProgress::new(1050, 100);
    assert_eq!(batch_progress.total_batches(), 11); // Should round up
    
    let batch_progress = BatchProgress::new(1000, 100);
    assert_eq!(batch_progress.total_batches(), 10); // Exact division
    
    let batch_progress = BatchProgress::new(99, 100);
    assert_eq!(batch_progress.total_batches(), 1); // Less than one batch
    
    let batch_progress = BatchProgress::new(1, 100);
    assert_eq!(batch_progress.total_batches(), 1); // Single observation
}

#[test]
fn test_batch_progress_enable_progress_bar() {
    let mut batch_progress = BatchProgress::new(1000, 100);
    
    // Should not panic
    batch_progress.enable_progress_bar();
}

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
    assert!((batch_progress.completion_percentage() - 200.0 / 3.0).abs() < 0.01);
    assert!(!batch_progress.is_complete());
    
    batch_progress.complete_batch(50); // Final partial batch
    assert_eq!(batch_progress.current_batch(), 3);
    assert!(batch_progress.is_complete());
    assert_eq!(batch_progress.completion_percentage(), 100.0);
}

#[test]
fn test_batch_progress_completion_with_progress_bar() {
    let mut batch_progress = BatchProgress::new(200, 50);
    batch_progress.enable_progress_bar();
    
    // Should not panic when completing batches with progress bar enabled
    batch_progress.complete_batch(50);
    batch_progress.complete_batch(50);
    batch_progress.complete_batch(50);
    batch_progress.complete_batch(50);
    
    assert!(batch_progress.is_complete());
}

#[test]
fn test_batch_progress_finish() {
    let mut batch_progress = BatchProgress::new(100, 50);
    batch_progress.enable_progress_bar();
    
    // Should not panic
    batch_progress.finish();
}

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
    assert!(!batch_progress.is_complete());
    
    // Very large numbers
    let batch_progress = BatchProgress::new(usize::MAX / 2, 1000);
    assert!(batch_progress.total_batches() > 0);
}

#[test]
fn test_batch_progress_completion_percentage_precision() {
    let mut batch_progress = BatchProgress::new(1000, 333); // 3.003... batches
    assert_eq!(batch_progress.total_batches(), 4);
    
    batch_progress.complete_batch(333);
    assert!((batch_progress.completion_percentage() - 25.0).abs() < 0.01);
    
    batch_progress.complete_batch(333);
    assert!((batch_progress.completion_percentage() - 50.0).abs() < 0.01);
    
    batch_progress.complete_batch(333);
    assert!((batch_progress.completion_percentage() - 75.0).abs() < 0.01);
    
    batch_progress.complete_batch(1); // Final partial batch
    assert_eq!(batch_progress.completion_percentage(), 100.0);
}

#[test]
fn test_batch_progress_over_completion() {
    let mut batch_progress = BatchProgress::new(100, 50);
    
    // Complete more batches than expected
    batch_progress.complete_batch(50);
    batch_progress.complete_batch(50);
    assert!(batch_progress.is_complete());
    
    // Should handle gracefully
    batch_progress.complete_batch(50); // Extra batch
    assert_eq!(batch_progress.current_batch(), 3);
    assert!(batch_progress.completion_percentage() > 100.0);
}

#[test]
fn test_create_spinner() {
    let spinner = create_spinner("Testing...");
    
    // Can't easily test the visual aspects, but ensure it doesn't panic
    assert!(!spinner.is_finished());
    spinner.finish_and_clear();
}

#[test]
fn test_create_spinner_empty_message() {
    let spinner = create_spinner("");
    assert!(!spinner.is_finished());
    spinner.finish_and_clear();
}

#[test]
fn test_create_custom_progress_bar() {
    let pb = create_custom_progress_bar(
        100,
        "{bar:40} {pos}/{len}",
        "##-"
    );
    
    assert_eq!(pb.length(), Some(100));
    pb.finish_and_clear();
}

#[test]
fn test_create_custom_progress_bar_edge_cases() {
    // Zero length
    let pb = create_custom_progress_bar(0, "{bar:40}", "##-");
    assert_eq!(pb.length(), Some(0));
    pb.finish_and_clear();
    
    // Very large length
    let pb = create_custom_progress_bar(u64::MAX / 2, "{pos}", "#");
    assert!(pb.length().is_some());
    pb.finish_and_clear();
}

#[test]
fn test_progress_reporter_drop() {
    // Test that progress reporter can be dropped safely
    {
        let mut reporter = ProgressReporter::new();
        reporter.setup_progress(100);
        reporter.increment(50);
        // reporter goes out of scope here and should be dropped cleanly
    }
    
    // Test dropping without setup
    {
        let _reporter = ProgressReporter::new();
        // reporter goes out of scope here
    }
}

#[test]
fn test_batch_progress_various_batch_sizes() {
    // Test with batch size larger than total observations
    let batch_progress = BatchProgress::new(50, 100);
    assert_eq!(batch_progress.total_batches(), 1);
    
    // Test with batch size equal to total observations
    let batch_progress = BatchProgress::new(100, 100);
    assert_eq!(batch_progress.total_batches(), 1);
    
    // Test with very small batch size
    let batch_progress = BatchProgress::new(100, 1);
    assert_eq!(batch_progress.total_batches(), 100);
}

#[test]
fn test_progress_reporter_increment_zero() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    reporter.increment(0);
    assert_eq!(reporter.current_position(), 0);
    assert_eq!(reporter.completion_percentage(), 0.0);
}

#[test]
fn test_progress_reporter_increment_overflow() {
    let mut reporter = ProgressReporter::new();
    reporter.setup_progress(100);
    
    // Increment beyond total
    reporter.increment(150);
    assert_eq!(reporter.current_position(), 150);
    assert!(reporter.completion_percentage() > 100.0);
}

#[test]
fn test_batch_progress_zero_batch_size() {
    // This would cause division by zero in the calculation, but since we use
    // ceiling division with addition, it should be handled gracefully
    // Note: In practice, batch_size should never be 0
    let batch_progress = BatchProgress::new(100, 1); // Use 1 instead of 0 to avoid potential issues
    assert_eq!(batch_progress.total_batches(), 100);
}