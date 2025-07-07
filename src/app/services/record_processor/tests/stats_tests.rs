//! Tests for processing statistics and result structures

use super::*;
use crate::app::services::record_processor::stats::{ProcessingResult, ProcessingStats};

#[test]
fn test_processing_stats_new() {
    let stats = ProcessingStats::new();

    assert_eq!(stats.total_input, 0);
    assert_eq!(stats.enriched, 0);
    assert_eq!(stats.deduplicated, 0);
    assert_eq!(stats.quality_filtered, 0);
    assert_eq!(stats.final_output, 0);
    assert_eq!(stats.errors, 0);
    assert!(stats.error_messages.is_empty());
}

#[test]
fn test_processing_stats_default() {
    let stats = ProcessingStats::default();
    assert_eq!(stats, ProcessingStats::new());
}

#[test]
fn test_processing_stats_add_error() {
    let mut stats = ProcessingStats::new();

    stats.add_error("Test error 1".to_string());
    assert_eq!(stats.errors, 1);
    assert_eq!(stats.error_messages.len(), 1);
    assert_eq!(stats.error_messages[0], "Test error 1");

    stats.add_error("Test error 2".to_string());
    assert_eq!(stats.errors, 2);
    assert_eq!(stats.error_messages.len(), 2);
    assert_eq!(stats.error_messages[1], "Test error 2");
}

#[test]
fn test_processing_stats_success_rate() {
    let mut stats = ProcessingStats::new();

    // Empty case
    assert_eq!(stats.success_rate(), 100.0);

    // Perfect success
    stats.total_input = 100;
    stats.final_output = 100;
    assert_eq!(stats.success_rate(), 100.0);

    // Partial success
    stats.final_output = 80;
    assert_eq!(stats.success_rate(), 80.0);

    // Complete failure
    stats.final_output = 0;
    assert_eq!(stats.success_rate(), 0.0);
}

#[test]
fn test_processing_stats_is_successful() {
    let mut stats = ProcessingStats::new();

    // Empty case (considered successful)
    assert!(stats.is_successful());

    // Above threshold
    stats.total_input = 100;
    stats.final_output = 95;
    assert!(stats.is_successful());

    // At threshold (90.0 is not > 90.0)
    stats.final_output = 90;
    assert!(!stats.is_successful());

    // Below threshold
    stats.final_output = 85;
    assert!(!stats.is_successful());
}

#[test]
fn test_processing_stats_enrichment_rate() {
    let mut stats = ProcessingStats::new();

    // Empty case
    assert_eq!(stats.enrichment_rate(), 0.0);

    // Normal case
    stats.total_input = 100;
    stats.enriched = 85;
    assert_eq!(stats.enrichment_rate(), 85.0);

    // Perfect enrichment
    stats.enriched = 100;
    assert_eq!(stats.enrichment_rate(), 100.0);
}

#[test]
fn test_processing_stats_deduplication_effectiveness() {
    let mut stats = ProcessingStats::new();

    // Empty case
    assert_eq!(stats.deduplication_effectiveness(), 0.0);

    // No deduplication needed (all unique)
    stats.enriched = 100;
    stats.deduplicated = 100;
    assert_eq!(stats.deduplication_effectiveness(), 100.0);

    // Some duplicates removed
    stats.deduplicated = 80;
    assert_eq!(stats.deduplication_effectiveness(), 80.0);
}

#[test]
fn test_processing_stats_quality_pass_rate() {
    let mut stats = ProcessingStats::new();

    // Empty case
    assert_eq!(stats.quality_pass_rate(), 0.0);

    // All pass quality checks
    stats.deduplicated = 100;
    stats.quality_filtered = 100;
    assert_eq!(stats.quality_pass_rate(), 100.0);

    // Some fail quality checks
    stats.quality_filtered = 75;
    assert_eq!(stats.quality_pass_rate(), 75.0);
}

#[test]
fn test_processing_stats_summary() {
    let mut stats = ProcessingStats::new();
    stats.total_input = 100;
    stats.enriched = 95;
    stats.deduplicated = 80;
    stats.quality_filtered = 75;
    stats.final_output = 75;
    stats.errors = 5;

    let summary = stats.summary();

    // Check that summary contains key information
    assert!(summary.contains("100 -> 75 observations"));
    assert!(summary.contains("75.0% success"));
    assert!(summary.contains("Enriched: 95.0%"));
    assert!(summary.contains("Deduplicated: 84.2% effective"));
    assert!(summary.contains("Quality passed: 93.8%"));
    assert!(summary.contains("Errors: 5"));
}

#[test]
fn test_processing_result_new() {
    let observations = vec![create_observation_with_good_station("test", 123)];
    let stats = ProcessingStats::new();

    let result = ProcessingResult::new(observations, stats);

    assert_eq!(result.observation_count(), 1);
    assert_eq!(result.observations.len(), 1);
}

#[test]
fn test_processing_result_observation_count() {
    let observations = vec![
        create_observation_with_good_station("test1", 123),
        create_observation_with_good_station("test2", 124),
        create_observation_with_good_station("test3", 125),
    ];
    let stats = ProcessingStats::new();

    let result = ProcessingResult::new(observations, stats);

    assert_eq!(result.observation_count(), 3);
}

#[test]
fn test_processing_result_is_successful() {
    let observations = vec![create_observation_with_good_station("test", 123)];
    let mut stats = ProcessingStats::new();

    // Successful case
    stats.total_input = 100;
    stats.final_output = 95;
    let result = ProcessingResult::new(observations.clone(), stats);
    assert!(result.is_successful());

    // Unsuccessful case
    let mut stats = ProcessingStats::new();
    stats.total_input = 100;
    stats.final_output = 85;
    let result = ProcessingResult::new(observations, stats);
    assert!(!result.is_successful());
}

#[test]
fn test_processing_result_success_rate() {
    let observations = vec![create_observation_with_good_station("test", 123)];
    let mut stats = ProcessingStats::new();
    stats.total_input = 100;
    stats.final_output = 82;

    let result = ProcessingResult::new(observations, stats);

    assert_eq!(result.success_rate(), 82.0);
}

#[test]
fn test_processing_result_summary() {
    let observations = vec![create_observation_with_good_station("test", 123)];
    let mut stats = ProcessingStats::new();
    stats.total_input = 100;
    stats.final_output = 75;

    let result = ProcessingResult::new(observations, stats);
    let summary = result.summary();

    // Should delegate to stats.summary()
    assert!(summary.contains("100 -> 75 observations"));
}

#[test]
fn test_processing_stats_edge_cases() {
    let mut stats = ProcessingStats::new();

    // Test with zero values
    assert_eq!(stats.success_rate(), 100.0);
    assert_eq!(stats.enrichment_rate(), 0.0);
    assert_eq!(stats.deduplication_effectiveness(), 0.0);
    assert_eq!(stats.quality_pass_rate(), 0.0);

    // Test with realistic processing scenario
    stats.total_input = 1000;
    stats.enriched = 950; // 5% couldn't be enriched
    stats.deduplicated = 800; // 15.8% were duplicates
    stats.quality_filtered = 780; // 2.5% failed quality checks
    stats.final_output = 780;
    stats.errors = 25;

    assert_eq!(stats.success_rate(), 78.0);
    assert_eq!(stats.enrichment_rate(), 95.0);
    assert_eq!(stats.deduplication_effectiveness(), 84.21052631578947); // 800/950
    assert_eq!(stats.quality_pass_rate(), 97.5); // 780/800

    assert!(!stats.is_successful()); // Below 90% threshold
    assert_eq!(stats.errors, 25);
}

#[test]
fn test_processing_stats_realistic_scenarios() {
    // Scenario 1: High quality data with minimal issues
    let mut stats1 = ProcessingStats::new();
    stats1.total_input = 10000;
    stats1.enriched = 9999; // Almost perfect enrichment
    stats1.deduplicated = 9950; // Very few duplicates
    stats1.quality_filtered = 9940; // Minimal quality issues
    stats1.final_output = 9940;
    stats1.errors = 1;

    assert!(stats1.is_successful());
    assert!(stats1.success_rate() > 99.0);

    // Scenario 2: Poor quality data with many issues
    let mut stats2 = ProcessingStats::new();
    stats2.total_input = 10000;
    stats2.enriched = 8500; // Many missing stations
    stats2.deduplicated = 6000; // Many duplicates
    stats2.quality_filtered = 5500; // Quality issues
    stats2.final_output = 5500;
    stats2.errors = 150;

    assert!(!stats2.is_successful());
    assert!(stats2.success_rate() < 60.0);

    // Scenario 3: Edge case with no successful processing
    let mut stats3 = ProcessingStats::new();
    stats3.total_input = 100;
    stats3.enriched = 0;
    stats3.deduplicated = 0;
    stats3.quality_filtered = 0;
    stats3.final_output = 0;
    stats3.errors = 100;

    assert!(!stats3.is_successful());
    assert_eq!(stats3.success_rate(), 0.0);
}
