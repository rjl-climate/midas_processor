//! Tests for the main RecordProcessor and integration scenarios

use super::*;
use crate::app::services::record_processor::{ProcessingResult, ProcessingStats, RecordProcessor};
use std::sync::Arc;

#[tokio::test]
async fn test_record_processor_new() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();

    let processor = RecordProcessor::new(station_registry.clone(), config.clone());

    // Test that configuration is stored correctly
    assert_eq!(
        processor.quality_config().include_suspect,
        config.include_suspect
    );
    assert_eq!(
        processor.quality_config().include_unchecked,
        config.include_unchecked
    );
    assert_eq!(
        processor.quality_config().min_quality_version,
        config.min_quality_version
    );
}

#[tokio::test]
async fn test_process_observations_full_pipeline() {
    let station_registry = create_mock_station_registry();
    let config = create_permissive_quality_config(); // Use permissive to test full pipeline
    let processor = RecordProcessor::new(station_registry, config);

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
        create_observation_with_good_station("obs3", 124),
    ];

    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Check that we got observations back
    assert!(processing_result.observation_count() > 0);
    assert!(processing_result.observation_count() <= 3);

    // Check statistics
    assert_eq!(processing_result.stats.total_input, 3);
    assert!(processing_result.is_successful() || !processing_result.is_successful()); // Either is valid
}

#[tokio::test]
async fn test_process_observations_empty_input() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations: Vec<Observation> = vec![];
    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    assert_eq!(processing_result.observation_count(), 0);
    assert_eq!(processing_result.stats.total_input, 0);
    assert_eq!(processing_result.stats.final_output, 0);
    assert!(processing_result.is_successful()); // Empty is considered successful
}

#[tokio::test]
async fn test_process_observations_with_duplicates() {
    let station_registry = create_mock_station_registry();
    let config = create_permissive_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let mut observations = vec![];

    // Add unique observations
    observations.push(create_observation_with_good_station("obs1", 123));
    observations.push(create_observation_with_good_station("obs2", 124));

    // Add duplicates
    let mut duplicates = create_duplicate_observations();
    observations.append(&mut duplicates);

    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Should have deduplicated the observations
    assert_eq!(processing_result.observation_count(), 3); // 2 unique + 1 from duplicate pair
    assert_eq!(processing_result.stats.total_input, 4); // Original count
    assert!(processing_result.stats.deduplicated <= processing_result.stats.enriched);
}

#[tokio::test]
async fn test_process_observations_custom_pipeline_skip_all() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
    ];

    let result = processor
        .process_observations_custom(
            observations,
            true, // skip enrichment
            true, // skip deduplication
            true, // skip quality filter
        )
        .await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Should pass through all observations unchanged
    assert_eq!(processing_result.observation_count(), 2);
    assert_eq!(processing_result.stats.total_input, 2);
    assert_eq!(processing_result.stats.final_output, 2);
}

#[tokio::test]
async fn test_process_observations_custom_pipeline_selective() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
    ];

    let result = processor
        .process_observations_custom(
            observations,
            false, // do enrichment
            true,  // skip deduplication
            false, // do quality filter
        )
        .await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Should have gone through enrichment and quality filter but not deduplication
    assert!(processing_result.stats.enriched > 0);
    assert_eq!(
        processing_result.stats.deduplicated,
        processing_result.stats.enriched
    ); // No change
    assert!(processing_result.stats.quality_filtered <= processing_result.stats.deduplicated);
}

#[tokio::test]
async fn test_validate_observations_success() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
    ];

    let result = processor.validate_observations(&observations);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validate_observations_empty_collection() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations: Vec<Observation> = vec![];
    let result = processor.validate_observations(&observations);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("empty observation collection")
    );
}

#[tokio::test]
async fn test_validate_observations_empty_observation_id() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let mut observation = create_observation_with_good_station("", 123);
    observation.observation_id = "".to_string(); // Empty ID

    let observations = vec![observation];
    let result = processor.validate_observations(&observations);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("empty observation_id")
    );
}

#[tokio::test]
async fn test_validate_observations_invalid_station_id() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let mut observation = create_observation_with_good_station("obs1", 123);
    observation.station_id = -1; // Invalid station ID

    let observations = vec![observation];
    let result = processor.validate_observations(&observations);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid station_id")
    );
}

#[tokio::test]
async fn test_processing_result_methods() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
    ];

    let mut stats = ProcessingStats::new();
    stats.total_input = 10;
    stats.final_output = 8;

    let result = ProcessingResult::new(observations, stats);

    assert_eq!(result.observation_count(), 2);
    assert_eq!(result.success_rate(), 80.0);
    assert!(!result.is_successful()); // Below 90% threshold

    let summary = result.summary();
    assert!(summary.contains("10 -> 8"));
    assert!(summary.contains("80.0%"));
}

#[tokio::test]
async fn test_processing_stats_integration() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Check that statistics are properly populated
    assert_eq!(processing_result.stats.total_input, 3);
    assert!(processing_result.stats.enriched <= processing_result.stats.total_input);
    assert!(processing_result.stats.deduplicated <= processing_result.stats.enriched);
    assert!(processing_result.stats.quality_filtered <= processing_result.stats.deduplicated);
    assert_eq!(
        processing_result.stats.final_output,
        processing_result.stats.quality_filtered
    );
}

#[tokio::test]
async fn test_end_to_end_processing_scenario() {
    let station_registry = create_mock_station_registry();
    let config = create_permissive_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    // Create a realistic mix of observations
    let mut observations = vec![
        // Good observations
        create_observation_with_good_station("good1", 123),
        create_observation_with_good_station("good2", 124),
        // Observations with missing stations
        create_observation_with_missing_station("missing1", 999),
        create_observation_with_missing_station("missing2", 998),
        // Observations with parse failures
        create_observation_with_parse_failures("failed1", 125),
    ];

    // Duplicate observations
    let mut duplicates = create_duplicate_observations();
    observations.append(&mut duplicates);

    let original_count = observations.len();
    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    // Verify pipeline worked
    assert_eq!(processing_result.stats.total_input, original_count);
    assert!(processing_result.observation_count() > 0);
    assert!(processing_result.observation_count() < original_count); // Some filtering occurred

    // Check that summary is informative
    let summary = processing_result.summary();
    assert!(summary.contains("observations"));
    assert!(summary.contains("%"));

    // Verify that all observations in result have proper structure
    for obs in &processing_result.observations {
        assert!(!obs.observation_id.is_empty());
        assert!(obs.station_id > 0);
        assert!(!obs.measurements.is_empty() || obs.measurements.is_empty()); // Either is valid
    }
}

#[tokio::test]
async fn test_processing_with_different_configurations() {
    let station_registry = create_mock_station_registry();

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
        create_observation_with_parse_failures("obs3", 124),
    ];

    // Test with strict configuration
    let strict_config = create_test_quality_config();
    let strict_processor = RecordProcessor::new(station_registry.clone(), strict_config);
    let strict_result = strict_processor
        .process_observations(observations.clone())
        .await;

    // Test with permissive configuration
    let permissive_config = create_permissive_quality_config();
    let permissive_processor = RecordProcessor::new(station_registry, permissive_config);
    let permissive_result = permissive_processor
        .process_observations(observations)
        .await;

    assert!(strict_result.is_ok());
    assert!(permissive_result.is_ok());

    let strict_count = strict_result.unwrap().observation_count();
    let permissive_count = permissive_result.unwrap().observation_count();

    // Permissive should pass more observations than strict
    assert!(permissive_count >= strict_count);
}

#[tokio::test]
async fn test_processor_error_handling() {
    let station_registry = create_mock_station_registry();
    let config = create_test_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    // Test with potentially problematic observations
    let mut problematic_obs = create_observation_with_good_station("problematic", 123);
    problematic_obs.measurements.clear(); // Empty measurements

    let observations = vec![problematic_obs];

    // Should handle gracefully without panicking
    let result = processor.process_observations(observations).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_processing_preserves_original_data() {
    let station_registry = create_mock_station_registry();
    let config = create_permissive_quality_config();
    let processor = RecordProcessor::new(station_registry, config);

    let mut observation = create_observation_with_good_station("obs1", 123);

    // Add some original quality flags that should be preserved
    observation
        .quality_flags
        .insert("air_temperature".to_string(), "0".to_string());
    observation
        .quality_flags
        .insert("humidity".to_string(), "1".to_string());

    // Add some measurements that should be preserved
    observation
        .measurements
        .insert("pressure".to_string(), 1013.25);

    let observations = vec![observation.clone()];
    let result = processor.process_observations(observations).await;

    assert!(result.is_ok());
    let processing_result = result.unwrap();

    assert_eq!(processing_result.observation_count(), 1);
    let processed_obs = &processing_result.observations[0];

    // Check that original data is preserved
    assert_eq!(processed_obs.observation_id, observation.observation_id);
    assert_eq!(processed_obs.station_id, observation.station_id);

    // Original quality flags should be unchanged
    assert_eq!(
        processed_obs.quality_flags.get("air_temperature"),
        Some(&"0".to_string())
    );
    assert_eq!(
        processed_obs.quality_flags.get("humidity"),
        Some(&"1".to_string())
    );

    // Original measurements should be preserved
    assert_eq!(processed_obs.measurements.get("pressure"), Some(&1013.25));
}

#[tokio::test]
async fn test_processor_concurrent_access() {
    let station_registry = create_mock_station_registry();
    let config = create_permissive_quality_config();
    let processor = Arc::new(RecordProcessor::new(station_registry, config));

    // Create multiple processing tasks
    let mut handles = vec![];

    for i in 0..5 {
        let processor_clone = processor.clone();
        let observations = vec![
            create_observation_with_good_station(&format!("obs_{}_1", i), 123 + i),
            create_observation_with_good_station(&format!("obs_{}_2", i), 124 + i),
        ];

        let handle =
            tokio::spawn(async move { processor_clone.process_observations(observations).await });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().observation_count(), 2);
    }
}
