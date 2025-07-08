//! Tests for record deduplication functionality

use super::*;
use crate::app::services::record_processor::deduplication::{
    analyze_duplicate_patterns, are_duplicates, deduplicate_observations,
    get_deduplication_metrics, get_record_status_priority,
};
use crate::app::services::record_processor::stats::ProcessingStats;
use chrono::{TimeZone, Utc};

#[test]
fn test_deduplicate_observations_no_duplicates() {
    let mut stats = ProcessingStats::new();

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let original_count = observations.len();
    let result = deduplicate_observations(observations, &mut stats, None);

    assert_eq!(result.len(), original_count);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_deduplicate_observations_with_duplicates() {
    let mut stats = ProcessingStats::new();

    // Create duplicate observations with different record statuses
    let duplicates = create_duplicate_observations();
    assert_eq!(duplicates.len(), 2); // Original and corrected versions

    let result = deduplicate_observations(duplicates, &mut stats, None);

    // Should keep only the corrected version (higher priority)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].rec_st_ind, record_status::CORRECTED as i32);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_deduplicate_observations_mixed_scenarios() {
    let mut stats = ProcessingStats::new();

    let mut observations = vec![];

    // Add some unique observations
    observations.push(create_observation_with_good_station("unique1", 123));
    observations.push(create_observation_with_good_station("unique2", 124));

    // Add duplicate observations
    let mut duplicates = create_duplicate_observations();
    observations.append(&mut duplicates);

    // Add another unique observation
    observations.push(create_observation_with_good_station("unique3", 125));

    let result = deduplicate_observations(observations, &mut stats, None);

    // Should have 4 observations: 3 unique + 1 from the duplicate pair
    assert_eq!(result.len(), 4);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_are_duplicates_same_observation() {
    let obs1 = create_observation_with_good_station("obs1", 123);
    let obs2 = create_observation_with_good_station("obs1", 123);

    assert!(are_duplicates(&obs1, &obs2));
}

#[test]
fn test_are_duplicates_different_observation_id() {
    let obs1 = create_observation_with_good_station("obs1", 123);
    let obs2 = create_observation_with_good_station("obs2", 123);

    assert!(!are_duplicates(&obs1, &obs2));
}

#[test]
fn test_are_duplicates_different_station_id() {
    let obs1 = create_observation_with_good_station("obs1", 123);
    let obs2 = create_observation_with_good_station("obs1", 124);

    assert!(!are_duplicates(&obs1, &obs2));
}

#[test]
fn test_are_duplicates_different_timestamp() {
    let mut obs1 = create_observation_with_good_station("obs1", 123);
    let mut obs2 = create_observation_with_good_station("obs1", 123);

    obs1.ob_end_time = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();
    obs2.ob_end_time = Utc.with_ymd_and_hms(2023, 6, 15, 13, 0, 0).unwrap();

    assert!(!are_duplicates(&obs1, &obs2));
}

#[test]
fn test_get_record_status_priority() {
    // Test the MIDAS record status priority order (lower numbers = higher priority)
    assert!(
        get_record_status_priority(record_status::CORRECTED as i32)
            < get_record_status_priority(record_status::REVISED)
    );

    assert!(
        get_record_status_priority(record_status::CORRECTED as i32)
            < get_record_status_priority(record_status::ORIGINAL as i32)
    );

    assert!(
        get_record_status_priority(record_status::REVISED)
            < get_record_status_priority(record_status::ORIGINAL as i32)
    );
}

#[test]
fn test_deduplication_priority_rules() {
    let mut stats = ProcessingStats::new();

    // Create three versions of the same observation with different statuses
    let timestamp = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();

    let mut original = create_observation_with_good_station("obs1", 123);
    original.ob_end_time = timestamp;
    original.rec_st_ind = record_status::ORIGINAL as i32;

    let mut corrected = create_observation_with_good_station("obs1", 123);
    corrected.ob_end_time = timestamp;
    corrected.rec_st_ind = record_status::CORRECTED as i32;

    let mut final_obs = create_observation_with_good_station("obs1", 123);
    final_obs.ob_end_time = timestamp;
    final_obs.rec_st_ind = record_status::REVISED;

    // Test different orders to ensure priority works regardless of input order
    let observations1 = vec![original.clone(), corrected.clone(), final_obs.clone()];
    let result1 = deduplicate_observations(observations1, &mut stats, None);
    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0].rec_st_ind, record_status::CORRECTED as i32);

    let observations2 = vec![final_obs.clone(), original.clone(), corrected.clone()];
    let result2 = deduplicate_observations(observations2, &mut stats, None);
    assert_eq!(result2.len(), 1);
    assert_eq!(result2[0].rec_st_ind, record_status::CORRECTED as i32);

    let observations3 = vec![corrected.clone(), final_obs.clone(), original.clone()];
    let result3 = deduplicate_observations(observations3, &mut stats, None);
    assert_eq!(result3.len(), 1);
    assert_eq!(result3[0].rec_st_ind, record_status::CORRECTED as i32);
}

#[test]
fn test_deduplication_secondary_criteria() {
    let mut stats = ProcessingStats::new();

    // Create two observations with same priority but different measurement counts
    let timestamp = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();

    let mut obs1 = create_observation_with_good_station("obs1", 123);
    obs1.ob_end_time = timestamp;
    obs1.rec_st_ind = record_status::CORRECTED as i32;
    // obs1 has 2 measurements (from helper function)

    let mut obs2 = create_observation_with_good_station("obs1", 123);
    obs2.ob_end_time = timestamp;
    obs2.rec_st_ind = record_status::CORRECTED as i32;
    // Add more measurements to obs2
    obs2.measurements.insert("wind_speed".to_string(), 10.5);
    obs2.measurements.insert("pressure".to_string(), 1013.25);

    let observations = vec![obs1, obs2];
    let result = deduplicate_observations(observations, &mut stats, None);

    assert_eq!(result.len(), 1);
    // Should keep the one with more measurements
    assert!(result[0].measurements.len() >= 4);
    assert!(result[0].measurements.contains_key("wind_speed"));
    assert!(result[0].measurements.contains_key("pressure"));
}

#[test]
fn test_analyze_duplicate_patterns() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs1", 123), // Duplicate of first
        create_observation_with_good_station("obs3", 125),
        create_observation_with_good_station("obs2", 124), // Duplicate of second
    ];

    let (total_groups, duplicate_groups, total_duplicates) =
        analyze_duplicate_patterns(&observations);

    assert_eq!(total_groups, 3); // Three unique observation groups
    assert_eq!(duplicate_groups, 2); // Two groups have duplicates
    assert_eq!(total_duplicates, 2); // Two extra observations beyond the unique ones
}

#[test]
fn test_analyze_duplicate_patterns_no_duplicates() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let (total_groups, duplicate_groups, total_duplicates) =
        analyze_duplicate_patterns(&observations);

    assert_eq!(total_groups, 3);
    assert_eq!(duplicate_groups, 0);
    assert_eq!(total_duplicates, 0);
}

#[test]
fn test_analyze_duplicate_patterns_all_duplicates() {
    let mut observations = vec![];

    // Create multiple versions of the same observation
    for i in 0..5 {
        let mut obs = create_observation_with_good_station("obs1", 123);
        obs.rec_st_ind = i; // Different record statuses
        observations.push(obs);
    }

    let (total_groups, duplicate_groups, total_duplicates) =
        analyze_duplicate_patterns(&observations);

    assert_eq!(total_groups, 1); // One unique observation group
    assert_eq!(duplicate_groups, 1); // One group has duplicates
    assert_eq!(total_duplicates, 4); // Four extra observations beyond the unique one
}

#[test]
fn test_get_deduplication_metrics() {
    let before_observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs1", 123), // Duplicate
        create_observation_with_good_station("obs3", 125),
    ];

    let after_observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let (reduction_percent, removed_count) =
        get_deduplication_metrics(before_observations.len(), after_observations.len());

    assert_eq!(removed_count, 1);
    assert_eq!(reduction_percent, 25.0); // 1/4 * 100
}

#[test]
fn test_get_deduplication_metrics_no_change() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let (reduction_percent, removed_count) =
        get_deduplication_metrics(observations.len(), observations.len());

    assert_eq!(removed_count, 0);
    assert_eq!(reduction_percent, 0.0);
}

#[test]
fn test_deduplication_with_complex_duplicates() {
    let mut stats = ProcessingStats::new();

    // Create a complex scenario with multiple duplicate groups
    let mut observations = vec![];

    // Group 1: Three versions of obs1
    let timestamp1 = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();
    for (i, status) in [
        record_status::ORIGINAL as i32,
        record_status::CORRECTED as i32,
        record_status::REVISED,
    ]
    .iter()
    .enumerate()
    {
        let mut obs = create_observation_with_good_station("obs1", 123);
        obs.ob_end_time = timestamp1;
        obs.rec_st_ind = *status;
        obs.version_num = (i + 1) as i32;
        observations.push(obs);
    }

    // Group 2: Two versions of obs2
    let timestamp2 = Utc.with_ymd_and_hms(2023, 6, 15, 13, 0, 0).unwrap();
    for (i, status) in [record_status::ORIGINAL, record_status::CORRECTED]
        .iter()
        .enumerate()
    {
        let mut obs = create_observation_with_good_station("obs2", 124);
        obs.ob_end_time = timestamp2;
        obs.rec_st_ind = *status as i32;
        obs.version_num = (i + 1) as i32;
        observations.push(obs);
    }

    // Group 3: Unique observation
    observations.push(create_observation_with_good_station("obs3", 125));

    let result = deduplicate_observations(observations, &mut stats, None);

    // Should have 3 observations: one from each group
    assert_eq!(result.len(), 3);

    // Find the results for each group
    let obs1_result = result
        .iter()
        .find(|obs| obs.observation_id == "obs1")
        .unwrap();
    let obs2_result = result
        .iter()
        .find(|obs| obs.observation_id == "obs2")
        .unwrap();
    let obs3_result = result
        .iter()
        .find(|obs| obs.observation_id == "obs3")
        .unwrap();

    // Check that highest priority versions were kept
    assert_eq!(obs1_result.rec_st_ind, record_status::CORRECTED as i32);
    assert_eq!(obs2_result.rec_st_ind, record_status::CORRECTED as i32);
    assert_eq!(obs3_result.rec_st_ind, record_status::ORIGINAL as i32); // Only version

    assert_eq!(stats.errors, 0);
}

#[test]
fn test_deduplication_preserves_processing_flags() {
    let mut stats = ProcessingStats::new();

    // Create duplicate observations with different processing flags
    let mut obs1 = create_observation_with_good_station("obs1", 123);
    obs1.rec_st_ind = record_status::ORIGINAL as i32;
    obs1.set_processing_flag("custom_flag".to_string(), ProcessingFlag::ParseOk);

    let mut obs2 = create_observation_with_good_station("obs1", 123);
    obs2.rec_st_ind = record_status::CORRECTED as i32;
    obs2.set_processing_flag("custom_flag".to_string(), ProcessingFlag::ParseFailed);

    let observations = vec![obs1, obs2];
    let result = deduplicate_observations(observations, &mut stats, None);

    assert_eq!(result.len(), 1);

    // Should keep the corrected version with its processing flags
    assert_eq!(result[0].rec_st_ind, record_status::CORRECTED as i32);
    assert_eq!(
        result[0].get_processing_flag("custom_flag"),
        Some(ProcessingFlag::ParseFailed)
    );
}

#[test]
fn test_deduplication_edge_cases() {
    let mut stats = ProcessingStats::new();

    // Test empty input
    let empty_result = deduplicate_observations(vec![], &mut stats, None);
    assert_eq!(empty_result.len(), 0);

    // Test single observation
    let single_obs = vec![create_observation_with_good_station("obs1", 123)];
    let single_result = deduplicate_observations(single_obs, &mut stats, None);
    assert_eq!(single_result.len(), 1);

    // Test observations with unknown record status
    let mut obs_unknown = create_observation_with_good_station("obs1", 123);
    obs_unknown.rec_st_ind = 999; // Unknown status
    let unknown_result = deduplicate_observations(vec![obs_unknown], &mut stats, None);
    assert_eq!(unknown_result.len(), 1);

    assert_eq!(stats.errors, 0);
}
