//! Tests for station metadata re-enrichment functionality

use super::*;
use crate::app::services::record_processor::enrichment::{
    analyze_enrichment_needs, needs_station_enrichment, re_enrich_single_observation,
    re_enrich_station_metadata,
};
use crate::app::services::record_processor::stats::ProcessingStats;

#[tokio::test]
async fn test_re_enrich_station_metadata_with_good_stations() {
    let station_registry = create_mock_station_registry();
    let mut stats = ProcessingStats::new();
    stats.total_input = 3;

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let result =
        re_enrich_station_metadata(observations, &station_registry, &mut stats, None).await;

    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), 3);
    assert_eq!(stats.errors, 0);

    // All observations should maintain their good station status
    for obs in &enriched {
        assert_eq!(
            obs.get_processing_flag("station"),
            Some(ProcessingFlag::StationFound)
        );
    }
}

#[tokio::test]
async fn test_re_enrich_station_metadata_with_missing_stations() {
    let station_registry = create_mock_station_registry();
    let mut stats = ProcessingStats::new();
    stats.total_input = 2;

    let observations = vec![
        create_observation_with_missing_station("obs1", 999),
        create_observation_with_missing_station("obs2", 998),
    ];

    let result =
        re_enrich_station_metadata(observations, &station_registry, &mut stats, None).await;

    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), 2);

    // Observations should still be marked as missing stations
    for obs in &enriched {
        assert_eq!(
            obs.get_processing_flag("station"),
            Some(ProcessingFlag::StationMissing)
        );
    }
}

#[tokio::test]
async fn test_re_enrich_station_metadata_mixed_scenarios() {
    let station_registry = create_mock_station_registry();
    let mut stats = ProcessingStats::new();
    stats.total_input = 4;

    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
        create_observation_with_good_station("obs3", 124),
        create_observation_with_missing_station("obs4", 998),
    ];

    let result =
        re_enrich_station_metadata(observations, &station_registry, &mut stats, None).await;

    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), 4);

    // Check mixed results
    assert_eq!(
        enriched[0].get_processing_flag("station"),
        Some(ProcessingFlag::StationFound)
    );
    assert_eq!(
        enriched[1].get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
    assert_eq!(
        enriched[2].get_processing_flag("station"),
        Some(ProcessingFlag::StationFound)
    );
    assert_eq!(
        enriched[3].get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
}

#[tokio::test]
async fn test_re_enrich_single_observation_already_good() {
    let station_registry = create_mock_station_registry();
    let mut observation = create_observation_with_good_station("obs1", 123);

    let result = re_enrich_single_observation(&mut observation, &station_registry).await;

    assert!(result.is_ok());
    assert_eq!(
        observation.get_processing_flag("station"),
        Some(ProcessingFlag::StationFound)
    );
}

#[tokio::test]
async fn test_re_enrich_single_observation_missing_station() {
    let station_registry = create_mock_station_registry();
    let mut observation = create_observation_with_missing_station("obs1", 999);

    let result = re_enrich_single_observation(&mut observation, &station_registry).await;

    assert!(result.is_ok());
    assert_eq!(
        observation.get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
}

#[tokio::test]
async fn test_re_enrich_single_observation_no_processing_flag() {
    let station_registry = create_mock_station_registry();
    let station = create_test_station(123, "TEST STATION");
    let mut observation = create_test_observation(
        "obs1",
        123,
        station,
        record_status::ORIGINAL as i32,
        HashMap::new(), // No processing flags
    );

    let result = re_enrich_single_observation(&mut observation, &station_registry).await;

    assert!(result.is_ok());
    // Should set station missing since we can't find station 123 in our mock registry
    assert_eq!(
        observation.get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
}

#[test]
fn test_needs_station_enrichment() {
    // Test observation with good station
    let good_obs = create_observation_with_good_station("obs1", 123);
    assert!(!needs_station_enrichment(&good_obs));

    // Test observation with missing station
    let missing_obs = create_observation_with_missing_station("obs1", 999);
    assert!(needs_station_enrichment(&missing_obs));

    // Test observation with no processing flags
    let station = create_test_station(123, "TEST");
    let no_flags_obs = create_test_observation(
        "obs1",
        123,
        station,
        record_status::ORIGINAL as i32,
        HashMap::new(),
    );
    assert!(needs_station_enrichment(&no_flags_obs));

    // Test observation with different processing flag
    let mut other_flags = HashMap::new();
    other_flags.insert("measurement".to_string(), ProcessingFlag::ParseOk);
    let station = create_test_station(123, "TEST");
    let other_obs = create_test_observation(
        "obs1",
        123,
        station,
        record_status::ORIGINAL as i32,
        other_flags,
    );
    assert!(needs_station_enrichment(&other_obs));
}

#[test]
fn test_analyze_enrichment_needs() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
        create_observation_with_good_station("obs3", 124),
        create_observation_with_missing_station("obs4", 998),
    ];

    let (total, needs_enrichment, missing_stations) = analyze_enrichment_needs(&observations);

    assert_eq!(total, 4);
    assert_eq!(needs_enrichment, 2); // The two with missing stations
    assert_eq!(missing_stations, 2); // The two explicitly marked as missing
}

#[test]
fn test_analyze_enrichment_needs_empty() {
    let observations: Vec<Observation> = vec![];
    let (total, needs_enrichment, missing_stations) = analyze_enrichment_needs(&observations);

    assert_eq!(total, 0);
    assert_eq!(needs_enrichment, 0);
    assert_eq!(missing_stations, 0);
}

#[test]
fn test_analyze_enrichment_needs_all_good() {
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_good_station("obs2", 124),
        create_observation_with_good_station("obs3", 125),
    ];

    let (total, needs_enrichment, missing_stations) = analyze_enrichment_needs(&observations);

    assert_eq!(total, 3);
    assert_eq!(needs_enrichment, 0);
    assert_eq!(missing_stations, 0);
}

#[test]
fn test_analyze_enrichment_needs_all_missing() {
    let observations = vec![
        create_observation_with_missing_station("obs1", 999),
        create_observation_with_missing_station("obs2", 998),
        create_observation_with_missing_station("obs3", 997),
    ];

    let (total, needs_enrichment, missing_stations) = analyze_enrichment_needs(&observations);

    assert_eq!(total, 3);
    assert_eq!(needs_enrichment, 3);
    assert_eq!(missing_stations, 3);
}

#[test]
fn test_analyze_enrichment_needs_mixed_flags() {
    // Create observations with various processing flag scenarios
    let mut observations = vec![];

    // Good station
    observations.push(create_observation_with_good_station("obs1", 123));

    // Missing station
    observations.push(create_observation_with_missing_station("obs2", 999));

    // No processing flags at all
    let station = create_test_station(124, "TEST");
    observations.push(create_test_observation(
        "obs3",
        124,
        station,
        record_status::ORIGINAL as i32,
        HashMap::new(),
    ));

    let (total, needs_enrichment, missing_stations) = analyze_enrichment_needs(&observations);

    assert_eq!(total, 3);
    assert_eq!(needs_enrichment, 2); // Missing station + no flags
    assert_eq!(missing_stations, 1); // Only the explicitly missing one
}

#[tokio::test]
async fn test_re_enrich_station_metadata_error_handling() {
    // Create a registry that might fail lookups
    let station_registry = create_mock_station_registry();
    let mut stats = ProcessingStats::new();
    stats.total_input = 1;

    // Create an observation that might cause issues during enrichment
    let observation = create_observation_with_missing_station("obs1", 999);

    // Test that even with a "failed" lookup, the function continues processing
    let observations = vec![observation];
    let result =
        re_enrich_station_metadata(observations, &station_registry, &mut stats, None).await;

    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), 1);

    // The observation should still be present, just marked as missing station
    assert_eq!(
        enriched[0].get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
}

#[tokio::test]
async fn test_re_enrich_station_metadata_preserves_other_flags() {
    let station_registry = create_mock_station_registry();
    let mut stats = ProcessingStats::new();
    stats.total_input = 1;

    // Create observation with multiple processing flags
    let mut processing_flags = HashMap::new();
    processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("humidity".to_string(), ProcessingFlag::ParseFailed);
    processing_flags.insert("station".to_string(), ProcessingFlag::StationMissing);

    let station = create_test_station(999, "TEST STATION (simulated placeholder)");
    let observations = vec![create_test_observation(
        "obs1",
        999,
        station,
        record_status::ORIGINAL as i32,
        processing_flags,
    )];

    let result =
        re_enrich_station_metadata(observations, &station_registry, &mut stats, None).await;

    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert_eq!(enriched.len(), 1);

    let obs = &enriched[0];

    // Non-station flags should be preserved
    assert_eq!(
        obs.get_processing_flag("air_temperature"),
        Some(ProcessingFlag::ParseOk)
    );
    assert_eq!(
        obs.get_processing_flag("humidity"),
        Some(ProcessingFlag::ParseFailed)
    );

    // Station flag should be updated (still missing in this case)
    assert_eq!(
        obs.get_processing_flag("station"),
        Some(ProcessingFlag::StationMissing)
    );
}
