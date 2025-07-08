//! Comprehensive tests for record processor module
//!
//! This module provides unit and integration tests for all record processing components.

pub mod deduplication_tests;
pub mod enrichment_tests;
pub mod processor_tests;
pub mod quality_filter_tests;
pub mod stats_tests;

// Test helper functions and fixtures
use crate::app::models::{Observation, ProcessingFlag, Station};
use crate::app::services::station_registry::StationRegistry;
use crate::config::QualityControlConfig;
use crate::constants::record_status;
use chrono::{TimeZone, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Create a test station for testing purposes
pub fn create_test_station(src_id: i32, name: &str) -> Station {
    Station {
        src_id,
        src_name: name.to_string(),
        high_prcn_lat: 51.5074,
        high_prcn_lon: -0.1278,
        east_grid_ref: Some(530000),
        north_grid_ref: Some(180000),
        grid_ref_type: Some("OSGB".to_string()),
        src_bgn_date: Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
        src_end_date: Utc.with_ymd_and_hms(2099, 12, 31, 23, 59, 59).unwrap(),
        authority: "Met Office".to_string(),
        historic_county: "Greater London".to_string(),
        height_meters: 25.0,
        id_history: Vec::new(),
    }
}

/// Create a test observation with specified parameters
pub fn create_test_observation(
    observation_id: &str,
    station_id: i32,
    station: Station,
    rec_st_ind: i32,
    processing_flags: HashMap<String, ProcessingFlag>,
) -> Observation {
    let mut measurements = HashMap::new();
    measurements.insert("air_temperature".to_string(), 15.5);
    measurements.insert("humidity".to_string(), 75.0);

    let mut quality_flags = HashMap::new();
    quality_flags.insert("air_temperature".to_string(), "0".to_string());
    quality_flags.insert("humidity".to_string(), "1".to_string());

    Observation {
        ob_end_time: Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
        ob_hour_count: 1,
        observation_id: observation_id.to_string(),
        station_id,
        id_type: "SRCE".to_string(),
        met_domain_name: "UK-DAILY-TEMPERATURE-OBS".to_string(),
        rec_st_ind,
        version_num: 1,
        station,
        measurements,
        quality_flags,
        processing_flags,
        meto_stmp_time: Some(Utc.with_ymd_and_hms(2023, 6, 15, 13, 0, 0).unwrap()),
        midas_stmp_etime: Some(3600),
    }
}

/// Create a test observation with good station metadata
pub fn create_observation_with_good_station(observation_id: &str, station_id: i32) -> Observation {
    let station = create_test_station(station_id, "TEST STATION");
    let mut processing_flags = HashMap::new();
    processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("humidity".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

    create_test_observation(
        observation_id,
        station_id,
        station,
        record_status::ORIGINAL as i32,
        processing_flags,
    )
}

/// Create a test observation with missing station metadata
/// Create a test observation that simulates what would have been created with missing station
/// NOTE: This is now only for testing enrichment behavior. In production, missing stations
/// cause parsing to fail entirely.
pub fn create_observation_with_missing_station(
    observation_id: &str,
    station_id: i32,
) -> Observation {
    // Use a valid test station but mark it as missing for enrichment testing
    let station = create_test_station(station_id, "TEST STATION (simulated missing)");
    let mut processing_flags = HashMap::new();
    processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("humidity".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("station".to_string(), ProcessingFlag::StationMissing);

    create_test_observation(
        observation_id,
        station_id,
        station,
        record_status::ORIGINAL as i32,
        processing_flags,
    )
}

/// Create a test observation with parse failures
pub fn create_observation_with_parse_failures(
    observation_id: &str,
    station_id: i32,
) -> Observation {
    let station = create_test_station(station_id, "TEST STATION");
    let mut processing_flags = HashMap::new();
    processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseFailed);
    processing_flags.insert("humidity".to_string(), ProcessingFlag::ParseOk);
    processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

    let mut observation = create_test_observation(
        observation_id,
        station_id,
        station,
        record_status::ORIGINAL as i32,
        processing_flags,
    );

    // Remove the failed measurement
    observation.measurements.remove("air_temperature");
    observation
}

/// Create duplicate observations for testing deduplication
pub fn create_duplicate_observations() -> Vec<Observation> {
    let station_id = 12345;
    let observation_id = "TEST_OBS_001";
    let timestamp = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();

    // Create original and corrected versions of the same observation
    let mut original = create_observation_with_good_station(observation_id, station_id);
    original.ob_end_time = timestamp;
    original.rec_st_ind = record_status::ORIGINAL as i32;

    let mut corrected = create_observation_with_good_station(observation_id, station_id);
    corrected.ob_end_time = timestamp;
    corrected.rec_st_ind = record_status::CORRECTED as i32;
    // Add an extra measurement to make it "better"
    corrected
        .measurements
        .insert("wind_speed".to_string(), 10.5);

    vec![original, corrected]
}

/// Create a mock station registry for testing
pub fn create_mock_station_registry() -> Arc<StationRegistry> {
    // Create a registry with some test stations
    let registry = StationRegistry::new(PathBuf::from("/tmp/test"));

    // Manually insert test stations (this would normally be loaded from files)
    // Note: This requires friendship with StationRegistry internals for testing
    // In a real implementation, we'd use dependency injection or test fixtures

    Arc::new(registry)
}

/// Create a test quality control configuration (strict processing requirements)
pub fn create_test_quality_config() -> QualityControlConfig {
    QualityControlConfig {
        require_station_metadata: true,
        exclude_empty_measurements: true,
    }
}

/// Create a permissive quality control configuration (lenient processing requirements)
pub fn create_permissive_quality_config() -> QualityControlConfig {
    QualityControlConfig {
        require_station_metadata: false,
        exclude_empty_measurements: false,
    }
}
