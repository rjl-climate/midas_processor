//! Comprehensive unit tests for the parquet_writer module
//!
//! This module contains unit tests for all components of the parquet_writer
//! organized by logical functionality.

pub mod config_tests;
pub mod conversion_tests;
pub mod progress_tests;
pub mod schema_tests;
pub mod utils_tests;
pub mod writer_tests;

// Common test utilities used across multiple test modules
use crate::app::models::{Station, Observation};
use chrono::Utc;
use std::collections::HashMap;

/// Create a test station with reasonable operational dates
pub fn create_test_station() -> Station {
    let start_date = Utc::now() - chrono::Duration::try_days(365).unwrap();
    let end_date = Utc::now() + chrono::Duration::try_days(365).unwrap();

    Station::new(
        12345,
        "TEST STATION".to_string(),
        51.5074,
        -0.1278,
        None,
        None,
        None,
        start_date,
        end_date,
        "Met Office".to_string(),
        "Greater London".to_string(),
        25.0,
    ).unwrap()
}

/// Create a test station with specific ID
pub fn create_test_station_with_id(station_id: i32) -> Station {
    let start_date = Utc::now() - chrono::Duration::try_days(365).unwrap();
    let end_date = Utc::now() + chrono::Duration::try_days(365).unwrap();

    Station::new(
        station_id,
        format!("TEST STATION {}", station_id),
        51.5074 + (station_id as f64 * 0.001),
        -0.1278 + (station_id as f64 * 0.001),
        None,
        None,
        None,
        start_date,
        end_date,
        "Met Office".to_string(),
        "Greater London".to_string(),
        25.0 + (station_id as f32 * 0.1),
    ).unwrap()
}

/// Create a test observation with default measurements
pub fn create_test_observation() -> Observation {
    create_test_observation_with_measurements(vec![
        ("air_temperature", 15.5),
        ("wind_speed", 10.2),
    ])
}

/// Create a test observation with specific measurements
pub fn create_test_observation_with_measurements(measurements: Vec<(&str, f64)>) -> Observation {
    let mut measurement_map = HashMap::new();
    let mut quality_flags = HashMap::new();

    for (name, value) in measurements {
        measurement_map.insert(name.to_string(), value);
        quality_flags.insert(name.to_string(), "0".to_string());
    }

    Observation::new(
        Utc::now(),
        24,
        "12345".to_string(),
        12345,
        "SRCE".to_string(),
        "UK-DAILY-TEMPERATURE-OBS".to_string(),
        1001,
        1,
        create_test_station(),
        measurement_map,
        quality_flags,
        HashMap::new(),
        None,
        None,
    ).unwrap()
}

/// Create a test observation with specific station
pub fn create_test_observation_with_station(station: Station) -> Observation {
    let mut measurements = HashMap::new();
    measurements.insert("air_temperature".to_string(), 15.5);

    let mut quality_flags = HashMap::new();
    quality_flags.insert("air_temperature".to_string(), "0".to_string());

    Observation::new(
        Utc::now(),
        24,
        station.src_id.to_string(),
        station.src_id,
        "SRCE".to_string(),
        "UK-DAILY-TEMPERATURE-OBS".to_string(),
        1001,
        1,
        station,
        measurements,
        quality_flags,
        HashMap::new(),
        None,
        None,
    ).unwrap()
}

/// Create multiple test observations with varying data
pub fn create_test_observations(count: usize) -> Vec<Observation> {
    (0..count)
        .map(|i| {
            let measurements = vec![
                ("air_temperature", 15.0 + (i as f64 * 0.1)),
                ("wind_speed", 10.0 + (i as f64 * 0.2)),
                ("humidity", 80.0 + (i as f64 * 0.3)),
            ];
            create_test_observation_with_measurements(measurements)
        })
        .collect()
}

/// Create test observations with missing data
pub fn create_test_observations_with_missing_data(count: usize) -> Vec<Observation> {
    (0..count)
        .map(|i| {
            let measurements = if i % 3 == 0 {
                // Every third observation has missing wind_speed
                vec![("air_temperature", 15.0 + (i as f64 * 0.1))]
            } else if i % 5 == 0 {
                // Every fifth observation has missing air_temperature
                vec![("wind_speed", 10.0 + (i as f64 * 0.2))]
            } else {
                // Normal observations with both measurements
                vec![
                    ("air_temperature", 15.0 + (i as f64 * 0.1)),
                    ("wind_speed", 10.0 + (i as f64 * 0.2)),
                ]
            };
            create_test_observation_with_measurements(measurements)
        })
        .collect()
}

/// Create test observations with NaN values
pub fn create_test_observations_with_nan(count: usize) -> Vec<Observation> {
    (0..count)
        .map(|i| {
            let measurements = if i % 4 == 0 {
                // Every fourth observation has NaN air_temperature
                vec![
                    ("air_temperature", f64::NAN),
                    ("wind_speed", 10.0 + (i as f64 * 0.2)),
                ]
            } else {
                vec![
                    ("air_temperature", 15.0 + (i as f64 * 0.1)),
                    ("wind_speed", 10.0 + (i as f64 * 0.2)),
                ]
            };
            create_test_observation_with_measurements(measurements)
        })
        .collect()
}

/// Assert that two f64 values are approximately equal
pub fn assert_approx_eq(a: f64, b: f64, tolerance: f64) {
    assert!(
        (a - b).abs() < tolerance,
        "Values {} and {} are not approximately equal (tolerance: {})",
        a, b, tolerance
    );
}

/// Assert that two f32 values are approximately equal
pub fn assert_approx_eq_f32(a: f32, b: f32, tolerance: f32) {
    assert!(
        (a - b).abs() < tolerance,
        "Values {} and {} are not approximately equal (tolerance: {})",
        a, b, tolerance
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_station() {
        let station = create_test_station();
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "TEST STATION");
        assert!(station.high_prcn_lat > 0.0);
        assert!(station.high_prcn_lon < 0.0);
    }

    #[test]
    fn test_create_test_station_with_id() {
        let station = create_test_station_with_id(54321);
        assert_eq!(station.src_id, 54321);
        assert!(station.src_name.contains("54321"));
    }

    #[test]
    fn test_create_test_observation() {
        let observation = create_test_observation();
        assert_eq!(observation.station_id, 12345);
        assert!(observation.measurements.contains_key("air_temperature"));
        assert!(observation.quality_flags.contains_key("air_temperature"));
    }

    #[test]
    fn test_create_test_observations() {
        let observations = create_test_observations(5);
        assert_eq!(observations.len(), 5);
        
        // Check that measurements vary
        let temps: Vec<f64> = observations.iter()
            .map(|obs| obs.measurements["air_temperature"])
            .collect();
        assert_ne!(temps[0], temps[1]);
    }

    #[test]
    fn test_create_test_observations_with_missing_data() {
        let observations = create_test_observations_with_missing_data(10);
        assert_eq!(observations.len(), 10);
        
        // Check that some observations have missing data
        let has_missing = observations.iter().any(|obs| obs.measurements.len() < 2);
        assert!(has_missing);
    }

    #[test]
    fn test_create_test_observations_with_nan() {
        let observations = create_test_observations_with_nan(8);
        assert_eq!(observations.len(), 8);
        
        // Check that some observations have NaN values
        let has_nan = observations.iter().any(|obs| {
            obs.measurements.values().any(|&v| v.is_nan())
        });
        assert!(has_nan);
    }

    #[test]
    fn test_assert_approx_eq() {
        assert_approx_eq(1.0, 1.001, 0.01);
        assert_approx_eq_f32(1.0f32, 1.001f32, 0.01f32);
    }

    #[test]
    #[should_panic]
    fn test_assert_approx_eq_fails() {
        assert_approx_eq(1.0, 2.0, 0.01);
    }
}