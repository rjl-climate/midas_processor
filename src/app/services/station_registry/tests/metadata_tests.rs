//! Tests for station registry metadata and statistics

use crate::app::services::station_registry::{
    StationRegistry,
    metadata::{LoadStats, RegistryMetadata},
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[test]
fn test_load_stats_new() {
    let stats = LoadStats::new();
    assert_eq!(stats.datasets_processed, 0);
    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.total_records_found, 0);
    assert_eq!(stats.stations_loaded, 0);
    assert_eq!(stats.records_filtered, 0);
    assert_eq!(stats.load_duration, Duration::ZERO);
    assert!(stats.errors.is_empty());
}

#[test]
fn test_load_stats_default() {
    let stats = LoadStats::default();
    assert_eq!(stats.datasets_processed, 0);
    assert_eq!(stats.stations_loaded, 0);
    assert!(!stats.has_errors());
}

#[test]
fn test_load_stats_calculations() {
    let mut stats = LoadStats::new();
    stats.datasets_processed = 2;
    stats.files_processed = 10;
    stats.total_records_found = 1000;
    stats.stations_loaded = 800;
    stats.records_filtered = 200;
    stats.load_duration = Duration::from_secs(4);

    // Test filter rate calculation
    assert_eq!(stats.filter_rate(), 20.0);

    // Test loading rate calculation
    assert_eq!(stats.loading_rate(), 200.0);

    // Test error status
    assert!(!stats.has_errors());
    stats.errors.push("test error".to_string());
    assert!(stats.has_errors());

    // Test summary
    let summary = stats.summary();
    assert!(summary.contains("2 datasets"));
    assert!(summary.contains("10 files"));
    assert!(summary.contains("800 stations"));
    assert!(summary.contains("20.0% filtered"));
    assert!(summary.contains("4.00s"));
}

#[test]
fn test_load_stats_edge_cases() {
    let mut stats = LoadStats::new();

    // Test zero values
    assert_eq!(stats.filter_rate(), 0.0);
    assert_eq!(stats.loading_rate(), 0.0);

    // Test division by zero cases
    stats.total_records_found = 0;
    stats.stations_loaded = 100;
    assert_eq!(stats.filter_rate(), 0.0);

    stats.load_duration = Duration::ZERO;
    assert_eq!(stats.loading_rate(), 0.0);
}

#[test]
fn test_load_stats_filter_rate_edge_cases() {
    let mut stats = LoadStats::new();

    // Case 1: No filtering (all records loaded)
    stats.total_records_found = 100;
    stats.stations_loaded = 100;
    stats.records_filtered = 0;
    assert_eq!(stats.filter_rate(), 0.0);

    // Case 2: All records filtered
    stats.total_records_found = 100;
    stats.stations_loaded = 0;
    stats.records_filtered = 100;
    assert_eq!(stats.filter_rate(), 100.0);

    // Case 3: Partial filtering
    stats.total_records_found = 100;
    stats.stations_loaded = 75;
    stats.records_filtered = 25;
    assert_eq!(stats.filter_rate(), 25.0);
}

#[test]
fn test_registry_metadata() {
    let metadata = RegistryMetadata {
        cache_path: PathBuf::from("/test/cache"),
        loaded_datasets: vec![
            "uk-daily-temperature-obs".to_string(),
            "uk-daily-rain-obs".to_string(),
        ],
        station_count: 500,
        load_time: Instant::now(),
        files_processed: 10,
        total_records_found: 600,
    };

    // Test multi-dataset detection
    assert!(metadata.is_multi_dataset());

    // Test age (should be very recent)
    assert!(metadata.age().as_millis() < 100);

    // Test summary
    let summary = metadata.summary();
    assert!(summary.contains("500 stations"));
    assert!(summary.contains("2 datasets"));
    assert!(summary.contains("age:"));
}

#[test]
fn test_registry_metadata_single_dataset() {
    let metadata = RegistryMetadata {
        cache_path: PathBuf::from("/test/cache"),
        loaded_datasets: vec!["uk-daily-temperature-obs".to_string()],
        station_count: 300,
        load_time: Instant::now(),
        files_processed: 5,
        total_records_found: 350,
    };

    // Test single dataset detection
    assert!(!metadata.is_multi_dataset());

    let summary = metadata.summary();
    assert!(summary.contains("300 stations"));
    assert!(summary.contains("1 datasets"));
}

#[test]
fn test_registry_metadata_empty() {
    let metadata = RegistryMetadata {
        cache_path: PathBuf::from("/test/cache"),
        loaded_datasets: vec![],
        station_count: 0,
        load_time: Instant::now(),
        files_processed: 0,
        total_records_found: 0,
    };

    assert!(!metadata.is_multi_dataset());

    let summary = metadata.summary();
    assert!(summary.contains("0 stations"));
    assert!(summary.contains("0 datasets"));
}

#[test]
fn test_station_registry_metadata_integration() {
    let cache_path = PathBuf::from("/test/cache");
    let mut registry = StationRegistry::new(cache_path.clone());

    // Simulate loaded data
    registry.loaded_datasets = vec!["uk-daily-temperature-obs".to_string()];
    registry.files_processed = 5;
    registry.total_records_found = 100;

    let metadata = registry.metadata();

    assert_eq!(metadata.cache_path, cache_path);
    assert_eq!(metadata.loaded_datasets, vec!["uk-daily-temperature-obs"]);
    assert_eq!(metadata.station_count, 0); // No stations added in this test
    assert_eq!(metadata.files_processed, 5);
    assert_eq!(metadata.total_records_found, 100);
    assert!(!metadata.is_multi_dataset());
}

#[test]
fn test_load_stats_realistic_scenario() {
    let mut stats = LoadStats::new();

    // Simulate realistic MIDAS loading scenario
    stats.datasets_processed = 3;
    stats.files_processed = 150;
    stats.total_records_found = 2500;
    stats.stations_loaded = 1800;
    stats.records_filtered = 700;
    stats.load_duration = Duration::from_millis(2500);
    stats.errors = vec![
        "Failed to parse station 12345: invalid date".to_string(),
        "Missing capability file for dataset xyz".to_string(),
    ];

    assert!((stats.filter_rate() - 28.0).abs() < 0.0001);
    assert_eq!(stats.loading_rate(), 720.0);
    assert!(stats.has_errors());
    assert_eq!(stats.errors.len(), 2);

    let summary = stats.summary();
    assert!(summary.contains("3 datasets"));
    assert!(summary.contains("150 files"));
    assert!(summary.contains("1800 stations"));
    assert!(summary.contains("28.0% filtered"));
    assert!(summary.contains("2.50s"));
}

#[test]
fn test_load_stats_performance_metrics() {
    let mut stats = LoadStats::new();

    // Test very fast loading
    stats.stations_loaded = 1000;
    stats.load_duration = Duration::from_millis(100);
    assert_eq!(stats.loading_rate(), 10000.0);

    // Test slower loading
    stats.stations_loaded = 100;
    stats.load_duration = Duration::from_secs(10);
    assert_eq!(stats.loading_rate(), 10.0);

    // Test very slow loading
    stats.stations_loaded = 10;
    stats.load_duration = Duration::from_secs(60);
    assert!((stats.loading_rate() - 0.1666666666666667).abs() < 0.0001);
}

#[test]
fn test_registry_age_tracking() {
    let start_time = Instant::now();

    let metadata = RegistryMetadata {
        cache_path: PathBuf::from("/test"),
        loaded_datasets: vec!["test".to_string()],
        station_count: 1,
        load_time: start_time,
        files_processed: 1,
        total_records_found: 1,
    };

    // Sleep briefly to ensure measurable age
    std::thread::sleep(Duration::from_millis(10));

    let age = metadata.age();
    assert!(age.as_millis() >= 10);
    assert!(age.as_millis() < 1000); // Should be less than 1 second
}
