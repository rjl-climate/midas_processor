//! Tests for station registry loading functionality

use super::*;
use crate::app::services::station_registry::StationRegistry;
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
async fn test_station_registry_new() {
    let cache_path = PathBuf::from("/test/cache");
    let registry = StationRegistry::new(cache_path.clone());

    assert_eq!(registry.cache_path, cache_path);
    assert_eq!(registry.station_count(), 0);
    assert!(registry.loaded_datasets.is_empty());
}

#[tokio::test]
async fn test_load_from_cache_success() {
    let temp_dir = TempDir::new().unwrap();
    let cache_path = create_test_cache_structure(&temp_dir).unwrap();

    // Test single-dataset loading for temperature dataset
    let dataset = "uk-daily-temperature-obs";

    let (registry, stats) = StationRegistry::load_for_dataset(&cache_path, dataset, false)
        .await
        .unwrap();

    // Verify registry properties - single dataset loading
    assert_eq!(registry.station_count(), 1);
    assert_eq!(registry.loaded_datasets, vec![dataset]);

    // Verify load statistics
    assert_eq!(stats.datasets_processed, 1);
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.stations_loaded, 1);
    assert!(stats.total_records_found > 0);
    assert!(stats.errors.is_empty());

    // Verify specific station (only from temperature dataset)
    assert!(registry.contains_station(12345));

    let station = registry.get_station(12345).unwrap();
    assert_eq!(station.src_name, "test-station-1");
    assert_eq!(station.authority, "Met Office");
}

#[tokio::test]
async fn test_load_for_dataset_nonexistent_path() {
    let cache_path = PathBuf::from("/nonexistent/path");
    let dataset = "uk-daily-temperature-obs";

    let result = StationRegistry::load_for_dataset(&cache_path, dataset, false).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_discover_station_files() {
    let temp_dir = TempDir::new().unwrap();
    let cache_path = create_test_cache_structure(&temp_dir).unwrap();

    let datasets = vec!["uk-daily-temperature-obs".to_string()];
    let (capability_files, metadata_files) =
        StationRegistry::discover_station_files(&cache_path, &datasets).unwrap();

    assert_eq!(capability_files.len(), 1);
    assert!(
        capability_files[0]
            .to_string_lossy()
            .contains("station1.csv")
    );
    assert_eq!(metadata_files.len(), 0);
}

#[tokio::test]
async fn test_load_capability_file() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    create_test_capability_file(temp_dir.path(), "test.csv", 12345, "test-station-a").unwrap();

    let (stations, total_records) = StationRegistry::load_capability_file(&file_path)
        .await
        .unwrap();

    assert_eq!(stations.len(), 1);
    assert!(total_records > 0);

    let station = &stations[0];
    assert_eq!(station.src_id, 12345);
    assert_eq!(station.src_name, "test-station-a");
    assert_eq!(station.authority, "Met Office");
}
