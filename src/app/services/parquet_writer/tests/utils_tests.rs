//! Comprehensive unit tests for utility functions

use super::{
    create_test_observation, create_test_observations, create_test_observations_with_missing_data,
    create_test_station_with_id, assert_approx_eq,
};
use crate::app::services::parquet_writer::{
    config::{WriterConfig, WritingStats},
    utils::*,
};
use parquet::basic::Compression;
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
async fn test_write_dataset_to_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(10);
    let config = WriterConfig::default().with_write_batch_size(5);

    let stats = write_dataset_to_parquet(
        "test_dataset",
        observations,
        temp_dir.path(),
        config,
    ).await.unwrap();

    assert_eq!(stats.observations_written, 10);
    assert!(stats.batches_written > 0);
    assert!(stats.bytes_written > 0);

    let expected_path = temp_dir.path().join("test_dataset.parquet");
    assert!(expected_path.exists());
}

#[tokio::test]
async fn test_write_dataset_to_parquet_empty() {
    let temp_dir = TempDir::new().unwrap();
    let observations = vec![];
    let config = WriterConfig::default();

    let stats = write_dataset_to_parquet(
        "empty_dataset",
        observations,
        temp_dir.path(),
        config,
    ).await.unwrap();

    assert_eq!(stats.observations_written, 0);
    assert_eq!(stats.batches_written, 0);
    assert_eq!(stats.bytes_written, 0);

    let expected_path = temp_dir.path().join("empty_dataset.parquet");
    assert!(expected_path.exists());
}

#[tokio::test]
async fn test_write_multiple_datasets() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriterConfig::default();

    let datasets = vec![
        ("dataset1", create_test_observations(5)),
        ("dataset2", create_test_observations(8)),
        ("dataset3", create_test_observations(12)),
    ];

    let results = write_multiple_datasets(datasets, temp_dir.path(), config).await.unwrap();
    
    assert_eq!(results.len(), 3);
    assert_eq!(results["dataset1"].observations_written, 5);
    assert_eq!(results["dataset2"].observations_written, 8);
    assert_eq!(results["dataset3"].observations_written, 12);

    // Check all files exist
    assert!(temp_dir.path().join("dataset1.parquet").exists());
    assert!(temp_dir.path().join("dataset2.parquet").exists());
    assert!(temp_dir.path().join("dataset3.parquet").exists());
}

#[tokio::test]
async fn test_write_multiple_datasets_empty() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriterConfig::default();
    let datasets = vec![];

    let results = write_multiple_datasets(datasets, temp_dir.path(), config).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_write_station_grouped_data() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriterConfig::default();

    // Create observations for different stations
    let station1 = create_test_station_with_id(1001);
    let station2 = create_test_station_with_id(1002);
    let station3 = create_test_station_with_id(1003);

    let mut all_observations = vec![];
    
    // Add observations for each station
    for _ in 0..5 {
        all_observations.push(super::create_test_observation_with_station(station1.clone()));
    }
    for _ in 0..3 {
        all_observations.push(super::create_test_observation_with_station(station2.clone()));
    }
    for _ in 0..7 {
        all_observations.push(super::create_test_observation_with_station(station3.clone()));
    }

    let results = write_station_grouped_data(
        all_observations,
        temp_dir.path(),
        config,
    ).await.unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results["station_1001"].observations_written, 5);
    assert_eq!(results["station_1002"].observations_written, 3);
    assert_eq!(results["station_1003"].observations_written, 7);

    // Check all station files exist
    assert!(temp_dir.path().join("station_1001.parquet").exists());
    assert!(temp_dir.path().join("station_1002.parquet").exists());
    assert!(temp_dir.path().join("station_1003.parquet").exists());
}

#[tokio::test]
async fn test_write_station_grouped_data_empty() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriterConfig::default();

    let results = write_station_grouped_data(
        vec![],
        temp_dir.path(),
        config,
    ).await.unwrap();

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_create_dataset_writer() {
    let temp_dir = TempDir::new().unwrap();
    let config = WriterConfig::default().with_compression(Compression::GZIP);

    let writer = create_dataset_writer("test_dataset", temp_dir.path(), config.clone()).await.unwrap();
    
    assert_eq!(writer.config().compression, Compression::GZIP);
    
    let expected_path = temp_dir.path().join("test_dataset.parquet");
    assert_eq!(writer.output_path(), &expected_path);
}

#[tokio::test]
async fn test_create_dataset_writer_invalid_path() {
    let invalid_path = PathBuf::from("/invalid/nonexistent/directory");
    let config = WriterConfig::default();

    let result = create_dataset_writer("test", &invalid_path, config).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_validate_output_directory() {
    let temp_dir = TempDir::new().unwrap();
    
    // Valid directory should pass
    let result = validate_output_directory(temp_dir.path());
    assert!(result.is_ok());

    // Non-existent directory should fail
    let invalid_path = temp_dir.path().join("nonexistent");
    let result = validate_output_directory(&invalid_path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));

    // File instead of directory should fail
    let file_path = temp_dir.path().join("not_a_directory");
    std::fs::write(&file_path, "test").unwrap();
    let result = validate_output_directory(&file_path);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("is not a directory"));
}

#[tokio::test]
async fn test_generate_output_filename() {
    assert_eq!(generate_output_filename("dataset1"), "dataset1.parquet");
    assert_eq!(generate_output_filename("my-data"), "my-data.parquet");
    assert_eq!(generate_output_filename("test_data_2024"), "test_data_2024.parquet");
    
    // Test with special characters (should be sanitized)
    assert_eq!(generate_output_filename("data/with\\slashes"), "data_with_slashes.parquet");
    assert_eq!(generate_output_filename("data with spaces"), "data_with_spaces.parquet");
    assert_eq!(generate_output_filename("data:with:colons"), "data_with_colons.parquet");
}

#[tokio::test]
async fn test_generate_output_filename_edge_cases() {
    // Empty name
    assert_eq!(generate_output_filename(""), "unnamed.parquet");
    
    // Only special characters
    assert_eq!(generate_output_filename("///\\\\"), "unnamed.parquet");
    
    // Very long name (should be truncated)
    let long_name = "a".repeat(300);
    let result = generate_output_filename(&long_name);
    assert!(result.len() < 260); // Should be truncated to reasonable length
    assert!(result.ends_with(".parquet"));
}

#[tokio::test]
async fn test_sanitize_filename() {
    assert_eq!(sanitize_filename("normal_name"), "normal_name");
    assert_eq!(sanitize_filename("name-with-dashes"), "name-with-dashes");
    assert_eq!(sanitize_filename("name.with.dots"), "name.with.dots");
    
    // Test replacement of invalid characters
    assert_eq!(sanitize_filename("path/with\\slashes"), "path_with_slashes");
    assert_eq!(sanitize_filename("name with spaces"), "name_with_spaces");
    assert_eq!(sanitize_filename("name:with:colons"), "name_with_colons");
    assert_eq!(sanitize_filename("name|with|pipes"), "name_with_pipes");
    assert_eq!(sanitize_filename("name\"with\"quotes"), "name_with_quotes");
    assert_eq!(sanitize_filename("name<with>brackets"), "name_with_brackets");
    assert_eq!(sanitize_filename("name?with*wildcards"), "name_with_wildcards");
}

#[tokio::test]
async fn test_estimate_memory_usage() {
    let observations = create_test_observations(100);
    let usage = estimate_memory_usage(&observations);
    
    assert!(usage > 0);
    // Should be roughly proportional to number of observations
    assert!(usage > 1000); // Each observation should use some memory
    
    let empty_observations = vec![];
    let empty_usage = estimate_memory_usage(&empty_observations);
    assert_eq!(empty_usage, 0);
}

#[tokio::test]
async fn test_estimate_memory_usage_scaling() {
    let small_observations = create_test_observations(10);
    let large_observations = create_test_observations(100);
    
    let small_usage = estimate_memory_usage(&small_observations);
    let large_usage = estimate_memory_usage(&large_observations);
    
    // Large dataset should use more memory
    assert!(large_usage > small_usage);
    // Should be roughly proportional (allowing for some overhead)
    assert!(large_usage > small_usage * 5);
}

#[tokio::test]
async fn test_optimize_config_for_dataset() {
    let small_observations = create_test_observations(50);
    let large_observations = create_test_observations(10000);
    
    let base_config = WriterConfig::default();
    
    let small_config = optimize_config_for_dataset(&base_config, &small_observations);
    let large_config = optimize_config_for_dataset(&base_config, &large_observations);
    
    // Small dataset might use smaller batch sizes
    // Large dataset might use larger row groups for better compression
    assert!(small_config.row_group_size <= large_config.row_group_size);
}

#[tokio::test]
async fn test_optimize_config_for_dataset_empty() {
    let empty_observations = vec![];
    let base_config = WriterConfig::default();
    
    let optimized_config = optimize_config_for_dataset(&base_config, &empty_observations);
    
    // Should return reasonable defaults for empty dataset
    assert!(optimized_config.row_group_size > 0);
    assert!(optimized_config.write_batch_size > 0);
}

#[tokio::test]
async fn test_calculate_optimal_batch_size() {
    let small_dataset = create_test_observations(10);
    let medium_dataset = create_test_observations(1000);
    let large_dataset = create_test_observations(100000);
    
    let small_batch = calculate_optimal_batch_size(&small_dataset, 100 * 1024 * 1024);
    let medium_batch = calculate_optimal_batch_size(&medium_dataset, 100 * 1024 * 1024);
    let large_batch = calculate_optimal_batch_size(&large_dataset, 100 * 1024 * 1024);
    
    // Batch sizes should be reasonable
    assert!(small_batch > 0);
    assert!(medium_batch > 0);
    assert!(large_batch > 0);
    
    // Larger datasets might use larger batches (up to memory constraints)
    assert!(small_batch <= medium_batch);
}

#[tokio::test]
async fn test_calculate_optimal_batch_size_memory_constraints() {
    let observations = create_test_observations(1000);
    
    let small_memory_batch = calculate_optimal_batch_size(&observations, 1024 * 1024); // 1MB
    let large_memory_batch = calculate_optimal_batch_size(&observations, 100 * 1024 * 1024); // 100MB
    
    // Should respect memory constraints
    assert!(small_memory_batch > 0);
    assert!(large_memory_batch > 0);
    assert!(small_memory_batch <= large_memory_batch);
}

#[tokio::test]
async fn test_get_compression_stats() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(100);
    let config = WriterConfig::default();

    // Write with compression
    let stats = write_dataset_to_parquet(
        "test_dataset",
        observations.clone(),
        temp_dir.path(),
        config,
    ).await.unwrap();

    let estimated_original_size = estimate_memory_usage(&observations);
    let compression_stats = get_compression_stats(&stats, estimated_original_size);
    
    assert!(compression_stats.compressed_size > 0);
    assert!(compression_stats.original_size > 0);
    assert!(compression_stats.compression_ratio > 0.0);
    assert!(compression_stats.compression_ratio <= 1.0);
    assert!(compression_stats.space_saved_bytes >= 0);
}

#[tokio::test]
async fn test_get_compression_stats_edge_cases() {
    let stats = WritingStats::new();
    
    // Zero bytes written
    let compression_stats = get_compression_stats(&stats, 1000);
    assert_eq!(compression_stats.compressed_size, 0);
    assert_eq!(compression_stats.original_size, 1000);
    assert_eq!(compression_stats.compression_ratio, 0.0);
    assert_eq!(compression_stats.space_saved_bytes, 1000);
    
    // Zero original size
    let mut stats = WritingStats::new();
    stats.bytes_written = 1000;
    let compression_stats = get_compression_stats(&stats, 0);
    assert_eq!(compression_stats.compression_ratio, 0.0);
}

#[tokio::test]
async fn test_write_processing_summary() {
    let temp_dir = TempDir::new().unwrap();
    let summary_path = temp_dir.path().join("summary.txt");

    let mut stats = WritingStats::new();
    stats.observations_written = 1000;
    stats.batches_written = 10;
    stats.bytes_written = 50000;
    stats.peak_memory_usage_bytes = 10 * 1024 * 1024;

    let result = write_processing_summary(&stats, &summary_path).await;
    assert!(result.is_ok());
    assert!(summary_path.exists());

    let content = std::fs::read_to_string(&summary_path).unwrap();
    assert!(content.contains("Processing Summary"));
    assert!(content.contains("1000"));
    assert!(content.contains("10"));
}

#[tokio::test]
async fn test_write_processing_summary_invalid_path() {
    let invalid_path = PathBuf::from("/invalid/nonexistent/directory/summary.txt");
    let stats = WritingStats::new();

    let result = write_processing_summary(&stats, &invalid_path).await;
    assert!(result.is_err());
}

#[test]
fn test_compression_stats_calculations() {
    let stats = CompressionStats {
        original_size: 100000,
        compressed_size: 25000,
        compression_ratio: 0.25,
        space_saved_bytes: 75000,
    };

    assert_approx_eq(stats.compression_percentage(), 75.0, 0.01);
    assert_eq!(stats.space_saved_mb(), 75000.0 / (1024.0 * 1024.0));
    
    let no_compression_stats = CompressionStats {
        original_size: 50000,
        compressed_size: 50000,
        compression_ratio: 1.0,
        space_saved_bytes: 0,
    };

    assert_approx_eq(no_compression_stats.compression_percentage(), 0.0, 0.01);
    assert_eq!(no_compression_stats.space_saved_mb(), 0.0);
}

#[test]
fn test_compression_stats_debug() {
    let stats = CompressionStats {
        original_size: 100000,
        compressed_size: 25000,
        compression_ratio: 0.25,
        space_saved_bytes: 75000,
    };

    let debug_str = format!("{:?}", stats);
    assert!(debug_str.contains("CompressionStats"));
    assert!(debug_str.contains("100000"));
    assert!(debug_str.contains("25000"));
}

#[test]
fn test_compression_stats_clone() {
    let stats1 = CompressionStats {
        original_size: 100000,
        compressed_size: 25000,
        compression_ratio: 0.25,
        space_saved_bytes: 75000,
    };

    let stats2 = stats1.clone();
    assert_eq!(stats1.original_size, stats2.original_size);
    assert_eq!(stats1.compressed_size, stats2.compressed_size);
    assert_approx_eq(stats1.compression_ratio, stats2.compression_ratio, 0.001);
}

#[tokio::test]
async fn test_write_dataset_to_parquet_with_progress() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(50);
    let config = WriterConfig::default().with_write_batch_size(10);

    let stats = write_dataset_to_parquet_with_progress(
        "test_dataset",
        observations,
        temp_dir.path(),
        config,
        true, // enable_progress
    ).await.unwrap();

    assert_eq!(stats.observations_written, 50);
    assert!(stats.batches_written > 0);

    let expected_path = temp_dir.path().join("test_dataset.parquet");
    assert!(expected_path.exists());
}

#[tokio::test]
async fn test_write_dataset_to_parquet_no_progress() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(25);
    let config = WriterConfig::default();

    let stats = write_dataset_to_parquet_with_progress(
        "test_dataset",
        observations,
        temp_dir.path(),
        config,
        false, // disable_progress
    ).await.unwrap();

    assert_eq!(stats.observations_written, 25);
}

#[tokio::test]
async fn test_batch_write_observations() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(100);
    let config = WriterConfig::default();

    let results = batch_write_observations(
        observations,
        temp_dir.path(),
        config,
        10, // batch_size
    ).await.unwrap();

    assert_eq!(results.len(), 10); // Should create 10 batches
    
    // Check all batch files exist
    for i in 0..10 {
        let batch_path = temp_dir.path().join(format!("batch_{:03}.parquet", i));
        assert!(batch_path.exists());
    }

    // Check total observations written
    let total_written: usize = results.iter().map(|s| s.observations_written).sum();
    assert_eq!(total_written, 100);
}

#[tokio::test]
async fn test_batch_write_observations_remainder() {
    let temp_dir = TempDir::new().unwrap();
    let observations = create_test_observations(23);
    let config = WriterConfig::default();

    let results = batch_write_observations(
        observations,
        temp_dir.path(),
        config,
        10, // batch_size
    ).await.unwrap();

    assert_eq!(results.len(), 3); // Should create 3 batches (10, 10, 3)
    
    assert_eq!(results[0].observations_written, 10);
    assert_eq!(results[1].observations_written, 10);
    assert_eq!(results[2].observations_written, 3);
}