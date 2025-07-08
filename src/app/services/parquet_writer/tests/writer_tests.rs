//! Comprehensive unit tests for core ParquetWriter functionality

use super::{
    create_test_observation, create_test_observations, create_test_observations_with_missing_data,
    create_test_observations_with_nan, assert_approx_eq,
};
use crate::app::services::parquet_writer::{
    config::{WriterConfig, WritingStats},
    writer::{ParquetWriter, create_optimized_writer},
    schema::create_weather_schema,
};
use parquet::basic::Compression;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_parquet_writer_creation() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();

    let writer = ParquetWriter::new(output_path.clone(), config.clone());
    
    assert_eq!(writer.output_path(), &output_path);
    assert_eq!(writer.config().row_group_size, config.row_group_size);
    assert!(!writer.is_finalized());
    assert!(!writer.schema_initialized());
    assert_eq!(writer.observations_written(), 0);
}

#[test]
fn test_parquet_writer_default() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");

    let writer = ParquetWriter::default(output_path.clone());
    
    assert_eq!(writer.output_path(), &output_path);
    assert_eq!(writer.config().compression, Compression::SNAPPY);
    assert!(!writer.is_finalized());
}

#[tokio::test]
async fn test_parquet_writer_setup_progress() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path, config);

    writer.setup_progress(1000);
    // Should not panic - progress is set up internally
}

#[tokio::test]
async fn test_parquet_writer_write_single_observation() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(1);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observation = create_test_observation();
    let result = writer.write_observations(vec![observation]).await;
    assert!(result.is_ok());

    assert!(writer.schema_initialized());
    assert_eq!(writer.observations_written(), 1);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 1);
    assert!(stats.batches_written > 0);
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_parquet_writer_write_multiple_observations() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(10);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(25);
    let result = writer.write_observations(observations).await;
    assert!(result.is_ok());

    assert_eq!(writer.observations_written(), 25);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 25);
    assert!(stats.batches_written > 1); // Should have multiple batches
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_parquet_writer_batch_processing() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(5);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    // Write in multiple calls to test buffering
    let observations1 = create_test_observations(3);
    let observations2 = create_test_observations(4);
    let observations3 = create_test_observations(8);

    writer.write_observations(observations1).await.unwrap();
    assert_eq!(writer.observations_written(), 3);

    writer.write_observations(observations2).await.unwrap();
    assert_eq!(writer.observations_written(), 7);

    writer.write_observations(observations3).await.unwrap();
    assert_eq!(writer.observations_written(), 15);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 15);
    assert!(stats.batches_written >= 3); // At least 3 batches due to buffering
}

#[tokio::test]
async fn test_parquet_writer_empty_observations() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let empty_observations = vec![];
    let result = writer.write_observations(empty_observations).await;
    assert!(result.is_ok());

    assert_eq!(writer.observations_written(), 0);
    assert!(!writer.schema_initialized());

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 0);
    assert_eq!(stats.batches_written, 0);
}

#[tokio::test]
async fn test_parquet_writer_with_missing_data() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(5);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations_with_missing_data(10);
    let result = writer.write_observations(observations).await;
    assert!(result.is_ok());

    assert_eq!(writer.observations_written(), 10);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 10);
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_parquet_writer_with_nan_values() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations_with_nan(8);
    let result = writer.write_observations(observations).await;
    assert!(result.is_ok());

    assert_eq!(writer.observations_written(), 8);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 8);
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_parquet_writer_different_compressions() {
    let temp_dir = TempDir::new().unwrap();

    for compression in [Compression::SNAPPY, Compression::GZIP, Compression::LZ4] {
        let output_path = temp_dir.path().join(format!("test_{:?}.parquet", compression));
        let config = WriterConfig::default().with_compression(compression);
        let mut writer = ParquetWriter::new(output_path.clone(), config);

        let observations = create_test_observations(10);
        writer.write_observations(observations).await.unwrap();

        let stats = writer.finalize().await.unwrap();
        assert_eq!(stats.observations_written, 10);
        assert!(output_path.exists());
    }
}

#[tokio::test]
async fn test_parquet_writer_memory_management() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default()
        .with_memory_limit_mb(1) // Very small limit to trigger flushes
        .with_write_batch_size(5);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    // Write enough data to trigger memory flushes
    let observations = create_test_observations(100);
    let result = writer.write_observations(observations).await;
    assert!(result.is_ok());

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 100);
    assert!(stats.memory_flushes > 0); // Should have triggered memory flushes
}

#[tokio::test]
async fn test_parquet_writer_large_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(1000);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(50);
    writer.write_observations(observations).await.unwrap();

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 50);
    // With large batch size, should only have 1 batch
    assert_eq!(stats.batches_written, 1);
}

#[tokio::test]
async fn test_parquet_writer_finalize_twice() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(5);
    writer.write_observations(observations).await.unwrap();

    let stats1 = writer.finalize().await.unwrap();
    assert!(writer.is_finalized());

    // Second finalize should return error
    let result = writer.finalize().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already finalized"));
}

#[tokio::test]
async fn test_parquet_writer_write_after_finalize() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(5);
    writer.write_observations(observations).await.unwrap();
    writer.finalize().await.unwrap();

    // Writing after finalize should return error
    let more_observations = create_test_observations(3);
    let result = writer.write_observations(more_observations).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("finalized"));
}

#[tokio::test]
async fn test_parquet_writer_statistics_calculation() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(10);
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(25);
    writer.write_observations(observations).await.unwrap();

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 25);
    assert!(stats.batches_written > 0);
    assert!(stats.bytes_written > 0);
    assert_eq!(stats.processing_errors, 0);
    assert!(stats.peak_memory_usage_bytes > 0);
    
    assert_approx_eq(stats.avg_observations_per_batch(), 25.0 / stats.batches_written as f64, 0.01);
    assert!(!stats.has_errors());
    assert_eq!(stats.success_rate(), 100.0);
}

#[tokio::test]
async fn test_parquet_writer_invalid_output_path() {
    let invalid_path = PathBuf::from("/invalid/nonexistent/directory/test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(invalid_path, config);

    let observations = create_test_observations(5);
    let result = writer.write_observations(observations).await;
    
    // Should fail due to invalid path
    assert!(result.is_err());
}

#[tokio::test]
async fn test_parquet_writer_get_stats_before_finalize() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path, config);

    let observations = create_test_observations(10);
    writer.write_observations(observations).await.unwrap();

    let stats = writer.get_current_stats();
    assert_eq!(stats.observations_written, 10);
    assert!(stats.batches_written > 0);
    // bytes_written might be 0 until finalized, depending on implementation
}

#[tokio::test]
async fn test_parquet_writer_progress_updates() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(5);
    let mut writer = ParquetWriter::new(output_path, config);

    writer.setup_progress(20);

    // Write in chunks to test progress updates
    let observations1 = create_test_observations(5);
    writer.write_observations(observations1).await.unwrap();

    let observations2 = create_test_observations(10);
    writer.write_observations(observations2).await.unwrap();

    let observations3 = create_test_observations(5);
    writer.write_observations(observations3).await.unwrap();

    assert_eq!(writer.observations_written(), 20);

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 20);
}

#[test]
fn test_create_optimized_writer() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");

    let writer = create_optimized_writer(&output_path);
    
    assert_eq!(writer.output_path(), &output_path);
    assert_eq!(writer.config().compression, Compression::SNAPPY);
    assert!(writer.config().enable_dictionary_encoding);
    assert!(writer.config().enable_statistics);
}

#[tokio::test]
async fn test_parquet_writer_schema_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path, config);

    // Write observations with different measurement sets
    let observations1 = create_test_observations(5);
    writer.write_observations(observations1).await.unwrap();

    // Schema should be initialized after first write
    assert!(writer.schema_initialized());

    // Write more observations - schema should remain consistent
    let observations2 = create_test_observations(3);
    writer.write_observations(observations2).await.unwrap();

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 8);
}

#[tokio::test]
async fn test_parquet_writer_clone_config() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default()
        .with_compression(Compression::GZIP)
        .with_write_batch_size(123);
    
    let writer = ParquetWriter::new(output_path, config.clone());
    
    assert_eq!(writer.config().compression, config.compression);
    assert_eq!(writer.config().write_batch_size, config.write_batch_size);
}

#[tokio::test]
async fn test_parquet_writer_buffer_management() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default().with_write_batch_size(10);
    let mut writer = ParquetWriter::new(output_path, config);

    // Write exactly batch size - should trigger write
    let observations = create_test_observations(10);
    writer.write_observations(observations).await.unwrap();

    // Write less than batch size - should buffer
    let observations = create_test_observations(5);
    writer.write_observations(observations).await.unwrap();

    assert_eq!(writer.observations_written(), 15);

    // Finalize should flush remaining buffer
    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 15);
}

#[tokio::test]
async fn test_parquet_writer_error_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path, config);

    // Write valid observations first
    let valid_observations = create_test_observations(5);
    let result = writer.write_observations(valid_observations).await;
    assert!(result.is_ok());

    // Writer should still be usable after successful writes
    let more_observations = create_test_observations(3);
    let result = writer.write_observations(more_observations).await;
    assert!(result.is_ok());

    let stats = writer.finalize().await.unwrap();
    assert_eq!(stats.observations_written, 8);
}

#[tokio::test]
async fn test_parquet_writer_file_size_calculation() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(output_path.clone(), config);

    let observations = create_test_observations(100);
    writer.write_observations(observations).await.unwrap();

    let stats = writer.finalize().await.unwrap();
    assert!(stats.bytes_written > 0);

    // Check actual file size matches stats
    let file_metadata = std::fs::metadata(&output_path).unwrap();
    assert_eq!(stats.bytes_written, file_metadata.len() as usize);
}