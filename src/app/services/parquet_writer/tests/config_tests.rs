//! Comprehensive unit tests for WriterConfig and WritingStats

use super::{assert_approx_eq};
use crate::app::services::parquet_writer::config::{WriterConfig, WritingStats};
use crate::constants::{PARQUET_ROW_GROUP_SIZE, PARQUET_WRITE_BATCH_SIZE};
use parquet::basic::Compression;

#[test]
fn test_writer_config_default() {
    let config = WriterConfig::default();
    assert_eq!(config.row_group_size, PARQUET_ROW_GROUP_SIZE);
    assert_eq!(config.compression, Compression::SNAPPY);
    assert_eq!(config.write_batch_size, PARQUET_WRITE_BATCH_SIZE);
    assert_eq!(config.memory_limit_bytes, 100 * 1024 * 1024);
    assert!(config.enable_dictionary_encoding);
    assert!(config.enable_statistics);
    assert_eq!(config.data_page_size_bytes, 1024 * 1024);
}

#[test]
fn test_writer_config_new() {
    let config = WriterConfig::new();
    // Should be identical to default
    let default_config = WriterConfig::default();
    assert_eq!(config.row_group_size, default_config.row_group_size);
    assert_eq!(config.compression, default_config.compression);
    assert_eq!(config.write_batch_size, default_config.write_batch_size);
}

#[test]
fn test_writer_config_builder_pattern() {
    let config = WriterConfig::new()
        .with_row_group_size(50000)
        .with_compression(Compression::GZIP)
        .with_write_batch_size(512)
        .with_memory_limit_mb(200)
        .with_dictionary_encoding(false)
        .with_statistics(false)
        .with_data_page_size_mb(2);

    assert_eq!(config.row_group_size, 50000);
    assert_eq!(config.compression, Compression::GZIP);
    assert_eq!(config.write_batch_size, 512);
    assert_eq!(config.memory_limit_bytes, 200 * 1024 * 1024);
    assert!(!config.enable_dictionary_encoding);
    assert!(!config.enable_statistics);
    assert_eq!(config.data_page_size_bytes, 2 * 1024 * 1024);
}

#[test]
fn test_writer_config_memory_limit_conversions() {
    let config = WriterConfig::new().with_memory_limit_mb(150);
    assert_eq!(config.memory_limit_bytes, 150 * 1024 * 1024);
    assert_eq!(config.memory_limit_mb(), 150);

    let config = WriterConfig::new().with_memory_limit_bytes(256 * 1024 * 1024);
    assert_eq!(config.memory_limit_mb(), 256);
}

#[test]
fn test_writer_config_data_page_size_conversions() {
    let config = WriterConfig::new().with_data_page_size_mb(3);
    assert_eq!(config.data_page_size_bytes, 3 * 1024 * 1024);
    assert_eq!(config.data_page_size_mb(), 3);

    let config = WriterConfig::new().with_data_page_size_bytes(512 * 1024);
    assert_eq!(config.data_page_size_mb(), 0); // 0.5 MB rounds down
}

#[test]
fn test_writer_config_validation_success() {
    let valid_config = WriterConfig::default();
    assert!(valid_config.validate().is_ok());

    let custom_config = WriterConfig::new()
        .with_row_group_size(1000)
        .with_write_batch_size(100)
        .with_memory_limit_bytes(1024 * 1024)
        .with_data_page_size_bytes(64 * 1024);
    assert!(custom_config.validate().is_ok());
}

#[test]
fn test_writer_config_validation_failures() {
    // Test zero row group size
    let mut config = WriterConfig::default();
    config.row_group_size = 0;
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("Row group size"));

    // Test zero write batch size
    config = WriterConfig::default();
    config.write_batch_size = 0;
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("Write batch size"));

    // Test zero memory limit
    config = WriterConfig::default();
    config.memory_limit_bytes = 0;
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("Memory limit"));

    // Test zero data page size
    config = WriterConfig::default();
    config.data_page_size_bytes = 0;
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("Data page size"));

    // Test batch size larger than row group size
    config = WriterConfig::default();
    config.write_batch_size = config.row_group_size + 1;
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("Write batch size should not exceed"));
}

#[test]
fn test_writer_config_edge_cases() {
    // Test with batch size equal to row group size (should be valid)
    let config = WriterConfig::new()
        .with_row_group_size(1000)
        .with_write_batch_size(1000);
    assert!(config.validate().is_ok());

    // Test with very large values
    let config = WriterConfig::new()
        .with_row_group_size(usize::MAX / 2)
        .with_write_batch_size(1000)
        .with_memory_limit_bytes(usize::MAX / 2);
    assert!(config.validate().is_ok());
}

#[test]
fn test_writing_stats_default() {
    let stats = WritingStats::default();
    assert_eq!(stats.observations_written, 0);
    assert_eq!(stats.batches_written, 0);
    assert_eq!(stats.memory_flushes, 0);
    assert_eq!(stats.bytes_written, 0);
    assert_eq!(stats.processing_errors, 0);
    assert_eq!(stats.peak_memory_usage_bytes, 0);
}

#[test]
fn test_writing_stats_new() {
    let stats = WritingStats::new();
    let default_stats = WritingStats::default();
    
    assert_eq!(stats.observations_written, default_stats.observations_written);
    assert_eq!(stats.batches_written, default_stats.batches_written);
    assert_eq!(stats.memory_flushes, default_stats.memory_flushes);
}

#[test]
fn test_writing_stats_calculations() {
    let mut stats = WritingStats::new();
    stats.observations_written = 1000;
    stats.batches_written = 10;
    stats.bytes_written = 50000;
    stats.processing_errors = 5;
    stats.peak_memory_usage_bytes = 10 * 1024 * 1024;

    assert_approx_eq(stats.avg_observations_per_batch(), 100.0, 0.01);
    assert_approx_eq(stats.compression_ratio(100000), 0.5, 0.01);
    assert_approx_eq(stats.processing_efficiency(), 200.0, 0.01);
    assert_approx_eq(stats.peak_memory_usage_mb(), 10.0, 0.01);
    assert!(stats.has_errors());

    let expected_success_rate = (1000.0 / 1005.0) * 100.0;
    assert_approx_eq(stats.success_rate(), expected_success_rate, 0.01);
}

#[test]
fn test_writing_stats_edge_cases() {
    // Test with zero values
    let stats = WritingStats::new();
    assert_eq!(stats.avg_observations_per_batch(), 0.0);
    assert_eq!(stats.compression_ratio(0), 0.0);
    assert_eq!(stats.processing_efficiency(), 0.0);
    assert!(!stats.has_errors());
    assert_eq!(stats.success_rate(), 100.0);

    // Test with no errors but observations
    let mut stats = WritingStats::new();
    stats.observations_written = 100;
    assert_eq!(stats.processing_efficiency(), f64::INFINITY);
    assert_eq!(stats.success_rate(), 100.0);

    // Test with no batches but observations (edge case)
    let mut stats = WritingStats::new();
    stats.observations_written = 100;
    stats.batches_written = 0;
    assert_eq!(stats.avg_observations_per_batch(), 0.0);

    // Test with very large numbers
    let mut stats = WritingStats::new();
    stats.observations_written = usize::MAX / 2;
    stats.batches_written = 1000;
    stats.processing_errors = usize::MAX / 4;
    
    assert!(stats.avg_observations_per_batch() > 0.0);
    assert!(stats.processing_efficiency() > 0.0);
    assert!(stats.success_rate() > 0.0);
    assert!(stats.success_rate() <= 100.0);
}

#[test]
fn test_writing_stats_compression_ratio() {
    let mut stats = WritingStats::new();
    stats.bytes_written = 25000;

    // Test different original sizes
    assert_approx_eq(stats.compression_ratio(50000), 0.5, 0.01);
    assert_approx_eq(stats.compression_ratio(100000), 0.25, 0.01);
    assert_eq!(stats.compression_ratio(0), 0.0);
    
    // Test with bytes_written = 0
    stats.bytes_written = 0;
    assert_eq!(stats.compression_ratio(50000), 0.0);
}

#[test]
fn test_format_bytes() {
    assert_eq!(WritingStats::format_bytes(0), "0 B");
    assert_eq!(WritingStats::format_bytes(512), "512 B");
    assert_eq!(WritingStats::format_bytes(1024), "1.00 KB");
    assert_eq!(WritingStats::format_bytes(1536), "1.50 KB");
    assert_eq!(WritingStats::format_bytes(1024 * 1024), "1.00 MB");
    assert_eq!(WritingStats::format_bytes(1024 * 1024 * 1024), "1.00 GB");
    assert_eq!(WritingStats::format_bytes(1024_u64.pow(4) as usize), "1.00 TB");
}

#[test]
fn test_format_bytes_edge_cases() {
    // Test with exactly 1024 boundaries
    assert_eq!(WritingStats::format_bytes(1024), "1.00 KB");
    assert_eq!(WritingStats::format_bytes(1024 * 1024), "1.00 MB");
    
    // Test with values just under boundaries
    assert_eq!(WritingStats::format_bytes(1023), "1023 B");
    assert_eq!(WritingStats::format_bytes(1024 * 1024 - 1), "1024.00 KB");
    
    // Test with very large values
    let large_value = usize::MAX / 1024; // Avoid overflow
    let result = WritingStats::format_bytes(large_value);
    assert!(result.contains("TB") || result.contains("GB"));
}

#[test]
fn test_writing_stats_summary() {
    let mut stats = WritingStats::new();
    stats.observations_written = 1000;
    stats.batches_written = 10;
    stats.memory_flushes = 2;
    stats.bytes_written = 50000;
    stats.peak_memory_usage_bytes = 10 * 1024 * 1024;
    stats.processing_errors = 1;

    let summary = stats.summary();
    assert!(summary.contains("observations: 1000"));
    assert!(summary.contains("batches: 10"));
    assert!(summary.contains("flushes: 2"));
    assert!(summary.contains("errors: 1"));
    assert!(summary.contains("WritingStats"));
}

#[test]
fn test_writing_stats_summary_empty() {
    let stats = WritingStats::new();
    let summary = stats.summary();
    
    assert!(summary.contains("observations: 0"));
    assert!(summary.contains("batches: 0"));
    assert!(summary.contains("errors: 0"));
}

#[test]
fn test_writing_stats_success_rate_precision() {
    let mut stats = WritingStats::new();
    
    // Test various success rates
    stats.observations_written = 999;
    stats.processing_errors = 1;
    assert_approx_eq(stats.success_rate(), 99.9, 0.01);
    
    stats.observations_written = 99;
    stats.processing_errors = 1;
    assert_approx_eq(stats.success_rate(), 99.0, 0.01);
    
    stats.observations_written = 1;
    stats.processing_errors = 99;
    assert_approx_eq(stats.success_rate(), 1.0, 0.01);
}

#[test]
fn test_writer_config_clone() {
    let config1 = WriterConfig::new()
        .with_row_group_size(25000)
        .with_compression(Compression::GZIP);
    
    let config2 = config1.clone();
    
    assert_eq!(config1.row_group_size, config2.row_group_size);
    assert_eq!(config1.compression, config2.compression);
    assert_eq!(config1.write_batch_size, config2.write_batch_size);
}

#[test]
fn test_writing_stats_clone() {
    let mut stats1 = WritingStats::new();
    stats1.observations_written = 100;
    stats1.processing_errors = 5;
    
    let stats2 = stats1.clone();
    
    assert_eq!(stats1.observations_written, stats2.observations_written);
    assert_eq!(stats1.processing_errors, stats2.processing_errors);
    assert_eq!(stats1.success_rate(), stats2.success_rate());
}

#[test]
fn test_writer_config_debug() {
    let config = WriterConfig::default();
    let debug_str = format!("{:?}", config);
    
    assert!(debug_str.contains("WriterConfig"));
    assert!(debug_str.contains("row_group_size"));
    assert!(debug_str.contains("compression"));
}

#[test]
fn test_writing_stats_debug() {
    let stats = WritingStats::default();
    let debug_str = format!("{:?}", stats);
    
    assert!(debug_str.contains("WritingStats"));
    assert!(debug_str.contains("observations_written"));
    assert!(debug_str.contains("batches_written"));
}