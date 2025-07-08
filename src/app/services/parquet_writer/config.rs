//! Configuration and statistics for Parquet writer operations
//!
//! This module provides configuration structures and statistics tracking for
//! optimized Parquet file generation with MIDAS weather data.

use crate::constants::{
    PARQUET_MEMORY_LIMIT_BYTES, PARQUET_PAGE_SIZE_MB, PARQUET_ROW_GROUP_SIZE,
    PARQUET_WRITE_BATCH_SIZE,
};
use parquet::basic::Compression;

/// Configuration for Parquet writer optimization
///
/// This structure controls all aspects of Parquet file generation including
/// compression, memory usage, and query optimization settings.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Row group size for optimal query performance
    /// Default: 100,000 rows (balance between memory and metadata overhead)
    pub row_group_size: usize,

    /// Compression algorithm for storage efficiency
    /// Default: Snappy (fastest decompression for analysis workloads)
    pub compression: Compression,

    /// Write batch size for memory management
    /// Default: 1,024 records (balance between memory and I/O efficiency)
    pub write_batch_size: usize,

    /// Maximum memory usage before forcing flush (bytes)
    /// Default: 100MB (prevents OOM on large datasets)
    pub memory_limit_bytes: usize,

    /// Enable dictionary encoding for categorical columns
    /// Default: true (highly beneficial for weather station data)
    pub enable_dictionary_encoding: bool,

    /// Enable column statistics for query optimization
    /// Default: true (enables predicate pushdown)
    pub enable_statistics: bool,

    /// Data page size limit in bytes
    /// Default: 1MB (optimal for sequential reading)
    pub data_page_size_bytes: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            row_group_size: PARQUET_ROW_GROUP_SIZE,
            compression: Compression::SNAPPY,
            write_batch_size: PARQUET_WRITE_BATCH_SIZE,
            memory_limit_bytes: PARQUET_MEMORY_LIMIT_BYTES,
            enable_dictionary_encoding: true,
            enable_statistics: true,
            data_page_size_bytes: PARQUET_PAGE_SIZE_MB * 1024 * 1024,
        }
    }
}

impl WriterConfig {
    /// Create a new WriterConfig with custom settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set row group size for query optimization
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Set compression algorithm
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set write batch size for memory management
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = size;
        self
    }

    /// Set memory limit in bytes
    pub fn with_memory_limit_bytes(mut self, limit: usize) -> Self {
        self.memory_limit_bytes = limit;
        self
    }

    /// Set memory limit in megabytes
    pub fn with_memory_limit_mb(mut self, limit_mb: usize) -> Self {
        self.memory_limit_bytes = limit_mb * 1024 * 1024;
        self
    }

    /// Enable or disable dictionary encoding
    pub fn with_dictionary_encoding(mut self, enabled: bool) -> Self {
        self.enable_dictionary_encoding = enabled;
        self
    }

    /// Enable or disable column statistics
    pub fn with_statistics(mut self, enabled: bool) -> Self {
        self.enable_statistics = enabled;
        self
    }

    /// Set data page size in bytes
    pub fn with_data_page_size_bytes(mut self, size: usize) -> Self {
        self.data_page_size_bytes = size;
        self
    }

    /// Set data page size in megabytes
    pub fn with_data_page_size_mb(mut self, size_mb: usize) -> Self {
        self.data_page_size_bytes = size_mb * 1024 * 1024;
        self
    }

    /// Validate configuration settings
    pub fn validate(&self) -> Result<(), String> {
        if self.row_group_size == 0 {
            return Err("Row group size must be greater than 0".to_string());
        }

        if self.write_batch_size == 0 {
            return Err("Write batch size must be greater than 0".to_string());
        }

        if self.memory_limit_bytes == 0 {
            return Err("Memory limit must be greater than 0".to_string());
        }

        if self.data_page_size_bytes == 0 {
            return Err("Data page size must be greater than 0".to_string());
        }

        // Ensure batch size is reasonable relative to row group size
        if self.write_batch_size > self.row_group_size {
            return Err("Write batch size should not exceed row group size".to_string());
        }

        Ok(())
    }

    /// Get memory limit in megabytes
    pub fn memory_limit_mb(&self) -> usize {
        self.memory_limit_bytes / (1024 * 1024)
    }

    /// Get data page size in megabytes
    pub fn data_page_size_mb(&self) -> usize {
        self.data_page_size_bytes / (1024 * 1024)
    }
}

/// Writing statistics for progress reporting and diagnostics
#[derive(Debug, Clone, Default)]
pub struct WritingStats {
    /// Total number of observations written
    pub observations_written: usize,
    /// Number of record batches processed
    pub batches_written: usize,
    /// Number of memory flushes performed
    pub memory_flushes: usize,
    /// Total bytes written to storage
    pub bytes_written: usize,
    /// Processing errors encountered
    pub processing_errors: usize,
    /// Memory usage statistics
    pub peak_memory_usage_bytes: usize,
}

impl WritingStats {
    /// Create new empty writing statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate average observations per batch
    pub fn avg_observations_per_batch(&self) -> f64 {
        if self.batches_written == 0 {
            0.0
        } else {
            self.observations_written as f64 / self.batches_written as f64
        }
    }

    /// Calculate compression ratio (if original size is known)
    pub fn compression_ratio(&self, original_bytes: usize) -> f64 {
        if original_bytes == 0 {
            0.0
        } else {
            self.bytes_written as f64 / original_bytes as f64
        }
    }

    /// Calculate processing efficiency (observations per error)
    pub fn processing_efficiency(&self) -> f64 {
        if self.processing_errors == 0 {
            if self.observations_written == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            self.observations_written as f64 / self.processing_errors as f64
        }
    }

    /// Get peak memory usage in megabytes
    pub fn peak_memory_usage_mb(&self) -> f64 {
        self.peak_memory_usage_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Check if any errors occurred during processing
    pub fn has_errors(&self) -> bool {
        self.processing_errors > 0
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total_attempts = self.observations_written + self.processing_errors;
        if total_attempts == 0 {
            100.0
        } else {
            (self.observations_written as f64 / total_attempts as f64) * 100.0
        }
    }

    /// Format bytes in human-readable format
    pub fn format_bytes(bytes: usize) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{:.2} {}", size, UNITS[unit_index])
        }
    }

    /// Format statistics as human-readable summary
    pub fn summary(&self) -> String {
        format!(
            "WritingStats {{ observations: {}, batches: {}, flushes: {}, size: {}, peak_memory: {}, errors: {} }}",
            self.observations_written,
            self.batches_written,
            self.memory_flushes,
            Self::format_bytes(self.bytes_written),
            Self::format_bytes(self.peak_memory_usage_bytes),
            self.processing_errors
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test default WriterConfig values ensure optimal settings for most use cases
    /// Validates that production-ready defaults are set for compression, memory, and performance
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

    /// Test builder pattern allows fluent configuration customization
    /// Ensures all configuration options can be set through method chaining
    #[test]
    fn test_writer_config_builder() {
        let config = WriterConfig::new()
            .with_row_group_size(50000)
            .with_compression(Compression::GZIP(Default::default()))
            .with_write_batch_size(512)
            .with_memory_limit_mb(200)
            .with_dictionary_encoding(false)
            .with_statistics(false)
            .with_data_page_size_mb(2);

        assert_eq!(config.row_group_size, 50000);
        assert_eq!(config.compression, Compression::GZIP(Default::default()));
        assert_eq!(config.write_batch_size, 512);
        assert_eq!(config.memory_limit_bytes, 200 * 1024 * 1024);
        assert!(!config.enable_dictionary_encoding);
        assert!(!config.enable_statistics);
        assert_eq!(config.data_page_size_bytes, 2 * 1024 * 1024);
    }

    /// Test configuration validation prevents invalid settings that could cause runtime errors
    /// Ensures early detection of problematic configurations before processing begins
    #[test]
    fn test_writer_config_validation() {
        let valid_config = WriterConfig::default();
        assert!(valid_config.validate().is_ok());

        // Test invalid row group size
        let invalid_config = WriterConfig {
            row_group_size: 0,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());

        // Test invalid write batch size
        let invalid_config = WriterConfig {
            write_batch_size: 0,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());

        // Test invalid memory limit
        let invalid_config = WriterConfig {
            memory_limit_bytes: 0,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());

        // Test invalid data page size
        let invalid_config = WriterConfig {
            data_page_size_bytes: 0,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());

        // Test batch size larger than row group size
        let row_group_size = 1000;
        let invalid_config = WriterConfig {
            row_group_size,
            write_batch_size: row_group_size + 1,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    /// Test convenience methods provide user-friendly byte/MB conversions
    /// Simplifies configuration display and debugging with readable units
    #[test]
    fn test_writer_config_convenience_methods() {
        let config = WriterConfig::default();
        assert_eq!(config.memory_limit_mb(), 100);
        assert_eq!(config.data_page_size_mb(), 1);
    }

    /// Test WritingStats starts with clean slate for accurate tracking
    /// Ensures statistics begin at zero for proper accumulation during processing
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

    /// Test statistical calculations provide meaningful performance metrics
    /// Validates compression ratios, batch efficiency, and success rates for monitoring
    #[test]
    fn test_writing_stats_calculations() {
        let mut stats = WritingStats::new();
        stats.observations_written = 1000;
        stats.batches_written = 10;
        stats.bytes_written = 50000;
        stats.processing_errors = 5;
        stats.peak_memory_usage_bytes = 10 * 1024 * 1024; // 10MB

        assert_eq!(stats.avg_observations_per_batch(), 100.0);
        assert_eq!(stats.compression_ratio(100000), 0.5);
        assert_eq!(stats.processing_efficiency(), 200.0);
        assert_eq!(stats.peak_memory_usage_mb(), 10.0);
        assert!(stats.has_errors());

        // Test success rate
        let expected_success_rate = (1000.0 / 1005.0) * 100.0;
        assert!((stats.success_rate() - expected_success_rate).abs() < 0.01);
    }

    /// Test edge cases prevent division-by-zero and handle empty datasets gracefully
    /// Ensures robust behavior when no data has been processed or errors occur
    #[test]
    fn test_writing_stats_edge_cases() {
        let stats = WritingStats::new();
        assert_eq!(stats.avg_observations_per_batch(), 0.0);
        assert_eq!(stats.compression_ratio(0), 0.0);
        assert_eq!(stats.processing_efficiency(), 0.0);
        assert!(!stats.has_errors());
        assert_eq!(stats.success_rate(), 100.0);

        let mut stats_no_errors = WritingStats::new();
        stats_no_errors.observations_written = 100;
        assert_eq!(stats_no_errors.processing_efficiency(), f64::INFINITY);
    }

    /// Test byte formatting creates human-readable size representations
    /// Ensures consistent display of file sizes and memory usage across units
    #[test]
    fn test_format_bytes() {
        assert_eq!(WritingStats::format_bytes(0), "0 B");
        assert_eq!(WritingStats::format_bytes(512), "512 B");
        assert_eq!(WritingStats::format_bytes(1024), "1.00 KB");
        assert_eq!(WritingStats::format_bytes(1536), "1.50 KB");
        assert_eq!(WritingStats::format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(WritingStats::format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    /// Test summary generation provides comprehensive operation overview
    /// Validates that all key metrics are included in status reports for users
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
    }
}
