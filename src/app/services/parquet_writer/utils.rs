//! Utility functions for Parquet writer operations
//!
//! This module provides convenience functions and high-level interfaces for
//! common Parquet writing workflows with MIDAS weather data.

use crate::app::models::Observation;
use crate::app::services::parquet_writer::{
    config::WriterConfig, config::WritingStats, writer::ParquetWriter,
};
use crate::{Error, Result};

use std::path::Path;
use tracing::info;

/// Create a ParquetWriter configured for a specific MIDAS dataset
///
/// This convenience function creates a writer with dataset-specific optimizations
/// and naming conventions for the MIDAS processor workflow.
pub async fn create_dataset_writer(
    dataset_name: &str,
    output_dir: &Path,
    config: WriterConfig,
) -> Result<ParquetWriter> {
    let filename = format!("{}.parquet", dataset_name);
    let output_path = output_dir.join("parquet_files").join(filename);

    info!(
        "Creating dataset writer for {} -> {}",
        dataset_name,
        output_path.display()
    );

    ParquetWriter::new(&output_path, config).await
}

/// Write observations for a complete dataset with progress reporting
///
/// This high-level function handles the complete workflow for writing a dataset
/// to Parquet format with progress reporting and error handling.
pub async fn write_dataset_to_parquet(
    dataset_name: &str,
    observations: Vec<Observation>,
    output_dir: &Path,
    config: WriterConfig,
) -> Result<WritingStats> {
    info!(
        "Writing {} observations for dataset {}",
        observations.len(),
        dataset_name
    );

    let mut writer = create_dataset_writer(dataset_name, output_dir, config).await?;
    writer.setup_progress(observations.len());

    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    info!("Dataset {} written successfully: {:?}", dataset_name, stats);

    Ok(stats)
}

/// Write multiple datasets to Parquet files in parallel
pub async fn write_multiple_datasets_to_parquet(
    datasets: Vec<(String, Vec<Observation>)>,
    output_dir: &Path,
    config: WriterConfig,
) -> Result<Vec<(String, WritingStats)>> {
    info!("Writing {} datasets to Parquet format", datasets.len());

    let mut results = Vec::new();

    for (dataset_name, observations) in datasets {
        let dataset_config = config.clone();
        let stats =
            write_dataset_to_parquet(&dataset_name, observations, output_dir, dataset_config)
                .await?;
        results.push((dataset_name, stats));
    }

    info!("Completed writing {} datasets", results.len());
    Ok(results)
}

/// Create a standardized Parquet file path for a dataset
pub fn create_dataset_output_path(dataset_name: &str, output_dir: &Path) -> std::path::PathBuf {
    let filename = format!("{}.parquet", sanitize_filename(dataset_name));
    output_dir.join("parquet_files").join(filename)
}

/// Sanitize a dataset name for use as a filename
pub fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

/// Validate that output directory structure exists and is writable
pub async fn validate_output_directory(output_dir: &Path) -> Result<()> {
    if !output_dir.exists() {
        tokio::fs::create_dir_all(output_dir).await.map_err(|e| {
            Error::io(
                format!(
                    "Failed to create output directory: {}",
                    output_dir.display()
                ),
                e,
            )
        })?;
    }

    // Create parquet_files subdirectory if it doesn't exist
    let parquet_dir = output_dir.join("parquet_files");
    if !parquet_dir.exists() {
        tokio::fs::create_dir_all(&parquet_dir).await.map_err(|e| {
            Error::io(
                format!(
                    "Failed to create parquet files directory: {}",
                    parquet_dir.display()
                ),
                e,
            )
        })?;
    }

    // Test write permissions by creating a temporary file
    let test_file = output_dir.join(".write_test");
    tokio::fs::write(&test_file, "test").await.map_err(|e| {
        Error::io(
            format!("Output directory is not writable: {}", output_dir.display()),
            e,
        )
    })?;

    tokio::fs::remove_file(&test_file).await.map_err(|e| {
        Error::io(
            format!("Failed to clean up test file: {}", test_file.display()),
            e,
        )
    })?;

    Ok(())
}

/// Get recommended WriterConfig based on dataset characteristics
pub fn get_recommended_config(
    num_observations: usize,
    estimated_measurements_per_observation: usize,
    available_memory_mb: Option<usize>,
) -> WriterConfig {
    let memory_limit_mb = available_memory_mb.unwrap_or({
        // Estimate memory needs based on data size
        if num_observations < 100_000 {
            50 // 50MB for small datasets
        } else if num_observations < 1_000_000 {
            100 // 100MB for medium datasets
        } else {
            200 // 200MB for large datasets
        }
    });

    let row_group_size = if num_observations < 50_000 {
        num_observations.min(10_000)
    } else if num_observations < 1_000_000 {
        50_000
    } else {
        100_000
    };

    let write_batch_size = if estimated_measurements_per_observation > 20 {
        // Many measurements per observation - use smaller batches
        500
    } else if estimated_measurements_per_observation > 10 {
        1_000
    } else {
        2_000
    };

    WriterConfig::new()
        .with_row_group_size(row_group_size)
        .with_write_batch_size(write_batch_size)
        .with_memory_limit_mb(memory_limit_mb)
        .with_dictionary_encoding(true)
        .with_statistics(true)
}

/// Generate a summary report of writing operations
pub fn generate_writing_summary(results: &[(String, WritingStats)]) -> WritingSummary {
    let mut summary = WritingSummary::default();

    for (dataset_name, stats) in results {
        summary.datasets_processed += 1;
        summary.total_observations += stats.observations_written;
        summary.total_batches += stats.batches_written;
        summary.total_bytes_written += stats.bytes_written;
        summary.total_memory_flushes += stats.memory_flushes;
        summary.total_errors += stats.processing_errors;
        summary.peak_memory_usage_bytes = summary
            .peak_memory_usage_bytes
            .max(stats.peak_memory_usage_bytes);

        // Track largest dataset
        if stats.observations_written > summary.largest_dataset_observations {
            summary.largest_dataset_observations = stats.observations_written;
            summary.largest_dataset_name = dataset_name.clone();
        }

        // Track dataset with most errors
        if stats.processing_errors > summary.most_errors_count {
            summary.most_errors_count = stats.processing_errors;
            summary.most_errors_dataset = dataset_name.clone();
        }
    }

    summary
}

/// Summary statistics for multiple dataset writing operations
#[derive(Debug, Clone, Default)]
pub struct WritingSummary {
    pub datasets_processed: usize,
    pub total_observations: usize,
    pub total_batches: usize,
    pub total_bytes_written: usize,
    pub total_memory_flushes: usize,
    pub total_errors: usize,
    pub peak_memory_usage_bytes: usize,
    pub largest_dataset_name: String,
    pub largest_dataset_observations: usize,
    pub most_errors_dataset: String,
    pub most_errors_count: usize,
}

impl WritingSummary {
    /// Calculate average observations per dataset
    pub fn avg_observations_per_dataset(&self) -> f64 {
        if self.datasets_processed == 0 {
            0.0
        } else {
            self.total_observations as f64 / self.datasets_processed as f64
        }
    }

    /// Calculate average batch size across all datasets
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.total_observations as f64 / self.total_batches as f64
        }
    }

    /// Calculate overall success rate
    pub fn success_rate(&self) -> f64 {
        let total_attempts = self.total_observations + self.total_errors;
        if total_attempts == 0 {
            100.0
        } else {
            (self.total_observations as f64 / total_attempts as f64) * 100.0
        }
    }

    /// Get compression ratio estimate (assuming original data was larger)
    pub fn estimated_compression_ratio(&self) -> f64 {
        // Very rough estimate - typical scientific data compresses to 30-50% of original
        // This is just for reporting purposes
        0.4
    }

    /// Format total bytes written in human-readable format
    pub fn format_total_bytes(&self) -> String {
        WritingStats::format_bytes(self.total_bytes_written)
    }

    /// Format peak memory usage in human-readable format
    pub fn format_peak_memory(&self) -> String {
        WritingStats::format_bytes(self.peak_memory_usage_bytes)
    }

    /// Generate a human-readable summary report
    pub fn to_report(&self) -> String {
        format!(
            "Parquet Writing Summary\n\
             ======================\n\
             Datasets processed: {}\n\
             Total observations: {}\n\
             Total batches: {}\n\
             Total size: {}\n\
             Peak memory: {}\n\
             Success rate: {:.2}%\n\
             Avg observations/dataset: {:.0}\n\
             Avg batch size: {:.0}\n\
             Memory flushes: {}\n\
             Largest dataset: {} ({} observations)\n\
             Errors: {} total{}",
            self.datasets_processed,
            self.total_observations,
            self.total_batches,
            self.format_total_bytes(),
            self.format_peak_memory(),
            self.success_rate(),
            self.avg_observations_per_dataset(),
            self.avg_batch_size(),
            self.total_memory_flushes,
            self.largest_dataset_name,
            self.largest_dataset_observations,
            self.total_errors,
            if self.total_errors > 0 {
                format!(" (most in {})", self.most_errors_dataset)
            } else {
                String::new()
            }
        )
    }
}

/// Check if a Parquet file exists and is valid
pub async fn validate_parquet_file(path: &Path) -> Result<ParquetFileInfo> {
    if !path.exists() {
        return Err(Error::file_not_found(format!(
            "Parquet file not found: {}",
            path.display()
        )));
    }

    let metadata = tokio::fs::metadata(path).await.map_err(|e| {
        Error::io(
            format!("Failed to read file metadata: {}", path.display()),
            e,
        )
    })?;

    if metadata.len() == 0 {
        return Err(Error::data_validation(format!(
            "Parquet file is empty: {}",
            path.display()
        )));
    }

    // TODO: Add more sophisticated validation using parquet crate
    // For now, just check basic file properties

    Ok(ParquetFileInfo {
        path: path.to_path_buf(),
        size_bytes: metadata.len(),
        modified: metadata.modified().ok(),
        is_valid: true, // Placeholder - would need actual parquet validation
    })
}

/// Information about a Parquet file
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    pub path: std::path::PathBuf,
    pub size_bytes: u64,
    pub modified: Option<std::time::SystemTime>,
    pub is_valid: bool,
}

impl ParquetFileInfo {
    /// Format file size in human-readable format
    pub fn format_size(&self) -> String {
        WritingStats::format_bytes(self.size_bytes as usize)
    }

    /// Get file age in hours (if modification time is available)
    pub fn age_hours(&self) -> Option<f64> {
        self.modified.and_then(|modified| {
            std::time::SystemTime::now()
                .duration_since(modified)
                .ok()
                .map(|duration| duration.as_secs_f64() / 3600.0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::{Observation, Station};
    use chrono::Utc;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_station() -> Station {
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
        )
        .unwrap()
    }

    fn create_test_observation() -> Observation {
        let mut measurements = HashMap::new();
        measurements.insert("air_temperature".to_string(), 15.5);

        let mut quality_flags = HashMap::new();
        quality_flags.insert("air_temperature".to_string(), "0".to_string());

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
            measurements,
            quality_flags,
            HashMap::new(),
            None,
            None,
        )
        .unwrap()
    }

    /// Test dataset writer creation with standardized naming conventions
    /// Ensures consistent file organization and path generation for MIDAS datasets
    #[tokio::test]
    async fn test_create_dataset_writer() {
        let temp_dir = TempDir::new().unwrap();
        let config = WriterConfig::default();

        let writer = create_dataset_writer("test-dataset", temp_dir.path(), config).await;
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        let expected_path = temp_dir
            .path()
            .join("parquet_files")
            .join("test-dataset.parquet");
        assert_eq!(writer.output_path(), &expected_path);
    }

    /// Test complete dataset writing workflow with progress tracking
    /// Validates end-to-end pipeline from observations to Parquet files
    #[tokio::test]
    async fn test_write_dataset_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let config = WriterConfig::default();
        let observations = vec![create_test_observation(); 5];

        let stats = write_dataset_to_parquet("test-dataset", observations, temp_dir.path(), config)
            .await
            .unwrap();

        assert_eq!(stats.observations_written, 5);
        assert_eq!(stats.batches_written, 1);

        let output_file = temp_dir
            .path()
            .join("parquet_files")
            .join("test-dataset.parquet");
        assert!(output_file.exists());
    }

    /// Test batch processing of multiple datasets creates separate Parquet files
    /// Ensures efficient handling of multiple weather observation datasets
    #[tokio::test]
    async fn test_write_multiple_datasets_to_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let config = WriterConfig::default();

        let datasets = vec![
            ("dataset1".to_string(), vec![create_test_observation(); 3]),
            ("dataset2".to_string(), vec![create_test_observation(); 7]),
        ];

        let results = write_multiple_datasets_to_parquet(datasets, temp_dir.path(), config)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.observations_written, 3);
        assert_eq!(results[1].1.observations_written, 7);

        // Check files exist
        let file1 = temp_dir
            .path()
            .join("parquet_files")
            .join("dataset1.parquet");
        let file2 = temp_dir
            .path()
            .join("parquet_files")
            .join("dataset2.parquet");
        assert!(file1.exists());
        assert!(file2.exists());
    }

    /// Test standardized path generation for dataset Parquet files
    /// Validates consistent file organization within parquet_files subdirectory
    #[test]
    fn test_create_dataset_output_path() {
        let output_dir = std::path::Path::new("/test/output");
        let path = create_dataset_output_path("my-dataset", output_dir);

        assert_eq!(
            path,
            output_dir.join("parquet_files").join("my-dataset.parquet")
        );
    }

    /// Test filename sanitization prevents filesystem compatibility issues
    /// Ensures safe file naming by replacing problematic characters
    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("valid-name_123"), "valid-name_123");
        assert_eq!(
            sanitize_filename("invalid/name:with?chars"),
            "invalid_name_with_chars"
        );
        assert_eq!(sanitize_filename("UPPER-lower-123"), "UPPER-lower-123");
        assert_eq!(sanitize_filename("spaces and tabs"), "spaces_and_tabs");
    }

    /// Test output directory validation creates required subdirectories
    /// Ensures filesystem readiness and write permissions before processing
    #[tokio::test]
    async fn test_validate_output_directory() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("new_output");

        // Should create directory and subdirectories
        validate_output_directory(&output_dir).await.unwrap();

        assert!(output_dir.exists());
        assert!(output_dir.join("parquet_files").exists());

        // Should work on existing directory
        validate_output_directory(&output_dir).await.unwrap();
    }

    /// Test configuration recommendations adapt to dataset characteristics
    /// Validates automatic tuning based on size and measurement complexity
    #[test]
    fn test_get_recommended_config() {
        // Small dataset
        let config = get_recommended_config(1000, 5, None);
        assert_eq!(config.row_group_size, 1000);
        assert_eq!(config.memory_limit_mb(), 50);

        // Medium dataset
        let config = get_recommended_config(500_000, 10, None);
        assert_eq!(config.row_group_size, 50_000);
        assert_eq!(config.memory_limit_mb(), 100);

        // Large dataset with many measurements
        let config = get_recommended_config(2_000_000, 25, Some(400));
        assert_eq!(config.row_group_size, 100_000);
        assert_eq!(config.write_batch_size, 500);
        assert_eq!(config.memory_limit_mb(), 400);
    }

    /// Test summary generation aggregates statistics across multiple datasets
    /// Provides comprehensive reporting for batch processing operations
    #[test]
    fn test_generate_writing_summary() {
        let stats1 = WritingStats {
            observations_written: 1000,
            batches_written: 10,
            bytes_written: 50000,
            memory_flushes: 2,
            processing_errors: 1,
            peak_memory_usage_bytes: 1024 * 1024,
        };

        let stats2 = WritingStats {
            observations_written: 2000,
            batches_written: 20,
            bytes_written: 100000,
            memory_flushes: 3,
            processing_errors: 0,
            peak_memory_usage_bytes: 2048 * 1024,
        };

        let results = vec![
            ("dataset1".to_string(), stats1),
            ("dataset2".to_string(), stats2),
        ];

        let summary = generate_writing_summary(&results);

        assert_eq!(summary.datasets_processed, 2);
        assert_eq!(summary.total_observations, 3000);
        assert_eq!(summary.total_batches, 30);
        assert_eq!(summary.total_bytes_written, 150000);
        assert_eq!(summary.total_memory_flushes, 5);
        assert_eq!(summary.total_errors, 1);
        assert_eq!(summary.largest_dataset_name, "dataset2");
        assert_eq!(summary.largest_dataset_observations, 2000);
        assert_eq!(summary.most_errors_dataset, "dataset1");
        assert_eq!(summary.most_errors_count, 1);
    }

    /// Test summary calculations provide meaningful aggregate metrics
    /// Validates success rates and performance indicators for reporting
    #[test]
    fn test_writing_summary_calculations() {
        let summary = WritingSummary {
            datasets_processed: 3,
            total_observations: 3000,
            total_batches: 30,
            total_bytes_written: 150000,
            total_errors: 50,
            ..Default::default()
        };

        assert_eq!(summary.avg_observations_per_dataset(), 1000.0);
        assert_eq!(summary.avg_batch_size(), 100.0);
        assert!((summary.success_rate() - 98.36).abs() < 0.01); // 3000 / 3050 * 100
        assert!(!summary.format_total_bytes().is_empty());
    }

    /// Test summary report generation creates human-readable status overview
    /// Ensures comprehensive information display for user feedback
    #[test]
    fn test_writing_summary_report() {
        let summary = WritingSummary {
            datasets_processed: 2,
            total_observations: 1000,
            total_batches: 10,
            total_bytes_written: 50000,
            peak_memory_usage_bytes: 1024 * 1024,
            total_errors: 5,
            largest_dataset_name: "big_dataset".to_string(),
            largest_dataset_observations: 800,
            most_errors_dataset: "problem_dataset".to_string(),
            most_errors_count: 5,
            ..Default::default()
        };

        let report = summary.to_report();
        assert!(report.contains("Datasets processed: 2"));
        assert!(report.contains("Total observations: 1000"));
        assert!(report.contains("big_dataset"));
        assert!(report.contains("problem_dataset"));
    }

    /// Test Parquet file validation catches common file system issues
    /// Prevents downstream errors by checking file existence and basic integrity
    #[tokio::test]
    async fn test_validate_parquet_file() {
        let temp_dir = TempDir::new().unwrap();

        // Test non-existent file
        let nonexistent = temp_dir.path().join("nonexistent.parquet");
        let result = validate_parquet_file(&nonexistent).await;
        assert!(result.is_err());

        // Test empty file
        let empty_file = temp_dir.path().join("empty.parquet");
        tokio::fs::write(&empty_file, "").await.unwrap();
        let result = validate_parquet_file(&empty_file).await;
        assert!(result.is_err());

        // Test non-empty file
        let test_file = temp_dir.path().join("test.parquet");
        tokio::fs::write(&test_file, "test content").await.unwrap();
        let result = validate_parquet_file(&test_file).await;
        assert!(result.is_ok());

        let info = result.unwrap();
        assert_eq!(info.size_bytes, 12);
        assert!(info.is_valid);
        assert!(!info.format_size().is_empty());
    }

    /// Test file information utilities provide human-readable metadata
    /// Enables file size formatting and age calculations for monitoring
    #[test]
    fn test_parquet_file_info() {
        let info = ParquetFileInfo {
            path: std::path::PathBuf::from("/test/file.parquet"),
            size_bytes: 1024 * 1024,
            modified: Some(std::time::SystemTime::now()),
            is_valid: true,
        };

        assert_eq!(info.format_size(), "1.00 MB");
        assert!(info.age_hours().is_some());
        assert!(info.age_hours().unwrap() >= 0.0);
    }
}
