//! Parquet writing module for MIDAS datasets
//!
//! Handles writing processed data to optimized Parquet files with various
//! strategies including streaming, batching, and optimal row group sizing.

use crate::config::{MidasConfig, SystemProfile};
use crate::error::Result;
use crate::models::DatasetType;

use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use polars::io::parquet::write::KeyValueMetadata;
use polars::prelude::StatisticsOptions;
use polars::prelude::{
    LazyFrame, ParquetWriteOptions, SortMultipleOptions, UnionArgs, col, concat,
};
use std::path::PathBuf;
use tracing::debug;

/// Parquet writer with optimization strategies
#[derive(Debug)]
pub struct ParquetWriter {
    output_path: PathBuf,
    config: MidasConfig,
    system_profile: SystemProfile,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(output_path: PathBuf, config: MidasConfig) -> Self {
        let system_profile = SystemProfile::detect();
        Self {
            output_path,
            config,
            system_profile,
        }
    }

    /// Create metadata for the Parquet file
    fn create_file_metadata(&self) -> KeyValueMetadata {
        let mut metadata = Vec::new();

        // Application name and version
        metadata.push((
            "created_by".to_string(),
            format!("{} v{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
        ));

        // Creation timestamp
        metadata.push(("created_at".to_string(), Utc::now().to_rfc3339()));

        // GitHub repository
        metadata.push((
            "repository".to_string(),
            env!("CARGO_PKG_REPOSITORY").to_string(),
        ));

        KeyValueMetadata::from_static(metadata)
    }

    /// Write final parquet file with optimized structure for timeseries queries
    pub async fn write_final_parquet(
        &self,
        frames: Vec<LazyFrame>,
        station_count: usize,
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        if frames.is_empty() {
            return Ok(0);
        }

        debug!(
            "Starting streaming parquet write with {} frames",
            frames.len()
        );

        // Always use streaming approach
        self.write_with_optimal_row_groups(frames, station_count, dataset_type)
            .await
    }

    /// Write batches using true Polars streaming
    async fn write_with_optimal_row_groups(
        &self,
        batches: Vec<LazyFrame>,
        station_count: usize,
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        debug!(
            "Starting true streaming parquet write for {} batches",
            batches.len()
        );

        // Calculate optimal row group size for streaming write
        let parquet_config = &self.config.parquet_optimization;
        let estimated_total_rows = batches.len() * 500_000; // Rough estimate
        let optimal_row_group_size = parquet_config.calculate_optimal_row_group_size(
            estimated_total_rows,
            station_count,
            &self.system_profile,
        );

        debug!(
            "Streaming write: calculated optimal row group size: {} rows for ~{} total rows from {} stations",
            optimal_row_group_size, estimated_total_rows, station_count
        );

        // Create parquet write options with metadata
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics {
                StatisticsOptions::full()
            } else {
                StatisticsOptions::empty()
            },
            row_group_size: Some(optimal_row_group_size),
            data_page_size: Some(parquet_config.data_page_size),
            key_value_metadata: Some(self.create_file_metadata()),
            ..Default::default()
        };

        // Concatenate all LazyFrames (no collection - stays lazy)
        let combined_frame = concat(batches, UnionArgs::default())?;

        // Always sort by station_id then timestamp for optimal parquet performance
        let final_frame = combined_frame.sort_by_exprs(
            [col("station_id"), col(dataset_type.primary_time_column())],
            SortMultipleOptions::default(),
        );

        // Ensure output directory exists
        if let Some(parent) = self.output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create spinner to show parquet creation progress
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        spinner.set_message("Creating parquet file, this will take a few minutes...");

        // Clone spinner for the animation task
        let spinner_clone = spinner.clone();

        // Start spinner animation task
        let animation_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(80));
            loop {
                interval.tick().await;
                spinner_clone.tick();
            }
        });

        // Write with minimal async complexity - use spawn_blocking only for the write operation
        let output_path = self.output_path.clone();
        debug!("Writing parquet to: {:?}", output_path);

        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<usize> {
            let df = final_frame.collect()?;
            let row_count = df.height();

            let mut file = std::fs::File::create(&output_path)?;

            use polars::prelude::ParquetWriter;
            let mut df_mut = df;
            let mut writer = ParquetWriter::new(&mut file)
                .with_compression(write_options.compression)
                .with_row_group_size(write_options.row_group_size)
                .with_data_page_size(write_options.data_page_size);

            // Add metadata if present
            if let Some(metadata) = write_options.key_value_metadata {
                writer = writer.with_key_value_metadata(Some(metadata));
            }

            writer.finish(&mut df_mut)?;

            Ok(row_count)
        })
        .await
        .map_err(|e| crate::error::MidasError::ProcessingFailed {
            path: self.output_path.clone(),
            reason: format!("Failed to spawn write task: {}", e),
        })??;

        // Stop the animation task
        animation_task.abort();

        spinner.finish_with_message("Parquet file created successfully");
        debug!(
            "Parquet write completed: {} rows to {:?}",
            result, self.output_path
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MidasConfig;
    use tempfile::TempDir;

    fn create_test_writer(temp_dir: &TempDir) -> ParquetWriter {
        let output_path = temp_dir.path().join("test.parquet");
        let config = MidasConfig::default();
        ParquetWriter::new(output_path, config)
    }

    #[tokio::test]
    async fn test_parquet_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        // Verify writer was created successfully
        assert!(writer.output_path.ends_with("test.parquet"));
    }

    #[tokio::test]
    async fn test_empty_frames_handling() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let empty_frames: Vec<LazyFrame> = vec![];
        let dataset_type = DatasetType::Temperature;
        let result = writer
            .write_final_parquet(empty_frames, 0, &dataset_type)
            .await;

        // Should handle empty frames gracefully
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_metadata_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let metadata = writer.create_file_metadata();

        // Verify metadata is the correct type
        match &metadata {
            KeyValueMetadata::Static(kv_pairs) => {
                assert_eq!(kv_pairs.len(), 3);

                // Check that all required fields are present
                let keys: Vec<&str> = kv_pairs.iter().map(|kv| kv.key.as_str()).collect();
                assert!(keys.contains(&"created_by"));
                assert!(keys.contains(&"created_at"));
                assert!(keys.contains(&"repository"));

                // Find and verify created_by value
                let created_by = kv_pairs
                    .iter()
                    .find(|kv| kv.key == "created_by")
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap();
                assert!(created_by.contains("midas_processor"));
                assert!(created_by.contains("v"));

                // Find and verify repository URL
                let repository = kv_pairs
                    .iter()
                    .find(|kv| kv.key == "repository")
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap();
                assert_eq!(repository, "https://github.com/rjl-climate/midas_processor");
            }
            _ => panic!("Expected Static metadata variant"),
        }
    }

    #[test]
    fn test_write_options_with_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;

        // Test that write options can be created with metadata
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics {
                StatisticsOptions::full()
            } else {
                StatisticsOptions::empty()
            },
            row_group_size: Some(1_000_000),
            data_page_size: Some(parquet_config.data_page_size),
            key_value_metadata: Some(writer.create_file_metadata()),
            ..Default::default()
        };

        // Verify metadata is included
        assert!(write_options.key_value_metadata.is_some());
        let metadata = write_options.key_value_metadata.unwrap();
        match metadata {
            KeyValueMetadata::Static(kv_pairs) => {
                assert_eq!(kv_pairs.len(), 3);
            }
            _ => panic!("Expected Static metadata variant"),
        }
    }

    #[test]
    fn test_system_profile_detection() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        // System profile should be detected during writer creation
        // This test just ensures the detection doesn't panic
        let _profile = &writer.system_profile;
    }

    #[tokio::test]
    async fn test_row_group_size_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;
        let station_count = 10;
        let total_rows = 1_000_000;

        let optimal_size = parquet_config.calculate_optimal_row_group_size(
            total_rows,
            station_count,
            &writer.system_profile,
        );

        // Should return a reasonable row group size
        assert!(optimal_size > 0);
        assert!(optimal_size <= total_rows);
    }
}
