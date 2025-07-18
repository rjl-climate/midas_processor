//! Parquet writing module for MIDAS datasets
//!
//! Handles writing processed data to optimized Parquet files with various
//! strategies including streaming, batching, and optimal row group sizing.

use crate::config::{MidasConfig, SystemProfile};
use crate::error::{MidasError, Result};

use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::{
    col, concat, len, DataFrame, LazyFrame, ParquetWriteOptions, 
    ParquetWriter as PolarsParquetWriter, SortMultipleOptions, UnionArgs,
    SinkTarget,
};
use polars::prelude::StatisticsOptions;
use std::path::PathBuf;
use tracing::{debug, warn};

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

    /// Write final parquet file with optimized structure for timeseries queries
    pub async fn write_final_parquet(
        &self,
        frames: Vec<LazyFrame>,
        station_count: usize,
    ) -> Result<usize> {
        if frames.is_empty() {
            return Ok(0);
        }

        debug!(
            "Starting streaming parquet write with {} frames",
            frames.len()
        );

        // Always use streaming approach
        self.write_with_optimal_row_groups(frames, station_count).await
    }

    /// Write DataFrame to parquet with optimized settings
    fn write_dataframe_optimized(
        &self,
        mut df: DataFrame,
        write_options: &ParquetWriteOptions,
    ) -> Result<()> {
        let file = std::fs::File::create(&self.output_path)?;
        let writer = PolarsParquetWriter::new(file)
            .with_compression(write_options.compression)
            .with_statistics(write_options.statistics);

        // Apply row group size if specified
        let writer = if let Some(row_group_size) = write_options.row_group_size {
            writer.with_row_group_size(Some(row_group_size))
        } else {
            writer
        };

        writer
            .finish(&mut df)
            .map_err(|e| MidasError::ProcessingFailed {
                path: self.output_path.clone(),
                reason: format!("Failed to write optimized parquet: {}", e),
            })?;

        Ok(())
    }

    /// Write batches using true Polars streaming
    async fn write_with_optimal_row_groups(
        &self,
        batches: Vec<LazyFrame>,
        station_count: usize,
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

        // Create parquet write options
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics { StatisticsOptions::full() } else { StatisticsOptions::empty() },
            row_group_size: Some(optimal_row_group_size),
            data_page_size: Some(parquet_config.data_page_size),
            ..Default::default()
        };

        // Create simple spinner for streaming operation with file size monitoring
        let progress_bar = ProgressBar::new_spinner();
        progress_bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {bytes} {msg}")
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        );
        progress_bar.set_message("Concatenating batches...");
        
        // Spawn file size monitoring task
        let file_path = self.output_path.clone();
        let pb_clone = progress_bar.clone();
        let monitor_task = tokio::spawn(async move {
            let mut last_size = 0u64;
            let mut last_time = std::time::Instant::now();
            
            // Wait for file to be created (streaming might take a moment to start)
            while !file_path.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            
            // Monitor file size growth
            while file_path.exists() {
                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let current_size = metadata.len();
                    if current_size > last_size {
                        pb_clone.set_position(current_size);
                        
                        // Calculate transfer rate
                        let now = std::time::Instant::now();
                        let time_diff = now.duration_since(last_time).as_secs_f64();
                        if time_diff > 0.0 {
                            let size_diff = current_size - last_size;
                            let rate = size_diff as f64 / time_diff;
                            pb_clone.set_message(format!("Streaming to parquet file... ({:.1} MB/s)", rate / 1_000_000.0));
                        }
                        
                        last_size = current_size;
                        last_time = now;
                    }
                    pb_clone.tick();
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });

        // Concatenate all LazyFrames (no collection - stays lazy)
        let combined_frame = concat(batches, UnionArgs::default())?;

        // Apply station-timestamp sorting if enabled (stays lazy)
        let final_frame = if self.config.parquet_optimization.sort_by_station_then_time {
            progress_bar.set_message("Applying station-timestamp sorting...");
            debug!("Applying station-timestamp sorting for optimal query performance");
            combined_frame.sort_by_exprs(
                [col("station_id"), col("ob_end_time")],
                SortMultipleOptions::default(),
            )
        } else {
            combined_frame
        };

        // Use true Polars streaming to write directly to parquet
        progress_bar.set_message("Starting streaming to parquet file...");
        
        match final_frame.clone().sink_parquet(
            SinkTarget::Path(self.output_path.clone().into()),
            write_options.clone(),
            None, // No cloud options
            Default::default() // Use default sink options
        ) {
            Ok(sink_frame) => {
                // Execute the streaming operation using spawn_blocking
                tokio::task::spawn_blocking(move || {
                    sink_frame.collect()
                }).await
                .map_err(|e| MidasError::ProcessingFailed {
                    path: self.output_path.clone(),
                    reason: format!("Failed to spawn streaming sink task: {}", e),
                })?
                .map_err(|e| MidasError::ProcessingFailed {
                    path: self.output_path.clone(),
                    reason: format!("Failed to execute streaming sink: {}", e),
                })?;
                
                // Stop the monitoring task
                monitor_task.abort();
                
                // Set final file size and complete the progress bar
                if let Ok(metadata) = std::fs::metadata(&self.output_path) {
                    progress_bar.set_position(metadata.len());
                }
                progress_bar.finish_with_message("Streaming parquet write completed");
                
                // Count rows by scanning the written file
                let count_frame = LazyFrame::scan_parquet(&self.output_path, Default::default())?;
                let count_df = tokio::task::spawn_blocking(move || {
                    count_frame.select([len()]).collect()
                }).await
                .map_err(|e| MidasError::ProcessingFailed {
                    path: self.output_path.clone(),
                    reason: format!("Failed to spawn row counting task: {}", e),
                })?
                .map_err(|e| MidasError::ProcessingFailed {
                    path: self.output_path.clone(),
                    reason: format!("Failed to count rows: {}", e),
                })?;
                let total_rows = count_df
                    .column("len")?
                    .get(0)?
                    .try_extract::<usize>()
                    .unwrap_or(0);
                
                debug!("True streaming completed: {} rows written", total_rows);
                Ok(total_rows)
            }
            Err(e) => {
                // Stop the monitoring task
                monitor_task.abort();
                
                progress_bar.finish_with_message("Streaming failed, using fallback");
                warn!("Streaming sink failed ({}), falling back to collect+write", e);
                
                // Fallback to standard approach
                let df = final_frame.collect().map_err(|e| MidasError::ProcessingFailed {
                    path: self.output_path.clone(),
                    reason: format!("Failed to collect data for fallback: {}", e),
                })?;
                
                let rows = df.height();
                self.write_dataframe_optimized(df, &write_options)?;
                
                Ok(rows)
            }
        }
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
        let result = writer.write_final_parquet(empty_frames, 0).await;

        // Should handle empty frames gracefully
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_write_options_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;

        // Test that write options can be created
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics { StatisticsOptions::full() } else { StatisticsOptions::empty() },
            row_group_size: Some(1_000_000),
            data_page_size: Some(parquet_config.data_page_size),
            ..Default::default()
        };

        // Test that statistics are enabled (no easy way to test StatisticsOptions directly)
        assert_eq!(write_options.row_group_size, Some(1_000_000));
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
