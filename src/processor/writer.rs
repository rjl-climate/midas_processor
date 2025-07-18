//! Parquet writing module for MIDAS datasets
//!
//! Handles writing processed data to optimized Parquet files with various
//! strategies including streaming, batching, and optimal row group sizing.

use crate::config::{MidasConfig, SystemProfile};
use crate::error::{MidasError, Result};

use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::{
    col, concat, len, DataFrame, IntoLazy, LazyFrame, ParquetWriteOptions, 
    ParquetWriter as PolarsParquetWriter, ScanArgsParquet, SortMultipleOptions, UnionArgs,
};
use std::path::{Path, PathBuf};
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
            "Starting optimized parquet write with {} frames",
            frames.len()
        );

        // For large datasets (many batches), write directly without concatenating everything
        // This avoids memory issues and expensive sorting operations on massive datasets
        if frames.len() > 2 {
            debug!(
                "Large dataset detected ({} batches), using streaming write approach",
                frames.len()
            );
            return self
                .write_final_parquet_streaming(frames, station_count)
                .await;
        }

        // Calculate optimal row group size based on total rows and system resources
        let total_rows = if frames.len() == 1 {
            // Estimate rows from the single frame
            match frames[0].clone().select([len()]).collect() {
                Ok(count_df) => count_df
                    .column("len")
                    .ok()
                    .and_then(|col| col.get(0).ok())
                    .and_then(|val| val.try_extract::<usize>().ok())
                    .unwrap_or(1_000_000), // Default estimate
                Err(_) => 1_000_000, // Default estimate
            }
        } else {
            // For multiple frames, use a reasonable estimate
            frames.len() * 500_000 // Rough estimate
        };

        // For smaller datasets, use the original concatenation approach
        let mut final_frame = if frames.len() == 1 {
            frames.into_iter().next().unwrap()
        } else {
            concat(frames, UnionArgs::default())?
        };

        // Apply sorting optimization if enabled (only for smaller datasets)
        if self.config.parquet_optimization.sort_by_station_then_time {
            debug!("Applying station-timestamp sorting for optimal query performance");
            final_frame = final_frame.sort_by_exprs(
                [col("station_id"), col("ob_end_time")],
                SortMultipleOptions::default(), // Use default ascending sort
            );
        }

        // Configure streaming chunk size
        if self.config.enable_streaming {
            unsafe {
                std::env::set_var(
                    "POLARS_STREAMING_CHUNK_SIZE",
                    self.config.streaming_chunk_size.to_string(),
                );
            }
        }

        // Note about GPU support
        if self.config.enable_gpu {
            warn!(
                "GPU acceleration requested but not available in polars 0.39 - using CPU streaming"
            );
        }

        // Calculate optimal row group size
        let parquet_config = &self.config.parquet_optimization;
        let optimal_row_group_size = parquet_config.calculate_optimal_row_group_size(
            total_rows,
            station_count,
            &self.system_profile,
        );

        debug!(
            "Calculated optimal row group size: {} rows for {} total rows from {} stations",
            optimal_row_group_size, total_rows, station_count
        );

        // Create optimized parquet write options
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: parquet_config.enable_statistics,
            row_group_size: Some(optimal_row_group_size),
            data_pagesize_limit: Some(parquet_config.data_page_size),
            ..Default::default()
        };

        debug!(
            "Parquet write config: compression={:?}, row_group_size={}, statistics={}",
            parquet_config.compression_algorithm,
            parquet_config.target_row_group_size,
            parquet_config.enable_statistics
        );

        // Use streaming execution or standard execution
        let total_rows = if self.config.enable_streaming {
            // Try sink_parquet for streaming, but need to handle the move
            let cloned_frame = final_frame.clone(); // Clone for the error case
            match final_frame.sink_parquet(self.output_path.clone(), write_options) {
                Ok(_) => {
                    debug!("Streaming sink_parquet completed successfully");
                    // Count rows by scanning the written file
                    let count_frame =
                        LazyFrame::scan_parquet(&self.output_path, ScanArgsParquet::default())?;
                    let count_df = count_frame.select([len()]).collect()?;
                    count_df
                        .column("len")?
                        .get(0)?
                        .try_extract::<usize>()
                        .unwrap_or(0)
                }
                Err(e) => {
                    warn!(
                        "Streaming sink failed ({}), falling back to collect+write",
                        e
                    );
                    // Fallback to collect with optimized writer using the cloned frame
                    let df = cloned_frame
                        .collect()
                        .map_err(|e| MidasError::ProcessingFailed {
                            path: self.output_path.clone(),
                            reason: format!("Failed to collect streaming data: {}", e),
                        })?;
                    let rows = df.height();

                    self.write_dataframe_optimized(df, &write_options)?;
                    rows
                }
            }
        } else {
            debug!("Using standard collect+write execution");
            // Standard execution with optimized settings
            let df = final_frame.collect()?;
            let rows = df.height();

            self.write_dataframe_optimized(df, &write_options)?;
            rows
        };

        debug!("Parquet write completed: {} rows written", total_rows);
        Ok(total_rows)
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

    /// Write final parquet using streaming approach for large datasets
    /// This avoids memory issues by accumulating batches to optimal row group sizes
    async fn write_final_parquet_streaming(
        &self,
        batches: Vec<LazyFrame>,
        station_count: usize,
    ) -> Result<usize> {
        debug!(
            "Starting streaming parquet write for {} batches",
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
            statistics: parquet_config.enable_statistics,
            row_group_size: Some(optimal_row_group_size),
            data_pagesize_limit: Some(parquet_config.data_page_size),
            ..Default::default()
        };

        // Create progress bar for batch processing
        let batch_pb = ProgressBar::new(batches.len() as u64);
        batch_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        batch_pb.set_message("Accumulating batches for optimal row groups");

        // Use the new accumulation-based approach
        let total_rows = self
            .write_with_optimal_row_groups(batches, &write_options, &batch_pb)
            .await?;

        batch_pb.finish_with_message("Optimal row groups written");
        Ok(total_rows)
    }

    /// Write batches with optimal row group sizing by accumulating data
    async fn write_with_optimal_row_groups(
        &self,
        batches: Vec<LazyFrame>,
        write_options: &ParquetWriteOptions,
        progress_bar: &ProgressBar,
    ) -> Result<usize> {
        let optimal_row_group_size = write_options.row_group_size.unwrap_or(1_000_000);
        let min_row_group_size = self.config.parquet_optimization.min_row_group_size;
        let max_memory_mb = self.config.parquet_optimization.max_accumulation_memory_mb;

        let mut accumulated_data: Vec<DataFrame> = Vec::new();
        let mut accumulated_rows = 0usize;
        let mut total_rows_written = 0usize;
        let mut row_group_count = 0usize;

        // Estimate memory usage per row (rough estimate)
        let estimated_bytes_per_row = 100; // Conservative estimate
        let max_rows_for_memory = (max_memory_mb * 1024 * 1024) / estimated_bytes_per_row;

        debug!(
            "Row group parameters: optimal={}, min={}, max_memory={}MB (max_rows={})",
            optimal_row_group_size, min_row_group_size, max_memory_mb, max_rows_for_memory
        );

        let total_batches = batches.len();
        for (i, batch) in batches.into_iter().enumerate() {
            progress_bar.set_message(format!(
                "Processing batch {}/{} (accumulated: {} rows)",
                i + 1,
                total_batches,
                accumulated_rows
            ));

            // Collect the batch
            let batch_df = batch.collect().map_err(|e| MidasError::ProcessingFailed {
                path: self.output_path.clone(),
                reason: format!("Failed to collect batch {}: {}", i, e),
            })?;

            let batch_rows = batch_df.height();
            accumulated_data.push(batch_df);
            accumulated_rows += batch_rows;

            // Check if we should write accumulated data
            let should_write = accumulated_rows >= optimal_row_group_size ||  // Reached optimal size
                accumulated_rows >= max_rows_for_memory ||     // Memory limit reached
                i == total_batches - 1; // Last batch

            if should_write {
                debug!(
                    "Writing accumulated data: {} rows from {} batches (reason: {})",
                    accumulated_rows,
                    accumulated_data.len(),
                    if accumulated_rows >= optimal_row_group_size {
                        "optimal size"
                    } else if accumulated_rows >= max_rows_for_memory {
                        "memory limit"
                    } else {
                        "last batch"
                    }
                );

                // Concatenate accumulated data
                let concatenated_df = if accumulated_data.len() == 1 {
                    accumulated_data.into_iter().next().unwrap()
                } else {
                    let lazy_frames: Vec<LazyFrame> =
                        accumulated_data.into_iter().map(|df| df.lazy()).collect();
                    concat(lazy_frames, UnionArgs::default())?.collect()?
                };

                // Apply station-timestamp sorting if enabled
                let sorted_df = if self.config.parquet_optimization.sort_by_station_then_time {
                    concatenated_df
                        .lazy()
                        .sort_by_exprs(
                            [col("station_id"), col("ob_end_time")],
                            SortMultipleOptions::default(),
                        )
                        .collect()?
                } else {
                    concatenated_df
                };

                // Write this accumulated batch
                self.write_accumulated_batch(sorted_df, write_options, row_group_count)
                    .await?;

                total_rows_written += accumulated_rows;
                row_group_count += 1;

                // Reset accumulation
                accumulated_data = Vec::new();
                accumulated_rows = 0;
            }

            progress_bar.inc(1);
        }

        debug!(
            "Completed accumulation phase: {} total rows in {} temporary files",
            total_rows_written, row_group_count
        );

        // Now concatenate all accumulated batch files into the final parquet file
        let final_rows = self
            .concatenate_accumulated_batches(row_group_count, write_options)
            .await?;

        debug!(
            "Final parquet file created with {} rows from {} row groups",
            final_rows, row_group_count
        );

        Ok(final_rows)
    }

    /// Write an accumulated batch as a temporary file
    async fn write_accumulated_batch(
        &self,
        df: DataFrame,
        write_options: &ParquetWriteOptions,
        batch_index: usize,
    ) -> Result<()> {
        let temp_path = self
            .output_path
            .with_extension(format!("accumulated.{}.parquet", batch_index));

        // Write the DataFrame with optimal row group sizing
        let temp_file = std::fs::File::create(&temp_path)?;
        let writer = PolarsParquetWriter::new(temp_file)
            .with_compression(write_options.compression)
            .with_statistics(write_options.statistics);

        let writer = if let Some(row_group_size) = write_options.row_group_size {
            writer.with_row_group_size(Some(row_group_size))
        } else {
            writer
        };

        let mut df_copy = df;
        writer
            .finish(&mut df_copy)
            .map_err(|e| MidasError::ProcessingFailed {
                path: temp_path.clone(),
                reason: format!("Failed to write accumulated batch {}: {}", batch_index, e),
            })?;

        debug!(
            "Wrote accumulated batch {}: {} rows to {}",
            batch_index,
            df_copy.height(),
            temp_path.display()
        );

        Ok(())
    }

    /// Get list of accumulated batch files for final concatenation
    fn get_accumulated_batch_files(&self) -> Result<Vec<PathBuf>> {
        let parent_dir = self.output_path.parent().unwrap_or_else(|| Path::new("."));
        let base_name = self.output_path.file_stem().unwrap_or_default();

        let mut batch_files = Vec::new();
        let mut index = 0;

        loop {
            let batch_path = parent_dir.join(format!(
                "{}.accumulated.{}.parquet",
                base_name.to_string_lossy(),
                index
            ));

            if batch_path.exists() {
                batch_files.push(batch_path);
                index += 1;
            } else {
                break;
            }
        }

        Ok(batch_files)
    }

    /// Concatenate accumulated batch files into the final parquet file
    async fn concatenate_accumulated_batches(
        &self,
        num_batches: usize,
        write_options: &ParquetWriteOptions,
    ) -> Result<usize> {
        debug!(
            "Starting final concatenation of {} accumulated batches",
            num_batches
        );

        // Get all accumulated batch files
        let batch_files = self.get_accumulated_batch_files()?;

        if batch_files.is_empty() {
            debug!("No accumulated batch files found, returning 0 rows");
            return Ok(0);
        }

        debug!(
            "Found {} accumulated batch files for concatenation",
            batch_files.len()
        );

        // Load all batch files as LazyFrames
        let mut batch_lazy_frames = Vec::new();
        for batch_file in &batch_files {
            let lazy_frame = LazyFrame::scan_parquet(batch_file, ScanArgsParquet::default())?;
            batch_lazy_frames.push(lazy_frame);
        }

        // Concatenate all batch files
        let mut final_frame = if batch_lazy_frames.len() == 1 {
            batch_lazy_frames.into_iter().next().unwrap()
        } else {
            concat(batch_lazy_frames, UnionArgs::default())?
        };

        // Apply global station-timestamp sorting if enabled
        if self.config.parquet_optimization.sort_by_station_then_time {
            debug!("Applying global station-timestamp sorting to final parquet file");
            final_frame = final_frame.sort_by_exprs(
                [col("station_id"), col("ob_end_time")],
                SortMultipleOptions::default(),
            );
        }

        // Write the final concatenated parquet file
        let final_df = final_frame.collect()?;
        let total_rows = final_df.height();

        self.write_dataframe_optimized(final_df, write_options)?;

        // Clean up temporary batch files
        for batch_file in &batch_files {
            if let Err(e) = std::fs::remove_file(batch_file) {
                warn!(
                    "Failed to remove temporary batch file {}: {}",
                    batch_file.display(),
                    e
                );
            } else {
                debug!("Removed temporary batch file: {}", batch_file.display());
            }
        }

        Ok(total_rows)
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

    #[tokio::test]
    async fn test_batch_file_detection() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        // Should return empty list when no batch files exist
        let batch_files = writer.get_accumulated_batch_files().unwrap();
        assert_eq!(batch_files.len(), 0);
    }

    #[test]
    fn test_write_options_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;

        // Test that write options can be created
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: parquet_config.enable_statistics,
            row_group_size: Some(1_000_000),
            data_pagesize_limit: Some(parquet_config.data_page_size),
            ..Default::default()
        };

        assert!(write_options.statistics);
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
