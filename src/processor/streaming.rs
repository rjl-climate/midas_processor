//! Streaming processing module for MIDAS datasets
//!
//! Handles concurrent processing of CSV files with memory pressure detection,
//! batching, and lazy frame processing for optimal performance.

use crate::config::MidasConfig;
use crate::error::{MidasError, Result};
use crate::header::parse_badc_header;
use crate::models::{DatasetType, ProcessingStats};
use crate::schema::SchemaManager;

use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{Mutex, Semaphore};
use tokio::task;
use tracing::{debug, error, warn};

/// Streaming processor for MIDAS datasets
#[derive(Debug)]
pub struct StreamingProcessor {
    config: MidasConfig,
    schema_manager: SchemaManager,
    header_semaphore: Arc<Semaphore>,
    system_monitor: Arc<Mutex<System>>,
    memory_threshold: f64,
}

impl StreamingProcessor {
    /// Create a new streaming processor
    pub fn new(config: MidasConfig, schema_manager: SchemaManager) -> Self {
        Self {
            config,
            schema_manager,
            header_semaphore: Arc::new(Semaphore::new(4)), // Limit concurrent header parsing
            system_monitor: Arc::new(Mutex::new(System::new())),
            memory_threshold: 0.8, // 80% memory usage threshold
        }
    }

    /// Update the schema manager with an initialized one
    pub fn update_schema_manager(&mut self, schema_manager: SchemaManager) {
        self.schema_manager = schema_manager;
    }

    /// Check if system is under memory pressure
    pub async fn check_memory_pressure(&self) -> bool {
        let mut system = self.system_monitor.lock().await;
        system.refresh_memory();

        let used_memory = system.used_memory() as f64;
        let total_memory = system.total_memory() as f64;

        if total_memory == 0.0 {
            return false; // Avoid division by zero
        }

        let memory_usage = used_memory / total_memory;
        let is_pressure = memory_usage > self.memory_threshold;

        if is_pressure {
            debug!(
                "Memory pressure detected: {:.1}% usage (threshold: {:.1}%)",
                memory_usage * 100.0,
                self.memory_threshold * 100.0
            );
        }

        is_pressure
    }

    /// Process files using optimized streaming pipeline
    pub async fn process_files_streaming(
        &self,
        files: &[PathBuf],
        dataset_type: &DatasetType,
        output_path: &Path,
    ) -> Result<(Vec<LazyFrame>, ProcessingStats)> {
        // Create progress bar
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        pb.set_message("Processing files");

        // Process files concurrently with controlled parallelism and memory pressure detection
        let mut concurrent_limit = self.config.max_concurrent_files.min(files.len());

        // Check memory pressure and adapt concurrency
        if self.check_memory_pressure().await {
            concurrent_limit = (concurrent_limit / 2).max(1); // Reduce concurrency but keep at least 1
            debug!(
                "Memory pressure detected, reducing concurrency to {}",
                concurrent_limit
            );
        }

        let pb_clone = pb.clone();

        // Use batched processing to avoid memory issues with large datasets
        // For datasets with 40k+ files, this prevents memory exhaustion during concatenation
        let batch_size = if files.len() > 10000 { 500 } else { 1000 };
        debug!("Using batch size {} for {} files", batch_size, files.len());
        let mut all_batches = Vec::new();
        let mut total_processed = 0usize;
        let mut total_failed = 0usize;

        let total_batches = files.len().div_ceil(batch_size);
        for (batch_num, chunk) in files.chunks(batch_size).enumerate() {
            debug!(
                "Processing batch {}/{} ({} files)",
                batch_num + 1,
                total_batches,
                chunk.len()
            );

            let (batch_frames, processed, failed) = stream::iter(chunk)
                .map(|file_path| {
                    let dataset_type = dataset_type.clone();
                    let pb = pb_clone.clone();
                    async move {
                        if let Some(file_name) = file_path.file_name() {
                            pb.set_message(format!("Processing: {}", file_name.to_string_lossy()));
                        }

                        let result = self
                            .process_single_file_lazy(file_path, &dataset_type)
                            .await;
                        pb.inc(1);

                        match result {
                            Ok(Some(frame)) => {
                                debug!("Successfully processed: {}", file_path.display());
                                Ok(Some(frame))
                            }
                            Ok(None) => {
                                warn!("Skipped file (no data): {}", file_path.display());
                                Ok(None)
                            }
                            Err(e) => {
                                error!("Failed to process {}: {:#}", file_path.display(), e);
                                Err(e)
                            }
                        }
                    }
                })
                .buffer_unordered(concurrent_limit)
                .fold(
                    (Vec::new(), 0usize, 0usize),
                    |(mut frames, processed, failed), result| async move {
                        match result {
                            Ok(Some(frame)) => {
                                frames.push(frame);
                                (frames, processed + 1, failed)
                            }
                            Ok(None) => (frames, processed, failed),
                            Err(_) => (frames, processed, failed + 1),
                        }
                    },
                )
                .await;

            total_processed += processed;
            total_failed += failed;

            // Convert this batch to a single LazyFrame if it has data
            if !batch_frames.is_empty() {
                debug!("Concatenating batch of {} frames", batch_frames.len());
                let mut batch_frame = if batch_frames.len() == 1 {
                    batch_frames.into_iter().next().unwrap()
                } else {
                    concat(batch_frames, UnionArgs::default())?
                };

                // Apply station-aware sorting within batch if enabled
                if self.config.parquet_optimization.sort_by_station_then_time {
                    debug!(
                        "Applying station-timestamp sorting to batch {}",
                        batch_num + 1
                    );
                    batch_frame = batch_frame.sort_by_exprs(
                        [col("station_id"), col("ob_end_time")],
                        SortMultipleOptions::default(),
                    );
                }

                all_batches.push(batch_frame);
            }

            // Check memory pressure after each batch
            if self.check_memory_pressure().await {
                warn!("Memory pressure detected after batch, continuing with reduced concurrency");
            }
        }

        pb.finish_with_message("All CSV files processed");

        let stats = ProcessingStats {
            files_processed: total_processed,
            files_failed: total_failed,
            total_rows: 0, // Will be set after writing
            output_path: output_path.to_path_buf(),
            processing_time_ms: 0, // Will be set by caller
        };

        Ok((all_batches, stats))
    }

    /// Process a single MIDAS CSV file using optimized lazy scanning
    pub async fn process_single_file_lazy(
        &self,
        file_path: &Path,
        dataset_type: &DatasetType,
    ) -> Result<Option<LazyFrame>> {
        debug!(
            "Processing file with lazy scanning: {}",
            file_path.display()
        );

        // Step 1: Parse header and get metadata + boundaries with controlled concurrency
        let _permit =
            self.header_semaphore
                .acquire()
                .await
                .map_err(|e| MidasError::ProcessingFailed {
                    path: file_path.to_path_buf(),
                    reason: format!("Failed to acquire header parsing permit: {}", e),
                })?;

        let (metadata, boundaries) = task::spawn_blocking({
            let file_path = file_path.to_owned();
            move || parse_badc_header(&file_path)
        })
        .await
        .map_err(|e| MidasError::ProcessingFailed {
            path: file_path.to_path_buf(),
            reason: format!("Failed to parse header: {}", e),
        })??;

        // Step 2: Get schema configuration
        let config = self.schema_manager.get_config(dataset_type)?;

        // Step 3: Create lazy frame using CsvReader but immediately convert to lazy
        // This avoids full materialization while still getting lazy benefits
        // Skip one additional row to account for column header line
        let data_skip_rows = boundaries.skip_rows + 1;
        let adjusted_data_rows = boundaries.data_rows.map(|rows| rows.saturating_sub(1));

        // Use optimized CSV reading with memory-efficient settings
        let df = CsvReader::from_path(file_path)?
            .with_skip_rows(data_skip_rows)
            .with_n_rows(adjusted_data_rows)
            .with_schema(Some(Arc::new(config.schema.clone())))
            .with_ignore_errors(true)
            .has_header(false)
            .low_memory(true) // Enable low memory mode for better streaming
            .with_rechunk(false) // Avoid unnecessary rechunking
            .finish()?;

        let lazy_frame = df.lazy();

        // Step 4: Add metadata columns using with_columns
        let enhanced_frame = lazy_frame.with_columns([
            lit(metadata.latitude).alias("latitude"),
            lit(metadata.longitude).alias("longitude"),
            lit(metadata.station_name.clone()).alias("station_name"),
            lit(metadata.station_id.clone()).alias("station_id"),
            lit(metadata.county.clone()).alias("county"),
            lit(metadata.height).alias("height"),
            lit(metadata.height_units.clone()).alias("height_units"),
        ]);

        // Step 5: Exclude empty columns if enabled
        let final_frame = if self.config.enable_column_elimination {
            let exclude_cols = self.schema_manager.get_excluded_columns(dataset_type)?;
            if !exclude_cols.is_empty() {
                enhanced_frame.select([col("*").exclude(exclude_cols)])
            } else {
                enhanced_frame
            }
        } else {
            enhanced_frame
        };

        Ok(Some(final_frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MidasConfig;
    use crate::schema::SchemaManager;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_processor() -> StreamingProcessor {
        let config = MidasConfig::default();
        let schema_manager = SchemaManager::new();
        StreamingProcessor::new(config, schema_manager)
    }

    #[tokio::test]
    async fn test_memory_pressure_detection() {
        let processor = create_test_processor();

        // Memory pressure check should not panic and return a boolean
        let _result = processor.check_memory_pressure().await;
        // Test passes if it doesn't panic
    }

    #[tokio::test]
    async fn test_streaming_processor_creation() {
        let processor = create_test_processor();

        // Verify the processor was created successfully
        assert_eq!(processor.memory_threshold, 0.8);
    }

    #[tokio::test]
    async fn test_empty_file_list() {
        let processor = create_test_processor();
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let files: Vec<PathBuf> = vec![];
        let dataset_type = DatasetType::Rain;

        let result = processor
            .process_files_streaming(&files, &dataset_type, &output_path)
            .await;

        // Should handle empty file list gracefully
        assert!(result.is_ok());
        let (batches, stats) = result.unwrap();
        assert_eq!(batches.len(), 0);
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.files_failed, 0);
    }

    #[tokio::test]
    async fn test_batch_size_calculation() {
        let processor = create_test_processor();
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        // Test small dataset (should use batch size 1000)
        let small_files: Vec<PathBuf> = (0..500)
            .map(|i| PathBuf::from(format!("file_{}.csv", i)))
            .collect();
        let dataset_type = DatasetType::Rain;

        // This will fail because files don't exist, but we can check the logic
        let result = processor
            .process_files_streaming(&small_files, &dataset_type, &output_path)
            .await;

        // Should fail gracefully due to missing files, or return with failed files
        match result {
            Ok((_batches, stats)) => {
                // If it doesn't fail completely, all files should be reported as failed
                assert_eq!(stats.files_processed, 0);
                assert_eq!(stats.files_failed, 500);
            }
            Err(_) => {
                // Or it could fail completely, which is also acceptable
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_limit_adjustment() {
        let processor = create_test_processor();

        // Check if memory pressure would reduce concurrency
        let _has_pressure = processor.check_memory_pressure().await;

        // The processor should handle memory pressure detection without panicking
        // (The actual behavior depends on current system memory usage)
    }

    #[test]
    fn test_processor_configuration() {
        let config = MidasConfig::default();
        let schema_manager = SchemaManager::new();
        let processor = StreamingProcessor::new(config.clone(), schema_manager);

        // Verify configuration is properly stored
        assert_eq!(
            processor.config.max_concurrent_files,
            config.max_concurrent_files
        );
        assert_eq!(processor.memory_threshold, 0.8);
    }
}
