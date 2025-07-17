//! Main processing engine with streaming pipeline.
//!
//! Orchestrates the complete MIDAS dataset processing workflow:
//! file discovery, header parsing, schema application, streaming
//! conversion, and Parquet output with batching for memory efficiency.

use crate::config::{MidasConfig, SystemProfile};
use crate::error::{MidasError, Result};
use crate::header::parse_badc_header;
use crate::models::{DatasetType, ProcessingStats};
use crate::schema::SchemaManager;

use colored::*;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use sysinfo::System;
use tokio::fs;
use tokio::sync::{Mutex, Semaphore};
use tokio::task;
use tracing::{debug, error, warn};

/// Main processor for MIDAS dataset conversion
pub struct DatasetProcessor {
    dataset_path: PathBuf,
    output_path: PathBuf,
    config: MidasConfig,
    schema_manager: SchemaManager,
    header_semaphore: Arc<Semaphore>,
    system_monitor: Arc<Mutex<System>>,
    memory_threshold: f64,
    system_profile: SystemProfile,
    station_count: usize,
}

impl DatasetProcessor {
    /// Create a new dataset processor
    pub fn new(dataset_path: PathBuf, output_path: Option<PathBuf>) -> Result<Self> {
        let output_path = output_path.unwrap_or_else(|| {
            let dataset_name = dataset_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            dataset_path
                .parent()
                .unwrap_or(&dataset_path)
                .join("parquet")
                .join(format!("{}.parquet", dataset_name))
        });

        // Verify dataset path exists
        if !dataset_path.exists() {
            return Err(MidasError::DatasetNotFound { path: dataset_path });
        }

        Ok(Self {
            dataset_path,
            output_path,
            config: MidasConfig::default(),
            schema_manager: SchemaManager::new(),
            header_semaphore: Arc::new(Semaphore::new(4)), // Limit concurrent header parsing
            system_monitor: Arc::new(Mutex::new(System::new())),
            memory_threshold: 0.8, // 80% memory usage threshold
            system_profile: SystemProfile::detect(),
            station_count: 0, // Will be calculated during processing
        })
    }

    /// Configure the processor
    pub fn with_config(mut self, config: MidasConfig) -> Self {
        self.config = config;
        self
    }

    /// Main processing entry point
    pub async fn process(&mut self) -> Result<ProcessingStats> {
        let start_time = Instant::now();
        println!(
            "{}",
            "Starting MIDAS dataset processing".bright_green().bold()
        );
        println!(
            "  {} {}",
            "Dataset:".bright_cyan(),
            self.dataset_path.display()
        );
        println!(
            "  {} {}",
            "Output:".bright_cyan(),
            self.output_path.display()
        );

        // Step 1: Discover CSV files
        println!("\n{}", "Discovering CSV files...".bright_yellow());
        let csv_files = self.discover_csv_files().await?;
        println!(
            "  {} {} CSV files from {} stations",
            "Found".bright_green(),
            csv_files.len().to_string().bright_white().bold(),
            self.station_count.to_string().bright_white().bold()
        );

        if csv_files.is_empty() {
            return Ok(ProcessingStats {
                files_processed: 0,
                files_failed: 0,
                total_rows: 0,
                output_path: self.output_path.clone(),
                processing_time_ms: start_time.elapsed().as_millis(),
            });
        }

        // Step 2: Detect dataset type from first file
        let dataset_type = self.schema_manager.detect_dataset_type(&csv_files[0])?;
        println!("  {} {:?}", "Dataset type:".bright_cyan(), dataset_type);

        // Step 2.5: Initialize schema dynamically by analyzing sample files
        let sample_size = self.config.sample_size.min(csv_files.len());
        let sample_files: Vec<_> = csv_files.iter().take(sample_size).cloned().collect();

        if !self.schema_manager.has_schema(&dataset_type) {
            println!("\n{}", "Analyzing schema...".bright_yellow());
            println!(
                "  {} {} sample files",
                "Examining".bright_cyan(),
                sample_files.len()
            );
            self.schema_manager
                .initialize_schema(dataset_type.clone(), &sample_files)
                .await?;
        }

        // Step 3: Handle discovery-only mode
        if self.config.discovery_only {
            println!(
                "\n{}",
                "Discovery mode - schema analysis complete".bright_green()
            );
            self.schema_manager.report_discovery(&dataset_type)?;
            return Ok(ProcessingStats {
                files_processed: 0,
                files_failed: 0,
                total_rows: 0,
                output_path: self.output_path.clone(),
                processing_time_ms: start_time.elapsed().as_millis(),
            });
        }

        // Step 4: Create output directory
        if let Some(parent) = self.output_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Step 5: Process files with streaming pipeline
        println!("\n{}", "Processing files...".bright_yellow());
        let stats = self
            .process_files_streaming(&csv_files, &dataset_type)
            .await?;

        let total_time = start_time.elapsed().as_millis();
        println!("\n{}", "Processing Summary".bright_green().bold());
        println!(
            "  {} {}ms",
            "Time elapsed:".bright_cyan(),
            total_time.to_string().bright_white()
        );
        println!(
            "  {} {}",
            "Files processed:".bright_cyan(),
            stats.files_processed.to_string().bright_white()
        );
        if stats.files_failed > 0 {
            println!(
                "  {} {}",
                "Files failed:".bright_red(),
                stats.files_failed.to_string().bright_red().bold()
            );
        }
        println!(
            "  {} {}",
            "Total rows:".bright_cyan(),
            stats.total_rows.to_string().bright_white().bold()
        );

        Ok(ProcessingStats {
            processing_time_ms: total_time,
            ..stats
        })
    }

    /// Discover all CSV files in the dataset and count stations
    async fn discover_csv_files(&mut self) -> Result<Vec<PathBuf>> {
        let pattern = self.dataset_path.join("qcv-1/**/*.csv");
        let pattern_str = pattern.to_string_lossy();

        debug!("Searching for CSV files with pattern: {}", pattern_str);

        let mut files = Vec::new();
        let mut stations = std::collections::HashSet::new();
        let mut dir = fs::read_dir(&self.dataset_path.join("qcv-1")).await?;

        while let Some(entry) = dir.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                // This is a county directory
                let county_path = entry.path();
                let mut county_dir = fs::read_dir(&county_path).await?;

                while let Some(station_entry) = county_dir.next_entry().await? {
                    if station_entry.file_type().await?.is_dir() {
                        // This is a station directory
                        let station_path = station_entry.path();
                        let station_name = station_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("unknown");
                        stations.insert(station_name.to_string());

                        let mut station_dir = fs::read_dir(&station_path).await?;

                        while let Some(file_entry) = station_dir.next_entry().await? {
                            let file_path = file_entry.path();
                            if file_path.extension().is_some_and(|ext| ext == "csv") {
                                files.push(file_path);
                            }
                        }
                    }
                }
            }
        }

        self.station_count = stations.len();
        debug!(
            "Found {} CSV files from {} stations",
            files.len(),
            self.station_count
        );
        Ok(files)
    }

    /// Check if system is under memory pressure
    async fn check_memory_pressure(&self) -> bool {
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
    async fn process_files_streaming(
        &self,
        files: &[PathBuf],
        dataset_type: &DatasetType,
    ) -> Result<ProcessingStats> {
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

        if all_batches.is_empty() {
            warn!("No files were successfully processed");
            return Ok(ProcessingStats {
                files_processed: total_processed,
                files_failed: total_failed,
                total_rows: 0,
                output_path: self.output_path.clone(),
                processing_time_ms: 0,
            });
        }

        // Write all batch frames to final output using streaming
        let total_rows = if all_batches.len() > 2 {
            // For large datasets, use streaming approach (progress bars inside)
            self.write_final_parquet(all_batches).await?
        } else {
            // For smaller datasets, use spinner
            let write_pb = ProgressBar::new_spinner();
            write_pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap(),
            );
            write_pb.set_message(format!(
                "Writing final parquet file ({} batches)...",
                all_batches.len()
            ));
            write_pb.enable_steady_tick(std::time::Duration::from_millis(100));

            let rows = self.write_final_parquet(all_batches).await?;

            write_pb.finish_with_message(format!(
                "Final parquet written to: {}",
                self.output_path.display()
            ));

            rows
        };

        Ok(ProcessingStats {
            files_processed: total_processed,
            files_failed: total_failed,
            total_rows,
            output_path: self.output_path.clone(),
            processing_time_ms: 0, // Will be set by caller
        })
    }

    /// Process a single MIDAS CSV file using optimized lazy scanning
    async fn process_single_file_lazy(
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

    /// Write final parquet file with optimized structure for timeseries queries
    async fn write_final_parquet(&self, frames: Vec<LazyFrame>) -> Result<usize> {
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
            return self.write_final_parquet_streaming(frames).await;
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
            self.station_count,
            &self.system_profile,
        );

        debug!(
            "Calculated optimal row group size: {} rows for {} total rows from {} stations",
            optimal_row_group_size, total_rows, self.station_count
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
        use polars::prelude::*;

        let file = std::fs::File::create(&self.output_path)?;
        let writer = ParquetWriter::new(file)
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
    async fn write_final_parquet_streaming(&self, batches: Vec<LazyFrame>) -> Result<usize> {
        debug!(
            "Starting streaming parquet write for {} batches",
            batches.len()
        );

        // Calculate optimal row group size for streaming write
        let parquet_config = &self.config.parquet_optimization;
        let estimated_total_rows = batches.len() * 500_000; // Rough estimate
        let optimal_row_group_size = parquet_config.calculate_optimal_row_group_size(
            estimated_total_rows,
            self.station_count,
            &self.system_profile,
        );

        debug!(
            "Streaming write: calculated optimal row group size: {} rows for ~{} total rows from {} stations",
            optimal_row_group_size, estimated_total_rows, self.station_count
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
        let writer = ParquetWriter::new(temp_file)
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

        // Store the temp file for later concatenation
        // We'll modify the concatenation logic to handle these files

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
