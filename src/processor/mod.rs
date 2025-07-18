//! Main processing engine with modular architecture.
//!
//! Orchestrates the complete MIDAS dataset processing workflow using
//! specialized modules for file discovery, streaming processing, and
//! parquet writing.

pub mod discovery;
pub mod streaming;
pub mod writer;

#[cfg(test)]
pub mod tests;

use self::{discovery::FileDiscovery, streaming::StreamingProcessor, writer::ParquetWriter};

use crate::config::MidasConfig;
use crate::error::{MidasError, Result};
use crate::header::parse_badc_header;
use crate::models::ProcessingStats;
use crate::schema::SchemaManager;

use colored::*;
use futures::stream::{self, StreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::task;

/// Main processor for MIDAS dataset conversion
#[derive(Debug)]
pub struct DatasetProcessor {
    dataset_path: PathBuf,
    output_path: PathBuf,
    config: MidasConfig,
    file_discovery: FileDiscovery,
    streaming_processor: StreamingProcessor,
    parquet_writer: ParquetWriter,
    schema_manager: SchemaManager,
    station_count: usize,
}

impl DatasetProcessor {
    /// Create a new dataset processor
    pub fn new(dataset_path: PathBuf, output_path: Option<PathBuf>) -> Result<Self> {
        let output_path = output_path.unwrap_or_else(|| {
            let dataset_name = dataset_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();

            dataset_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("parquet")
                .join(format!("{}.parquet", dataset_name))
        });

        // Verify dataset path exists
        if !dataset_path.exists() {
            return Err(MidasError::DatasetNotFound { path: dataset_path });
        }

        let config = MidasConfig::default();
        let schema_manager = SchemaManager::new();

        Ok(Self {
            dataset_path: dataset_path.clone(),
            output_path: output_path.clone(),
            config: config.clone(),
            file_discovery: FileDiscovery::new(dataset_path),
            streaming_processor: StreamingProcessor::new(config.clone(), schema_manager.clone()),
            parquet_writer: ParquetWriter::new(output_path, config),
            schema_manager,
            station_count: 0, // Will be calculated during processing
        })
    }

    /// Configure the processor
    pub fn with_config(mut self, config: MidasConfig) -> Self {
        self.config = config.clone();
        self.streaming_processor =
            StreamingProcessor::new(config.clone(), self.schema_manager.clone());
        self.parquet_writer = ParquetWriter::new(self.output_path.clone(), config);
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
        let csv_files = self.file_discovery.discover_csv_files().await?;
        self.station_count = self.file_discovery.station_count();
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

        println!(
            "  {} {} sample files for schema analysis",
            "Analyzing".bright_cyan(),
            sample_files.len()
        );

        // Use a semaphore to limit concurrent header parsing for schema analysis
        let semaphore = Arc::new(Semaphore::new(4));

        // Parse headers concurrently for schema analysis
        let sample_headers = stream::iter(sample_files.iter())
            .map(|file_path| {
                let sem = semaphore.clone();
                let file_path = file_path.clone();
                async move {
                    let _permit = sem.acquire().await.ok()?;
                    let file_path_for_task = file_path.clone();

                    let header_result =
                        task::spawn_blocking(move || parse_badc_header(&file_path_for_task))
                            .await
                            .ok()?
                            .ok()?;

                    Some((file_path, header_result))
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;

        // Filter out failed headers and check if we have enough valid samples
        let valid_samples: Vec<_> = sample_headers.into_iter().flatten().collect();

        if valid_samples.is_empty() {
            return Err(MidasError::ProcessingFailed {
                path: self.dataset_path.clone(),
                reason: "No valid headers found in sample files".to_string(),
            });
        }

        // Use only the successfully parsed headers for schema initialization
        let valid_files: Vec<_> = valid_samples.iter().map(|(path, _)| path.clone()).collect();

        if !self.config.skip_schema_validation {
            println!(
                "  {} {} valid files for schema analysis",
                "Examining".bright_cyan(),
                valid_files.len()
            );
            self.schema_manager
                .initialize_schema(dataset_type.clone(), &valid_files)
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

        // Update streaming processor with initialized schema manager
        self.streaming_processor
            .update_schema_manager(self.schema_manager.clone());

        let (batches, mut stats) = self
            .streaming_processor
            .process_files_streaming(&csv_files, &dataset_type, &self.output_path)
            .await?;

        // Step 6: Write final parquet file
        if !batches.is_empty() {
            let total_rows = self
                .parquet_writer
                .write_final_parquet(batches, self.station_count, &dataset_type)
                .await?;
            stats.total_rows = total_rows;
        }

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
}
