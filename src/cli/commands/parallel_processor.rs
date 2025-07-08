//! Pull-based parallel processing with streaming observation consumption
//!
//! This module implements high-performance parallel processing of MIDAS CSV files
//! using a pull-based streaming architecture where the Parquet writer controls
//! the pace of processing, providing natural backpressure and eliminating timeout issues.

use crate::Result;
use crate::app::services::parquet_writer::{
    WritingStats, writer::create_optimized_writer_with_dataset,
};
use crate::app::services::station_registry::StationRegistry;
use crate::cli::commands::observation_stream::ParallelObservationStream;
use crate::cli::commands::shared::create_progress_bar;
use crate::config::Config;
use crate::constants::DEFAULT_STREAMING_BUFFER_SIZE;

use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

/// Simplified parallel processor using pull-based streaming
pub struct ParallelProcessor {
    config: Arc<Config>,
    dataset_name: String,
    station_registry: Arc<StationRegistry>,
}

impl ParallelProcessor {
    /// Create a new parallel processor for a specific dataset
    pub fn new(
        config: Arc<Config>,
        dataset_name: String,
        station_registry: Arc<StationRegistry>,
    ) -> Self {
        Self {
            config,
            dataset_name,
            station_registry,
        }
    }

    /// Process multiple CSV files using pull-based streaming architecture
    ///
    /// This method creates an observation stream that processes files on-demand
    /// as the Parquet writer requests data, providing natural backpressure.
    pub async fn process_files_parallel(
        &self,
        csv_files: &[PathBuf],
        show_progress: bool,
    ) -> Result<ParallelProcessingResult> {
        info!(
            "Starting pull-based parallel processing of {} files with {} workers",
            csv_files.len(),
            self.config.performance.parallel_workers
        );

        let start_time = std::time::Instant::now();

        // Set up progress tracking
        let progress_bar = if show_progress && !csv_files.is_empty() {
            Some(create_progress_bar(
                csv_files.len() as u64,
                &format!(
                    "Processing {} files with pull-based streaming...",
                    self.dataset_name
                ),
            ))
        } else {
            None
        };

        // Create the parallel observation stream
        let mut observation_stream = ParallelObservationStream::new(
            csv_files.to_vec(),
            self.station_registry.clone(),
            self.config.quality_control.clone(),
            self.config.performance.parallel_workers,
        );

        // Create Parquet writer with pre-defined schema
        let parquet_output_path = self.config.get_parquet_output_path();
        let output_file = crate::app::services::parquet_writer::utils::create_dataset_output_path(
            &self.dataset_name,
            &parquet_output_path,
        );

        let mut writer = create_optimized_writer_with_dataset(
            &output_file,
            1_000_000, // Estimate for optimization
            Some(&self.dataset_name),
        )
        .await?;

        info!("Created Parquet writer for: {}", output_file.display());

        // Let the Parquet writer consume the stream at its own pace
        info!("Starting stream consumption by Parquet writer");

        // Process the stream observation-by-observation with natural buffering
        let mut observation_buffer = Vec::new();
        let mut total_observations = 0;
        let mut last_stats_update = std::time::Instant::now();

        while observation_stream.has_more() {
            match observation_stream.next_observation().await {
                Some(Ok(observation)) => {
                    observation_buffer.push(observation);
                    total_observations += 1;

                    // Update progress periodically (every 500ms) to avoid too many async calls
                    if last_stats_update.elapsed().as_millis() > 500 {
                        let current_stats = observation_stream.stats().await;

                        if let Some(pb) = &progress_bar {
                            pb.set_position(current_stats.files_processed as u64);
                            pb.set_message(format!(
                                "Processed {} files, {} observations streaming...",
                                current_stats.files_processed, total_observations
                            ));
                        }

                        last_stats_update = std::time::Instant::now();
                    }

                    // Write observations in reasonably-sized batches for efficiency
                    if observation_buffer.len() >= DEFAULT_STREAMING_BUFFER_SIZE {
                        writer
                            .write_observations(std::mem::take(&mut observation_buffer))
                            .await?;
                    }
                }
                Some(Err(e)) => {
                    return Err(e);
                }
                None => break,
            }
        }

        // Write any remaining observations
        if !observation_buffer.is_empty() {
            writer.write_observations(observation_buffer).await?;
        }

        // Finalize the writer
        let writing_stats = writer.finalize().await?;

        let processing_time = start_time.elapsed();

        // Get final stream statistics
        let stream_stats = observation_stream.stats().await;

        // Update final progress
        if let Some(pb) = &progress_bar {
            pb.finish_with_message(format!(
                "Completed: {} files processed, {} observations written",
                stream_stats.files_processed, writing_stats.observations_written
            ));
        }

        info!(
            "Parallel processing complete: {} files in {:.2}s ({:.1} files/sec)",
            stream_stats.files_processed,
            processing_time.as_secs_f64(),
            stream_stats.files_processed as f64 / processing_time.as_secs_f64()
        );

        // Create processing stats from stream stats
        let processing_stats = ParallelProcessingStats {
            total_files_processed: stream_stats.files_processed,
            successful_files: stream_stats.files_processed,
            failed_files: stream_stats.files_failed,
            total_observations_parsed: stream_stats.observations_produced
                + stream_stats.observations_filtered,
            total_errors: stream_stats.files_failed,
            worker_failures: 0, // No workers to fail in pull-based architecture
            processing_time,
        };

        Ok(ParallelProcessingResult {
            processing_stats,
            writing_stats,
        })
    }
}

/// Results from parallel processing operation
#[derive(Debug)]
pub struct ParallelProcessingResult {
    pub processing_stats: ParallelProcessingStats,
    pub writing_stats: WritingStats,
}

/// Statistics for parallel processing operation
#[derive(Debug, Default)]
pub struct ParallelProcessingStats {
    pub total_files_processed: usize,
    pub successful_files: usize,
    pub failed_files: usize,
    pub total_observations_parsed: usize,
    pub total_errors: usize,
    pub worker_failures: usize,
    pub processing_time: std::time::Duration,
}

impl ParallelProcessingStats {
    /// Calculate files processed per second
    pub fn files_per_second(&self) -> f64 {
        if self.processing_time.as_secs_f64() > 0.0 {
            self.total_files_processed as f64 / self.processing_time.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate observations parsed per second
    pub fn observations_per_second(&self) -> f64 {
        if self.processing_time.as_secs_f64() > 0.0 {
            self.total_observations_parsed as f64 / self.processing_time.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate success rate percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_files_processed > 0 {
            (self.successful_files as f64 / self.total_files_processed as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Generate human-readable summary
    pub fn summary(&self) -> String {
        format!(
            "Pull-Based Processing Summary:\\n\\\
             Files: {} processed, {} successful ({:.1}% success rate)\\n\\\
             Observations: {} parsed\\n\\\
             Performance: {:.1} files/sec, {:.0} observations/sec\\n\\\
             Duration: {:.2}s\\n\\\
             Errors: {} file errors, {} worker failures",
            self.total_files_processed,
            self.successful_files,
            self.success_rate(),
            self.total_observations_parsed,
            self.files_per_second(),
            self.observations_per_second(),
            self.processing_time.as_secs_f64(),
            self.total_errors,
            self.worker_failures
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    fn create_test_config() -> Config {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input");
        let output_path = temp_dir.path().join("output");
        std::fs::create_dir_all(&input_path).unwrap();
        std::fs::create_dir_all(&output_path).unwrap();

        let mut config = Config::new(input_path, output_path);
        config.performance.parallel_workers = 2; // Use 2 workers for testing
        config
    }

    #[tokio::test]
    async fn test_parallel_processor_creation() {
        let config = create_test_config();
        let station_registry = Arc::new(
            crate::app::services::station_registry::StationRegistry::load_for_dataset(
                &config.processing.input_path,
                "test-dataset",
                false,
            )
            .await
            .map(|(registry, _)| registry)
            .unwrap_or_else(|_| {
                // Create empty registry for test
                crate::app::services::station_registry::StationRegistry::new(
                    config.processing.input_path.clone(),
                )
            }),
        );

        let processor = ParallelProcessor::new(
            Arc::new(config.clone()),
            "test-dataset".to_string(),
            station_registry,
        );

        assert_eq!(processor.dataset_name, "test-dataset");
    }

    #[test]
    fn test_processing_stats_calculations() {
        let stats = ParallelProcessingStats {
            total_files_processed: 10,
            successful_files: 8,
            failed_files: 2,
            total_observations_parsed: 5000,
            total_errors: 15,
            worker_failures: 0, // No worker failures in pull-based architecture
            processing_time: std::time::Duration::from_secs(120),
        };

        assert_eq!(stats.success_rate(), 80.0);
        assert!((stats.files_per_second() - (10.0 / 120.0)).abs() < f64::EPSILON);
        assert!((stats.observations_per_second() - (5000.0 / 120.0)).abs() < f64::EPSILON);

        let summary = stats.summary();
        assert!(summary.contains("80.0% success rate"));
        assert!(summary.contains("120.00s"));
        assert!(summary.contains("15 file errors"));
    }

    #[test]
    fn test_processing_stats_edge_cases() {
        // Test zero processing time
        let zero_time_stats = ParallelProcessingStats {
            processing_time: std::time::Duration::from_secs(0),
            ..Default::default()
        };
        assert_eq!(zero_time_stats.files_per_second(), 0.0);
        assert_eq!(zero_time_stats.observations_per_second(), 0.0);

        // Test zero files processed
        let zero_files_stats = ParallelProcessingStats {
            total_files_processed: 0,
            processing_time: std::time::Duration::from_secs(10),
            ..Default::default()
        };
        assert_eq!(zero_files_stats.success_rate(), 0.0);
        assert_eq!(zero_files_stats.files_per_second(), 0.0);
    }
}
