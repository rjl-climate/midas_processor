//! Pull-based observation streaming for efficient parallel processing
//!
//! This module implements a consumer-driven streaming architecture where the Parquet writer
//! controls the pace of file processing, providing natural backpressure and eliminating
//! timeout issues from producer-consumer speed mismatches.

use crate::app::models::Observation;
use crate::app::services::badc_csv_parser::BadcCsvParser;
use crate::app::services::record_processor::RecordProcessor;
use crate::app::services::station_registry::StationRegistry;
use crate::config::QualityControlConfig;
use crate::{Error, Result};

use futures::Stream;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Parallel observation stream that processes multiple files concurrently
///
/// This stream spawns worker tasks to process files in parallel while providing
/// a single unified stream interface with natural backpressure control.
pub struct ParallelObservationStream {
    /// Receiver for observations from worker tasks
    observation_receiver: mpsc::Receiver<Result<Observation>>,
    /// Join set to manage worker tasks
    workers: JoinSet<Result<usize>>,
    /// Statistics aggregated from all workers
    stats: Arc<tokio::sync::Mutex<StreamStats>>,
    /// Flag to track if workers have been spawned
    workers_spawned: bool,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
}

/// Single-file observation stream for sequential processing (kept for compatibility)
///
/// This stream processes files lazily, only when the consumer requests data.
/// This provides natural backpressure and prevents memory issues.
pub struct ObservationStream {
    /// Queue of files to process
    file_queue: VecDeque<PathBuf>,
    /// Current file being processed
    current_file_observations: Option<std::vec::IntoIter<Observation>>,
    /// BADC CSV parser for file parsing
    parser: BadcCsvParser,
    /// Record processor for observation processing
    processor: RecordProcessor,
    /// Semaphore for limiting concurrent operations
    semaphore: Arc<Semaphore>,
    /// Statistics for monitoring
    stats: StreamStats,
}

/// Statistics for stream processing
#[derive(Debug, Default, Clone)]
pub struct StreamStats {
    pub files_processed: usize,
    pub files_failed: usize,
    pub observations_produced: usize,
    pub observations_filtered: usize,
}

impl StreamStats {
    pub fn success_rate(&self) -> f64 {
        if self.files_processed + self.files_failed == 0 {
            0.0
        } else {
            (self.files_processed as f64) / ((self.files_processed + self.files_failed) as f64)
                * 100.0
        }
    }
}

impl ParallelObservationStream {
    /// Create a new parallel observation stream
    pub fn new(
        files: Vec<PathBuf>,
        station_registry: Arc<StationRegistry>,
        quality_config: QualityControlConfig,
        max_concurrent_files: usize,
        cancellation_token: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1000); // Buffer for natural backpressure
        let workers = JoinSet::new();
        let stats = Arc::new(tokio::sync::Mutex::new(StreamStats::default()));

        let mut stream = Self {
            observation_receiver: rx,
            workers,
            stats,
            workers_spawned: false,
            cancellation_token: cancellation_token.clone(),
        };

        // Spawn worker tasks
        stream.spawn_workers(
            files,
            station_registry,
            quality_config,
            max_concurrent_files,
            tx,
            cancellation_token,
        );

        stream
    }

    /// Spawn worker tasks to process files in parallel
    fn spawn_workers(
        &mut self,
        files: Vec<PathBuf>,
        station_registry: Arc<StationRegistry>,
        quality_config: QualityControlConfig,
        max_workers: usize,
        sender: mpsc::Sender<Result<Observation>>,
        cancellation_token: CancellationToken,
    ) {
        let semaphore = Arc::new(Semaphore::new(max_workers));
        let stats = self.stats.clone();

        // Create shared work queue
        let work_queue = Arc::new(tokio::sync::Mutex::new(
            files.into_iter().collect::<VecDeque<_>>(),
        ));

        info!("Spawning {} parallel file processing workers", max_workers);

        // Spawn worker tasks
        for worker_id in 0..max_workers {
            let work_queue = work_queue.clone();
            let station_registry = station_registry.clone();
            let quality_config = quality_config.clone();
            let sender = sender.clone();
            let semaphore = semaphore.clone();
            let stats = stats.clone();
            let cancellation_token = cancellation_token.clone();

            self.workers.spawn(async move {
                Self::worker_task(
                    worker_id,
                    work_queue,
                    station_registry,
                    quality_config,
                    sender,
                    semaphore,
                    stats,
                    cancellation_token,
                )
                .await
            });
        }

        self.workers_spawned = true;
    }

    /// Worker task that processes files from the shared queue
    #[allow(clippy::too_many_arguments)]
    async fn worker_task(
        worker_id: usize,
        work_queue: Arc<tokio::sync::Mutex<VecDeque<PathBuf>>>,
        station_registry: Arc<StationRegistry>,
        quality_config: QualityControlConfig,
        sender: mpsc::Sender<Result<Observation>>,
        semaphore: Arc<Semaphore>,
        stats: Arc<tokio::sync::Mutex<StreamStats>>,
        cancellation_token: CancellationToken,
    ) -> Result<usize> {
        let parser = BadcCsvParser::new(station_registry.clone());
        let processor = RecordProcessor::new(station_registry, quality_config);
        let mut files_processed = 0;

        debug!("Worker {} started", worker_id);

        loop {
            // Check for cancellation before processing each file
            if cancellation_token.is_cancelled() {
                debug!("Worker {} cancelled by user", worker_id);
                break;
            }

            // Get next file from queue
            let file_path = {
                let mut queue = work_queue.lock().await;
                match queue.pop_front() {
                    Some(path) => path,
                    None => {
                        debug!("Worker {} finished - no more files", worker_id);
                        break; // No more work
                    }
                }
            };

            // Acquire semaphore permit for controlled concurrency
            let _permit = semaphore.acquire().await.map_err(|e| {
                Error::data_validation(format!(
                    "Worker {} failed to acquire semaphore: {}",
                    worker_id, e
                ))
            })?;

            debug!(
                "Worker {} processing file: {}",
                worker_id,
                file_path.display()
            );

            // Process the file
            match Self::process_file(&parser, &processor, file_path.as_path()).await {
                Ok(observations) => {
                    files_processed += 1;

                    // Update shared statistics
                    {
                        let mut shared_stats = stats.lock().await;
                        shared_stats.files_processed += 1;
                        shared_stats.observations_produced += observations.len();
                    }

                    // Send observations to stream
                    for observation in observations {
                        if sender.send(Ok(observation)).await.is_err() {
                            debug!("Worker {} channel closed, stopping", worker_id);
                            return Ok(files_processed);
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Worker {} failed to process {}: {}",
                        worker_id,
                        file_path.display(),
                        e
                    );

                    // Update error statistics
                    {
                        let mut shared_stats = stats.lock().await;
                        shared_stats.files_failed += 1;
                    }

                    // Send error to stream
                    if sender.send(Err(e)).await.is_err() {
                        debug!("Worker {} channel closed after error, stopping", worker_id);
                        return Ok(files_processed);
                    }
                }
            }
        }

        debug!("Worker {} completed {} files", worker_id, files_processed);
        Ok(files_processed)
    }

    /// Process a single file and return observations
    async fn process_file(
        parser: &BadcCsvParser,
        processor: &RecordProcessor,
        file_path: &Path,
    ) -> Result<Vec<Observation>> {
        // Parse the CSV file
        let parse_result = parser.parse_file(file_path).await?;

        if parse_result.observations.is_empty() {
            debug!("No observations in file: {}", file_path.display());
            return Ok(Vec::new());
        }

        // Process observations (enrichment, deduplication, quality filtering)
        let processing_result = processor
            .process_observations(parse_result.observations, false)
            .await?;

        debug!(
            "Processed {}: {} observations produced",
            file_path.display(),
            processing_result.observations.len()
        );

        Ok(processing_result.observations)
    }

    /// Get current processing statistics
    pub async fn stats(&self) -> StreamStats {
        self.stats.lock().await.clone()
    }

    /// Check if there are still active workers or pending observations
    pub fn has_more(&self) -> bool {
        !self.workers.is_empty() || !self.observation_receiver.is_empty()
    }

    /// Get the next observation from any worker
    pub async fn next_observation(&mut self) -> Option<Result<Observation>> {
        loop {
            // First check if we have any observations in the channel
            match self.observation_receiver.try_recv() {
                Ok(observation) => return Some(observation),
                Err(mpsc::error::TryRecvError::Empty) => {
                    // Channel is empty, continue to wait or check workers
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // All senders dropped, no more observations
                    return None;
                }
            }

            // Wait for either an observation, worker completion, or cancellation
            tokio::select! {
                observation = self.observation_receiver.recv() => {
                    return observation;
                }
                _ = self.cancellation_token.cancelled() => {
                    debug!("Observation stream cancelled by user");
                    return Some(Err(Error::data_validation("Processing cancelled by user".to_string())));
                }
                worker_result = self.workers.join_next(), if !self.workers.is_empty() => {
                    match worker_result {
                        Some(Ok(Ok(files_processed))) => {
                            debug!("Worker completed processing {} files", files_processed);
                            // Worker finished, loop to check for more observations
                            continue;
                        }
                        Some(Ok(Err(e))) => {
                            error!("Worker failed: {}", e);
                            return Some(Err(e));
                        }
                        Some(Err(e)) => {
                            error!("Worker task panicked: {}", e);
                            return Some(Err(Error::data_validation(format!("Worker task failed: {}", e))));
                        }
                        None => {
                            // All workers finished, check for remaining observations
                            return self.observation_receiver.recv().await;
                        }
                    }
                }
                else => {
                    // No workers and no observations - we're done
                    return None;
                }
            }
        }
    }
}

/// Implement Stream trait for ParallelObservationStream
impl Stream for ParallelObservationStream {
    type Item = Result<Observation>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Create a future for the next observation
        let future = self.next_observation();
        tokio::pin!(future);

        // Poll the future
        future.poll(cx)
    }
}

impl ObservationStream {
    /// Create a new observation stream for the given files
    pub fn new(
        files: Vec<PathBuf>,
        station_registry: Arc<StationRegistry>,
        quality_config: QualityControlConfig,
        max_concurrent_files: usize,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_concurrent_files));

        Self {
            file_queue: files.into(),
            current_file_observations: None,
            parser: BadcCsvParser::new(station_registry.clone()),
            processor: RecordProcessor::new(station_registry, quality_config),
            semaphore,
            stats: StreamStats::default(),
        }
    }

    /// Process the next file and return its observations
    async fn process_next_file(&mut self) -> Result<Option<Vec<Observation>>> {
        // Get next file from queue
        let file_path = match self.file_queue.pop_front() {
            Some(path) => path,
            None => return Ok(None), // No more files
        };

        debug!("Processing file: {}", file_path.display());

        // Acquire semaphore permit for controlled concurrency
        let _permit =
            self.semaphore.acquire().await.map_err(|e| {
                Error::data_validation(format!("Failed to acquire semaphore: {}", e))
            })?;

        // Parse the CSV file
        let parse_result = match self.parser.parse_file(&file_path).await {
            Ok(result) => {
                self.stats.files_processed += 1;
                result
            }
            Err(e) => {
                error!("Failed to parse {}: {}", file_path.display(), e);
                self.stats.files_failed += 1;
                return Ok(Some(Vec::new())); // Return empty batch, continue processing
            }
        };

        // Process observations (enrichment, deduplication, quality filtering)
        if parse_result.observations.is_empty() {
            debug!("No observations in file: {}", file_path.display());
            return Ok(Some(Vec::new()));
        }

        let processing_result = match self
            .processor
            .process_observations(parse_result.observations, false)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                error!(
                    "Failed to process observations from {}: {}",
                    file_path.display(),
                    e
                );
                self.stats.files_failed += 1;
                return Ok(Some(Vec::new()));
            }
        };

        let observations = processing_result.observations;
        self.stats.observations_produced += observations.len();

        debug!(
            "Processed {}: {} observations produced",
            file_path.display(),
            observations.len()
        );

        Ok(Some(observations))
    }

    /// Get the next individual observation from the stream
    pub async fn next_observation(&mut self) -> Option<Result<Observation>> {
        // First, try to get observation from current file if available
        if let Some(ref mut current_iter) = self.current_file_observations {
            if let Some(observation) = current_iter.next() {
                return Some(Ok(observation));
            } else {
                // Current file exhausted
                self.current_file_observations = None;
            }
        }

        // If no current file, process next file
        while !self.file_queue.is_empty() {
            match self.process_next_file().await {
                Ok(Some(observations)) => {
                    if !observations.is_empty() {
                        let mut obs_iter = observations.into_iter();

                        // Get first observation from this file
                        if let Some(observation) = obs_iter.next() {
                            // Save remaining observations for next calls
                            if obs_iter.len() > 0 {
                                self.current_file_observations = Some(obs_iter);
                            }
                            return Some(Ok(observation));
                        }
                    }
                }
                Ok(None) => {
                    // No more files to process
                    break;
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }

        // Stream exhausted
        None
    }

    /// Get current processing statistics
    pub fn stats(&self) -> &StreamStats {
        &self.stats
    }

    /// Get remaining file count
    pub fn remaining_files(&self) -> usize {
        self.file_queue.len()
    }

    /// Check if stream has more data
    pub fn has_more(&self) -> bool {
        !self.file_queue.is_empty() || self.current_file_observations.is_some()
    }
}

/// Implement Stream trait for async iteration
impl Stream for ObservationStream {
    type Item = Result<Observation>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Create a future for the next observation
        let future = self.next_observation();
        tokio::pin!(future);

        // Poll the future
        future.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::services::station_registry::StationRegistry;
    use crate::config::QualityControlConfig;
    use crate::constants::DEFAULT_CONCURRENT_FILES;
    use tempfile::TempDir;

    fn create_test_stream() -> ObservationStream {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let station_registry = Arc::new(StationRegistry::new(cache_path));
        let quality_config = QualityControlConfig::default();

        ObservationStream::new(
            vec![], // Empty file list for testing
            station_registry,
            quality_config,
            DEFAULT_CONCURRENT_FILES,
        )
    }

    #[test]
    fn test_stream_creation() {
        let stream = create_test_stream();
        assert_eq!(stream.remaining_files(), 0);
        assert!(!stream.has_more());
    }

    #[test]
    fn test_stream_stats() {
        let stream = create_test_stream();
        let stats = stream.stats();
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.success_rate(), 0.0);
    }

    #[tokio::test]
    async fn test_parallel_stream_creation() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let station_registry = Arc::new(StationRegistry::new(cache_path));
        let quality_config = QualityControlConfig::default();

        let parallel_stream = ParallelObservationStream::new(
            vec![], // Empty file list for testing
            station_registry,
            quality_config,
            DEFAULT_CONCURRENT_FILES,
            CancellationToken::new(),
        );

        // Initial state should show no activity
        let stats = parallel_stream.stats().await;
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.success_rate(), 0.0);
    }
}
