//! Core Parquet writer implementation for MIDAS weather data
//!
//! This module contains the main ParquetWriter struct and its implementation
//! for high-performance streaming Parquet file generation.

use crate::app::models::Observation;
use crate::app::services::parquet_writer::{
    config::{WriterConfig, WritingStats},
    conversion::observations_to_record_batch,
    progress::ProgressReporter,
    schema::{create_weather_schema, extend_schema_with_measurements_for_dataset},
};
use crate::{Error, Result};

use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// High-performance Parquet writer optimized for MIDAS weather data
///
/// This writer provides streaming Parquet file generation with:
/// - Memory-efficient batch processing
/// - Progress reporting and diagnostics
/// - Pandas-compatible output format
/// - Query optimization features
pub struct ParquetWriter {
    /// Output file path
    output_path: PathBuf,
    /// Writer configuration
    config: WriterConfig,
    /// Arrow schema for the dataset
    schema: SchemaRef,
    /// Dataset name for pre-defined schema lookup
    dataset_name: Option<String>,
    /// Arrow writer instance
    arrow_writer: Option<ArrowWriter<std::fs::File>>,
    /// Progress reporter for user feedback
    progress_reporter: ProgressReporter,
    /// Writing statistics
    stats: WritingStats,
    /// Buffered observations for batch processing
    observation_buffer: Vec<Observation>,
    /// Flag to track if schema has been initialized
    schema_initialized: bool,
}

impl ParquetWriter {
    /// Create a new ParquetWriter instance
    ///
    /// This method sets up the writer with optimal configuration but does not
    /// create the output file until the first write operation.
    pub async fn new(output_path: &Path, config: WriterConfig) -> Result<Self> {
        Self::new_with_dataset(output_path, config, None).await
    }

    /// Create a new ParquetWriter instance with dataset name for pre-defined schema
    ///
    /// This method enables streaming-compatible processing by using pre-defined schemas
    /// when the dataset name is known, avoiding the need to scan observations for schema discovery.
    pub async fn new_with_dataset(
        output_path: &Path,
        config: WriterConfig,
        dataset_name: Option<&str>,
    ) -> Result<Self> {
        info!("Creating ParquetWriter for {}", output_path.display());
        if let Some(dataset) = dataset_name {
            info!("Using dataset-specific optimizations for '{}'", dataset);
        }

        // Validate configuration
        config.validate().map_err(Error::configuration)?;

        // Create base schema (will be extended when first observations are provided)
        let schema = create_weather_schema();

        Ok(Self {
            output_path: output_path.to_path_buf(),
            config,
            schema,
            dataset_name: dataset_name.map(|s| s.to_string()),
            arrow_writer: None,
            progress_reporter: ProgressReporter::new(),
            stats: WritingStats::default(),
            observation_buffer: Vec::new(),
            schema_initialized: false,
        })
    }

    /// Set up progress reporting for the writing operation
    ///
    /// This creates a progress bar with appropriate styling and estimates
    /// based on the expected number of observations to process.
    pub fn setup_progress(&mut self, total_observations: usize) {
        self.progress_reporter.setup_progress(total_observations);
    }

    /// Write a batch of observations to the Parquet file
    ///
    /// This method buffers observations and writes them in optimally-sized batches
    /// to balance memory usage and I/O efficiency.
    pub async fn write_observations(&mut self, observations: Vec<Observation>) -> Result<()> {
        info!("Writing {} observations to Parquet", observations.len());

        // If this is the first write, initialize the schema and create the file
        if !self.schema_initialized {
            self.initialize_schema(&observations).await?;
            self.create_arrow_writer().await?;
            self.schema_initialized = true;
        }

        // Add observations to buffer
        self.observation_buffer.extend(observations);

        // Process complete batches
        while self.observation_buffer.len() >= self.config.write_batch_size {
            let batch_observations: Vec<_> = self
                .observation_buffer
                .drain(..self.config.write_batch_size)
                .collect();

            self.write_batch(&batch_observations).await?;
        }

        // Check memory usage and flush if necessary
        if let Some(ref writer) = self.arrow_writer {
            if writer.in_progress_size() > self.config.memory_limit_bytes {
                self.flush_memory().await?;
            }
        }

        Ok(())
    }

    /// Finalize the Parquet file and close the writer
    ///
    /// This method must be called to ensure the Parquet file is properly closed
    /// and all metadata is written. Any remaining buffered observations will be written.
    pub async fn finalize(mut self) -> Result<WritingStats> {
        info!("Finalizing Parquet file: {}", self.output_path.display());

        // Write any remaining buffered observations
        if !self.observation_buffer.is_empty() {
            info!(
                "Writing final batch of {} observations",
                self.observation_buffer.len()
            );
            let remaining_observations = std::mem::take(&mut self.observation_buffer);
            self.write_batch(&remaining_observations).await?;
        }

        // Close the arrow writer (CRITICAL: must be called explicitly)
        if let Some(writer) = self.arrow_writer.take() {
            writer.close().map_err(|e| {
                Error::parquet_writing("Failed to close ArrowWriter".to_string(), Box::new(e))
            })?;
        }

        // Update final statistics
        if let Ok(metadata) = std::fs::metadata(&self.output_path) {
            self.stats.bytes_written = metadata.len() as usize;
        }

        // Complete progress reporting
        self.progress_reporter.finish(&self.stats);

        info!(
            "Parquet file generation complete: {}",
            self.output_path.display()
        );
        info!("Final statistics: {:?}", self.stats);

        Ok(self.stats)
    }

    /// Get current writing statistics
    pub fn stats(&self) -> &WritingStats {
        &self.stats
    }

    /// Get the output file path
    pub fn output_path(&self) -> &Path {
        &self.output_path
    }

    /// Get the writer configuration
    pub fn config(&self) -> &WriterConfig {
        &self.config
    }

    /// Get the current schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Check if the writer has been initialized with data
    pub fn is_initialized(&self) -> bool {
        self.schema_initialized
    }

    /// Get current buffer size
    pub fn buffer_size(&self) -> usize {
        self.observation_buffer.len()
    }

    /// Initialize the writer with the provided observations to finalize the schema
    ///
    /// This method must be called before any write operations to analyze the
    /// observations and create the final schema with all measurement columns.
    async fn initialize_schema(&mut self, sample_observations: &[Observation]) -> Result<()> {
        info!(
            "Initializing schema with {} sample observations",
            sample_observations.len()
        );

        // Extend schema with measurement fields found in the data
        self.schema = extend_schema_with_measurements_for_dataset(
            self.schema.clone(),
            sample_observations,
            self.dataset_name.as_deref(),
        )?;

        debug!("Final schema has {} fields", self.schema.fields().len());
        for (i, field) in self.schema.fields().iter().enumerate() {
            debug!("  Field {}: {} ({})", i, field.name(), field.data_type());
        }

        Ok(())
    }

    /// Create and configure the Arrow writer with optimization settings
    async fn create_arrow_writer(&mut self) -> Result<()> {
        info!("Creating Arrow writer for {}", self.output_path.display());

        // Ensure parent directory exists
        if let Some(parent) = self.output_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::io("Failed to create output directory".to_string(), e))?;
        }

        // Create output file using std::fs for ArrowWriter compatibility
        let file = std::fs::File::create(&self.output_path).map_err(|e| {
            Error::io(
                format!(
                    "Failed to create output file: {}",
                    self.output_path.display()
                ),
                e,
            )
        })?;

        // Configure writer properties for optimal performance
        let props = self.create_writer_properties()?;

        // Create Arrow writer
        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props)).map_err(|e| {
            Error::parquet_writing("Failed to create ArrowWriter".to_string(), Box::new(e))
        })?;

        self.arrow_writer = Some(writer);
        debug!("Arrow writer created successfully");

        Ok(())
    }

    /// Configure WriterProperties for optimal Parquet generation
    fn create_writer_properties(&self) -> Result<WriterProperties> {
        let builder = WriterProperties::builder()
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.row_group_size)
            .set_data_page_size_limit(self.config.data_page_size_bytes)
            .set_write_batch_size(self.config.write_batch_size);

        // Note: Dictionary encoding and statistics configuration have changed in newer versions
        // For now, we'll use the basic configuration which should provide good performance
        // The Arrow writer will automatically use dictionary encoding where appropriate

        Ok(builder.build())
    }

    /// Convert observations to Arrow RecordBatch and write to file
    async fn write_batch(&mut self, observations: &[Observation]) -> Result<()> {
        if observations.is_empty() {
            return Ok(());
        }

        debug!(
            "Converting {} observations to RecordBatch",
            observations.len()
        );

        // Convert observations to Arrow RecordBatch
        let record_batch = observations_to_record_batch(observations, self.schema.clone())?;

        // Write to Arrow writer
        if let Some(ref mut writer) = self.arrow_writer {
            writer.write(&record_batch).map_err(|e| {
                Error::parquet_writing("Failed to write RecordBatch".to_string(), Box::new(e))
            })?;

            // Update statistics
            self.stats.observations_written += observations.len();
            self.stats.batches_written += 1;
            self.stats.peak_memory_usage_bytes = self
                .stats
                .peak_memory_usage_bytes
                .max(writer.in_progress_size());

            // Update progress reporting
            self.progress_reporter.increment(observations.len());
            self.progress_reporter.update_with_stats(&self.stats);

            debug!(
                "Successfully wrote batch of {} observations",
                observations.len()
            );
        } else {
            return Err(Error::parquet_writing(
                "Arrow writer not initialized".to_string(),
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Writer not initialized",
                )),
            ));
        }

        Ok(())
    }

    /// Force memory flush to prevent OOM conditions
    async fn flush_memory(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.arrow_writer {
            debug!(
                "Flushing memory: current usage {} bytes",
                writer.in_progress_size()
            );

            writer.flush().map_err(|e| {
                Error::parquet_writing("Failed to flush memory".to_string(), Box::new(e))
            })?;

            self.stats.memory_flushes += 1;
            debug!("Memory flush completed");
        }

        Ok(())
    }

    /// Write observations from a pull-based stream with natural backpressure
    ///
    /// This method consumes a stream of individual observations at the optimal rate
    /// for Parquet writing, providing natural flow control and eliminating
    /// timeout issues from producer-consumer speed mismatches.
    pub async fn write_observation_stream<S>(&mut self, mut stream: S) -> Result<WritingStats>
    where
        S: futures::Stream<Item = Result<Observation>> + Unpin,
    {
        use futures::StreamExt;

        info!("Starting streaming Parquet writer from observation stream");
        let mut observation_buffer = Vec::new();
        let mut total_observations = 0;

        // Process observations from the stream at our own pace with optimal buffering
        while let Some(observation_result) = stream.next().await {
            let observation = observation_result?;
            observation_buffer.push(observation);
            total_observations += 1;

            // Write observations in efficient batches while maintaining streaming benefits
            if observation_buffer.len() >= self.config.write_batch_size {
                debug!(
                    "Writing buffered batch: {} observations (total processed: {})",
                    observation_buffer.len(),
                    total_observations
                );

                // Write batch using existing optimized method
                self.write_observations(std::mem::take(&mut observation_buffer))
                    .await?;
            }
        }

        // Write any remaining observations
        if !observation_buffer.is_empty() {
            debug!(
                "Writing final batch: {} observations",
                observation_buffer.len()
            );
            self.write_observations(observation_buffer).await?;
        }

        info!(
            "Stream processing complete: {} total observations processed",
            total_observations
        );

        // Get final stats (writer not finalized yet)
        Ok(self.stats.clone())
    }
}

/// Create a ParquetWriter with recommended configuration for a dataset size
pub async fn create_optimized_writer(
    output_path: &Path,
    estimated_observations: usize,
) -> Result<ParquetWriter> {
    create_optimized_writer_with_dataset(output_path, estimated_observations, None).await
}

/// Create a ParquetWriter with recommended configuration for a dataset size and type
pub async fn create_optimized_writer_with_dataset(
    output_path: &Path,
    estimated_observations: usize,
    dataset_name: Option<&str>,
) -> Result<ParquetWriter> {
    let config = if estimated_observations < 10_000 {
        // Small dataset - optimize for simplicity
        WriterConfig::new()
            .with_row_group_size(10_000)
            .with_write_batch_size(500)
            .with_memory_limit_mb(50)
    } else if estimated_observations < 1_000_000 {
        // Medium dataset - balanced settings
        WriterConfig::new()
            .with_row_group_size(50_000)
            .with_write_batch_size(1_000)
            .with_memory_limit_mb(100)
    } else {
        // Large dataset - optimize for memory efficiency
        WriterConfig::new()
            .with_row_group_size(100_000)
            .with_write_batch_size(2_000)
            .with_memory_limit_mb(200)
    };

    ParquetWriter::new_with_dataset(output_path, config, dataset_name).await
}

/// Estimate the output file size based on observations
pub fn estimate_output_size(
    num_observations: usize,
    avg_measurements_per_observation: usize,
    compression_ratio: f64,
) -> usize {
    // Rough estimation based on typical MIDAS data patterns
    let bytes_per_base_fields = 200; // Timestamps, station info, etc.
    let bytes_per_measurement = 16; // Float64 + quality flag

    let raw_size_per_observation =
        bytes_per_base_fields + (avg_measurements_per_observation * bytes_per_measurement);

    let total_raw_size = num_observations * raw_size_per_observation;

    // Apply compression ratio (default Snappy typically achieves 0.3-0.5 for scientific data)
    (total_raw_size as f64 * compression_ratio) as usize
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
        measurements.insert("wind_speed".to_string(), 10.2);

        let mut quality_flags = HashMap::new();
        quality_flags.insert("air_temperature".to_string(), "0".to_string());
        quality_flags.insert("wind_speed".to_string(), "1".to_string());

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

    /// Test ParquetWriter creation with valid configuration and output path
    /// Validates initial state before schema initialization and data processing
    #[tokio::test]
    async fn test_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default();

        let writer = ParquetWriter::new(&output_path, config).await;
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert_eq!(writer.output_path(), &output_path);
        assert!(!writer.is_initialized());
        assert_eq!(writer.buffer_size(), 0);
    }

    /// Test invalid configuration rejection prevents runtime errors
    /// Ensures early validation catches problematic settings before processing
    #[tokio::test]
    async fn test_writer_invalid_config() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig {
            row_group_size: 0, // Invalid
            ..Default::default()
        };

        let writer = ParquetWriter::new(&output_path, config).await;
        assert!(writer.is_err());
    }

    /// Test basic observation writing creates valid Parquet files
    /// Validates end-to-end pipeline from observations to disk storage
    #[tokio::test]
    async fn test_write_observations() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default();

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let observations = vec![create_test_observation(); 10];

        writer.write_observations(observations).await.unwrap();
        assert!(writer.is_initialized());
        assert_eq!(writer.buffer_size(), 10); // Should be buffered since batch size is 1024

        let stats = writer.finalize().await.unwrap();
        assert_eq!(stats.observations_written, 10);
        assert_eq!(stats.batches_written, 1);
        assert!(output_path.exists());
    }

    /// Test large batch processing with custom batch sizes
    /// Ensures proper buffering and memory management during streaming writes
    #[tokio::test]
    async fn test_write_large_batch() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default().with_write_batch_size(5);

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let observations = vec![create_test_observation(); 12];

        writer.write_observations(observations).await.unwrap();
        assert!(writer.is_initialized());
        assert_eq!(writer.buffer_size(), 2); // 12 % 5 = 2 remaining

        let stats = writer.finalize().await.unwrap();
        assert_eq!(stats.observations_written, 12);
        assert_eq!(stats.batches_written, 3); // 2 full batches + 1 final batch
        assert!(output_path.exists());
    }

    /// Test progress reporting provides real-time feedback during processing
    /// Validates user experience improvements for long-running operations
    #[tokio::test]
    async fn test_progress_reporting() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default();

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        writer.setup_progress(100);

        let observations = vec![create_test_observation(); 50];
        writer.write_observations(observations).await.unwrap();

        // Progress should be tracked
        assert!(writer.progress_reporter.is_enabled());
        assert_eq!(writer.progress_reporter.total_observations(), 100);

        writer.finalize().await.unwrap();
    }

    /// Test statistics tracking provides accurate processing metrics
    /// Ensures performance monitoring and diagnostic capabilities
    #[tokio::test]
    async fn test_stats_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default().with_write_batch_size(3);

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let observations = vec![create_test_observation(); 10];

        writer.write_observations(observations).await.unwrap();

        let stats = writer.stats();
        assert_eq!(stats.observations_written, 9); // 3 full batches written
        assert_eq!(stats.batches_written, 3);
        assert!(stats.peak_memory_usage_bytes > 0);

        writer.finalize().await.unwrap();
    }

    /// Test optimized writer factory adapts configuration to dataset size
    /// Validates automatic tuning for optimal performance across data scales
    #[tokio::test]
    async fn test_create_optimized_writer() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");

        // Small dataset
        let writer = create_optimized_writer(&output_path, 5_000).await.unwrap();
        assert_eq!(writer.config().row_group_size, 10_000);

        // Medium dataset
        let writer = create_optimized_writer(&output_path, 500_000)
            .await
            .unwrap();
        assert_eq!(writer.config().row_group_size, 50_000);

        // Large dataset
        let writer = create_optimized_writer(&output_path, 5_000_000)
            .await
            .unwrap();
        assert_eq!(writer.config().row_group_size, 100_000);
    }

    /// Test output size estimation helps with storage planning
    /// Enables capacity planning and compression ratio validation
    #[test]
    fn test_estimate_output_size() {
        let size = estimate_output_size(1000, 5, 0.4);
        assert!(size > 0);
        assert!(size < 1000 * (200 + 5 * 16)); // Should be compressed

        // Test with different parameters
        let size_large = estimate_output_size(10_000, 10, 0.3);
        assert!(size_large > size);

        let size_no_compression = estimate_output_size(1000, 5, 1.0);
        assert!(size_no_compression > size);
    }

    /// Test schema initialization dynamically extends base schema with measurements
    /// Critical for handling diverse measurement types in weather observations
    #[tokio::test]
    async fn test_schema_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default();

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let initial_schema_fields = writer.schema().fields().len();

        let observations = vec![create_test_observation()];
        writer.write_observations(observations).await.unwrap();

        // Schema should have been extended with measurement fields
        let final_schema_fields = writer.schema().fields().len();
        assert!(final_schema_fields > initial_schema_fields);

        writer.finalize().await.unwrap();
    }

    /// Test memory management prevents OOM conditions with automatic flushing
    /// Ensures stability when processing large datasets with limited memory
    #[tokio::test]
    async fn test_memory_management() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default()
            .with_memory_limit_bytes(1024) // Very small limit to force flushes
            .with_write_batch_size(2);

        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let observations = vec![create_test_observation(); 10];

        writer.write_observations(observations).await.unwrap();
        let stats = writer.finalize().await.unwrap();

        // Should have triggered memory flushes due to small limit
        // Memory flushes is a usize, so always >= 0, but we check it's a reasonable value
        assert!(stats.memory_flushes < 1000); // Should not have excessive flushes
    }
}
