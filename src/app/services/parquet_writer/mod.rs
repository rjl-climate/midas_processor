//! Optimized Parquet writer for MIDAS weather data
//!
//! This module provides high-performance Parquet file generation optimized for time series
//! analysis of UK Met Office MIDAS weather observations. The implementation follows best
//! practices for scientific data storage with pandas compatibility and query optimization.
//!
//! # Key Features
//!
//! - **Pandas compatibility**: Uses TimestampNanosecond and proper metadata for seamless pandas integration
//! - **Memory management**: Configurable batch sizes with memory monitoring to prevent OOM conditions
//! - **Dictionary encoding**: Optimized categorical data storage for station IDs and quality flags
//! - **Progress reporting**: Real-time progress tracking with detailed statistics
//! - **Error recovery**: Graceful handling of malformed data with detailed error reporting
//!
//! # Architecture
//!
//! The module is organized into logical components:
//!
//! - [`config`] - Configuration structures and statistics tracking
//! - [`schema`] - Arrow schema generation and management
//! - [`writer`] - Core ParquetWriter implementation
//! - [`conversion`] - Data conversion from MIDAS observations to Arrow format
//! - [`progress`] - Progress reporting and user feedback
//! - [`utils`] - Convenience functions and high-level workflows
//!
//! # Basic Usage
//!
//! ```rust
//! use std::path::Path;
//! use midas_processor::app::services::parquet_writer::{ParquetWriter, WriterConfig};
//! use midas_processor::app::models::Observation;
//!
//! # async fn example(observations: Vec<Observation>) -> midas_processor::Result<()> {
//! let config = WriterConfig::default();
//! let output_path = Path::new("weather_data.parquet");
//!
//! let mut writer = ParquetWriter::new(output_path, config).await?;
//! writer.setup_progress(observations.len());
//! writer.write_observations(observations).await?;
//! let stats = writer.finalize().await?;
//!
//! println!("Wrote {} observations", stats.observations_written);
//! # Ok(())
//! # }
//! ```
//!
//! # High-Level Workflow
//!
//! For simple dataset processing, use the convenience functions:
//!
//! ```rust
//! use midas_processor::app::services::parquet_writer::{utils, WriterConfig};
//!
//! # async fn example() -> midas_processor::Result<()> {
//! let observations = vec![]; // Your MIDAS observations
//! let output_dir = std::path::Path::new("./output");
//! let config = WriterConfig::default();
//!
//! let stats = utils::write_dataset_to_parquet(
//!     "uk-daily-temperature-obs",
//!     observations,
//!     output_dir,
//!     config
//! ).await?;
//!
//! println!("Success rate: {:.1}%", stats.success_rate());
//! # Ok(())
//! # }
//! ```
//!
//! # Configuration
//!
//! The writer behavior can be customized through [`WriterConfig`]:
//!
//! ```rust
//! use midas_processor::app::services::parquet_writer::{WriterConfig, ParquetWriter};
//! use parquet::basic::Compression;
//!
//! # async fn example() -> midas_processor::Result<()> {
//! let config = WriterConfig::new()
//!     .with_row_group_size(50_000)
//!     .with_compression(Compression::GZIP(Default::default()))
//!     .with_memory_limit_mb(200)
//!     .with_write_batch_size(2_000);
//!
//! let writer = ParquetWriter::new(
//!     std::path::Path::new("output.parquet"),
//!     config
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Performance Considerations
//!
//! - **Row group size**: Larger row groups improve compression but increase memory usage
//! - **Batch size**: Affects memory usage and I/O efficiency
//! - **Memory limit**: Prevents OOM by forcing periodic flushes
//! - **Compression**: Snappy is fastest, GZIP provides better compression
//!
//! # Error Handling
//!
//! All operations return [`Result`] types with detailed error information:
//!
//! - [`Error::ParquetWriting`] - Issues with Parquet file generation
//! - [`Error::DataValidation`] - Problems with input data
//! - [`Error::Configuration`] - Invalid configuration settings
//! - [`Error::Io`] - File system operations
//!
//! # Thread Safety
//!
//! The [`ParquetWriter`] is not thread-safe and should be used from a single task.
//! For parallel processing, create separate writers for each dataset or use the
//! utility functions which handle concurrency appropriately.

pub mod config;
pub mod conversion;
pub mod progress;
pub mod schema;
pub mod utils;
pub mod writer;

// Re-export main types for convenient access
pub use config::{WriterConfig, WritingStats};
pub use conversion::{ConversionStats, observations_to_record_batch};
pub use progress::{BatchProgress, ProgressReporter};
pub use schema::{SchemaStats, create_weather_schema, extend_schema_with_measurements};
pub use utils::{
    ParquetFileInfo, WritingSummary, create_dataset_writer, write_dataset_to_parquet,
    write_multiple_datasets_to_parquet,
};
pub use writer::{ParquetWriter, create_optimized_writer, estimate_output_size};

// Note: utils functions already exported above in line 129

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

    /// Test complete end-to-end workflow from observations to Parquet file
    /// Validates integration of all components: writer, schema, conversion, progress
    #[tokio::test]
    async fn test_complete_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig::default();

        // Test complete workflow using main exports
        let mut writer = ParquetWriter::new(&output_path, config).await.unwrap();
        let observations = vec![create_test_observation(); 10];

        writer.setup_progress(observations.len());
        writer.write_observations(observations).await.unwrap();
        let stats = writer.finalize().await.unwrap();

        assert_eq!(stats.observations_written, 10);
        assert!(output_path.exists());
    }

    /// Test high-level convenience functions simplify common use cases
    /// Ensures easy-to-use API for standard dataset processing workflows
    #[tokio::test]
    async fn test_convenience_functions() {
        let temp_dir = TempDir::new().unwrap();
        let config = WriterConfig::default();
        let observations = vec![create_test_observation(); 5];

        // Test high-level convenience function
        let stats = write_dataset_to_parquet("test-dataset", observations, temp_dir.path(), config)
            .await
            .unwrap();

        assert_eq!(stats.observations_written, 5);

        let output_file = temp_dir
            .path()
            .join("parquet_files")
            .join("test-dataset.parquet");
        assert!(output_file.exists());
    }

    /// Test schema factory and conversion integration work together seamlessly
    /// Validates dynamic schema generation feeds properly into data conversion
    #[tokio::test]
    async fn test_schema_integration() {
        let base_schema = create_weather_schema();
        let observations = vec![create_test_observation()];

        let extended_schema =
            extend_schema_with_measurements(base_schema.clone(), &observations).unwrap();

        // Should have more fields than base schema
        assert!(extended_schema.fields().len() > base_schema.fields().len());

        // Test conversion with extended schema
        let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
        assert_eq!(record_batch.num_rows(), 1);
    }

    /// Test configuration builder provides fluent API for customization
    /// Ensures all configuration options work together without conflicts
    #[test]
    fn test_config_builder() {
        let config = WriterConfig::new()
            .with_row_group_size(25_000)
            .with_write_batch_size(500)
            .with_memory_limit_mb(150);

        assert_eq!(config.row_group_size, 25_000);
        assert_eq!(config.write_batch_size, 500);
        assert_eq!(config.memory_limit_mb(), 150);
        assert!(config.validate().is_ok());
    }

    /// Test optimized writer factory creates properly tuned configurations
    /// Validates automatic optimization based on dataset size characteristics
    #[tokio::test]
    async fn test_optimized_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("optimized.parquet");

        let writer = create_optimized_writer(&output_path, 100_000)
            .await
            .unwrap();

        // Should have appropriate settings for medium dataset
        assert_eq!(writer.config().row_group_size, 50_000);
        assert!(writer.config().memory_limit_mb() > 0);
    }

    /// Test output size estimation provides reasonable capacity planning
    /// Helps users plan storage requirements before processing large datasets
    #[test]
    fn test_size_estimation() {
        let estimated_size = estimate_output_size(10_000, 5, 0.4);
        assert!(estimated_size > 0);

        // Larger datasets should have larger estimated sizes
        let larger_size = estimate_output_size(100_000, 5, 0.4);
        assert!(larger_size > estimated_size);
    }

    /// Test error handling prevents invalid configurations from causing panics
    /// Ensures graceful failure with meaningful error messages
    #[tokio::test]
    async fn test_error_handling() {
        // Test with invalid configuration
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.parquet");
        let config = WriterConfig {
            row_group_size: 0, // Invalid
            ..Default::default()
        };

        let result = ParquetWriter::new(&output_path, config).await;
        assert!(result.is_err());
    }

    /// Test module exports make all essential types accessible from root
    /// Validates public API structure for external consumers
    #[test]
    fn test_module_exports() {
        // Test that all main types are accessible
        let _config = WriterConfig::default();
        let _stats = WritingStats::default();
        let _schema = create_weather_schema();
        let _progress = ProgressReporter::new();

        // Test that convenience functions are available
        let temp_dir = TempDir::new().unwrap();
        let _path = utils::create_dataset_output_path("test", temp_dir.path());
        let _sanitized = utils::sanitize_filename("test-name");
    }
}
