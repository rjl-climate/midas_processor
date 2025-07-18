//! Configuration management and validation.
//!
//! Provides configuration structures for processing parameters,
//! dataset-specific settings, and validation rules for different
//! MIDAS dataset types.

use crate::models::DatasetType;
use polars::prelude::ParquetCompression;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// Parquet-specific optimization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetOptimizationConfig {
    /// Enable station-based partitioning for optimal query performance
    pub partition_by_station: bool,

    /// Sort data by station_id then timestamp before writing
    pub sort_by_station_then_time: bool,

    /// Target row group size (rows per group) - will be dynamically calculated if 0
    pub target_row_group_size: usize,

    /// Compression algorithm selection
    pub compression_algorithm: CompressionAlgorithm,

    /// Enable column statistics for query pruning
    pub enable_statistics: bool,

    /// Number of parallel write threads
    pub parallel_write_threads: usize,

    /// Dictionary page size in bytes
    pub dictionary_page_size: usize,

    /// Data page size in bytes  
    pub data_page_size: usize,

    /// Row group sizing strategy
    pub row_group_strategy: RowGroupStrategy,

    /// Memory limit in MB for operations (0 = auto-detect)
    pub memory_limit_mb: usize,

    /// Enable external sorting for large datasets
    pub external_sort_enabled: bool,

    /// Optimize grouping for station-based queries
    pub optimize_for_stations: bool,

    /// Minimum row group size (rows)
    pub min_row_group_size: usize,

    /// Maximum memory to use for accumulating batches (MB)
    pub max_accumulation_memory_mb: usize,
}

/// Row group sizing strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowGroupStrategy {
    /// Fixed size row groups
    Fixed,
    /// Adaptive sizing based on system resources
    Adaptive,
    /// Station-aware sizing to align with station boundaries
    StationAware,
}

/// Supported compression algorithms for parquet files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// Snappy compression - good balance of speed and compression
    Snappy,
    /// ZSTD compression - better compression ratio, slower
    Zstd,
    /// LZ4 compression - fastest, lower compression ratio
    Lz4,
    /// No compression
    Uncompressed,
}

impl Default for ParquetOptimizationConfig {
    fn default() -> Self {
        Self {
            partition_by_station: true,
            sort_by_station_then_time: true,
            target_row_group_size: 0, // 0 = auto-calculate based on strategy
            compression_algorithm: CompressionAlgorithm::Snappy,
            enable_statistics: true,
            parallel_write_threads: 4,
            dictionary_page_size: 1024 * 1024, // 1MB
            data_page_size: 1024 * 1024,       // 1MB
            row_group_strategy: RowGroupStrategy::Adaptive,
            memory_limit_mb: 0, // 0 = auto-detect
            external_sort_enabled: true,
            optimize_for_stations: true,
            min_row_group_size: 250_000, // 250K minimum row group size
            max_accumulation_memory_mb: 2048, // 2GB max memory for accumulation
        }
    }
}

impl CompressionAlgorithm {
    /// Convert to polars ParquetCompression type
    pub fn to_polars_compression(&self) -> ParquetCompression {
        match self {
            CompressionAlgorithm::Snappy => ParquetCompression::Snappy,
            CompressionAlgorithm::Zstd => ParquetCompression::Zstd(None),
            CompressionAlgorithm::Lz4 => ParquetCompression::Lz4Raw,
            CompressionAlgorithm::Uncompressed => ParquetCompression::Uncompressed,
        }
    }
}

/// System profiling information for optimization
#[derive(Debug, Clone)]
pub struct SystemProfile {
    /// Number of CPU cores available
    pub cpu_cores: usize,
    /// Available memory in MB
    pub memory_mb: usize,
    /// Performance cores (for systems with efficiency cores)
    pub performance_cores: usize,
}

impl SystemProfile {
    /// Auto-detect system capabilities
    pub fn detect() -> Self {
        use sysinfo::System;

        let cpu_cores = num_cpus::get();
        let performance_cores = num_cpus::get_physical();

        let mut system = System::new();
        system.refresh_memory();
        let memory_mb = (system.total_memory() / 1024 / 1024) as usize;

        Self {
            cpu_cores,
            memory_mb,
            performance_cores,
        }
    }
}

impl ParquetOptimizationConfig {
    /// Calculate optimal row group size based on system resources and dataset characteristics
    pub fn calculate_optimal_row_group_size(
        &self,
        total_rows: usize,
        station_count: usize,
        system_profile: &SystemProfile,
    ) -> usize {
        if self.target_row_group_size > 0 {
            return self.target_row_group_size;
        }

        let optimal_size = match self.row_group_strategy {
            RowGroupStrategy::Fixed => 500_000, // Default fixed size
            RowGroupStrategy::Adaptive => {
                self.calculate_adaptive_row_group_size(total_rows, system_profile)
            }
            RowGroupStrategy::StationAware => self.calculate_station_aware_row_group_size(
                total_rows,
                station_count,
                system_profile,
            ),
        };

        // Log the calculation details in debug mode only
        debug!(
            "Row group optimization: {} rows (strategy: {:?}, {} stations, {} cores, {}MB memory)",
            optimal_size,
            self.row_group_strategy,
            station_count,
            system_profile.performance_cores,
            system_profile.memory_mb
        );

        optimal_size
    }

    /// Calculate adaptive row group size based on system resources
    fn calculate_adaptive_row_group_size(
        &self,
        _total_rows: usize,
        system_profile: &SystemProfile,
    ) -> usize {
        // Base calculation: target 256MB row groups for optimal performance
        // Assuming ~100 bytes per row average
        let target_mb = 256;
        let estimated_bytes_per_row = 100;
        let target_rows_from_size = (target_mb * 1024 * 1024) / estimated_bytes_per_row;

        // Core-based calculation: ~100K rows per core for parallelism
        let target_rows_from_cores = system_profile.performance_cores * 100_000;

        // Memory-based calculation: don't exceed 1/8 of available memory
        let memory_limit_mb = if self.memory_limit_mb > 0 {
            self.memory_limit_mb
        } else {
            system_profile.memory_mb
        };
        let max_rows_from_memory = (memory_limit_mb / 8) * 1024 * 1024 / estimated_bytes_per_row;

        // Take the minimum of size-based and core-based, but respect memory limits
        let optimal_size = target_rows_from_size.min(target_rows_from_cores);
        let final_size = optimal_size.min(max_rows_from_memory);

        // Clamp to reasonable bounds
        final_size.clamp(100_000, 2_000_000)
    }

    /// Calculate station-aware row group size
    fn calculate_station_aware_row_group_size(
        &self,
        total_rows: usize,
        station_count: usize,
        system_profile: &SystemProfile,
    ) -> usize {
        if station_count == 0 {
            return self.calculate_adaptive_row_group_size(total_rows, system_profile);
        }

        // Estimate rows per station
        let avg_rows_per_station = total_rows / station_count;

        // Target: stations per row group based on available cores
        let target_stations_per_group = (station_count / system_profile.performance_cores).max(1);

        // Calculate rows per group based on station grouping
        let station_based_size = avg_rows_per_station * target_stations_per_group;

        // Ensure we don't exceed adaptive limits
        let adaptive_size = self.calculate_adaptive_row_group_size(total_rows, system_profile);

        // Balance between station alignment and optimal size
        station_based_size
            .min(adaptive_size)
            .clamp(250_000, 2_000_000)
    }
}

/// Global configuration for MIDAS processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MidasConfig {
    /// Number of worker threads for concurrent processing
    pub workers: usize,

    /// Sample size for column analysis
    pub sample_size: usize,

    /// Enable column elimination optimization
    pub enable_column_elimination: bool,

    /// Polars streaming chunk size
    pub chunk_size: usize,

    /// Files per batch for processing
    pub batch_size: usize,

    /// Force reprocessing of existing files
    pub force_reprocess: bool,

    /// Discovery only mode (analyze schema and exit without processing)
    pub discovery_only: bool,

    /// Skip schema validation (for testing)
    pub skip_schema_validation: bool,

    /// Enable GPU acceleration (requires CUDA-compatible GPU)
    pub enable_gpu: bool,

    /// Streaming chunk size for memory management
    pub streaming_chunk_size: usize,

    /// Maximum concurrent file processing
    pub max_concurrent_files: usize,

    /// Parquet optimization configuration
    pub parquet_optimization: ParquetOptimizationConfig,

    /// Dataset-specific configurations
    pub dataset_configs: HashMap<DatasetType, DatasetSpecificConfig>,
}

/// Configuration specific to each dataset type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSpecificConfig {
    /// Columns to always exclude (known to be empty)
    pub excluded_columns: Vec<String>,

    /// Expected file patterns for this dataset
    pub file_patterns: Vec<String>,

    /// Schema validation settings
    pub schema_validation: SchemaValidation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaValidation {
    /// Strict schema checking (fail on mismatches)
    pub strict: bool,

    /// Allow column count variations
    pub allow_column_variations: bool,

    /// Maximum acceptable column count difference
    pub max_column_diff: usize,
}

impl Default for MidasConfig {
    fn default() -> Self {
        let mut dataset_configs = HashMap::new();

        // Rain dataset configuration
        dataset_configs.insert(
            DatasetType::Rain,
            DatasetSpecificConfig {
                excluded_columns: vec![
                    "prcp_amt_j".to_string(),
                    "meto_stmp_time".to_string(),
                    "midas_stmp_etime".to_string(),
                ],
                file_patterns: vec!["*rain*.csv".to_string()],
                schema_validation: SchemaValidation {
                    strict: false,
                    allow_column_variations: true,
                    max_column_diff: 2,
                },
            },
        );

        // Temperature dataset configuration
        dataset_configs.insert(
            DatasetType::Temperature,
            DatasetSpecificConfig {
                excluded_columns: vec![
                    "min_grss_temp".to_string(),
                    "min_conc_temp".to_string(),
                    "min_grss_temp_q".to_string(),
                    "min_conc_temp_q".to_string(),
                    "min_grss_temp_j".to_string(),
                    "min_conc_temp_j".to_string(),
                ],
                file_patterns: vec!["*temperature*.csv".to_string()],
                schema_validation: SchemaValidation {
                    strict: false,
                    allow_column_variations: true,
                    max_column_diff: 3,
                },
            },
        );

        // Wind dataset configuration
        dataset_configs.insert(
            DatasetType::Wind,
            DatasetSpecificConfig {
                excluded_columns: vec![
                    "mean_wind_dir".to_string(),
                    "max_gust_dir".to_string(),
                    "max_gust_ctime".to_string(),
                ],
                file_patterns: vec!["*wind*.csv".to_string()],
                schema_validation: SchemaValidation {
                    strict: false,
                    allow_column_variations: true,
                    max_column_diff: 4,
                },
            },
        );

        // Radiation dataset configuration
        dataset_configs.insert(
            DatasetType::Radiation,
            DatasetSpecificConfig {
                excluded_columns: vec![],
                file_patterns: vec!["*radiation*.csv".to_string()],
                schema_validation: SchemaValidation {
                    strict: false,
                    allow_column_variations: true,
                    max_column_diff: 2,
                },
            },
        );

        Self {
            workers: 4,
            sample_size: 20,
            enable_column_elimination: true,
            chunk_size: 8192,
            batch_size: 80, // Increase for better throughput with streaming
            force_reprocess: false,
            discovery_only: false,
            skip_schema_validation: false,
            enable_gpu: false,
            streaming_chunk_size: 32768,
            max_concurrent_files: 8, // Controlled concurrency
            parquet_optimization: ParquetOptimizationConfig::default(),
            dataset_configs,
        }
    }
}

impl MidasConfig {
    /// Create configuration with custom worker count
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Create configuration with custom sample size
    pub fn with_sample_size(mut self, sample_size: usize) -> Self {
        self.sample_size = sample_size;
        self
    }

    /// Create configuration with custom batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Disable column elimination optimization
    pub fn without_column_elimination(mut self) -> Self {
        self.enable_column_elimination = false;
        self
    }

    /// Enable force reprocessing
    pub fn with_force_reprocess(mut self) -> Self {
        self.force_reprocess = true;
        self
    }

    /// Enable discovery only mode
    pub fn with_discovery_only(mut self) -> Self {
        self.discovery_only = true;
        self
    }

    /// Enable GPU acceleration
    pub fn with_gpu(mut self) -> Self {
        self.enable_gpu = true;
        self
    }

    /// Configure for minimal memory usage (reduces streaming chunk size)
    pub fn with_minimal_memory(mut self) -> Self {
        self.streaming_chunk_size = 8192; // Smaller chunk size
        self
    }

    /// Set streaming chunk size
    pub fn with_streaming_chunk_size(mut self, chunk_size: usize) -> Self {
        self.streaming_chunk_size = chunk_size;
        self
    }

    /// Set maximum concurrent files
    pub fn with_max_concurrent_files(mut self, max_files: usize) -> Self {
        self.max_concurrent_files = max_files;
        self
    }

    /// Configure parquet optimization settings
    pub fn with_parquet_optimization(mut self, config: ParquetOptimizationConfig) -> Self {
        self.parquet_optimization = config;
        self
    }

    /// Get dataset-specific configuration
    pub fn get_dataset_config(&self, dataset_type: &DatasetType) -> Option<&DatasetSpecificConfig> {
        self.dataset_configs.get(dataset_type)
    }
}
