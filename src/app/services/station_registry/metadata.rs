//! Station registry metadata and statistics tracking
//!
//! This module defines the data structures and functionality for tracking
//! station registry loading statistics and metadata.

use std::path::PathBuf;
use std::time::Instant;

/// Statistics about the station registry loading process
#[derive(Debug, Clone)]
pub struct LoadStats {
    /// Number of datasets processed
    pub datasets_processed: usize,

    /// Number of capability files processed
    pub files_processed: usize,

    /// Total number of station records found
    pub total_records_found: usize,

    /// Number of stations loaded after filtering
    pub stations_loaded: usize,

    /// Number of records filtered out (non-definitive)
    pub records_filtered: usize,

    /// Time taken to load the registry
    pub load_duration: std::time::Duration,

    /// Any errors encountered during loading
    pub errors: Vec<String>,
}

impl LoadStats {
    /// Create new empty load statistics
    pub fn new() -> Self {
        Self {
            datasets_processed: 0,
            files_processed: 0,
            total_records_found: 0,
            stations_loaded: 0,
            records_filtered: 0,
            load_duration: std::time::Duration::ZERO,
            errors: Vec::new(),
        }
    }

    /// Calculate the filtering rate as a percentage
    pub fn filter_rate(&self) -> f64 {
        if self.total_records_found == 0 {
            0.0
        } else {
            (self.records_filtered as f64 / self.total_records_found as f64) * 100.0
        }
    }

    /// Calculate the loading rate in stations per second
    pub fn loading_rate(&self) -> f64 {
        if self.load_duration.is_zero() {
            0.0
        } else {
            self.stations_loaded as f64 / self.load_duration.as_secs_f64()
        }
    }

    /// Check if any errors occurred during loading
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get a summary string of the loading process
    pub fn summary(&self) -> String {
        format!(
            "Processed {} datasets, {} files, loaded {} stations ({:.1}% filtered) in {:.2}s",
            self.datasets_processed,
            self.files_processed,
            self.stations_loaded,
            self.filter_rate(),
            self.load_duration.as_secs_f64()
        )
    }
}

impl Default for LoadStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata about the station registry
#[derive(Debug, Clone)]
pub struct RegistryMetadata {
    /// Path to the MIDAS cache
    pub cache_path: PathBuf,

    /// Datasets that were loaded
    pub loaded_datasets: Vec<String>,

    /// Total number of stations in registry
    pub station_count: usize,

    /// When the registry was loaded
    pub load_time: Instant,

    /// Number of files processed during loading
    pub files_processed: usize,

    /// Total number of records found before filtering
    pub total_records_found: usize,
}

impl RegistryMetadata {
    /// Get the age of the registry since loading
    pub fn age(&self) -> std::time::Duration {
        self.load_time.elapsed()
    }

    /// Check if the registry was loaded from multiple datasets
    pub fn is_multi_dataset(&self) -> bool {
        self.loaded_datasets.len() > 1
    }

    /// Get a summary string of the registry
    pub fn summary(&self) -> String {
        format!(
            "Registry with {} stations from {} datasets (age: {:.1}s)",
            self.station_count,
            self.loaded_datasets.len(),
            self.age().as_secs_f64()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_load_stats_new() {
        let stats = LoadStats::new();
        assert_eq!(stats.datasets_processed, 0);
        assert_eq!(stats.stations_loaded, 0);
        assert!(!stats.has_errors());
        assert_eq!(stats.filter_rate(), 0.0);
        assert_eq!(stats.loading_rate(), 0.0);
    }

    #[test]
    fn test_load_stats_calculations() {
        let mut stats = LoadStats::new();
        stats.total_records_found = 1000;
        stats.stations_loaded = 800;
        stats.records_filtered = 200;
        stats.load_duration = Duration::from_secs(4);

        assert_eq!(stats.filter_rate(), 20.0);
        assert_eq!(stats.loading_rate(), 200.0);
        assert!(!stats.has_errors());

        stats.errors.push("test error".to_string());
        assert!(stats.has_errors());
    }

    #[test]
    fn test_load_stats_summary() {
        let mut stats = LoadStats::new();
        stats.datasets_processed = 2;
        stats.files_processed = 10;
        stats.stations_loaded = 800;
        stats.total_records_found = 1000;
        stats.records_filtered = 200;
        stats.load_duration = Duration::from_millis(1500);

        let summary = stats.summary();
        assert!(summary.contains("2 datasets"));
        assert!(summary.contains("10 files"));
        assert!(summary.contains("800 stations"));
        assert!(summary.contains("20.0% filtered"));
        assert!(summary.contains("1.50s"));
    }

    #[test]
    fn test_registry_metadata() {
        let metadata = RegistryMetadata {
            cache_path: PathBuf::from("/test/cache"),
            loaded_datasets: vec!["dataset1".to_string(), "dataset2".to_string()],
            station_count: 500,
            load_time: Instant::now(),
            files_processed: 10,
            total_records_found: 600,
        };

        assert!(metadata.is_multi_dataset());
        assert!(metadata.age().as_millis() < 100); // Should be very recent

        let summary = metadata.summary();
        assert!(summary.contains("500 stations"));
        assert!(summary.contains("2 datasets"));
    }

    #[test]
    fn test_registry_metadata_single_dataset() {
        let metadata = RegistryMetadata {
            cache_path: PathBuf::from("/test/cache"),
            loaded_datasets: vec!["dataset1".to_string()],
            station_count: 300,
            load_time: Instant::now(),
            files_processed: 5,
            total_records_found: 350,
        };

        assert!(!metadata.is_multi_dataset());
    }
}
