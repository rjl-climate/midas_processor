//! Station registry service for O(1) station metadata lookups
//!
//! This module provides a high-performance station metadata lookup service that loads
//! station information from MIDAS capability files and provides O(1) access by station ID.

use crate::app::models::Station;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

pub mod loader;
pub mod metadata;
pub mod parser;
pub mod query;

#[cfg(test)]
pub mod tests;

// Re-export key types for convenience
pub use metadata::{LoadStats, RegistryMetadata};

/// Station registry providing O(1) station metadata lookups
///
/// The registry loads station metadata from MIDAS capability files and indexes
/// them by source ID for fast lookups. It handles the MIDAS cache structure
/// and filters records to only include definitive station information.
#[derive(Debug, Clone)]
pub struct StationRegistry {
    /// Station metadata indexed by src_id for O(1) lookups
    pub(crate) stations: HashMap<i32, Station>,

    /// Path to the MIDAS cache root directory
    pub(crate) cache_path: PathBuf,

    /// Datasets that were loaded into this registry
    pub(crate) loaded_datasets: Vec<String>,

    /// Timestamp when the registry was last loaded
    pub(crate) load_time: Instant,

    /// Number of capability files processed
    pub(crate) files_processed: usize,

    /// Number of station records found before filtering
    pub(crate) total_records_found: usize,
}

impl StationRegistry {
    /// Create a new empty station registry
    pub fn new(cache_path: PathBuf) -> Self {
        Self {
            stations: HashMap::new(),
            cache_path,
            loaded_datasets: Vec::new(),
            load_time: Instant::now(),
            files_processed: 0,
            total_records_found: 0,
        }
    }

    /// Get station metadata by source ID (O(1) lookup)
    pub fn get_station(&self, src_id: i32) -> Option<&Station> {
        self.stations.get(&src_id)
    }

    /// Check if a station exists in the registry
    pub fn contains_station(&self, src_id: i32) -> bool {
        self.stations.contains_key(&src_id)
    }

    /// Get the total number of stations in the registry
    pub fn station_count(&self) -> usize {
        self.stations.len()
    }

    /// Get registry metadata
    pub fn metadata(&self) -> RegistryMetadata {
        RegistryMetadata {
            cache_path: self.cache_path.clone(),
            loaded_datasets: self.loaded_datasets.clone(),
            station_count: self.stations.len(),
            load_time: self.load_time,
            files_processed: self.files_processed,
            total_records_found: self.total_records_found,
        }
    }
}
