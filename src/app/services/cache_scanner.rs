//! Cache scanner for discovering MIDAS CSV files in midas_fetcher cache structure
//!
//! This module provides functionality to scan the midas_fetcher cache directory
//! and discover available BADC-CSV files for processing validation. It implements
//! intelligent sampling strategies to ensure representative coverage across datasets,
//! time periods, and geographic regions.

// Remove unused import
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};
use walkdir::WalkDir;

/// Information about a discovered CSV file in the cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheFileInfo {
    /// Full path to the CSV file
    pub path: PathBuf,
    /// Dataset type (e.g., "uk-daily-temperature-obs")
    pub dataset: String,
    /// QC version (e.g., "qc-version-1")
    pub qc_version: String,
    /// Geographic region (e.g., "devon", "yorkshire")
    pub region: String,
    /// Station or location identifier
    pub location: String,
    /// Year extracted from filename
    pub year: Option<u32>,
    /// File size in bytes
    pub size_bytes: u64,
    /// Whether this is a capability file (station metadata)
    pub is_capability: bool,
}

impl CacheFileInfo {
    /// Get a unique identifier for this file
    pub fn unique_id(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.dataset, self.qc_version, self.region, self.location
        )
    }

    /// Get the base filename without path
    pub fn filename(&self) -> String {
        self.path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned()
    }

    /// Check if this file represents observation data (not capability)
    pub fn is_observation_data(&self) -> bool {
        !self.is_capability
    }

    /// Get estimated record count based on file size (rough approximation)
    pub fn estimated_record_count(&self) -> usize {
        // Rough estimate: 200 bytes per record average
        (self.size_bytes / 200) as usize
    }
}

/// Statistics about discovered cache files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total number of files discovered
    pub total_files: usize,
    /// Number of observation data files (non-capability)
    pub observation_files: usize,
    /// Number of capability files (station metadata)
    pub capability_files: usize,
    /// Files by dataset type
    pub files_by_dataset: HashMap<String, usize>,
    /// Files by QC version
    pub files_by_qc_version: HashMap<String, usize>,
    /// Files by year
    pub files_by_year: HashMap<u32, usize>,
    /// Total size of all files in bytes
    pub total_size_bytes: u64,
    /// Estimated total record count
    pub estimated_total_records: usize,
}

/// Configuration for cache scanning
#[derive(Debug, Clone)]
pub struct CacheScanConfig {
    /// Maximum number of files to include in sample
    pub max_files: Option<usize>,
    /// Specific datasets to include (empty = all)
    pub datasets: Vec<String>,
    /// Minimum file size in bytes to include
    pub min_file_size: u64,
    /// Maximum file size in bytes to include
    pub max_file_size: u64,
    /// Whether to include capability files
    pub include_capability: bool,
    /// Years to include (empty = all)
    pub years: Vec<u32>,
}

impl Default for CacheScanConfig {
    fn default() -> Self {
        Self {
            max_files: Some(1000),
            datasets: Vec::new(),
            min_file_size: 1024,              // 1KB minimum
            max_file_size: 100 * 1024 * 1024, // 100MB maximum
            include_capability: true,
            years: Vec::new(),
        }
    }
}

/// Cache scanner for discovering MIDAS CSV files
pub struct CacheScanner {
    config: CacheScanConfig,
}

impl CacheScanner {
    /// Create a new cache scanner with default configuration
    pub fn new() -> Self {
        Self {
            config: CacheScanConfig::default(),
        }
    }

    /// Create a new cache scanner with custom configuration
    pub fn with_config(config: CacheScanConfig) -> Self {
        Self { config }
    }

    /// Scan the cache directory and discover CSV files
    pub fn scan_cache(&self, cache_path: &Path) -> Result<Vec<CacheFileInfo>> {
        info!("Starting cache scan at: {}", cache_path.display());

        if !cache_path.exists() {
            return Err(crate::Error::io(
                format!("Cache directory does not exist: {}", cache_path.display()),
                std::io::Error::new(std::io::ErrorKind::NotFound, "Directory not found"),
            ));
        }

        let mut files = Vec::new();
        let mut total_scanned = 0;

        // Walk through the cache directory structure
        for entry in WalkDir::new(cache_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            total_scanned += 1;
            if total_scanned % 1000 == 0 {
                debug!("Scanned {} entries", total_scanned);
            }

            let path = entry.path();

            // Only process CSV files
            if !path.is_file() || path.extension().is_none_or(|ext| ext != "csv") {
                continue;
            }

            // Try to parse the file information from the path
            if let Some(file_info) = self.parse_file_path(path)? {
                // Apply filtering based on configuration
                if self.should_include_file(&file_info) {
                    files.push(file_info);
                }
            }
        }

        info!("Discovered {} CSV files after filtering", files.len());

        // Apply sampling if max_files is set
        if let Some(max_files) = self.config.max_files {
            if files.len() > max_files {
                files = self.sample_files(files, max_files)?;
                info!("Sampled {} files from {} total", files.len(), max_files);
            }
        }

        Ok(files)
    }

    /// Parse file information from a cache file path
    fn parse_file_path(&self, path: &Path) -> Result<Option<CacheFileInfo>> {
        // Get file metadata
        let metadata = std::fs::metadata(path)?;
        let size_bytes = metadata.len();

        // Parse the path components
        let path_str = path.to_string_lossy();
        let components: Vec<&str> = path_str.split('/').collect();

        // Find dataset and qc-version in path
        let mut dataset = None;
        let mut qc_version = None;
        let mut region = None;
        let mut location = None;

        for (i, component) in components.iter().enumerate() {
            // Look for dataset patterns
            if component.ends_with("-obs") || component.ends_with("-weather") {
                dataset = Some(component.to_string());

                // Check if next component is qc-version, capability, or other variations
                if i + 1 < components.len() {
                    let next_component = components[i + 1];
                    if next_component.starts_with("qc-version") {
                        qc_version = Some(next_component.to_string());

                        // Following components are region and location
                        if i + 2 < components.len() {
                            region = Some(components[i + 2].to_string());
                        }
                        if i + 3 < components.len() {
                            location = Some(components[i + 3].to_string());
                        }
                    } else if next_component == "capability" {
                        // For capability files, use a default qc-version
                        qc_version = Some("capability".to_string());

                        // Following components are region and location
                        if i + 2 < components.len() {
                            region = Some(components[i + 2].to_string());
                        }
                        if i + 3 < components.len() {
                            location = Some(components[i + 3].to_string());
                        }
                    } else if next_component == "no-quality" {
                        // For no-quality files, use a default qc-version
                        qc_version = Some("no-quality".to_string());

                        // Following components might be different
                        if i + 2 < components.len() {
                            region = Some(components[i + 2].to_string());
                        }
                        if i + 3 < components.len() {
                            location = Some(components[i + 3].to_string());
                        }
                    } else {
                        // For other structures, try to find a reasonable default
                        qc_version = Some("unknown".to_string());
                        region = Some(next_component.to_string());

                        if i + 2 < components.len() {
                            location = Some(components[i + 2].to_string());
                        }
                    }
                }
                break;
            }
        }

        // Must have at least dataset and qc_version
        let dataset = dataset.ok_or_else(|| {
            crate::Error::data_validation(format!(
                "Could not determine dataset from path: {}",
                path_str
            ))
        })?;
        let qc_version = qc_version.ok_or_else(|| {
            crate::Error::data_validation(format!(
                "Could not determine QC version from path: {}",
                path_str
            ))
        })?;

        // Extract year from filename if possible
        let filename = path.file_name().unwrap_or_default().to_string_lossy();
        let year = self.extract_year_from_filename(&filename);

        // Check if this is a capability file
        let is_capability = path_str.contains("/capability/");

        Ok(Some(CacheFileInfo {
            path: path.to_path_buf(),
            dataset,
            qc_version,
            region: region.unwrap_or_else(|| "unknown".to_string()),
            location: location.unwrap_or_else(|| "unknown".to_string()),
            year,
            size_bytes,
            is_capability,
        }))
    }

    /// Extract year from filename (e.g., "2023.csv" -> Some(2023))
    fn extract_year_from_filename(&self, filename: &str) -> Option<u32> {
        // Look for 4-digit year pattern
        for part in filename.split(&['.', '_', '-']) {
            if let Ok(year) = part.parse::<u32>() {
                if (1900..=2100).contains(&year) {
                    return Some(year);
                }
            }
        }
        None
    }

    /// Check if a file should be included based on configuration
    fn should_include_file(&self, file_info: &CacheFileInfo) -> bool {
        // Check file size limits
        if file_info.size_bytes < self.config.min_file_size
            || file_info.size_bytes > self.config.max_file_size
        {
            return false;
        }

        // Check if capability files are included
        if file_info.is_capability && !self.config.include_capability {
            return false;
        }

        // Check dataset filter
        if !self.config.datasets.is_empty() && !self.config.datasets.contains(&file_info.dataset) {
            return false;
        }

        // Check year filter
        if !self.config.years.is_empty() {
            if let Some(year) = file_info.year {
                if !self.config.years.contains(&year) {
                    return false;
                }
            } else if !self.config.years.is_empty() {
                // If years are specified but file has no year, exclude it
                return false;
            }
        }

        true
    }

    /// Sample files to ensure representative coverage
    fn sample_files(
        &self,
        mut files: Vec<CacheFileInfo>,
        max_files: usize,
    ) -> Result<Vec<CacheFileInfo>> {
        // Sort files by size to ensure we get a good distribution
        files.sort_by_key(|f| f.size_bytes);

        // Use stratified sampling to ensure coverage across:
        // 1. Different datasets
        // 2. Different years
        // 3. Different file sizes

        let mut sampled = Vec::new();
        let mut files_by_dataset: HashMap<String, Vec<CacheFileInfo>> = HashMap::new();

        // Group files by dataset
        for file in files {
            files_by_dataset
                .entry(file.dataset.clone())
                .or_default()
                .push(file);
        }

        // Calculate how many files to take from each dataset
        let datasets_count = files_by_dataset.len();
        let base_per_dataset = max_files / datasets_count;
        let remainder = max_files % datasets_count;

        let mut remaining_slots = max_files;

        for (_dataset, dataset_files) in files_by_dataset {
            let mut slots_for_dataset = base_per_dataset;
            if remainder > 0 && sampled.len() < remainder {
                slots_for_dataset += 1;
            }

            slots_for_dataset = slots_for_dataset
                .min(remaining_slots)
                .min(dataset_files.len());

            // Take evenly distributed sample from this dataset
            if slots_for_dataset > 0 {
                let step = dataset_files.len() / slots_for_dataset;
                for i in 0..slots_for_dataset {
                    let index = i * step;
                    if index < dataset_files.len() {
                        sampled.push(dataset_files[index].clone());
                    }
                }
            }

            remaining_slots -= slots_for_dataset;
            if remaining_slots == 0 {
                break;
            }
        }

        debug!(
            "Sampled {} files across {} datasets",
            sampled.len(),
            datasets_count
        );
        Ok(sampled)
    }

    /// Generate statistics about discovered files
    pub fn generate_stats(&self, files: &[CacheFileInfo]) -> CacheStats {
        let mut stats = CacheStats {
            total_files: files.len(),
            observation_files: 0,
            capability_files: 0,
            files_by_dataset: HashMap::new(),
            files_by_qc_version: HashMap::new(),
            files_by_year: HashMap::new(),
            total_size_bytes: 0,
            estimated_total_records: 0,
        };

        for file in files {
            // Count file types
            if file.is_capability {
                stats.capability_files += 1;
            } else {
                stats.observation_files += 1;
            }

            // Count by dataset
            *stats
                .files_by_dataset
                .entry(file.dataset.clone())
                .or_insert(0) += 1;

            // Count by QC version
            *stats
                .files_by_qc_version
                .entry(file.qc_version.clone())
                .or_insert(0) += 1;

            // Count by year
            if let Some(year) = file.year {
                *stats.files_by_year.entry(year).or_insert(0) += 1;
            }

            // Accumulate size and estimated records
            stats.total_size_bytes += file.size_bytes;
            stats.estimated_total_records += file.estimated_record_count();
        }

        stats
    }
}

impl Default for CacheScanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_extract_year_from_filename() {
        let scanner = CacheScanner::new();

        assert_eq!(scanner.extract_year_from_filename("2023.csv"), Some(2023));
        assert_eq!(
            scanner.extract_year_from_filename("data_2022.csv"),
            Some(2022)
        );
        assert_eq!(
            scanner.extract_year_from_filename("temp-2021-data.csv"),
            Some(2021)
        );
        assert_eq!(scanner.extract_year_from_filename("no_year.csv"), None);
        assert_eq!(scanner.extract_year_from_filename("99.csv"), None); // Too short
        assert_eq!(scanner.extract_year_from_filename("9999.csv"), None); // Too far in future
    }

    #[test]
    fn test_cache_scan_config_filtering() {
        let scanner = CacheScanner::new();

        let file_info = CacheFileInfo {
            path: PathBuf::from("test.csv"),
            dataset: "uk-daily-temperature-obs".to_string(),
            qc_version: "qc-version-1".to_string(),
            region: "devon".to_string(),
            location: "exeter".to_string(),
            year: Some(2023),
            size_bytes: 10000,
            is_capability: false,
        };

        // Default config should include the file
        assert!(scanner.should_include_file(&file_info));

        // Test size filtering
        let config = CacheScanConfig {
            min_file_size: 20000,
            ..Default::default()
        };
        let scanner = CacheScanner::with_config(config);
        assert!(!scanner.should_include_file(&file_info));

        // Test dataset filtering
        let config = CacheScanConfig {
            datasets: vec!["uk-daily-rainfall-obs".to_string()],
            ..Default::default()
        };
        let scanner = CacheScanner::with_config(config);
        assert!(!scanner.should_include_file(&file_info));

        // Test year filtering
        let config = CacheScanConfig {
            years: vec![2022, 2024],
            ..Default::default()
        };
        let scanner = CacheScanner::with_config(config);
        assert!(!scanner.should_include_file(&file_info));
    }

    #[test]
    fn test_parse_file_path() {
        let scanner = CacheScanner::new();

        // Test typical cache path
        let _path = Path::new("/cache/uk-daily-temperature-obs/qc-version-1/devon/exeter/2023.csv");

        // Create a temporary file for testing
        let temp_dir = TempDir::new().unwrap();
        // Create a path that matches the expected BADC cache structure
        let test_file = temp_dir
            .path()
            .join("uk-daily-temperature-obs/qc-version-1/devon/exeter/2023.csv");
        fs::create_dir_all(test_file.parent().unwrap()).unwrap();
        fs::write(&test_file, "test content").unwrap();

        let result = scanner.parse_file_path(&test_file);
        assert!(result.is_ok());

        let info = result.unwrap();
        assert!(info.is_some());
        let file_info = info.unwrap();
        assert_eq!(file_info.dataset, "uk-daily-temperature-obs");
        assert_eq!(file_info.qc_version, "qc-version-1");
        assert_eq!(file_info.region, "devon");
        assert_eq!(file_info.location, "exeter");
        assert_eq!(file_info.year, Some(2023));
    }
}
