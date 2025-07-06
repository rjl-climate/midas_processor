//! Station registry service for O(1) station metadata lookups
//!
//! This module provides a high-performance station metadata lookup service that loads
//! station information from MIDAS capability files and provides O(1) access by station ID.

use crate::app::models::Station;
use crate::constants::{CAPABILITY_DIR_NAME, record_status};
use crate::{Error, Result};
use csv::StringRecord;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

/// Station registry providing O(1) station metadata lookups
///
/// The registry loads station metadata from MIDAS capability files and indexes
/// them by source ID for fast lookups. It handles the MIDAS cache structure
/// and filters records to only include definitive station information.
#[derive(Debug, Clone)]
pub struct StationRegistry {
    /// Station metadata indexed by src_id for O(1) lookups
    stations: HashMap<i32, Station>,

    /// Path to the MIDAS cache root directory
    cache_path: PathBuf,

    /// Datasets that were loaded into this registry
    loaded_datasets: Vec<String>,

    /// Timestamp when the registry was last loaded
    load_time: Instant,

    /// Number of capability files processed
    files_processed: usize,

    /// Number of station records found before filtering
    total_records_found: usize,
}

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

    /// Load station metadata from MIDAS cache capability files
    ///
    /// This method scans the specified datasets in the MIDAS cache and loads
    /// station metadata from capability files. It filters records to only
    /// include definitive station information (rec_st_ind = 9).
    ///
    /// # Arguments
    /// * `cache_path` - Root path to the MIDAS cache directory
    /// * `datasets` - List of dataset names to process
    /// * `show_progress` - Whether to display a progress bar
    ///
    /// # Returns
    /// * `Result<(StationRegistry, LoadStats)>` - Registry and loading statistics
    ///
    /// # Errors
    /// * Returns `Error::StationRegistry` if cache directory doesn't exist
    /// * Returns `Error::Io` for file system access issues
    /// * Returns `Error::CsvParsing` for malformed capability files
    pub async fn load_from_cache(
        cache_path: &Path,
        datasets: &[String],
        show_progress: bool,
    ) -> Result<(Self, LoadStats)> {
        info!(
            "Loading station registry from cache: {}",
            cache_path.display()
        );

        let start_time = Instant::now();
        let mut registry = Self::new(cache_path.to_path_buf());
        let mut stats = LoadStats {
            datasets_processed: 0,
            files_processed: 0,
            total_records_found: 0,
            stations_loaded: 0,
            records_filtered: 0,
            load_duration: std::time::Duration::ZERO,
            errors: Vec::new(),
        };

        // Validate cache path exists
        if !cache_path.exists() {
            return Err(Error::station_registry(format!(
                "Cache path does not exist: {}",
                cache_path.display()
            )));
        }

        // Discover all capability files across datasets
        let capability_files = Self::discover_capability_files(cache_path, datasets)?;

        info!(
            "Found {} capability files to process",
            capability_files.len()
        );

        // Set up progress reporting
        let progress_bar = if show_progress {
            let pb = ProgressBar::new(capability_files.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message("Loading capability files...");
            Some(pb)
        } else {
            None
        };

        // Process each capability file
        for (i, file_path) in capability_files.iter().enumerate() {
            if let Some(pb) = &progress_bar {
                pb.set_position(i as u64);
                pb.set_message(format!(
                    "Processing {}",
                    file_path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }

            match Self::load_capability_file(file_path).await {
                Ok((stations, total_records)) => {
                    stats.total_records_found += total_records;

                    // Filter and add stations to registry
                    for station in stations {
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            registry.stations.entry(station.src_id)
                        {
                            e.insert(station);
                            stats.stations_loaded += 1;
                        } else {
                            // Handle duplicate stations - log warning but keep existing
                            warn!(
                                "Duplicate station found: src_id = {}, name = '{}', keeping existing",
                                station.src_id, station.src_name
                            );
                        }
                    }

                    stats.files_processed += 1;
                }
                Err(e) => {
                    warn!(
                        "Failed to load capability file {}: {}",
                        file_path.display(),
                        e
                    );
                    stats.errors.push(format!("{}: {}", file_path.display(), e));
                }
            }
        }

        if let Some(pb) = &progress_bar {
            pb.finish_with_message("Station registry loading complete");
        }

        // Update registry metadata
        registry.loaded_datasets = datasets.to_vec();
        registry.load_time = start_time;
        registry.files_processed = stats.files_processed;
        registry.total_records_found = stats.total_records_found;

        // Finalize statistics
        stats.datasets_processed = datasets.len();
        stats.records_filtered = stats.total_records_found - stats.stations_loaded;
        stats.load_duration = start_time.elapsed();

        info!(
            "Station registry loaded: {} stations from {} files in {:.2}s",
            stats.stations_loaded,
            stats.files_processed,
            stats.load_duration.as_secs_f64()
        );

        Ok((registry, stats))
    }

    /// Discover all capability files in the specified datasets
    fn discover_capability_files(cache_path: &Path, datasets: &[String]) -> Result<Vec<PathBuf>> {
        let mut capability_files = Vec::new();

        for dataset in datasets {
            let dataset_path = cache_path.join(dataset).join(CAPABILITY_DIR_NAME);

            if !dataset_path.exists() {
                warn!(
                    "Capability directory not found for dataset '{}': {}",
                    dataset,
                    dataset_path.display()
                );
                continue;
            }

            debug!("Scanning capability directory: {}", dataset_path.display());

            // Recursively find all CSV files in the capability directory
            for entry in WalkDir::new(&dataset_path) {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file() && path.extension().is_some_and(|ext| ext == "csv") {
                            capability_files.push(path.to_path_buf());
                        }
                    }
                    Err(e) => {
                        warn!("Error walking directory {}: {}", dataset_path.display(), e);
                    }
                }
            }
        }

        debug!("Discovered {} capability files", capability_files.len());
        Ok(capability_files)
    }

    /// Load station metadata from a single capability file
    async fn load_capability_file(file_path: &Path) -> Result<(Vec<Station>, usize)> {
        debug!("Loading capability file: {}", file_path.display());

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false) // Don't automatically read headers
            .flexible(true) // Allow records with varying length
            .comment(Some(b'#')) // Skip comment lines starting with #
            .from_path(file_path)
            .map_err(|e| {
                Error::csv_parsing(
                    file_path.to_string_lossy().to_string(),
                    "Failed to open CSV file".to_string(),
                    Some(e),
                )
            })?;

        // Check if this is a BADC-CSV file by looking for the data section marker
        let mut in_data_section = false;
        let mut stations = Vec::new();
        let mut total_records = 0;
        let mut record = StringRecord::new();
        let mut headers: Option<StringRecord> = None;

        // Process each record
        while reader.read_record(&mut record).map_err(|e| {
            Error::csv_parsing(
                file_path.to_string_lossy().to_string(),
                "Failed to read CSV record".to_string(),
                Some(e),
            )
        })? {
            // Handle BADC-CSV format - look for data section marker
            if !in_data_section {
                if record.get(0).is_some_and(|val| val.trim() == "data") {
                    in_data_section = true;
                    continue;
                }
                // Skip header section
                continue;
            }

            // If we just entered the data section, the next record should be headers
            if headers.is_none() {
                headers = Some(record.clone());
                continue;
            }

            // Stop at end data marker if present
            if record.get(0).is_some_and(|val| val.trim() == "end data") {
                break;
            }

            total_records += 1;

            // Parse station record - only include definitive records (rec_st_ind = 9)
            if let Some(ref header_record) = headers {
                match Self::parse_station_record(&record, header_record) {
                    Ok(Some(station)) => {
                        stations.push(station);
                    }
                    Ok(None) => {
                        // Record filtered out (non-definitive)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse station record in {}: {}",
                            file_path.display(),
                            e
                        );
                    }
                }
            }
        }

        debug!(
            "Loaded {} stations from {} total records in {}",
            stations.len(),
            total_records,
            file_path.display()
        );

        Ok((stations, total_records))
    }

    /// Parse a station record from CSV fields, filtering by record status
    fn parse_station_record(
        record: &StringRecord,
        headers: &StringRecord,
    ) -> Result<Option<Station>> {
        // Create a map of column name to value for easier parsing
        let mut fields = HashMap::new();
        for (i, value) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                fields.insert(header.trim().to_lowercase(), value.trim());
            }
        }

        // Helper function to parse optional values, treating "NA" as None
        let parse_optional = |key: &str| -> Option<String> {
            fields
                .get(key)
                .filter(|&val| !val.is_empty() && *val != "NA")
                .map(|s| s.to_string())
        };

        // Helper function to parse required values
        let parse_required = |key: &str| -> Result<String> {
            parse_optional(key)
                .ok_or_else(|| Error::data_validation(format!("Missing required field: {}", key)))
        };

        // Check record status indicator - only process definitive records (rec_st_ind = 9)
        let rec_st_ind: i8 = parse_optional("rec_st_ind")
            .ok_or_else(|| Error::data_validation("Missing rec_st_ind field".to_string()))?
            .parse()
            .map_err(|_| Error::data_validation("Invalid rec_st_ind value".to_string()))?;

        if rec_st_ind != record_status::ORIGINAL {
            // Filter out non-definitive records
            return Ok(None);
        }

        // Parse station fields using the Station::new constructor for validation
        let src_id: i32 = parse_required("src_id")?
            .parse()
            .map_err(|_| Error::data_validation("Invalid src_id".to_string()))?;

        let src_name = parse_required("src_name")?;

        let high_prcn_lat: f64 = parse_required("high_prcn_lat")?
            .parse()
            .map_err(|_| Error::data_validation("Invalid high_prcn_lat".to_string()))?;

        let high_prcn_lon: f64 = parse_required("high_prcn_lon")?
            .parse()
            .map_err(|_| Error::data_validation("Invalid high_prcn_lon".to_string()))?;

        let east_grid_ref = parse_optional("east_grid_ref")
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(|_| Error::data_validation("Invalid east_grid_ref".to_string()))?;

        let north_grid_ref = parse_optional("north_grid_ref")
            .map(|s| s.parse::<i32>())
            .transpose()
            .map_err(|_| Error::data_validation("Invalid north_grid_ref".to_string()))?;

        let grid_ref_type = parse_optional("grid_ref_type");

        // Parse dates
        let src_bgn_date = parse_required("src_bgn_date")?
            .parse::<chrono::DateTime<chrono::Utc>>()
            .map_err(|_| Error::data_validation("Invalid src_bgn_date format".to_string()))?;

        let src_end_date = parse_required("src_end_date")?
            .parse::<chrono::DateTime<chrono::Utc>>()
            .map_err(|_| Error::data_validation("Invalid src_end_date format".to_string()))?;

        let authority = parse_required("authority")?;
        let historic_county = parse_required("historic_county")?;

        let height_meters: f32 = parse_required("height_meters")?
            .parse()
            .map_err(|_| Error::data_validation("Invalid height_meters".to_string()))?;

        // Create and validate station using the model's constructor
        let station = Station::new(
            src_id,
            src_name,
            high_prcn_lat,
            high_prcn_lon,
            east_grid_ref,
            north_grid_ref,
            grid_ref_type,
            src_bgn_date,
            src_end_date,
            authority,
            historic_county,
            height_meters,
        )?;

        Ok(Some(station))
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

    /// Get all station IDs in the registry
    pub fn station_ids(&self) -> Vec<i32> {
        self.stations.keys().copied().collect()
    }

    /// Get all stations in the registry
    pub fn stations(&self) -> Vec<&Station> {
        self.stations.values().collect()
    }

    /// Find stations by name pattern (case-insensitive)
    pub fn find_stations_by_name(&self, pattern: &str) -> Vec<&Station> {
        let pattern_lower = pattern.to_lowercase();
        self.stations
            .values()
            .filter(|station| station.src_name.to_lowercase().contains(&pattern_lower))
            .collect()
    }

    /// Find stations within a geographic bounding box
    pub fn find_stations_in_region(
        &self,
        min_lat: f64,
        max_lat: f64,
        min_lon: f64,
        max_lon: f64,
    ) -> Vec<&Station> {
        self.stations
            .values()
            .filter(|station| {
                station.high_prcn_lat >= min_lat
                    && station.high_prcn_lat <= max_lat
                    && station.high_prcn_lon >= min_lon
                    && station.high_prcn_lon <= max_lon
            })
            .collect()
    }

    /// Find stations active during a specific date range
    pub fn find_active_stations(
        &self,
        start_date: chrono::DateTime<chrono::Utc>,
        end_date: chrono::DateTime<chrono::Utc>,
    ) -> Vec<&Station> {
        self.stations
            .values()
            .filter(|station| {
                // Station is active if its operational period overlaps with the query period
                station.src_bgn_date <= end_date && station.src_end_date >= start_date
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::fs;
    use tempfile::TempDir;

    /// Create a test capability CSV file with BADC format
    fn create_test_capability_file(
        dir: &Path,
        filename: &str,
        stations: &[(i32, &str, i8)],
    ) -> std::io::Result<()> {
        let file_path = dir.join(filename);

        // Create BADC-CSV format with header section and data section
        let mut content = String::new();
        content.push_str("# BADC-CSV capability file\n");
        content.push_str("# Header information\n");
        content.push_str("data\n");
        content.push_str("src_id,src_name,high_prcn_lat,high_prcn_lon,east_grid_ref,north_grid_ref,grid_ref_type,src_bgn_date,src_end_date,authority,historic_county,height_meters,rec_st_ind\n");

        for (src_id, src_name, rec_st_ind) in stations {
            content.push_str(&format!(
                "{},{},51.4778,-0.4614,507500,176500,OSGB,2000-01-01T00:00:00Z,2050-12-31T23:59:59Z,Met Office,Greater London,25.0,{}\n",
                src_id, src_name, rec_st_ind
            ));
        }

        content.push_str("end data\n");

        fs::write(file_path, content)
    }

    /// Create a test MIDAS cache directory structure
    fn create_test_cache_structure(temp_dir: &TempDir) -> std::io::Result<PathBuf> {
        let cache_root = temp_dir.path();

        // Create dataset directories with capability subdirectories
        let dataset1 = cache_root
            .join("uk-daily-temperature-obs")
            .join("capability");
        fs::create_dir_all(&dataset1)?;

        let dataset2 = cache_root.join("uk-daily-rain-obs").join("capability");
        fs::create_dir_all(&dataset2)?;

        // Create test capability files
        create_test_capability_file(
            &dataset1,
            "station1.csv",
            &[
                (12345, "TEST_STATION_1", 9), // Definitive record
                (12346, "TEST_STATION_2", 1), // Non-definitive (filtered out)
                (12347, "TEST_STATION_3", 9), // Definitive record
            ],
        )?;

        create_test_capability_file(
            &dataset2,
            "station2.csv",
            &[
                (12348, "TEST_STATION_4", 9),     // Definitive record
                (12345, "TEST_STATION_1_DUP", 9), // Duplicate (should be ignored)
            ],
        )?;

        Ok(cache_root.to_path_buf())
    }

    #[tokio::test]
    async fn test_station_registry_new() {
        let cache_path = PathBuf::from("/test/cache");
        let registry = StationRegistry::new(cache_path.clone());

        assert_eq!(registry.cache_path, cache_path);
        assert_eq!(registry.station_count(), 0);
        assert!(registry.loaded_datasets.is_empty());
    }

    #[tokio::test]
    async fn test_load_from_cache_success() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let datasets = vec![
            "uk-daily-temperature-obs".to_string(),
            "uk-daily-rain-obs".to_string(),
        ];

        let (registry, stats) = StationRegistry::load_from_cache(&cache_path, &datasets, false)
            .await
            .unwrap();

        // Verify registry properties
        assert_eq!(registry.station_count(), 3); // 2 from file1, 1 from file2 (1 duplicate ignored)
        assert_eq!(registry.loaded_datasets, datasets);

        // Verify load statistics
        assert_eq!(stats.datasets_processed, 2);
        assert_eq!(stats.files_processed, 2);
        assert_eq!(stats.stations_loaded, 3);
        assert_eq!(stats.total_records_found, 5); // Total records before filtering
        assert_eq!(stats.records_filtered, 2); // 1 non-definitive + 1 duplicate
        assert!(stats.errors.is_empty());

        // Verify specific stations
        assert!(registry.contains_station(12345));
        assert!(registry.contains_station(12347));
        assert!(registry.contains_station(12348));
        assert!(!registry.contains_station(12346)); // Filtered out (rec_st_ind = 1)

        let station = registry.get_station(12345).unwrap();
        assert_eq!(station.src_name, "TEST_STATION_1");
        assert_eq!(station.authority, "Met Office");
    }

    #[tokio::test]
    async fn test_load_from_cache_nonexistent_path() {
        let cache_path = PathBuf::from("/nonexistent/path");
        let datasets = vec!["uk-daily-temperature-obs".to_string()];

        let result = StationRegistry::load_from_cache(&cache_path, &datasets, false).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::StationRegistry { message } => {
                assert!(message.contains("Cache path does not exist"));
            }
            _ => panic!("Expected StationRegistry error"),
        }
    }

    #[tokio::test]
    async fn test_discover_capability_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let datasets = vec!["uk-daily-temperature-obs".to_string()];
        let files = StationRegistry::discover_capability_files(&cache_path, &datasets).unwrap();

        assert_eq!(files.len(), 1);
        assert!(files[0].to_string_lossy().contains("station1.csv"));
    }

    #[tokio::test]
    async fn test_discover_capability_files_missing_dataset() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let datasets = vec!["nonexistent-dataset".to_string()];
        let files = StationRegistry::discover_capability_files(&cache_path, &datasets).unwrap();

        assert_eq!(files.len(), 0);
    }

    #[tokio::test]
    async fn test_load_capability_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        create_test_capability_file(
            temp_dir.path(),
            "test.csv",
            &[
                (12345, "STATION_A", 9),
                (12346, "STATION_B", 1), // Should be filtered out
                (12347, "STATION_C", 9),
            ],
        )
        .unwrap();

        let (stations, total_records) = StationRegistry::load_capability_file(&file_path)
            .await
            .unwrap();

        assert_eq!(total_records, 3);
        assert_eq!(stations.len(), 2); // Only rec_st_ind = 9 records

        assert_eq!(stations[0].src_id, 12345);
        assert_eq!(stations[0].src_name, "STATION_A");
        assert_eq!(stations[1].src_id, 12347);
        assert_eq!(stations[1].src_name, "STATION_C");
    }

    #[tokio::test]
    async fn test_parse_station_record_valid() {
        let headers = StringRecord::from(vec![
            "src_id",
            "src_name",
            "high_prcn_lat",
            "high_prcn_lon",
            "east_grid_ref",
            "north_grid_ref",
            "grid_ref_type",
            "src_bgn_date",
            "src_end_date",
            "authority",
            "historic_county",
            "height_meters",
            "rec_st_ind",
        ]);

        let record = StringRecord::from(vec![
            "12345",
            "TEST_STATION",
            "51.4778",
            "-0.4614",
            "507500",
            "176500",
            "OSGB",
            "2000-01-01T00:00:00Z",
            "2050-12-31T23:59:59Z",
            "Met Office",
            "Greater London",
            "25.0",
            "9",
        ]);

        let result = StationRegistry::parse_station_record(&record, &headers).unwrap();
        assert!(result.is_some());

        let station = result.unwrap();
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "TEST_STATION");
        assert_eq!(station.high_prcn_lat, 51.4778);
        assert_eq!(station.high_prcn_lon, -0.4614);
        assert_eq!(station.authority, "Met Office");
    }

    #[tokio::test]
    async fn test_parse_station_record_filtered() {
        let headers = StringRecord::from(vec![
            "src_id",
            "src_name",
            "high_prcn_lat",
            "high_prcn_lon",
            "east_grid_ref",
            "north_grid_ref",
            "grid_ref_type",
            "src_bgn_date",
            "src_end_date",
            "authority",
            "historic_county",
            "height_meters",
            "rec_st_ind",
        ]);

        let record = StringRecord::from(vec![
            "12345",
            "TEST_STATION",
            "51.4778",
            "-0.4614",
            "507500",
            "176500",
            "OSGB",
            "2000-01-01T00:00:00Z",
            "2050-12-31T23:59:59Z",
            "Met Office",
            "Greater London",
            "25.0",
            "1", // Non-definitive
        ]);

        let result = StationRegistry::parse_station_record(&record, &headers).unwrap();
        assert!(result.is_none()); // Should be filtered out
    }

    #[test]
    fn test_station_lookup_methods() {
        let mut registry = StationRegistry::new(PathBuf::from("/test"));

        // Create test stations
        let station1 = Station::new(
            12345,
            "HEATHROW".to_string(),
            51.4778,
            -0.4614,
            Some(507500),
            Some(176500),
            Some("OSGB".to_string()),
            DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2050-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
            "Met Office".to_string(),
            "Greater London".to_string(),
            25.0,
        )
        .unwrap();

        let station2 = Station::new(
            12346,
            "BIRMINGHAM".to_string(),
            52.4539,
            -1.7481,
            Some(407500),
            Some(287500),
            Some("OSGB".to_string()),
            DateTime::parse_from_rfc3339("1990-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2010-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
            "Met Office".to_string(),
            "West Midlands".to_string(),
            161.0,
        )
        .unwrap();

        registry.stations.insert(station1.src_id, station1);
        registry.stations.insert(station2.src_id, station2);

        // Test basic lookups
        assert_eq!(registry.station_count(), 2);
        assert!(registry.contains_station(12345));
        assert!(!registry.contains_station(99999));

        let station = registry.get_station(12345).unwrap();
        assert_eq!(station.src_name, "HEATHROW");

        // Test name search
        let heathrow_stations = registry.find_stations_by_name("HEATH");
        assert_eq!(heathrow_stations.len(), 1);
        assert_eq!(heathrow_stations[0].src_name, "HEATHROW");

        let birmingham_stations = registry.find_stations_by_name("birmingham");
        assert_eq!(birmingham_stations.len(), 1);
        assert_eq!(birmingham_stations[0].src_name, "BIRMINGHAM");

        // Test regional search (both stations are in UK)
        let uk_stations = registry.find_stations_in_region(50.0, 55.0, -5.0, 2.0);
        assert_eq!(uk_stations.len(), 2);

        // Test smaller region (only London area)
        let london_stations = registry.find_stations_in_region(51.0, 52.0, -1.0, 0.0);
        assert_eq!(london_stations.len(), 1);
        assert_eq!(london_stations[0].src_name, "HEATHROW");

        // Test active stations
        let active_2005 = registry.find_active_stations(
            DateTime::parse_from_rfc3339("2005-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2005-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(active_2005.len(), 2); // Both active in 2005

        let active_2015 = registry.find_active_stations(
            DateTime::parse_from_rfc3339("2015-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2015-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(active_2015.len(), 1); // Only Heathrow active in 2015
        assert_eq!(active_2015[0].src_name, "HEATHROW");
    }

    #[test]
    fn test_registry_metadata() {
        let cache_path = PathBuf::from("/test/cache");
        let mut registry = StationRegistry::new(cache_path.clone());
        registry.loaded_datasets = vec!["uk-daily-temperature-obs".to_string()];
        registry.files_processed = 5;
        registry.total_records_found = 100;

        let metadata = registry.metadata();
        assert_eq!(metadata.cache_path, cache_path);
        assert_eq!(metadata.loaded_datasets, vec!["uk-daily-temperature-obs"]);
        assert_eq!(metadata.station_count, 0);
        assert_eq!(metadata.files_processed, 5);
        assert_eq!(metadata.total_records_found, 100);
    }

    #[test]
    fn test_station_ids_and_stations() {
        let mut registry = StationRegistry::new(PathBuf::from("/test"));

        let station = Station::new(
            12345,
            "TEST_STATION".to_string(),
            51.4778,
            -0.4614,
            None,
            None,
            None,
            DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2050-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
            "Met Office".to_string(),
            "Test County".to_string(),
            25.0,
        )
        .unwrap();

        registry.stations.insert(station.src_id, station);

        let ids = registry.station_ids();
        assert_eq!(ids, vec![12345]);

        let stations = registry.stations();
        assert_eq!(stations.len(), 1);
        assert_eq!(stations[0].src_id, 12345);
    }
}
