//! Station registry loading and file discovery
//!
//! This module handles loading station metadata from MIDAS cache files,
//! including both capability files and centralized metadata files.

use super::StationRegistry;
use super::metadata::LoadStats;
use super::parser::{
    extract_capability_metadata, parse_capability_station_metadata, parse_id_evolution_record,
    parse_station_metadata_record,
};
use crate::app::models::Station;
use crate::constants::CAPABILITY_DIR_NAME;
use crate::{Error, Result};
use csv::StringRecord;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

impl StationRegistry {
    /// Load station metadata from MIDAS cache for a single dataset
    ///
    /// This method scans the specified dataset in the MIDAS cache and loads
    /// station metadata from capability files and centralized metadata files.
    /// It filters records to only include definitive station information (rec_st_ind = 9).
    ///
    /// # Arguments
    /// * `cache_path` - Root path to the MIDAS cache directory
    /// * `dataset` - Name of the dataset to process (e.g., "uk-mean-wind-obs")
    /// * `show_progress` - Whether to display a progress bar
    ///
    /// # Returns
    /// * `Result<(StationRegistry, LoadStats)>` - Registry and loading statistics
    ///
    /// # Errors
    /// * Returns `Error::StationRegistry` if cache directory doesn't exist
    /// * Returns `Error::Io` for file system access issues
    /// * Returns `Error::CsvParsing` for malformed capability files
    pub async fn load_for_dataset(
        cache_path: &Path,
        dataset: &str,
        show_progress: bool,
    ) -> Result<(Self, LoadStats)> {
        info!(
            "Loading station registry for dataset '{}' from cache: {}",
            dataset,
            cache_path.display()
        );

        let start_time = Instant::now();
        let mut registry = Self::new(cache_path.to_path_buf());
        let mut stats = LoadStats::new();

        // Validate cache path exists
        if !cache_path.exists() {
            return Err(Error::station_registry(format!(
                "Cache path does not exist: {}",
                cache_path.display()
            )));
        }

        // Discover all station files (capability and metadata) for single dataset
        let (capability_files, metadata_files) =
            Self::discover_station_files_for_dataset(cache_path, dataset)?;

        let total_files = capability_files.len() + metadata_files.len();
        info!(
            "Found {} capability files and {} metadata files to process",
            capability_files.len(),
            metadata_files.len()
        );

        // Set up progress reporting
        let progress_bar = if show_progress {
            let pb = ProgressBar::new(total_files as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            pb.set_message("Loading station files...");
            Some(pb)
        } else {
            None
        };

        let mut file_index = 0;

        // Process capability files first (metadata files will override these if present)
        for file_path in capability_files.iter() {
            if let Some(pb) = &progress_bar {
                pb.set_position(file_index as u64);
                pb.set_message(format!(
                    "Processing capability file {}",
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
            file_index += 1;
        }

        // Process centralized metadata files (these take priority over capability files)
        for file_path in metadata_files.iter() {
            if let Some(pb) = &progress_bar {
                pb.set_position(file_index as u64);
                pb.set_message(format!(
                    "Processing metadata file {}",
                    file_path.file_name().unwrap_or_default().to_string_lossy()
                ));
            }

            match Self::load_metadata_file(file_path).await {
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
                            // Handle duplicate stations - prefer authoritative metadata file data over capability file
                            debug!(
                                "Station {} found in both capability and metadata files, using authoritative metadata file data (operational period: {} to {})",
                                station.src_id, station.src_bgn_date, station.src_end_date
                            );
                            registry.stations.insert(station.src_id, station);
                        }
                    }

                    stats.files_processed += 1;
                }
                Err(e) => {
                    warn!(
                        "Failed to load metadata file {}: {}",
                        file_path.display(),
                        e
                    );
                    stats.errors.push(format!("{}: {}", file_path.display(), e));
                }
            }
            file_index += 1;
        }

        if let Some(pb) = &progress_bar {
            pb.finish_with_message("Station registry loading complete");
        }

        // Update registry metadata
        registry.loaded_datasets = vec![dataset.to_string()];
        registry.load_time = start_time;
        registry.files_processed = stats.files_processed;
        registry.total_records_found = stats.total_records_found;

        // Finalize statistics
        stats.datasets_processed = 1;
        stats.records_filtered = stats.total_records_found - stats.stations_loaded;
        stats.load_duration = start_time.elapsed();

        info!(
            "Station registry loaded for '{}': {} stations from {} files in {:.2}s",
            dataset,
            stats.stations_loaded,
            stats.files_processed,
            stats.load_duration.as_secs_f64()
        );

        Ok((registry, stats))
    }

    /// Discover all capability files and station metadata files in the specified datasets
    pub fn discover_station_files(
        cache_path: &Path,
        datasets: &[String],
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let mut capability_files = Vec::new();
        let mut metadata_files = Vec::new();

        for dataset in datasets {
            // Look for capability files in capability/ subdirectory
            let capability_path = cache_path.join(dataset).join(CAPABILITY_DIR_NAME);
            if capability_path.exists() {
                debug!(
                    "Scanning capability directory: {}",
                    capability_path.display()
                );

                for entry in WalkDir::new(&capability_path) {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if path.is_file() && path.extension().is_some_and(|ext| ext == "csv") {
                                capability_files.push(path.to_path_buf());
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Error walking capability directory {}: {}",
                                capability_path.display(),
                                e
                            );
                        }
                    }
                }
            } else {
                warn!(
                    "Capability directory not found for dataset '{}': {}",
                    dataset,
                    capability_path.display()
                );
            }

            // Look for centralized station metadata files
            debug!(
                "Scanning dataset directory for metadata files: {}",
                cache_path.join(dataset).display()
            );
            for entry in WalkDir::new(cache_path.join(dataset)) {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file()
                            && path
                                .file_name()
                                .and_then(|name| name.to_str())
                                .is_some_and(|name| {
                                    name.contains("station-metadata") && name.ends_with(".csv")
                                })
                        {
                            metadata_files.push(path.to_path_buf());
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error walking dataset directory {}: {}",
                            cache_path.join(dataset).display(),
                            e
                        );
                    }
                }
            }
        }

        debug!(
            "Discovered {} capability files and {} metadata files",
            capability_files.len(),
            metadata_files.len()
        );
        Ok((capability_files, metadata_files))
    }

    /// Discover capability files and station metadata files for a single dataset
    pub fn discover_station_files_for_dataset(
        cache_path: &Path,
        dataset: &str,
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let mut capability_files = Vec::new();
        let mut metadata_files = Vec::new();

        // Look for capability files in capability/ subdirectory
        let capability_path = cache_path.join(dataset).join(CAPABILITY_DIR_NAME);
        if capability_path.exists() {
            debug!(
                "Scanning capability directory: {}",
                capability_path.display()
            );

            for entry in WalkDir::new(&capability_path) {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file() && path.extension().is_some_and(|ext| ext == "csv") {
                            capability_files.push(path.to_path_buf());
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Error walking capability directory {}: {}",
                            capability_path.display(),
                            e
                        );
                    }
                }
            }
        } else {
            warn!(
                "Capability directory not found for dataset '{}': {}",
                dataset,
                capability_path.display()
            );
        }

        // Look for centralized station metadata files
        debug!(
            "Scanning dataset directory for metadata files: {}",
            cache_path.join(dataset).display()
        );
        for entry in WalkDir::new(cache_path.join(dataset)) {
            match entry {
                Ok(entry) => {
                    let path = entry.path();
                    if path.is_file()
                        && path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| {
                                name.contains("station-metadata") && name.ends_with(".csv")
                            })
                    {
                        metadata_files.push(path.to_path_buf());
                    }
                }
                Err(e) => {
                    warn!(
                        "Error walking dataset directory {}: {}",
                        cache_path.join(dataset).display(),
                        e
                    );
                }
            }
        }

        debug!(
            "Discovered {} capability files and {} metadata files for dataset '{}'",
            capability_files.len(),
            metadata_files.len(),
            dataset
        );
        Ok((capability_files, metadata_files))
    }

    /// Load station metadata from a single capability file
    ///
    /// Capability files contain station metadata in the BADC header section,
    /// not in the data section like centralized metadata files.
    pub async fn load_capability_file(file_path: &Path) -> Result<(Vec<Station>, usize)> {
        debug!("Loading capability file: {}", file_path.display());

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .comment(Some(b'#'))
            .from_path(file_path)
            .map_err(|e| {
                Error::csv_parsing(
                    file_path.to_string_lossy().to_string(),
                    "Failed to open CSV file".to_string(),
                    Some(e),
                )
            })?;

        // Parse BADC header section to extract station metadata
        let mut station_metadata = HashMap::new();
        let mut record = StringRecord::new();
        let mut total_records = 0;

        let mut in_data_section = false;
        let mut headers: Option<StringRecord> = None;
        let mut id_periods = Vec::new();

        while reader.read_record(&mut record).map_err(|e| {
            Error::csv_parsing(
                file_path.to_string_lossy().to_string(),
                "Failed to read CSV record".to_string(),
                Some(e),
            )
        })? {
            // Check for data section marker
            if record.get(0).is_some_and(|val| val.trim() == "data") {
                in_data_section = true;
                continue;
            }

            // Check for end data marker
            if record.get(0).is_some_and(|val| val.trim() == "end data") {
                break;
            }

            total_records += 1;

            if !in_data_section {
                // Extract station metadata from header record
                extract_capability_metadata(&record, &mut station_metadata);
            } else {
                // Parse data section - first record after "data" should be headers
                if headers.is_none() {
                    headers = Some(record.clone());
                    continue;
                }

                // Parse ID evolution records
                if let Some(ref header_record) = headers {
                    if let Ok(id_period) = parse_id_evolution_record(&record, header_record) {
                        id_periods.push(id_period);
                    }
                }
            }
        }

        // Extract required station metadata from header
        let mut station = parse_capability_station_metadata(&station_metadata, file_path)?;

        // Add ID evolution history to station
        for id_period in id_periods {
            station.add_id_period(id_period);
        }

        debug!(
            "Loaded station {} from capability file {} (ID periods: {})",
            station.src_id,
            file_path.display(),
            station.id_history.len()
        );

        Ok((vec![station], total_records))
    }

    /// Load station metadata from a centralized metadata file
    ///
    /// These files contain multiple station records in the BADC data section.
    pub async fn load_metadata_file(file_path: &Path) -> Result<(Vec<Station>, usize)> {
        debug!("Loading metadata file: {}", file_path.display());

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .comment(Some(b'#'))
            .from_path(file_path)
            .map_err(|e| {
                Error::csv_parsing(
                    file_path.to_string_lossy().to_string(),
                    "Failed to open CSV file".to_string(),
                    Some(e),
                )
            })?;

        // Skip BADC header section and find the data section
        let mut in_data_section = false;
        let mut stations = Vec::new();
        let mut total_records = 0;
        let mut record = StringRecord::new();
        let mut headers: Option<StringRecord> = None;

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

            // Parse station record using the metadata file method
            if let Some(ref header_record) = headers {
                match parse_station_metadata_record(&record, header_record) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use std::fs;
    use tempfile::TempDir;

    /// Create a test capability CSV file with proper BADC format
    fn create_test_capability_file(
        dir: &Path,
        filename: &str,
        src_id: i32,
        station_name: &str,
    ) -> std::io::Result<()> {
        let file_path = dir.join(filename);

        // Create proper BADC-CSV capability file format (station metadata in header)
        let content = format!(
            r#"Conventions,G,BADC-CSV,1
title,G,Midas Open: Station capability information for uk-daily-temperature-obs
comments,G,This file documents the range of identifier types associated with a unique station in this version of the dataset
source,G,Met Office MIDAS database
creator,G,Met Office
activity,G,Met Office MIDAS Open: UK Land Surface Stations Data
feature_type,G,point collection
collection_name,G,midas-open
collection_version_number,G,dataset-version-202507
history,G,Created 2025-06-24
last_revised_date,G,2025-06-24
observation_station,G,{}
historic_county_name,G,test-county
authority,G,Met Office
src_id,G,{:05}
location,G,51.4778,-0.4614
height,G,25,m
date_valid,G,2000-01-01 00:00:00,2050-12-31 23:59:59
coordinate_variable,id,x
long_name,id,The identifier associated with a particular report type for this station,1
type,id,int
long_name,id_type,The identifier type associated with a particular reporting capability for this station,1
type,id_type,char
long_name,met_domain_name,message type,1
type,met_domain_name,char
long_name,first_year,first year of data associated with this id/id_type in this version of this dataset,year
type,first_year,float
long_name,last_year,last year of data associated with this id/id_type in this version of this dataset,year
type,last_year,float
data
id,id_type,met_domain_name,first_year,last_year
{},DCNN,NCM,2000,2024
end data
"#,
            station_name,
            src_id,
            src_id + 5000
        );

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

        // Create test capability files with proper BADC format
        create_test_capability_file(&dataset1, "station1.csv", 12345, "test-station-1")?;
        create_test_capability_file(&dataset2, "station2.csv", 12348, "test-station-2")?;

        Ok(cache_root.to_path_buf())
    }

    #[tokio::test]
    async fn test_load_from_cache_success() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        // Test single-dataset loading for temperature dataset
        let dataset = "uk-daily-temperature-obs";

        let (registry, stats) = StationRegistry::load_for_dataset(&cache_path, dataset, false)
            .await
            .unwrap();

        // Verify registry properties - now we have 1 station from 1 capability file
        assert_eq!(registry.station_count(), 1);
        assert_eq!(registry.loaded_datasets, vec![dataset]);

        // Verify load statistics
        assert_eq!(stats.datasets_processed, 1);
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.stations_loaded, 1);
        assert!(stats.total_records_found > 0); // Header records processed (multiple lines per file)
        assert_eq!(
            stats.records_filtered,
            stats.total_records_found - stats.stations_loaded
        ); // Filtered = total - loaded
        assert!(stats.errors.is_empty());

        // Verify specific station (only from temperature dataset)
        assert!(registry.contains_station(12345));

        let station = registry.get_station(12345).unwrap();
        assert_eq!(station.src_name, "test-station-1");
        assert_eq!(station.authority, "Met Office");
    }

    #[tokio::test]
    async fn test_load_for_dataset_nonexistent_path() {
        let cache_path = PathBuf::from("/nonexistent/path");
        let dataset = "uk-daily-temperature-obs";

        let result = StationRegistry::load_for_dataset(&cache_path, dataset, false).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::StationRegistry { message } => {
                assert!(message.contains("Cache path does not exist"));
            }
            _ => panic!("Expected StationRegistry error"),
        }
    }

    #[tokio::test]
    async fn test_discover_station_files() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let datasets = vec!["uk-daily-temperature-obs".to_string()];
        let (capability_files, metadata_files) =
            StationRegistry::discover_station_files(&cache_path, &datasets).unwrap();

        assert_eq!(capability_files.len(), 1);
        assert!(
            capability_files[0]
                .to_string_lossy()
                .contains("station1.csv")
        );
        assert_eq!(metadata_files.len(), 0); // No metadata files in test structure
    }

    #[tokio::test]
    async fn test_discover_station_files_missing_dataset() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let datasets = vec!["nonexistent-dataset".to_string()];
        let (capability_files, metadata_files) =
            StationRegistry::discover_station_files(&cache_path, &datasets).unwrap();

        assert_eq!(capability_files.len(), 0);
        assert_eq!(metadata_files.len(), 0);
    }

    #[tokio::test]
    async fn test_load_capability_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        create_test_capability_file(temp_dir.path(), "test.csv", 12345, "test-station-a").unwrap();

        let (stations, total_records) = StationRegistry::load_capability_file(&file_path)
            .await
            .unwrap();

        // Should return 1 station from capability file header
        assert_eq!(stations.len(), 1);
        assert!(total_records > 0); // Header records processed

        // Verify the station
        let station = &stations[0];
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "test-station-a");
        assert_eq!(station.authority, "Met Office");
        assert_eq!(station.historic_county, "test-county");
    }

    #[tokio::test]
    async fn test_load_real_capability_file() {
        // Test with a real MIDAS capability file to ensure parsing works correctly
        let real_file_path = Path::new(
            "/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/capability/devon/01330_clawton/midas-open_uk-daily-temperature-obs_dv-202507_devon_01330_clawton_capability.csv",
        );

        if real_file_path.exists() {
            let (stations, total_records) = StationRegistry::load_capability_file(real_file_path)
                .await
                .unwrap();

            // Should return 1 station from capability file header
            assert_eq!(stations.len(), 1);
            assert!(total_records > 0);

            // Verify the station
            let station = &stations[0];
            assert_eq!(station.src_id, 1330);
            assert_eq!(station.src_name, "clawton");
            assert_eq!(station.authority, "Met Office");
            assert_eq!(station.historic_county, "devon");

            // Check coordinates are reasonable for Devon, UK
            assert!((station.high_prcn_lat - 50.77).abs() < 0.01);
            assert!((station.high_prcn_lon - (-4.346)).abs() < 0.01);
        } else {
            println!("Real capability file not found, skipping test");
        }
    }

    #[tokio::test]
    async fn test_load_stats_calculations() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = create_test_cache_structure(&temp_dir).unwrap();

        let dataset = "uk-daily-temperature-obs";

        let (_registry, stats) = StationRegistry::load_for_dataset(&cache_path, dataset, false)
            .await
            .unwrap();

        // Test LoadStats methods
        assert!(stats.load_duration.as_nanos() > 0);
        assert!(stats.loading_rate() > 0.0);
        assert!(!stats.has_errors());

        let summary = stats.summary();
        assert!(summary.contains("1 datasets"));
        assert!(summary.contains("1 files"));
    }

    #[tokio::test]
    async fn test_id_evolution_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_id_evolution.csv");

        // Create a capability file with ID evolution data like London Weather Centre
        let content = r#"Conventions,G,BADC-CSV,1
title,G,Midas Open: Station capability information for uk-radiation-obs
source,G,Met Office MIDAS database
creator,G,Met Office
observation_station,G,LONDON WEATHER CENTRE
src_id,G,19144
location,G,51.4776,-0.4613
height,G,25,m
date_valid,G,1958-01-01 00:00:00,2006-12-31 23:59:59
authority,G,Met Office
historic_county_name,G,greater-london
data
id,id_type,met_domain_name,first_year,last_year
5046,DCNN,MODLERAD,1958,1992
5037,DCNN,MODLERAD,1992,1997
5047,DCNN,MODLERAD,1997,2006
2188,DCNN,MODLERAD,2004,2004
end data
"#;

        std::fs::write(&file_path, content).unwrap();

        let (stations, _total_records) = StationRegistry::load_capability_file(&file_path)
            .await
            .unwrap();

        // Should return 1 station with ID evolution data
        assert_eq!(stations.len(), 1);
        let station = &stations[0];

        // Verify station basic info
        assert_eq!(station.src_id, 19144);
        assert_eq!(station.src_name, "LONDON WEATHER CENTRE");

        // Verify ID evolution history was parsed
        assert_eq!(station.id_history.len(), 4);
        assert!(station.has_id_history());

        // Verify specific ID periods
        let id_5046 = station.find_observation_id_for_year(1980);
        assert!(id_5046.is_some());
        assert_eq!(id_5046.unwrap().observation_id, "5046");
        assert_eq!(id_5046.unwrap().first_year, 1958);
        assert_eq!(id_5046.unwrap().last_year, 1992);

        let id_5037 = station.find_observation_id_for_year(1995);
        assert!(id_5037.is_some());
        assert_eq!(id_5037.unwrap().observation_id, "5037");

        let id_5047 = station.find_observation_id_for_year(2002);
        assert!(id_5047.is_some());
        assert_eq!(id_5047.unwrap().observation_id, "5047");

        let id_2188 = station.find_observation_id_for_year(2004);
        // Should find one of the overlapping IDs (depends on iteration order)
        assert!(id_2188.is_some());

        // Verify all observation IDs are tracked
        let all_ids = station.get_all_observation_ids();
        assert_eq!(all_ids.len(), 4);
        assert!(all_ids.contains(&"5046"));
        assert!(all_ids.contains(&"5037"));
        assert!(all_ids.contains(&"5047"));
        assert!(all_ids.contains(&"2188"));

        // Verify overlap detection
        let issues = station.analyze_id_evolution();
        assert!(!issues.is_empty()); // Should detect overlap between 5047 and 2188

        println!("✅ ID evolution parsing test completed successfully");
        println!(
            "   Station {} has {} ID periods",
            station.src_id,
            station.id_history.len()
        );
        for period in &station.id_history {
            println!(
                "   ID {}: {}-{} ({} {})",
                period.observation_id,
                period.first_year,
                period.last_year,
                period.id_type,
                period.met_domain_name
            );
        }
        if !issues.is_empty() {
            println!("   Issues detected:");
            for issue in issues {
                println!("     - {}", issue);
            }
        }
    }

    #[tokio::test]
    async fn test_station_19144_operational_dates() {
        // Test that station 19144 (London Weather Centre) loads with correct operational dates
        // from the authoritative metadata file: 1958-2006, not the incorrect 1974-2010
        let cache_path =
            Path::new("/Users/richardlyon/Library/Application Support/midas-fetcher/cache");

        if !cache_path.exists() {
            println!("MIDAS cache not found, skipping test");
            return;
        }

        let dataset = "uk-radiation-obs";

        let (registry, _stats) =
            match StationRegistry::load_for_dataset(cache_path, dataset, false).await {
                Ok(result) => result,
                Err(e) => {
                    println!("Failed to load registry: {}, skipping test", e);
                    return;
                }
            };

        // Check if station 19144 was loaded
        if let Some(station) = registry.get_station(19144) {
            println!("✅ Station 19144 loaded successfully:");
            println!("   Name: {}", station.src_name);
            println!(
                "   Operational period: {} to {}",
                station.src_bgn_date, station.src_end_date
            );

            // Verify the station has the correct operational dates from metadata file
            assert_eq!(station.src_name, "LONDON WEATHER CENTRE");

            // Check that operational period is 1958-2006 (from metadata) not 1974-2010 (from unknown source)
            assert_eq!(
                station.src_bgn_date.year(),
                1958,
                "Station 19144 should start in 1958 according to metadata file"
            );
            assert_eq!(
                station.src_end_date.year(),
                2006,
                "Station 19144 should end in 2006 according to metadata file"
            );

            println!("✅ Station 19144 has correct operational dates: 1958-2006");
        } else {
            panic!("Station 19144 (London Weather Centre) not found in registry");
        }
    }
}
