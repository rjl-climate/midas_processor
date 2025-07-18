//! File discovery module for MIDAS datasets
//!
//! Handles discovering CSV files in MIDAS dataset directory structure
//! and counting unique stations for processing statistics.

use crate::error::{MidasError, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::fs;
use tracing::debug;

/// File discovery component for MIDAS datasets
#[derive(Debug)]
pub struct FileDiscovery {
    dataset_path: PathBuf,
    station_count: usize,
}

impl FileDiscovery {
    /// Create a new file discovery instance
    pub fn new(dataset_path: PathBuf) -> Self {
        Self {
            dataset_path,
            station_count: 0,
        }
    }

    /// Get the current station count
    pub fn station_count(&self) -> usize {
        self.station_count
    }

    /// Discover all CSV files in the dataset and count stations
    ///
    /// MIDAS datasets follow this structure:
    /// ```text
    /// dataset/
    ///   qcv-1/
    ///     county1/
    ///       station1/
    ///         year1.csv
    ///         year2.csv
    ///       station2/
    ///         year1.csv
    ///     county2/
    ///       station3/
    ///         year1.csv
    /// ```
    pub async fn discover_csv_files(&mut self) -> Result<Vec<PathBuf>> {
        let qcv_path = self.dataset_path.join("qcv-1");

        if !qcv_path.exists() {
            return Err(MidasError::DatasetNotFound { path: qcv_path });
        }

        debug!("Searching for CSV files in: {}", qcv_path.display());

        let mut files = Vec::new();
        let mut stations = HashSet::new();

        let mut dir = fs::read_dir(&qcv_path).await?;

        while let Some(entry) = dir.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                // This is a county directory
                let county_files = self
                    .discover_county_files(entry.path(), &mut stations)
                    .await?;
                files.extend(county_files);
            }
        }

        self.station_count = stations.len();
        debug!(
            "Found {} CSV files from {} stations",
            files.len(),
            self.station_count
        );

        Ok(files)
    }

    /// Discover CSV files within a county directory
    async fn discover_county_files(
        &self,
        county_path: PathBuf,
        stations: &mut HashSet<String>,
    ) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut county_dir = fs::read_dir(&county_path).await?;

        while let Some(station_entry) = county_dir.next_entry().await? {
            if station_entry.file_type().await?.is_dir() {
                // This is a station directory
                let station_path = station_entry.path();
                let station_name = station_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                stations.insert(station_name.to_string());

                let station_files = self.discover_station_files(station_path).await?;
                files.extend(station_files);
            }
        }

        Ok(files)
    }

    /// Discover CSV files within a station directory
    async fn discover_station_files(&self, station_path: PathBuf) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut station_dir = fs::read_dir(&station_path).await?;

        while let Some(file_entry) = station_dir.next_entry().await? {
            let file_path = file_entry.path();
            if is_csv_file(&file_path) {
                files.push(file_path);
            }
        }

        Ok(files)
    }
}

/// Check if a path is a CSV file
fn is_csv_file(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == "csv")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Helper to create a test MIDAS dataset structure
    fn create_test_dataset(temp_dir: &TempDir) -> PathBuf {
        let dataset_path = temp_dir.path().join("test-dataset");
        let qcv_path = dataset_path.join("qcv-1");

        // Create directory structure
        fs::create_dir_all(&qcv_path).unwrap();

        // Create county1/station1 with 2 CSV files
        let station1_path = qcv_path.join("county1").join("station1");
        fs::create_dir_all(&station1_path).unwrap();
        fs::write(station1_path.join("2020.csv"), "test data").unwrap();
        fs::write(station1_path.join("2021.csv"), "test data").unwrap();

        // Create county1/station2 with 1 CSV file
        let station2_path = qcv_path.join("county1").join("station2");
        fs::create_dir_all(&station2_path).unwrap();
        fs::write(station2_path.join("2020.csv"), "test data").unwrap();

        // Create county2/station3 with 3 CSV files
        let station3_path = qcv_path.join("county2").join("station3");
        fs::create_dir_all(&station3_path).unwrap();
        fs::write(station3_path.join("2019.csv"), "test data").unwrap();
        fs::write(station3_path.join("2020.csv"), "test data").unwrap();
        fs::write(station3_path.join("2021.csv"), "test data").unwrap();

        // Create a non-CSV file that should be ignored
        fs::write(station3_path.join("metadata.txt"), "metadata").unwrap();

        dataset_path
    }

    #[tokio::test]
    async fn test_discover_csv_files() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = create_test_dataset(&temp_dir);

        let mut discovery = FileDiscovery::new(dataset_path);
        let files = discovery.discover_csv_files().await.unwrap();

        // Should find 6 CSV files total (2 + 1 + 3)
        assert_eq!(files.len(), 6);

        // Should find 3 unique stations
        assert_eq!(discovery.station_count(), 3);

        // All files should be CSV files
        for file in &files {
            assert!(is_csv_file(file));
        }

        // Check that specific files exist
        let file_names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&"2020.csv".to_string()));
        assert!(file_names.contains(&"2021.csv".to_string()));
        assert!(file_names.contains(&"2019.csv".to_string()));
    }

    #[tokio::test]
    async fn test_discover_empty_dataset() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("empty-dataset");
        let qcv_path = dataset_path.join("qcv-1");
        fs::create_dir_all(&qcv_path).unwrap();

        let mut discovery = FileDiscovery::new(dataset_path);
        let files = discovery.discover_csv_files().await.unwrap();

        assert_eq!(files.len(), 0);
        assert_eq!(discovery.station_count(), 0);
    }

    #[tokio::test]
    async fn test_discover_missing_qcv_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("missing-qcv");
        fs::create_dir_all(&dataset_path).unwrap();

        let mut discovery = FileDiscovery::new(dataset_path.clone());
        let result = discovery.discover_csv_files().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            MidasError::DatasetNotFound { path } => {
                assert_eq!(path, dataset_path.join("qcv-1"));
            }
            _ => panic!("Expected DatasetNotFound error"),
        }
    }

    #[test]
    fn test_is_csv_file() {
        assert!(is_csv_file(Path::new("test.csv")));
        assert!(is_csv_file(Path::new("/path/to/data.csv")));
        assert!(!is_csv_file(Path::new("test.txt")));
        assert!(!is_csv_file(Path::new("test")));
        assert!(!is_csv_file(Path::new("test.CSV"))); // Case sensitive
    }

    #[tokio::test]
    async fn test_discovery_with_nested_structure() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("nested-dataset");
        let qcv_path = dataset_path.join("qcv-1");

        // Create a more complex nested structure
        let deep_station_path = qcv_path.join("county1").join("station1").join("subdir"); // This shouldn't be processed as it's too deep
        fs::create_dir_all(&deep_station_path).unwrap();
        fs::write(deep_station_path.join("ignored.csv"), "ignored").unwrap();

        // Create proper structure
        let proper_station_path = qcv_path.join("county1").join("station1");
        fs::write(proper_station_path.join("2020.csv"), "valid data").unwrap();

        let mut discovery = FileDiscovery::new(dataset_path);
        let files = discovery.discover_csv_files().await.unwrap();

        // Should only find the properly placed CSV file
        assert_eq!(files.len(), 1);
        assert_eq!(discovery.station_count(), 1);
        assert!(files[0].file_name().unwrap() == "2020.csv");
    }
}
