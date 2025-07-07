//! Shared test utilities and fixtures for station registry tests

use crate::app::models::Station;
use chrono::{DateTime, Utc};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

pub mod loader_tests;
pub mod metadata_tests;
pub mod parser_tests;
pub mod query_tests;

/// Create a test station with standard parameters
#[allow(clippy::too_many_arguments)]
pub fn create_test_station(
    src_id: i32,
    name: &str,
    lat: f64,
    lon: f64,
    start_year: i32,
    end_year: i32,
    county: &str,
    height: f32,
) -> Station {
    Station::new(
        src_id,
        name.to_string(),
        lat,
        lon,
        None,
        None,
        None,
        DateTime::parse_from_rfc3339(&format!("{}-01-01T00:00:00Z", start_year))
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339(&format!("{}-12-31T23:59:59Z", end_year))
            .unwrap()
            .with_timezone(&Utc),
        "Met Office".to_string(),
        county.to_string(),
        height,
    )
    .unwrap()
}

/// Create a test capability CSV file with proper BADC format
pub fn create_test_capability_file(
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
pub fn create_test_cache_structure(temp_dir: &TempDir) -> std::io::Result<std::path::PathBuf> {
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

/// Create a test metadata CSV file with BADC data section
pub fn create_test_metadata_file(dir: &Path, filename: &str) -> std::io::Result<()> {
    let file_path = dir.join(filename);

    let content = r#"Conventions,G,BADC-CSV,1
title,G,Station metadata file
source,G,Met Office MIDAS database
data
src_id,src_name,high_prcn_lat,high_prcn_lon,east_grid_ref,north_grid_ref,grid_ref_type,src_bgn_date,src_end_date,authority,historic_county,height_meters,rec_st_ind
12345,TEST_STATION_1,51.4778,-0.4614,507500,176500,OSGB,2000-01-01T00:00:00Z,2050-12-31T23:59:59Z,Met Office,Greater London,25.0,9
12346,TEST_STATION_2,52.4539,-1.7481,407500,287500,OSGB,1990-01-01T00:00:00Z,2010-12-31T23:59:59Z,Met Office,West Midlands,161.0,9
12347,FILTERED_STATION,53.0000,-2.0000,400000,300000,OSGB,1985-01-01T00:00:00Z,2005-12-31T23:59:59Z,Met Office,Some County,100.0,1
end data
"#;

    fs::write(file_path, content)
}

/// Assert that two stations are functionally equivalent
pub fn assert_stations_equal(actual: &Station, expected: &Station) {
    assert_eq!(actual.src_id, expected.src_id);
    assert_eq!(actual.src_name, expected.src_name);
    assert!((actual.high_prcn_lat - expected.high_prcn_lat).abs() < 0.0001);
    assert!((actual.high_prcn_lon - expected.high_prcn_lon).abs() < 0.0001);
    assert_eq!(actual.authority, expected.authority);
    assert_eq!(actual.historic_county, expected.historic_county);
    assert!((actual.height_meters - expected.height_meters).abs() < 0.01);
}

/// Common test dataset names
pub const TEST_DATASETS: &[&str] = &["uk-daily-temperature-obs", "uk-daily-rain-obs"];

/// Common test station IDs
pub const TEST_STATION_IDS: &[i32] = &[12345, 12346, 12347, 12348];

/// Test geographic bounds for UK
pub struct TestBounds {
    pub uk_min_lat: f64,
    pub uk_max_lat: f64,
    pub uk_min_lon: f64,
    pub uk_max_lon: f64,
}

impl TestBounds {
    pub const fn new() -> Self {
        Self {
            uk_min_lat: 49.0,
            uk_max_lat: 61.0,
            uk_min_lon: -8.0,
            uk_max_lon: 2.0,
        }
    }
}

impl Default for TestBounds {
    fn default() -> Self {
        Self::new()
    }
}

pub const UK_BOUNDS: TestBounds = TestBounds::new();
