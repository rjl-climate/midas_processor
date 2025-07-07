//! Test utilities and mock infrastructure for BADC-CSV parser testing
//!
//! This module provides common test utilities, mock objects, and helper functions
//! used across different test modules.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

use crate::app::models::Station;

// Test modules
mod header_tests;
mod parser_tests;
mod stats_tests;

/// Mock station registry for testing
#[derive(Debug, Clone)]
pub struct MockStationRegistry {
    stations: HashMap<i32, Station>,
}

impl Default for MockStationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MockStationRegistry {
    pub fn new() -> Self {
        let mut stations = HashMap::new();

        // Add test station
        let test_station = Station::new(
            1001,
            "TEST_STATION".to_string(),
            51.4816, // London latitude
            -0.0077, // London longitude
            Some(529090),
            Some(181680),
            Some("OSGB".to_string()),
            DateTime::parse_from_str("2020-01-01 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_str("2025-12-31 23:59:59 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            "Met Office".to_string(),
            "Greater London".to_string(),
            25.0,
        )
        .unwrap();

        stations.insert(1001, test_station);

        Self { stations }
    }

    pub fn get_station(&self, src_id: i32) -> Option<&Station> {
        self.stations.get(&src_id)
    }
}

/// Helper to create a test station registry
pub fn create_test_registry() -> Arc<MockStationRegistry> {
    Arc::new(MockStationRegistry::new())
}

/// Helper to create a complete test BADC-CSV content
pub fn create_test_badc_csv() -> String {
    r#"Conventions,G,BADC-CSV,1
title,G,Test Temperature Data
source,G,MIDAS Test
missing_value,G,NA
long_name,max_air_temp,maximum air temperature,degC
long_name,min_air_temp,minimum air temperature,degC
type,max_air_temp,float
type,min_air_temp,float
data
ob_end_time,id,src_id,id_type,met_domain_name,rec_st_ind,version_num,ob_hour_count,max_air_temp,max_air_temp_q,min_air_temp,min_air_temp_q,meto_stmp_time,midas_stmp_etime
2023-01-01 09:00:00,123,1001,SRCE,TEST-OBS,1001,1,24,15.5,0,8.2,0,2023-01-01 10:00:00,0
2023-01-02 09:00:00,123,1001,SRCE,TEST-OBS,1001,1,24,12.1,1,4.8,0,2023-01-02 10:00:00,0
2023-01-03 09:00:00,123,1001,SRCE,TEST-OBS,1001,1,24,NA,9,2.3,0,2023-01-03 10:00:00,0
end data"#.to_string()
}

/// Helper to create minimal BADC-CSV content
pub fn create_minimal_badc_csv() -> String {
    r#"Conventions,G,BADC-CSV,1
missing_value,G,NA
data
ob_end_time,id,src_id,id_type,met_domain_name,rec_st_ind,version_num,ob_hour_count
2023-01-01 09:00:00,123,1001,SRCE,TEST-OBS,1001,1,24"#
        .to_string()
}

/// Helper to create a temporary file with given content
pub fn create_temp_file(content: &str) -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "{}", content).unwrap();
    temp_file
}
