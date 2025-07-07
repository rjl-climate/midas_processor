//! Tests for station record parsing functionality

use crate::app::services::station_registry::parser::*;
use csv::StringRecord;
use std::collections::HashMap;
use std::path::Path;

#[test]
fn test_parse_station_record_valid() {
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

    let result = parse_station_record(&record, &headers).unwrap();
    assert!(result.is_some());

    let station = result.unwrap();
    assert_eq!(station.src_id, 12345);
    assert_eq!(station.src_name, "TEST_STATION");
    assert_eq!(station.high_prcn_lat, 51.4778);
    assert_eq!(station.high_prcn_lon, -0.4614);
    assert_eq!(station.authority, "Met Office");
}

#[test]
fn test_parse_station_record_filtered() {
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

    let result = parse_station_record(&record, &headers).unwrap();
    assert!(result.is_none()); // Should be filtered out
}

#[test]
fn test_parse_capability_station_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("src_id".to_string(), "12345".to_string());
    metadata.insert(
        "observation_station".to_string(),
        "test-station".to_string(),
    );
    metadata.insert("authority".to_string(), "Met Office".to_string());
    metadata.insert(
        "historic_county_name".to_string(),
        "test-county".to_string(),
    );
    metadata.insert("location".to_string(), "51.4778,-0.4614".to_string());
    metadata.insert("height".to_string(), "25.0,m".to_string());
    metadata.insert(
        "date_valid".to_string(),
        "2000-01-01 00:00:00,2050-12-31 23:59:59".to_string(),
    );

    let station = parse_capability_station_metadata(&metadata, Path::new("test.csv")).unwrap();

    assert_eq!(station.src_id, 12345);
    assert_eq!(station.src_name, "test-station");
    assert_eq!(station.authority, "Met Office");
    assert_eq!(station.historic_county, "test-county");
    assert_eq!(station.high_prcn_lat, 51.4778);
    assert_eq!(station.high_prcn_lon, -0.4614);
    assert_eq!(station.height_meters, 25.0);
}

#[test]
fn test_extract_capability_metadata() {
    let mut metadata = HashMap::new();

    // Test simple global attribute
    let record1 = StringRecord::from(vec!["src_id", "G", "12345"]);
    extract_capability_metadata(&record1, &mut metadata);
    assert_eq!(metadata.get("src_id"), Some(&"12345".to_string()));

    // Test location with separate lat/lon fields
    let record2 = StringRecord::from(vec!["location", "G", "51.4778", "-0.4614"]);
    extract_capability_metadata(&record2, &mut metadata);
    assert_eq!(
        metadata.get("location"),
        Some(&"51.4778,-0.4614".to_string())
    );

    // Test non-global attribute (should be ignored)
    let record3 = StringRecord::from(vec!["some_field", "V", "value"]);
    extract_capability_metadata(&record3, &mut metadata);
    assert!(!metadata.contains_key("some_field"));
}
