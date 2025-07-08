//! Station record parsing from CSV data
//!
//! This module handles parsing station metadata from different MIDAS file formats:
//! - Capability files (station metadata in BADC header)
//! - Centralized metadata files (station records in data section)

use crate::app::models::{IdPeriod, Station};
use crate::constants::record_status;
use crate::{Error, Result};
use csv::StringRecord;
use std::collections::HashMap;
use std::path::Path;

/// Parse station metadata from BADC capability file header
///
/// Capability files contain station metadata as global attributes in the header section.
/// This function extracts the required fields and creates a Station instance.
pub fn parse_capability_station_metadata(
    metadata: &HashMap<String, String>,
    _file_path: &Path,
) -> Result<Station> {
    // Extract required fields from capability file header
    let src_id: i32 = metadata
        .get("src_id")
        .ok_or_else(|| Error::data_validation("Missing src_id in capability file".to_string()))?
        .parse()
        .map_err(|_| Error::data_validation("Invalid src_id format".to_string()))?;

    let station_name = metadata
        .get("observation_station")
        .ok_or_else(|| {
            Error::data_validation("Missing observation_station in capability file".to_string())
        })?
        .to_string();

    let authority = metadata
        .get("authority")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Met Office".to_string());

    let historic_county = metadata
        .get("historic_county_name")
        .ok_or_else(|| {
            Error::data_validation("Missing historic_county_name in capability file".to_string())
        })?
        .to_string();

    // Parse location (format: "lat,lon")
    let location = metadata
        .get("location")
        .ok_or_else(|| Error::data_validation("Missing location in capability file".to_string()))?;

    let location_parts: Vec<&str> = location.split(',').map(|s| s.trim()).collect();
    if location_parts.len() < 2 {
        return Err(Error::data_validation(format!(
            "Invalid location format in capability file: '{}', expected 'lat,lon'",
            location
        )));
    }

    let high_prcn_lat: f64 = location_parts[0].parse().map_err(|_| {
        Error::data_validation(format!(
            "Invalid latitude '{}' in capability file",
            location_parts[0]
        ))
    })?;

    let high_prcn_lon: f64 = location_parts[1].parse().map_err(|_| {
        Error::data_validation(format!(
            "Invalid longitude '{}' in capability file",
            location_parts[1]
        ))
    })?;

    // Parse height (format: "value,unit" or just "value")
    let height_str = metadata
        .get("height")
        .ok_or_else(|| Error::data_validation("Missing height in capability file".to_string()))?;

    let height_meters: f32 = if height_str.contains(',') {
        height_str.split(',').next().unwrap_or("0").trim()
    } else {
        height_str.trim()
    }
    .parse()
    .map_err(|_| Error::data_validation("Invalid height format in capability file".to_string()))?;

    // Parse date_valid (format: "start_date,end_date")
    let date_valid = metadata.get("date_valid").ok_or_else(|| {
        Error::data_validation("Missing date_valid in capability file".to_string())
    })?;

    let date_parts: Vec<&str> = date_valid.split(',').collect();
    if date_parts.len() != 2 {
        return Err(Error::data_validation(
            "Invalid date_valid format in capability file".to_string(),
        ));
    }

    let src_bgn_date =
        chrono::NaiveDateTime::parse_from_str(date_parts[0].trim(), "%Y-%m-%d %H:%M:%S")
            .map_err(|e| {
                Error::data_validation(format!(
                    "Invalid start date format '{}' in capability file: {}",
                    date_parts[0].trim(),
                    e
                ))
            })?
            .and_utc();

    let src_end_date =
        chrono::NaiveDateTime::parse_from_str(date_parts[1].trim(), "%Y-%m-%d %H:%M:%S")
            .map_err(|e| {
                Error::data_validation(format!(
                    "Invalid end date format '{}' in capability file: {}",
                    date_parts[1].trim(),
                    e
                ))
            })?
            .and_utc();

    // Create station with no grid reference data (capability files don't contain it)
    Station::new(
        src_id,
        station_name,
        high_prcn_lat,
        high_prcn_lon,
        None, // east_grid_ref
        None, // north_grid_ref
        None, // grid_ref_type
        src_bgn_date,
        src_end_date,
        authority,
        historic_county,
        height_meters,
    )
}

/// Parse a station record from CSV fields, filtering by record status
///
/// This function parses station metadata from centralized metadata files
/// where station information is stored as CSV records in the data section.
/// Only processes definitive records (rec_st_ind = 9).
pub fn parse_station_record(
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

    // Helper function to parse authority field, allowing "NA" as a valid value
    let parse_authority = || -> Result<String> {
        fields
            .get("authority")
            .filter(|&val| !val.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| Error::data_validation("Missing required field: authority".to_string()))
    };

    let authority = parse_authority()?;
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

/// Parse a station record from centralized station metadata files
///
/// This function parses station metadata from the centralized metadata files that use
/// a different field naming convention than capability files. These files contain
/// fields like station_name, station_latitude, first_year, last_year instead of
/// the capability file format.
pub fn parse_station_metadata_record(
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

    // Helper function to parse authority field, allowing "NA" as a valid value
    let parse_authority = || -> Result<String> {
        fields
            .get("authority")
            .filter(|&val| !val.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| Error::data_validation("Missing required field: authority".to_string()))
    };

    // Parse station fields using station metadata file format
    let src_id: i32 = parse_required("src_id")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid src_id".to_string()))?;

    let src_name = parse_required("station_name")?;

    let high_prcn_lat: f64 = parse_required("station_latitude")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid station_latitude".to_string()))?;

    let high_prcn_lon: f64 = parse_required("station_longitude")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid station_longitude".to_string()))?;

    // Grid references are not present in station metadata files
    let east_grid_ref = None;
    let north_grid_ref = None;
    let grid_ref_type = None;

    // Convert first_year/last_year to proper DateTime fields
    let first_year: i32 = parse_required("first_year")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid first_year".to_string()))?;

    let last_year: i32 = parse_required("last_year")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid last_year".to_string()))?;

    // Create src_bgn_date and src_end_date from year values
    let src_bgn_date = chrono::NaiveDate::from_ymd_opt(first_year, 1, 1)
        .ok_or_else(|| Error::data_validation(format!("Invalid first_year: {}", first_year)))?
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();

    let src_end_date = chrono::NaiveDate::from_ymd_opt(last_year, 12, 31)
        .ok_or_else(|| Error::data_validation(format!("Invalid last_year: {}", last_year)))?
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .and_utc();

    let authority = parse_authority()?;
    let historic_county = parse_required("historic_county")?;

    let height_meters: f32 = parse_required("station_elevation")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid station_elevation".to_string()))?;

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

/// Extract station metadata from BADC capability file header records
///
/// Processes header records in the format: key,scope,value[,additional_values...]
/// and extracts station-specific global attributes.
pub fn extract_capability_metadata(record: &StringRecord, metadata: &mut HashMap<String, String>) {
    // Parse header records in format: key,scope,value[,additional_values...]
    if record.len() >= 3 {
        let key = record.get(0).unwrap_or("").trim();
        let scope = record.get(1).unwrap_or("").trim();

        // Only collect global (G) attributes that are station metadata
        if scope == "G" {
            // Handle special cases where multiple values are in separate fields
            let value = if key == "location" && record.len() >= 4 {
                // Location has lat,lon in separate fields
                format!(
                    "{},{}",
                    record.get(2).unwrap_or("").trim(),
                    record.get(3).unwrap_or("").trim()
                )
            } else if key == "height" && record.len() >= 4 {
                // Height has value,unit in separate fields
                format!(
                    "{},{}",
                    record.get(2).unwrap_or("").trim(),
                    record.get(3).unwrap_or("").trim()
                )
            } else if key == "date_valid" && record.len() >= 4 {
                // Date range has start,end in separate fields
                format!(
                    "{},{}",
                    record.get(2).unwrap_or("").trim(),
                    record.get(3).unwrap_or("").trim()
                )
            } else {
                // Standard single value case
                record.get(2).unwrap_or("").trim().to_string()
            };

            metadata.insert(key.to_string(), value);
        }
    }
}

/// Parse ID evolution record from capability file data section
///
/// Capability files contain observation ID evolution data in their data section
/// with columns like: id,id_type,met_domain_name,first_year,last_year
pub fn parse_id_evolution_record(
    record: &StringRecord,
    headers: &StringRecord,
) -> Result<IdPeriod> {
    // Create mapping from header names to column indices
    let mut header_map = HashMap::new();
    for (i, header) in headers.iter().enumerate() {
        header_map.insert(header.trim().to_lowercase(), i);
    }

    // Extract required fields
    let observation_id = get_field_by_name(record, &header_map, "id")?;
    let id_type = get_field_by_name(record, &header_map, "id_type")?;
    let met_domain_name = get_field_by_name(record, &header_map, "met_domain_name")?;

    let first_year: i32 = get_field_by_name(record, &header_map, "first_year")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid first_year format".to_string()))?;

    let last_year: i32 = get_field_by_name(record, &header_map, "last_year")?
        .parse()
        .map_err(|_| Error::data_validation("Invalid last_year format".to_string()))?;

    // Validate year range
    if first_year > last_year {
        return Err(Error::data_validation(format!(
            "Invalid year range: first_year ({}) > last_year ({})",
            first_year, last_year
        )));
    }

    // Create IdPeriod
    let id_period = IdPeriod::new(
        observation_id,
        id_type,
        met_domain_name,
        first_year,
        last_year,
    );

    Ok(id_period)
}

/// Helper function to get field value by name from record
fn get_field_by_name(
    record: &StringRecord,
    header_map: &HashMap<String, usize>,
    field_name: &str,
) -> Result<String> {
    let index = header_map
        .get(&field_name.to_lowercase())
        .ok_or_else(|| Error::data_validation(format!("Missing required field: {}", field_name)))?;

    let value = record
        .get(*index)
        .ok_or_else(|| Error::data_validation(format!("Missing value for field: {}", field_name)))?
        .trim()
        .to_string();

    if value.is_empty() {
        return Err(Error::data_validation(format!(
            "Empty value for required field: {}",
            field_name
        )));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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

        // Test height with value and unit
        let record3 = StringRecord::from(vec!["height", "G", "25.0", "m"]);
        extract_capability_metadata(&record3, &mut metadata);
        assert_eq!(metadata.get("height"), Some(&"25.0,m".to_string()));

        // Test non-global attribute (should be ignored)
        let record4 = StringRecord::from(vec!["some_field", "V", "value"]);
        extract_capability_metadata(&record4, &mut metadata);
        assert!(!metadata.contains_key("some_field"));
    }

    #[test]
    fn test_parse_station_record_missing_required_field() {
        let headers = StringRecord::from(vec!["src_id", "rec_st_ind"]);
        let record = StringRecord::from(vec!["12345", "9"]);

        let result = parse_station_record(&record, &headers);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::DataValidation { message } => {
                assert!(message.contains("Missing required field"));
            }
            _ => panic!("Expected DataValidation error"),
        }
    }

    #[test]
    fn test_parse_capability_missing_required_field() {
        let mut metadata = HashMap::new();
        metadata.insert("src_id".to_string(), "12345".to_string());
        // Missing observation_station

        let result = parse_capability_station_metadata(&metadata, Path::new("test.csv"));
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::DataValidation { message } => {
                assert!(message.contains("Missing observation_station"));
            }
            _ => panic!("Expected DataValidation error"),
        }
    }

    #[test]
    fn test_parse_station_metadata_record_with_na_authority() {
        let headers = StringRecord::from(vec![
            "src_id",
            "station_name",
            "station_latitude",
            "station_longitude",
            "first_year",
            "last_year",
            "authority",
            "historic_county",
            "station_elevation",
        ]);

        let record = StringRecord::from(vec![
            "12345",
            "TEST_STATION",
            "51.4778",
            "-0.4614",
            "2000",
            "2050",
            "NA", // NA authority value
            "Greater London",
            "25.0",
        ]);

        let result = parse_station_metadata_record(&record, &headers).unwrap();
        assert!(result.is_some());

        let station = result.unwrap();
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "TEST_STATION");
        assert_eq!(station.authority, "NA"); // NA should be preserved as valid
        assert_eq!(station.historic_county, "Greater London");
    }

    #[test]
    fn test_parse_station_metadata_record_with_empty_authority() {
        let headers = StringRecord::from(vec![
            "src_id",
            "station_name",
            "station_latitude",
            "station_longitude",
            "first_year",
            "last_year",
            "authority",
            "historic_county",
            "station_elevation",
        ]);

        let record = StringRecord::from(vec![
            "12345",
            "TEST_STATION",
            "51.4778",
            "-0.4614",
            "2000",
            "2050",
            "", // Empty authority value
            "Greater London",
            "25.0",
        ]);

        let result = parse_station_metadata_record(&record, &headers);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::DataValidation { message } => {
                assert!(message.contains("Missing required field: authority"));
            }
            _ => panic!("Expected DataValidation error"),
        }
    }

    #[test]
    fn test_parse_station_metadata_record_with_normal_authority() {
        let headers = StringRecord::from(vec![
            "src_id",
            "station_name",
            "station_latitude",
            "station_longitude",
            "first_year",
            "last_year",
            "authority",
            "historic_county",
            "station_elevation",
        ]);

        let record = StringRecord::from(vec![
            "12345",
            "TEST_STATION",
            "51.4778",
            "-0.4614",
            "2000",
            "2050",
            "Met Office", // Normal authority value
            "Greater London",
            "25.0",
        ]);

        let result = parse_station_metadata_record(&record, &headers).unwrap();
        assert!(result.is_some());

        let station = result.unwrap();
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "TEST_STATION");
        assert_eq!(station.authority, "Met Office");
        assert_eq!(station.historic_county, "Greater London");
    }

    #[test]
    fn test_parse_station_record_with_na_authority() {
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
            "NA", // NA authority value
            "Greater London",
            "25.0",
            "9",
        ]);

        let result = parse_station_record(&record, &headers).unwrap();
        assert!(result.is_some());

        let station = result.unwrap();
        assert_eq!(station.src_id, 12345);
        assert_eq!(station.src_name, "TEST_STATION");
        assert_eq!(station.authority, "NA"); // NA should be preserved as valid
        assert_eq!(station.historic_county, "Greater London");
    }
}
