//! Individual CSV record parsing for BADC-CSV files
//!
//! This module handles the parsing of individual observation records,
//! including measurement extraction and quality flag processing.

use csv::StringRecord;
use std::collections::HashMap;
use tracing::debug;

use super::column_mapping::ColumnMapping;
use super::field_parsers::{
    parse_optional_datetime, parse_optional_i32, parse_required_datetime, parse_required_i32,
    parse_required_string,
};
use super::header::SimpleHeader;
use crate::Result;
use crate::app::models::{Observation, ProcessingFlag};
use crate::app::services::station_registry::StationRegistry;

/// Parse a single observation record from CSV data
pub async fn parse_observation_record(
    record: &StringRecord,
    mapping: &ColumnMapping,
    header: &SimpleHeader,
    station_registry: &StationRegistry,
) -> Result<Observation> {
    // Extract required fields
    let ob_end_time = parse_required_datetime(record, mapping, "ob_end_time")?;
    let ob_hour_count = parse_required_i32(record, mapping, "ob_hour_count")?;
    let observation_id = parse_required_string(record, mapping, "id")?;
    let station_id = parse_required_i32(record, mapping, "src_id")?;
    let id_type = parse_required_string(record, mapping, "id_type")?;
    let met_domain_name = parse_required_string(record, mapping, "met_domain_name")?;
    let rec_st_ind = parse_required_i32(record, mapping, "rec_st_ind")?;
    let version_num = parse_required_i32(record, mapping, "version_num")?;

    // Get station metadata from registry (strict requirement)
    let station = station_registry.get_station(station_id)
        .ok_or_else(|| {
            crate::Error::data_validation(format!(
                "Station {} not found in registry. All observations must have valid station metadata.",
                station_id
            ))
        })?
        .clone();

    // Parse optional processing fields (administrative metadata - may be missing in historical data)
    let meto_stmp_time = parse_optional_datetime(record, mapping, "meto_stmp_time");
    let midas_stmp_etime = parse_optional_i32(record, mapping, "midas_stmp_etime");

    // Parse measurements and build processing flags
    let (measurements, measurement_processing_flags) =
        parse_measurements_with_flags(record, mapping, &header.missing_value);

    // Parse quality flags (pass-through only)
    let quality_flags = parse_quality_flags(record, mapping, &header.missing_value);

    // Build complete processing flags map
    let mut processing_flags = measurement_processing_flags;
    processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);
    processing_flags.insert("record".to_string(), ProcessingFlag::Original); // Will be updated by record processor

    // Create and validate observation
    Observation::new(
        ob_end_time,
        ob_hour_count,
        observation_id,
        station_id,
        id_type,
        met_domain_name,
        rec_st_ind,
        version_num,
        station,
        measurements,
        quality_flags,
        processing_flags,
        meto_stmp_time,
        midas_stmp_etime,
    )
}

/// Parse measurements from dynamic columns with processing flags
pub fn parse_measurements_with_flags(
    record: &StringRecord,
    mapping: &ColumnMapping,
    missing_value: &str,
) -> (HashMap<String, f64>, HashMap<String, ProcessingFlag>) {
    let mut measurements = HashMap::new();
    let mut processing_flags = HashMap::new();

    for column_name in &mapping.measurement_columns {
        if let Some(&index) = mapping.name_to_index.get(column_name) {
            if let Some(value_str) = record.get(index) {
                let trimmed = value_str.trim();

                // Handle missing values
                if trimmed == missing_value || trimmed.is_empty() {
                    processing_flags.insert(column_name.clone(), ProcessingFlag::MissingValue);
                    continue;
                }

                // Try to parse as f64
                match trimmed.parse::<f64>() {
                    Ok(value) => {
                        measurements.insert(column_name.clone(), value);
                        processing_flags.insert(column_name.clone(), ProcessingFlag::ParseOk);
                    }
                    Err(_) => {
                        // Log parse failure but continue processing
                        debug!(
                            "Failed to parse measurement '{}' = '{}' as float",
                            column_name, trimmed
                        );
                        processing_flags.insert(column_name.clone(), ProcessingFlag::ParseFailed);
                    }
                }
            } else {
                processing_flags.insert(column_name.clone(), ProcessingFlag::MissingValue);
            }
        }
    }

    (measurements, processing_flags)
}

/// Parse measurements from dynamic columns (legacy function for compatibility)
pub fn parse_measurements(
    record: &StringRecord,
    mapping: &ColumnMapping,
    missing_value: &str,
) -> HashMap<String, f64> {
    let mut measurements = HashMap::new();

    for column_name in &mapping.measurement_columns {
        if let Some(&index) = mapping.name_to_index.get(column_name) {
            if let Some(value_str) = record.get(index) {
                let trimmed = value_str.trim();

                // Skip missing values
                if trimmed == missing_value || trimmed.is_empty() {
                    continue;
                }

                // Try to parse as f64
                match trimmed.parse::<f64>() {
                    Ok(value) => {
                        measurements.insert(column_name.clone(), value);
                    }
                    Err(_) => {
                        // Log parse failure but continue processing
                        debug!(
                            "Failed to parse measurement '{}' = '{}' as float",
                            column_name, trimmed
                        );
                    }
                }
            }
        }
    }

    measurements
}

/// Parse quality flags from _q suffixed columns (pass through raw CSV values)
pub fn parse_quality_flags(
    record: &StringRecord,
    mapping: &ColumnMapping,
    missing_value: &str,
) -> HashMap<String, String> {
    let mut quality_flags = HashMap::new();

    for column_name in &mapping.quality_columns {
        if let Some(&index) = mapping.name_to_index.get(column_name) {
            if let Some(value_str) = record.get(index) {
                let trimmed = value_str.trim();

                // Skip missing values (but store actual empty/missing values)
                if trimmed == missing_value {
                    // Store the missing value marker as-is for downstream processing
                    let measurement_name = column_name
                        .strip_suffix("_q")
                        .unwrap_or(column_name)
                        .to_string();
                    quality_flags.insert(measurement_name, missing_value.to_string());
                } else if !trimmed.is_empty() {
                    // Store non-empty quality flag values as-is
                    let measurement_name = column_name
                        .strip_suffix("_q")
                        .unwrap_or(column_name)
                        .to_string();
                    quality_flags.insert(measurement_name, trimmed.to_string());
                }
                // Skip only truly empty values (empty strings)
            }
        }
    }

    quality_flags
}
