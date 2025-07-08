//! Data models for MIDAS processing
//!
//! This module contains the core data structures for representing MIDAS weather station
//! metadata and observation records, following the UK Met Office MIDAS specification.

pub mod audit;

use crate::constants::{self, quality_flags, record_status};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

// =============================================================================
// Processing Quality Control Framework
// =============================================================================

/// Processing quality flags for tracking parsing and processing operations
///
/// These flags track what happened during our processing pipeline and are completely
/// separate from the original MIDAS quality control flags which are passed through
/// without interpretation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProcessingFlag {
    /// Value was successfully parsed and converted
    ParseOk,
    /// Value could not be parsed (e.g., "abc" as a number)
    ParseFailed,
    /// Value was missing ("NA" or empty in CSV)
    MissingValue,
    /// Station metadata was found and applied
    StationFound,
    /// Station metadata was missing, placeholder used
    StationMissing,
    /// Record kept as original (no duplicates found)
    Original,
    /// Record was superseded by a higher priority duplicate
    Superseded,
    /// Record was selected from multiple duplicates by processing rules
    DuplicateResolved,
}

impl ProcessingFlag {
    /// Check if this flag indicates a successful operation
    /// Missing values are considered normal/successful since they're common in scientific data
    pub fn is_success(self) -> bool {
        matches!(
            self,
            ProcessingFlag::ParseOk
                | ProcessingFlag::MissingValue  // Missing values are normal, not errors
                | ProcessingFlag::StationFound
                | ProcessingFlag::Original
                | ProcessingFlag::DuplicateResolved
        )
    }

    /// Get human-readable description of this processing flag
    pub fn description(self) -> &'static str {
        match self {
            ProcessingFlag::ParseOk => "Successfully parsed and converted",
            ProcessingFlag::ParseFailed => "Failed to parse value",
            ProcessingFlag::MissingValue => "Value was missing or empty",
            ProcessingFlag::StationFound => "Station metadata found and applied",
            ProcessingFlag::StationMissing => "Station metadata missing, placeholder used",
            ProcessingFlag::Original => "Original record, no duplicates",
            ProcessingFlag::Superseded => "Superseded by higher priority record",
            ProcessingFlag::DuplicateResolved => "Selected from multiple duplicates",
        }
    }
}

impl std::fmt::Display for ProcessingFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

// =============================================================================
// Station Metadata Structure
// =============================================================================

/// Station metadata structure containing all MIDAS station information
///
/// This structure represents a weather station with its complete metadata
/// including location, operational dates, and administrative information.
/// Based on the MIDAS Source Capability specification.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Station {
    /// Unique source identifier - Primary key for station lookups
    pub src_id: i32,

    /// Human-readable station name (e.g., "HEATHROW", "BIRMINGHAM")
    pub src_name: String,

    /// High-precision latitude in WGS84 decimal degrees
    pub high_prcn_lat: f64,

    /// High-precision longitude in WGS84 decimal degrees  
    pub high_prcn_lon: f64,

    /// UK grid reference - eastings coordinate (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub east_grid_ref: Option<i32>,

    /// UK grid reference - northings coordinate (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub north_grid_ref: Option<i32>,

    /// Grid reference system type (e.g., "OSGB") (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grid_ref_type: Option<String>,

    /// Station operational start date
    pub src_bgn_date: DateTime<Utc>,

    /// Station operational end date (far future indicates still active)
    pub src_end_date: DateTime<Utc>,

    /// Operating authority (e.g., "Met Office", "Environment Agency")
    pub authority: String,

    /// Historic county name for administrative context
    pub historic_county: String,

    /// Station elevation above sea level in meters
    pub height_meters: f32,
}

impl Station {
    /// Create a new Station with validation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        src_id: i32,
        src_name: String,
        high_prcn_lat: f64,
        high_prcn_lon: f64,
        east_grid_ref: Option<i32>,
        north_grid_ref: Option<i32>,
        grid_ref_type: Option<String>,
        src_bgn_date: DateTime<Utc>,
        src_end_date: DateTime<Utc>,
        authority: String,
        historic_county: String,
        height_meters: f32,
    ) -> Result<Self> {
        let station = Self {
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
        };

        station.validate()?;
        Ok(station)
    }

    /// Validate station data for consistency and valid ranges
    pub fn validate(&self) -> Result<()> {
        // Validate latitude range
        if !(-90.0..=90.0).contains(&self.high_prcn_lat) {
            return Err(Error::data_validation(format!(
                "Invalid latitude {}: must be between -90 and 90 degrees",
                self.high_prcn_lat
            )));
        }

        // Validate longitude range
        if !(-180.0..=180.0).contains(&self.high_prcn_lon) {
            return Err(Error::data_validation(format!(
                "Invalid longitude {}: must be between -180 and 180 degrees",
                self.high_prcn_lon
            )));
        }

        // Validate date consistency
        if self.src_bgn_date > self.src_end_date {
            return Err(Error::data_validation(format!(
                "Station start date {} cannot be after end date {}",
                self.src_bgn_date, self.src_end_date
            )));
        }

        // Validate required fields are not empty
        if self.src_name.trim().is_empty() {
            return Err(Error::data_validation(
                "Station name cannot be empty".to_string(),
            ));
        }

        if self.authority.trim().is_empty() {
            return Err(Error::data_validation(
                "Authority cannot be empty".to_string(),
            ));
        }

        if self.historic_county.trim().is_empty() {
            return Err(Error::data_validation(
                "Historic county cannot be empty".to_string(),
            ));
        }

        // Validate grid references are paired if provided
        match (self.east_grid_ref, self.north_grid_ref) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(Error::data_validation(
                    "Grid references must be provided as a pair (both east and north)".to_string(),
                ));
            }
            _ => {} // Both Some or both None is valid
        }

        // Validate against placeholder/fake data patterns
        if self.src_name.contains("UNKNOWN") || self.src_name.contains("PLACEHOLDER") {
            return Err(Error::data_validation(format!(
                "Station name '{}' appears to be placeholder data",
                self.src_name
            )));
        }

        if self.authority.contains("UNKNOWN") || self.authority.contains("PLACEHOLDER") {
            return Err(Error::data_validation(format!(
                "Station authority '{}' appears to be placeholder data",
                self.authority
            )));
        }

        if self.historic_county.contains("UNKNOWN") || self.historic_county.contains("PLACEHOLDER")
        {
            return Err(Error::data_validation(format!(
                "Station historic county '{}' appears to be placeholder data",
                self.historic_county
            )));
        }

        // Validate coordinates are not suspicious (0,0) or other placeholder values
        if self.high_prcn_lat == 0.0 && self.high_prcn_lon == 0.0 {
            return Err(Error::data_validation(
                "Station coordinates (0.0, 0.0) appear to be placeholder data".to_string(),
            ));
        }

        // Additional validation for clearly invalid coordinates
        if self.high_prcn_lat.abs() < 1e-6 && self.high_prcn_lon.abs() < 1e-6 {
            return Err(Error::data_validation(
                "Station coordinates appear to be placeholder data (too close to origin)"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Check if the station is currently operational
    pub fn is_active(&self, at_time: DateTime<Utc>) -> bool {
        at_time >= self.src_bgn_date && at_time <= self.src_end_date
    }

    /// Check if the station was operational during a time period
    pub fn was_active_during(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        // Station was active if there's any overlap between operational period and query period
        self.src_bgn_date <= end && self.src_end_date >= start
    }

    /// Get station location as (latitude, longitude) tuple
    pub fn location(&self) -> (f64, f64) {
        (self.high_prcn_lat, self.high_prcn_lon)
    }

    /// Get UK grid reference if available
    pub fn grid_reference(&self) -> Option<(i32, i32, &str)> {
        match (self.east_grid_ref, self.north_grid_ref, &self.grid_ref_type) {
            (Some(east), Some(north), Some(grid_type)) => Some((east, north, grid_type)),
            _ => None,
        }
    }
}

// =============================================================================
// Observation Record Structure
// =============================================================================

/// Observation record containing weather measurements and metadata
///
/// This structure represents a single observation record from a weather station,
/// including temporal information, station reference, measurements, and quality control data.
/// The structure uses dynamic fields to accommodate different dataset types.
#[derive(Debug, Clone)]
pub struct Observation {
    // Temporal information
    /// End time of the observation period
    pub ob_end_time: DateTime<Utc>,

    /// Duration of observation period in hours
    pub ob_hour_count: i32,

    // Station and observation identifiers
    /// Observation series identifier (from 'id' column in MIDAS data)
    /// Can be alphanumeric (e.g., "9167Z" in historical data) or numeric (e.g., "3253" in modern data)
    pub observation_id: String,

    /// Station registry identifier (from 'src_id' column, used for station lookup)
    pub station_id: i32,

    /// Identifier type (usually "SRCE" for source-based lookup)
    pub id_type: String,

    // Record metadata
    /// Dataset identifier (e.g., "UK-DAILY-TEMPERATURE-OBS")
    pub met_domain_name: String,

    /// Record status indicator (1 = corrected, 9 = original)
    pub rec_st_ind: i32,

    /// Quality control version number
    pub version_num: i32,

    // Station metadata (denormalized for no-join access)
    /// Complete station metadata for this observation
    pub station: Station,

    // Dynamic measurements (varies by dataset)
    /// Weather measurements as name-value pairs
    pub measurements: HashMap<String, f64>,

    /// Quality control flags for each measurement (raw values from CSV)
    /// These are passed through exactly as received without interpretation
    pub quality_flags: HashMap<String, String>,

    /// Processing quality flags tracking what happened during parsing/processing
    /// These track our processing operations and are separate from original MIDAS QC flags
    pub processing_flags: HashMap<String, ProcessingFlag>,

    // Processing metadata (optional - may be missing in historical data)
    /// Met Office processing timestamp (None if missing in historical data)
    pub meto_stmp_time: Option<DateTime<Utc>>,

    /// MIDAS processing elapsed time indicator (None if missing in historical data)
    pub midas_stmp_etime: Option<i32>,
}

impl Observation {
    /// Create a new observation with validation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ob_end_time: DateTime<Utc>,
        ob_hour_count: i32,
        observation_id: String,
        station_id: i32,
        id_type: String,
        met_domain_name: String,
        rec_st_ind: i32,
        version_num: i32,
        station: Station,
        measurements: HashMap<String, f64>,
        quality_flags: HashMap<String, String>,
        processing_flags: HashMap<String, ProcessingFlag>,
        meto_stmp_time: Option<DateTime<Utc>>,
        midas_stmp_etime: Option<i32>,
    ) -> Result<Self> {
        let observation = Self {
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
        };

        observation.validate()?;
        Ok(observation)
    }

    /// Validate observation data for consistency
    pub fn validate(&self) -> Result<()> {
        // Validate station_id matches the loaded station's src_id
        if self.station_id != self.station.src_id {
            return Err(Error::data_validation(format!(
                "Observation station_id {} does not match loaded station src_id {}",
                self.station_id, self.station.src_id
            )));
        }

        // Note: observation_id and station_id can be different - this is valid in MIDAS
        // observation_id identifies the measurement series, station_id identifies the station

        // Note: rec_st_ind (record status indicator) is a MIDAS quality control flag
        // All MIDAS QC flags are passed through without interpretation per directive
        // to avoid code complexity and maintenance issues

        // Validate observation hour count is positive
        if self.ob_hour_count <= 0 {
            return Err(Error::data_validation(format!(
                "Observation hour count {} must be positive",
                self.ob_hour_count
            )));
        }

        // Note: version_num is a MIDAS quality indicator that is preserved without validation
        // All MIDAS quality indicators are passed through according to the pass-through directive

        // Validate observation time is within station operational period
        if !self.station.is_active(self.ob_end_time) {
            return Err(Error::data_validation(format!(
                "Observation time {} is outside station operational period ({} to {})",
                self.ob_end_time, self.station.src_bgn_date, self.station.src_end_date
            )));
        }

        // Validate required fields
        if self.observation_id.trim().is_empty() {
            return Err(Error::data_validation(
                "Observation ID cannot be empty".to_string(),
            ));
        }

        if self.id_type.trim().is_empty() {
            return Err(Error::data_validation(
                "ID type cannot be empty".to_string(),
            ));
        }

        if self.met_domain_name.trim().is_empty() {
            return Err(Error::data_validation(
                "Met domain name cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Get a measurement value by name
    pub fn get_measurement(&self, name: &str) -> Option<f64> {
        self.measurements.get(name).copied()
    }

    /// Get a quality flag by measurement name (raw CSV value)
    pub fn get_quality_flag(&self, measurement_name: &str) -> Option<&str> {
        self.quality_flags.get(measurement_name).map(|s| s.as_str())
    }

    /// Check if a measurement has a quality flag (quality interpretation left to downstream processing)
    pub fn has_quality_flag(&self, measurement_name: &str) -> bool {
        self.quality_flags.contains_key(measurement_name)
    }

    /// Get all measurements (quality interpretation left to downstream processing)
    pub fn get_all_measurements(&self) -> &HashMap<String, f64> {
        &self.measurements
    }

    /// Get a processing flag by measurement or operation name
    pub fn get_processing_flag(&self, name: &str) -> Option<ProcessingFlag> {
        self.processing_flags.get(name).copied()
    }

    /// Set a processing flag for a measurement or operation
    pub fn set_processing_flag(&mut self, name: String, flag: ProcessingFlag) {
        self.processing_flags.insert(name, flag);
    }

    /// Check if any processing flags indicate failures
    pub fn has_processing_errors(&self) -> bool {
        self.processing_flags
            .values()
            .any(|flag| !flag.is_success())
    }

    /// Get all processing flags
    pub fn get_all_processing_flags(&self) -> &HashMap<String, ProcessingFlag> {
        &self.processing_flags
    }

    /// Check if this observation supersedes another (based on rec_st_ind)
    pub fn supersedes(&self, other: &Observation) -> bool {
        // Same observation series, station, and time
        if self.observation_id == other.observation_id
            && self.station_id == other.station_id
            && self.ob_end_time == other.ob_end_time
        {
            // Corrected (1) supersedes original (9)
            self.rec_st_ind == record_status::CORRECTED as i32
                && other.rec_st_ind == record_status::ORIGINAL as i32
        } else {
            false
        }
    }
}

// =============================================================================
// Quality Flag Enumeration
// =============================================================================

/// Quality control flag values for MIDAS observations
///
/// These flags indicate the quality assessment status of individual measurements
/// according to the UK Met Office quality control procedures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i8)]
pub enum QualityFlag {
    /// Data passed all quality control checks - highest quality
    Valid = quality_flags::VALID,

    /// Data failed at least one quality control check - use with caution
    Suspect = quality_flags::SUSPECT,

    /// Data considered erroneous - should not be used for most purposes
    Erroneous = quality_flags::ERRONEOUS,

    /// No quality control has been applied to this data
    NotChecked = quality_flags::NOT_CHECKED,

    /// Value reverted to original after previous modifications
    Reverted = quality_flags::REVERTED,

    /// No quality information available (missing data)
    Missing = quality_flags::MISSING,
}

impl QualityFlag {
    /// Check if this quality flag represents usable data
    pub fn is_usable(self, include_suspect: bool, include_unchecked: bool) -> bool {
        constants::is_usable_quality(self as i8, include_suspect, include_unchecked)
    }

    /// Get human-readable description of this quality flag
    pub fn description(self) -> &'static str {
        constants::quality_flag_description(self as i8)
    }

    /// Get all possible quality flag values
    pub fn all_values() -> [QualityFlag; 6] {
        [
            QualityFlag::Valid,
            QualityFlag::Suspect,
            QualityFlag::Erroneous,
            QualityFlag::NotChecked,
            QualityFlag::Reverted,
            QualityFlag::Missing,
        ]
    }
}

impl FromStr for QualityFlag {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let trimmed = s.trim();

        // First try basic quality flags
        match trimmed {
            "0" => Ok(QualityFlag::Valid),
            "1" => Ok(QualityFlag::Suspect),
            "2" => Ok(QualityFlag::Erroneous),
            "3" => Ok(QualityFlag::NotChecked),
            "6" => Ok(QualityFlag::Reverted),
            "9" => Ok(QualityFlag::Missing),
            _ => {
                // For multi-digit MIDAS quality flags (MESQL format), parse as integer
                if let Ok(value) = trimmed.parse::<i32>() {
                    // Multi-digit quality flags are legitimate MIDAS MESQL codes
                    // Map them to appropriate basic quality flags based on the most significant quality indicator

                    // For MESQL format, extract status information intelligently
                    if value >= 1000 {
                        // 4+ digit codes: likely MESQL format with level indicator
                        let level = value % 10; // Last digit is qc_level
                        match level {
                            9 => Ok(QualityFlag::Valid),      // Level 9 = normal processing complete
                            0 => Ok(QualityFlag::NotChecked), // Level 0 = no processing
                            _ => Ok(QualityFlag::Valid),      // Other levels = some processing
                        }
                    } else if value >= 100 {
                        // 3-digit codes: likely method + estimate + status
                        let status = value % 10; // Last digit might be status
                        match status {
                            0 => Ok(QualityFlag::Valid),   // Status 0 often valid
                            1 => Ok(QualityFlag::Suspect), // Status 1 often suspect
                            _ => Ok(QualityFlag::Valid),   // Default to valid for processed data
                        }
                    } else {
                        // 2-digit codes: treat based on first digit
                        let first_digit = value / 10;
                        match first_digit {
                            0..=3 => Ok(QualityFlag::Valid),   // Low values = good quality
                            4..=5 => Ok(QualityFlag::Suspect), // Mid values = suspect
                            _ => Ok(QualityFlag::Valid),       // Default to valid
                        }
                    }
                } else {
                    Err(Error::data_validation(format!(
                        "Invalid quality flag value '{}': must be a valid integer",
                        trimmed
                    )))
                }
            }
        }
    }
}

impl TryFrom<i8> for QualityFlag {
    type Error = Error;

    fn try_from(value: i8) -> Result<Self> {
        match value {
            quality_flags::VALID => Ok(QualityFlag::Valid),
            quality_flags::SUSPECT => Ok(QualityFlag::Suspect),
            quality_flags::ERRONEOUS => Ok(QualityFlag::Erroneous),
            quality_flags::NOT_CHECKED => Ok(QualityFlag::NotChecked),
            quality_flags::REVERTED => Ok(QualityFlag::Reverted),
            quality_flags::MISSING => Ok(QualityFlag::Missing),
            _ => {
                // For extended quality flags, treat as legitimate MIDAS codes
                // Note: i8 can only handle values -128 to 127, so multi-digit codes won't reach here
                // This handles any single-digit or small multi-digit values within i8 range
                Ok(QualityFlag::Valid) // Default to valid for any other legitimate values
            }
        }
    }
}

impl From<QualityFlag> for i8 {
    fn from(flag: QualityFlag) -> Self {
        flag as i8
    }
}

impl std::fmt::Display for QualityFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as i8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // Test data helpers
    fn create_test_station() -> Station {
        Station {
            src_id: 12345,
            src_name: "TEST STATION".to_string(),
            high_prcn_lat: 51.5074,
            high_prcn_lon: -0.1278,
            east_grid_ref: Some(530000),
            north_grid_ref: Some(180000),
            grid_ref_type: Some("OSGB".to_string()),
            src_bgn_date: Utc.with_ymd_and_hms(1800, 1, 1, 0, 0, 0).unwrap(),
            src_end_date: Utc.with_ymd_and_hms(2100, 12, 31, 23, 59, 59).unwrap(),
            authority: "Met Office".to_string(),
            historic_county: "Greater London".to_string(),
            height_meters: 25.0,
        }
    }

    fn create_test_observation() -> Observation {
        let station = create_test_station();
        let mut measurements = HashMap::new();
        measurements.insert("air_temperature".to_string(), 15.5);
        measurements.insert("humidity".to_string(), 75.0);

        let mut quality_flags = HashMap::new();
        quality_flags.insert("air_temperature".to_string(), "0".to_string());
        quality_flags.insert("humidity".to_string(), "1".to_string());

        let mut processing_flags = HashMap::new();
        processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
        processing_flags.insert("humidity".to_string(), ProcessingFlag::ParseOk);
        processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

        Observation {
            ob_end_time: Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
            ob_hour_count: 1,
            observation_id: "12345".to_string(),
            station_id: 12345,
            id_type: constants::ID_TYPE_SOURCE.to_string(),
            met_domain_name: "UK-DAILY-TEMPERATURE-OBS".to_string(),
            rec_st_ind: record_status::ORIGINAL as i32,
            version_num: 1,
            station,
            measurements,
            quality_flags,
            processing_flags,
            meto_stmp_time: Some(Utc.with_ymd_and_hms(2023, 6, 15, 13, 0, 0).unwrap()),
            midas_stmp_etime: Some(3600),
        }
    }

    mod station_tests {
        use super::*;

        #[test]
        fn test_station_creation_valid() {
            let station = create_test_station();
            assert_eq!(station.src_id, 12345);
            assert_eq!(station.src_name, "TEST STATION");
            assert!(station.validate().is_ok());
        }

        #[test]
        fn test_station_coordinate_validation() {
            let mut station = create_test_station();

            // Test invalid latitude
            station.high_prcn_lat = 95.0;
            assert!(station.validate().is_err());

            station.high_prcn_lat = -95.0;
            assert!(station.validate().is_err());

            // Test invalid longitude
            station.high_prcn_lat = 51.5074; // Reset to valid
            station.high_prcn_lon = 185.0;
            assert!(station.validate().is_err());

            station.high_prcn_lon = -185.0;
            assert!(station.validate().is_err());
        }

        #[test]
        fn test_station_date_validation() {
            let mut station = create_test_station();

            // Start date after end date should fail
            station.src_end_date = Utc.with_ymd_and_hms(1750, 12, 31, 23, 59, 59).unwrap();
            assert!(station.validate().is_err());
        }

        #[test]
        fn test_station_required_fields() {
            let mut station = create_test_station();

            // Empty station name
            station.src_name = "".to_string();
            assert!(station.validate().is_err());

            // Empty authority
            station.src_name = "TEST STATION".to_string();
            station.authority = "".to_string();
            assert!(station.validate().is_err());

            // Empty county
            station.authority = "Met Office".to_string();
            station.historic_county = "".to_string();
            assert!(station.validate().is_err());
        }

        #[test]
        fn test_station_grid_reference_pairing() {
            let mut station = create_test_station();

            // Only east reference provided - should fail
            station.north_grid_ref = None;
            assert!(station.validate().is_err());

            // Only north reference provided - should fail
            station.east_grid_ref = None;
            station.north_grid_ref = Some(180000);
            assert!(station.validate().is_err());

            // Both None - should pass
            station.east_grid_ref = None;
            station.north_grid_ref = None;
            assert!(station.validate().is_ok());
        }

        #[test]
        fn test_station_activity_checks() {
            let station = create_test_station();

            // Test point in operational period
            let test_time = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();
            assert!(station.is_active(test_time));

            // Test point before operational period
            let before_time = Utc.with_ymd_and_hms(1750, 1, 1, 0, 0, 0).unwrap();
            assert!(!station.is_active(before_time));

            // Test period overlap
            let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
            let end = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
            assert!(station.was_active_during(start, end));
        }

        #[test]
        fn test_station_location_methods() {
            let station = create_test_station();

            // Test location tuple
            let (lat, lon) = station.location();
            assert_eq!(lat, 51.5074);
            assert_eq!(lon, -0.1278);

            // Test grid reference
            let grid_ref = station.grid_reference().unwrap();
            assert_eq!(grid_ref, (530000, 180000, "OSGB"));
        }

        #[test]
        fn test_station_placeholder_detection() {
            let mut station = create_test_station();

            // Test placeholder name detection
            station.src_name = "UNKNOWN_STATION_123".to_string();
            assert!(station.validate().is_err());

            // Test placeholder authority detection
            station.src_name = "TEST STATION".to_string();
            station.authority = "UNKNOWN".to_string();
            assert!(station.validate().is_err());

            // Test placeholder county detection
            station.authority = "Met Office".to_string();
            station.historic_county = "UNKNOWN".to_string();
            assert!(station.validate().is_err());

            // Test placeholder coordinates detection
            station.historic_county = "Greater London".to_string();
            station.high_prcn_lat = 0.0;
            station.high_prcn_lon = 0.0;
            assert!(station.validate().is_err());

            // Test near-zero coordinates detection
            station.high_prcn_lat = 0.0000001;
            station.high_prcn_lon = 0.0000001;
            assert!(station.validate().is_err());
        }
    }

    mod observation_tests {
        use super::*;

        #[test]
        fn test_observation_creation_valid() {
            let observation = create_test_observation();
            assert!(observation.validate().is_ok());
            assert_eq!(observation.observation_id, "12345");
            assert_eq!(observation.measurements.len(), 2);
            assert_eq!(observation.quality_flags.len(), 2);
        }

        #[test]
        fn test_observation_station_id_mismatch() {
            let mut observation = create_test_observation();
            observation.station_id = 99999; // Different from station.src_id
            assert!(observation.validate().is_err());
        }

        #[test]
        fn test_observation_rec_st_ind_passthrough() {
            // Test that rec_st_ind values are passed through without validation
            // per MIDAS QC pass-through directive to avoid code complexity
            let mut observation = create_test_observation();
            observation.rec_st_ind = 1023; // Previously "invalid" value now accepted
            assert!(observation.validate().is_ok());

            observation.rec_st_ind = 5; // Any value should be accepted
            assert!(observation.validate().is_ok());

            observation.rec_st_ind = 9999; // Including unknown values
            assert!(observation.validate().is_ok());
        }

        #[test]
        fn test_observation_invalid_hour_count() {
            let mut observation = create_test_observation();
            observation.ob_hour_count = -1; // Negative hour count
            assert!(observation.validate().is_err());
        }

        #[test]
        fn test_observation_time_outside_station_period() {
            let mut observation = create_test_observation();
            observation.ob_end_time = Utc.with_ymd_and_hms(1750, 1, 1, 0, 0, 0).unwrap();
            assert!(observation.validate().is_err());
        }

        #[test]
        fn test_observation_measurement_access() {
            let observation = create_test_observation();

            // Test measurement retrieval
            assert_eq!(observation.get_measurement("air_temperature"), Some(15.5));
            assert_eq!(observation.get_measurement("nonexistent"), None);

            // Test quality flag retrieval (raw CSV values)
            assert_eq!(observation.get_quality_flag("air_temperature"), Some("0"));
            assert_eq!(observation.get_quality_flag("humidity"), Some("1"));
        }

        #[test]
        fn test_observation_all_measurements() {
            let observation = create_test_observation();

            // Get all measurements (quality interpretation left to downstream)
            let all_measurements = observation.get_all_measurements();
            assert_eq!(all_measurements.len(), 2); // Both measurements present
            assert!(all_measurements.contains_key("air_temperature"));
            assert!(all_measurements.contains_key("humidity"));
        }

        #[test]
        fn test_observation_supersedes() {
            let mut obs1 = create_test_observation();
            let mut obs2 = create_test_observation();

            // Same station and time, one is corrected
            obs1.rec_st_ind = record_status::CORRECTED as i32;
            obs2.rec_st_ind = record_status::ORIGINAL as i32;

            assert!(obs1.supersedes(&obs2));
            assert!(!obs2.supersedes(&obs1));

            // Different station
            obs2.observation_id = "99999".to_string();
            assert!(!obs1.supersedes(&obs2));
        }

        #[test]
        fn test_observation_with_missing_administrative_fields() {
            // Test creating observation with None administrative timestamp fields
            let station = create_test_station();
            let mut measurements = HashMap::new();
            measurements.insert("air_temperature".to_string(), 15.5);

            let mut quality_flags = HashMap::new();
            quality_flags.insert("air_temperature".to_string(), "0".to_string());

            let mut processing_flags = HashMap::new();
            processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
            processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

            let observation = Observation::new(
                Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
                1,
                "test_obs".to_string(),
                12345,
                "SRCE".to_string(),
                "UK-DAILY-TEMPERATURE-OBS".to_string(),
                record_status::ORIGINAL as i32,
                1,
                station,
                measurements,
                quality_flags,
                processing_flags,
                None, // Missing meto_stmp_time
                None, // Missing midas_stmp_etime
            );

            assert!(observation.is_ok());
            let obs = observation.unwrap();
            assert_eq!(obs.meto_stmp_time, None);
            assert_eq!(obs.midas_stmp_etime, None);
            assert!(obs.validate().is_ok());
        }

        #[test]
        fn test_observation_with_present_administrative_fields() {
            // Test creating observation with Some administrative timestamp fields
            let station = create_test_station();
            let mut measurements = HashMap::new();
            measurements.insert("air_temperature".to_string(), 15.5);

            let mut quality_flags = HashMap::new();
            quality_flags.insert("air_temperature".to_string(), "0".to_string());

            let mut processing_flags = HashMap::new();
            processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
            processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

            let meto_time = Utc.with_ymd_and_hms(2023, 6, 15, 13, 30, 0).unwrap();
            let midas_etime = 7200;

            let observation = Observation::new(
                Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
                1,
                "test_obs".to_string(),
                12345,
                "SRCE".to_string(),
                "UK-DAILY-TEMPERATURE-OBS".to_string(),
                record_status::ORIGINAL as i32,
                1,
                station,
                measurements,
                quality_flags,
                processing_flags,
                Some(meto_time),   // Present meto_stmp_time
                Some(midas_etime), // Present midas_stmp_etime
            );

            assert!(observation.is_ok());
            let obs = observation.unwrap();
            assert_eq!(obs.meto_stmp_time, Some(meto_time));
            assert_eq!(obs.midas_stmp_etime, Some(midas_etime));
            assert!(obs.validate().is_ok());
        }

        #[test]
        fn test_observation_mixed_administrative_fields() {
            // Test creating observation with only one administrative field present
            let station = create_test_station();
            let mut measurements = HashMap::new();
            measurements.insert("air_temperature".to_string(), 15.5);

            let mut quality_flags = HashMap::new();
            quality_flags.insert("air_temperature".to_string(), "0".to_string());

            let mut processing_flags = HashMap::new();
            processing_flags.insert("air_temperature".to_string(), ProcessingFlag::ParseOk);
            processing_flags.insert("station".to_string(), ProcessingFlag::StationFound);

            let meto_time = Utc.with_ymd_and_hms(2023, 6, 15, 13, 30, 0).unwrap();

            let observation = Observation::new(
                Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
                1,
                "test_obs".to_string(),
                12345,
                "SRCE".to_string(),
                "UK-DAILY-TEMPERATURE-OBS".to_string(),
                record_status::ORIGINAL as i32,
                1,
                station,
                measurements,
                quality_flags,
                processing_flags,
                Some(meto_time), // Present meto_stmp_time
                None,            // Missing midas_stmp_etime
            );

            assert!(observation.is_ok());
            let obs = observation.unwrap();
            assert_eq!(obs.meto_stmp_time, Some(meto_time));
            assert_eq!(obs.midas_stmp_etime, None);
            assert!(obs.validate().is_ok());
        }

        #[test]
        fn test_observation_core_validation_still_strict() {
            // Ensure that making administrative fields optional doesn't affect core validation
            let station = create_test_station();
            let measurements = HashMap::new(); // Empty measurements OK
            let quality_flags = HashMap::new();
            let processing_flags = HashMap::new();

            // Test that core fields are still required and validated

            // Empty observation ID should still fail
            let result = Observation::new(
                Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
                1,
                "".to_string(), // Empty observation ID
                12345,
                "SRCE".to_string(),
                "UK-DAILY-TEMPERATURE-OBS".to_string(),
                record_status::ORIGINAL as i32,
                1,
                station.clone(),
                measurements.clone(),
                quality_flags.clone(),
                processing_flags.clone(),
                None, // Administrative fields can be None
                None,
            );
            assert!(result.is_err()); // Should fail due to empty observation ID

            // Station ID mismatch should still fail
            let mut wrong_station = station.clone();
            wrong_station.src_id = 99999; // Different ID

            let result = Observation::new(
                Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
                1,
                "valid_id".to_string(),
                12345, // Station ID doesn't match wrong_station.src_id
                "SRCE".to_string(),
                "UK-DAILY-TEMPERATURE-OBS".to_string(),
                record_status::ORIGINAL as i32,
                1,
                wrong_station,
                measurements,
                quality_flags,
                processing_flags,
                None, // Administrative fields can be None
                None,
            );
            assert!(result.is_err()); // Should fail due to station ID mismatch
        }
    }

    mod quality_flag_tests {
        use super::*;

        #[test]
        fn test_quality_flag_from_string() {
            assert_eq!(QualityFlag::from_str("0").unwrap(), QualityFlag::Valid);
            assert_eq!(QualityFlag::from_str("1").unwrap(), QualityFlag::Suspect);
            assert_eq!(QualityFlag::from_str("2").unwrap(), QualityFlag::Erroneous);
            assert_eq!(QualityFlag::from_str("3").unwrap(), QualityFlag::NotChecked);
            assert_eq!(QualityFlag::from_str("9").unwrap(), QualityFlag::Missing);

            // Valid but non-standard values (treated as valid due to MESQL parsing)
            assert_eq!(QualityFlag::from_str("5").unwrap(), QualityFlag::Valid);

            // Invalid values
            assert!(QualityFlag::from_str("invalid").is_err());
        }

        #[test]
        fn test_quality_flag_from_i8() {
            assert_eq!(QualityFlag::try_from(0i8).unwrap(), QualityFlag::Valid);
            assert_eq!(QualityFlag::try_from(1i8).unwrap(), QualityFlag::Suspect);
            assert_eq!(QualityFlag::try_from(2i8).unwrap(), QualityFlag::Erroneous);
            assert_eq!(QualityFlag::try_from(3i8).unwrap(), QualityFlag::NotChecked);
            assert_eq!(QualityFlag::try_from(9i8).unwrap(), QualityFlag::Missing);

            // Non-standard but valid value (treated as valid in our implementation)
            assert_eq!(QualityFlag::try_from(5i8).unwrap(), QualityFlag::Valid);
        }

        #[test]
        fn test_quality_flag_to_i8() {
            assert_eq!(i8::from(QualityFlag::Valid), 0);
            assert_eq!(i8::from(QualityFlag::Suspect), 1);
            assert_eq!(i8::from(QualityFlag::Erroneous), 2);
            assert_eq!(i8::from(QualityFlag::NotChecked), 3);
            assert_eq!(i8::from(QualityFlag::Missing), 9);
        }

        #[test]
        fn test_quality_flag_usability() {
            // Valid is always usable
            assert!(QualityFlag::Valid.is_usable(false, false));
            assert!(QualityFlag::Valid.is_usable(true, false));
            assert!(QualityFlag::Valid.is_usable(false, true));

            // Suspect depends on include_suspect flag
            assert!(!QualityFlag::Suspect.is_usable(false, false));
            assert!(QualityFlag::Suspect.is_usable(true, false));

            // NotChecked depends on include_unchecked flag
            assert!(!QualityFlag::NotChecked.is_usable(false, false));
            assert!(QualityFlag::NotChecked.is_usable(false, true));

            // Erroneous and Missing are never usable
            assert!(!QualityFlag::Erroneous.is_usable(true, true));
            assert!(!QualityFlag::Missing.is_usable(true, true));
        }

        #[test]
        fn test_quality_flag_description() {
            assert!(
                QualityFlag::Valid
                    .description()
                    .contains("passed all QC checks")
            );
            assert!(
                QualityFlag::Suspect
                    .description()
                    .contains("failed at least one")
            );
            assert!(
                QualityFlag::Erroneous
                    .description()
                    .contains("considered incorrect")
            );
        }

        #[test]
        fn test_quality_flag_display() {
            assert_eq!(format!("{}", QualityFlag::Valid), "0");
            assert_eq!(format!("{}", QualityFlag::Suspect), "1");
            assert_eq!(format!("{}", QualityFlag::Missing), "9");
        }

        #[test]
        fn test_quality_flag_all_values() {
            let all = QualityFlag::all_values();
            assert_eq!(all.len(), 6);
            assert!(all.contains(&QualityFlag::Valid));
            assert!(all.contains(&QualityFlag::Missing));
            assert!(all.contains(&QualityFlag::Reverted));
        }
    }

    #[test]
    fn test_serde_serialization() {
        let station = create_test_station();

        // Test JSON serialization/deserialization
        let json = serde_json::to_string(&station).unwrap();
        let deserialized: Station = serde_json::from_str(&json).unwrap();
        assert_eq!(station, deserialized);

        // Test quality flag serialization
        let flag = QualityFlag::Valid;
        let flag_json = serde_json::to_string(&flag).unwrap();
        let flag_deserialized: QualityFlag = serde_json::from_str(&flag_json).unwrap();
        assert_eq!(flag, flag_deserialized);
    }

    mod processing_flag_tests {
        use super::*;

        #[test]
        fn test_processing_flag_is_success() {
            // Success flags
            assert!(ProcessingFlag::ParseOk.is_success());
            assert!(ProcessingFlag::MissingValue.is_success()); // Missing values are normal
            assert!(ProcessingFlag::StationFound.is_success());
            assert!(ProcessingFlag::Original.is_success());
            assert!(ProcessingFlag::DuplicateResolved.is_success());

            // Error flags
            assert!(!ProcessingFlag::ParseFailed.is_success());
            assert!(!ProcessingFlag::StationMissing.is_success());
            assert!(!ProcessingFlag::Superseded.is_success());
        }

        #[test]
        fn test_processing_flag_description() {
            assert_eq!(
                ProcessingFlag::ParseOk.description(),
                "Successfully parsed and converted"
            );
            assert_eq!(
                ProcessingFlag::ParseFailed.description(),
                "Failed to parse value"
            );
            assert_eq!(
                ProcessingFlag::StationMissing.description(),
                "Station metadata missing, placeholder used"
            );
        }

        #[test]
        fn test_processing_flag_display() {
            assert_eq!(format!("{}", ProcessingFlag::ParseOk), "ParseOk");
            assert_eq!(
                format!("{}", ProcessingFlag::StationMissing),
                "StationMissing"
            );
        }

        #[test]
        fn test_observation_processing_flags() {
            let mut observation = create_test_observation();

            // Test getting processing flags
            assert_eq!(
                observation.get_processing_flag("air_temperature"),
                Some(ProcessingFlag::ParseOk)
            );
            assert_eq!(
                observation.get_processing_flag("station"),
                Some(ProcessingFlag::StationFound)
            );
            assert_eq!(observation.get_processing_flag("nonexistent"), None);

            // Test has_processing_errors (should be false initially with only success flags)
            assert!(!observation.has_processing_errors()); // All success flags initially

            // Test setting processing flags
            observation.set_processing_flag("new_measurement".to_string(), ProcessingFlag::ParseOk);
            assert_eq!(
                observation.get_processing_flag("new_measurement"),
                Some(ProcessingFlag::ParseOk)
            );
            assert!(!observation.has_processing_errors()); // Still no errors

            // Add an error flag
            observation.set_processing_flag("error_field".to_string(), ProcessingFlag::ParseFailed);
            assert!(observation.has_processing_errors()); // Now has an error

            // Test get_all_processing_flags
            let all_flags = observation.get_all_processing_flags();
            assert!(all_flags.contains_key("air_temperature"));
            assert!(all_flags.contains_key("station"));
            assert!(all_flags.contains_key("error_field"));
        }
    }
}
