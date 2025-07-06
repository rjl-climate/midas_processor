//! Data models for MIDAS processing
//!
//! This module contains the core data structures for representing MIDAS weather station
//! metadata and observation records, following the UK Met Office MIDAS specification.

use crate::constants::{self, quality_flags, record_status};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

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

    // Station reference
    /// Station identifier (maps to Station.src_id)
    pub id: i32,

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

    /// Quality control flags for each measurement
    pub quality_flags: HashMap<String, QualityFlag>,

    // Processing metadata
    /// Met Office processing timestamp
    pub meto_stmp_time: DateTime<Utc>,

    /// MIDAS processing elapsed time indicator
    pub midas_stmp_etime: i32,
}

impl Observation {
    /// Create a new observation with validation
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ob_end_time: DateTime<Utc>,
        ob_hour_count: i32,
        id: i32,
        id_type: String,
        met_domain_name: String,
        rec_st_ind: i32,
        version_num: i32,
        station: Station,
        measurements: HashMap<String, f64>,
        quality_flags: HashMap<String, QualityFlag>,
        meto_stmp_time: DateTime<Utc>,
        midas_stmp_etime: i32,
    ) -> Result<Self> {
        let observation = Self {
            ob_end_time,
            ob_hour_count,
            id,
            id_type,
            met_domain_name,
            rec_st_ind,
            version_num,
            station,
            measurements,
            quality_flags,
            meto_stmp_time,
            midas_stmp_etime,
        };

        observation.validate()?;
        Ok(observation)
    }

    /// Validate observation data for consistency
    pub fn validate(&self) -> Result<()> {
        // Validate station ID matches
        if self.id != self.station.src_id {
            return Err(Error::data_validation(format!(
                "Observation station ID {} does not match station src_id {}",
                self.id, self.station.src_id
            )));
        }

        // Validate record status indicator
        if ![record_status::ORIGINAL, record_status::CORRECTED].contains(&(self.rec_st_ind as i8)) {
            return Err(Error::data_validation(format!(
                "Invalid record status indicator {}: must be {} (original) or {} (corrected)",
                self.rec_st_ind,
                record_status::ORIGINAL,
                record_status::CORRECTED
            )));
        }

        // Validate observation hour count is positive
        if self.ob_hour_count <= 0 {
            return Err(Error::data_validation(format!(
                "Observation hour count {} must be positive",
                self.ob_hour_count
            )));
        }

        // Validate quality control version
        if self.version_num < constants::MIN_QUALITY_VERSION {
            return Err(Error::data_validation(format!(
                "Quality control version {} is below minimum version {}",
                self.version_num,
                constants::MIN_QUALITY_VERSION
            )));
        }

        // Validate observation time is within station operational period
        if !self.station.is_active(self.ob_end_time) {
            return Err(Error::data_validation(format!(
                "Observation time {} is outside station operational period ({} to {})",
                self.ob_end_time, self.station.src_bgn_date, self.station.src_end_date
            )));
        }

        // Validate required fields
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

    /// Get a quality flag by measurement name
    pub fn get_quality_flag(&self, measurement_name: &str) -> Option<QualityFlag> {
        self.quality_flags.get(measurement_name).copied()
    }

    /// Check if a measurement has usable quality
    pub fn is_measurement_usable(
        &self,
        measurement_name: &str,
        include_suspect: bool,
        include_unchecked: bool,
    ) -> bool {
        if let Some(flag) = self.get_quality_flag(measurement_name) {
            constants::is_usable_quality(flag as i8, include_suspect, include_unchecked)
        } else {
            false
        }
    }

    /// Get all measurements with usable quality
    pub fn get_usable_measurements(
        &self,
        include_suspect: bool,
        include_unchecked: bool,
    ) -> HashMap<String, f64> {
        self.measurements
            .iter()
            .filter(|(name, _)| {
                self.is_measurement_usable(name, include_suspect, include_unchecked)
            })
            .map(|(name, value)| (name.clone(), *value))
            .collect()
    }

    /// Check if this observation supersedes another (based on rec_st_ind)
    pub fn supersedes(&self, other: &Observation) -> bool {
        // Same station and time
        if self.id == other.id && self.ob_end_time == other.ob_end_time {
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
    pub fn all_values() -> [QualityFlag; 5] {
        [
            QualityFlag::Valid,
            QualityFlag::Suspect,
            QualityFlag::Erroneous,
            QualityFlag::NotChecked,
            QualityFlag::Missing,
        ]
    }
}

impl FromStr for QualityFlag {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim() {
            "0" => Ok(QualityFlag::Valid),
            "1" => Ok(QualityFlag::Suspect),
            "2" => Ok(QualityFlag::Erroneous),
            "3" => Ok(QualityFlag::NotChecked),
            "9" => Ok(QualityFlag::Missing),
            _ => Err(Error::data_validation(format!(
                "Invalid quality flag value '{}': must be 0, 1, 2, 3, or 9",
                s
            ))),
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
            quality_flags::MISSING => Ok(QualityFlag::Missing),
            _ => Err(Error::data_validation(format!(
                "Invalid quality flag value {}: must be 0, 1, 2, 3, or 9",
                value
            ))),
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
            src_bgn_date: Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            src_end_date: Utc.with_ymd_and_hms(2099, 12, 31, 23, 59, 59).unwrap(),
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
        quality_flags.insert("air_temperature".to_string(), QualityFlag::Valid);
        quality_flags.insert("humidity".to_string(), QualityFlag::Suspect);

        Observation {
            ob_end_time: Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
            ob_hour_count: 1,
            id: 12345,
            id_type: constants::ID_TYPE_SOURCE.to_string(),
            met_domain_name: "UK-DAILY-TEMPERATURE-OBS".to_string(),
            rec_st_ind: record_status::ORIGINAL as i32,
            version_num: 1,
            station,
            measurements,
            quality_flags,
            meto_stmp_time: Utc.with_ymd_and_hms(2023, 6, 15, 13, 0, 0).unwrap(),
            midas_stmp_etime: 3600,
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
            station.src_end_date = Utc.with_ymd_and_hms(1999, 12, 31, 23, 59, 59).unwrap();
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
            let before_time = Utc.with_ymd_and_hms(1999, 1, 1, 0, 0, 0).unwrap();
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
    }

    mod observation_tests {
        use super::*;

        #[test]
        fn test_observation_creation_valid() {
            let observation = create_test_observation();
            assert!(observation.validate().is_ok());
            assert_eq!(observation.id, 12345);
            assert_eq!(observation.measurements.len(), 2);
            assert_eq!(observation.quality_flags.len(), 2);
        }

        #[test]
        fn test_observation_station_id_mismatch() {
            let mut observation = create_test_observation();
            observation.id = 99999; // Different from station.src_id
            assert!(observation.validate().is_err());
        }

        #[test]
        fn test_observation_invalid_record_status() {
            let mut observation = create_test_observation();
            observation.rec_st_ind = 5; // Invalid status
            assert!(observation.validate().is_err());
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
            observation.ob_end_time = Utc.with_ymd_and_hms(1990, 1, 1, 0, 0, 0).unwrap();
            assert!(observation.validate().is_err());
        }

        #[test]
        fn test_observation_measurement_access() {
            let observation = create_test_observation();

            // Test measurement retrieval
            assert_eq!(observation.get_measurement("air_temperature"), Some(15.5));
            assert_eq!(observation.get_measurement("nonexistent"), None);

            // Test quality flag retrieval
            assert_eq!(
                observation.get_quality_flag("air_temperature"),
                Some(QualityFlag::Valid)
            );
            assert_eq!(
                observation.get_quality_flag("humidity"),
                Some(QualityFlag::Suspect)
            );
        }

        #[test]
        fn test_observation_usable_measurements() {
            let observation = create_test_observation();

            // Without including suspect data
            let usable = observation.get_usable_measurements(false, false);
            assert_eq!(usable.len(), 1); // Only air_temperature (Valid)
            assert!(usable.contains_key("air_temperature"));

            // Including suspect data
            let usable_with_suspect = observation.get_usable_measurements(true, false);
            assert_eq!(usable_with_suspect.len(), 2); // Both measurements
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
            obs2.id = 99999;
            assert!(!obs1.supersedes(&obs2));
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

            // Invalid values
            assert!(QualityFlag::from_str("5").is_err());
            assert!(QualityFlag::from_str("invalid").is_err());
        }

        #[test]
        fn test_quality_flag_from_i8() {
            assert_eq!(QualityFlag::try_from(0i8).unwrap(), QualityFlag::Valid);
            assert_eq!(QualityFlag::try_from(1i8).unwrap(), QualityFlag::Suspect);
            assert_eq!(QualityFlag::try_from(2i8).unwrap(), QualityFlag::Erroneous);
            assert_eq!(QualityFlag::try_from(3i8).unwrap(), QualityFlag::NotChecked);
            assert_eq!(QualityFlag::try_from(9i8).unwrap(), QualityFlag::Missing);

            // Invalid value
            assert!(QualityFlag::try_from(5i8).is_err());
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
            assert_eq!(all.len(), 5);
            assert!(all.contains(&QualityFlag::Valid));
            assert!(all.contains(&QualityFlag::Missing));
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
}
