//! Station lookup and search functionality
//!
//! This module provides various query methods for finding stations in the registry
//! based on different criteria: name patterns, geographic regions, and temporal ranges.

use super::StationRegistry;
use crate::app::models::Station;

impl StationRegistry {
    /// Get all station IDs in the registry
    pub fn station_ids(&self) -> Vec<i32> {
        self.stations.keys().copied().collect()
    }

    /// Get all stations in the registry
    pub fn stations(&self) -> Vec<&Station> {
        self.stations.values().collect()
    }

    /// Find stations by name pattern (case-insensitive)
    ///
    /// Searches for stations whose names contain the given pattern.
    /// The search is case-insensitive and supports partial matches.
    ///
    /// # Arguments
    /// * `pattern` - Text pattern to search for in station names
    ///
    /// # Returns
    /// Vector of stations whose names contain the pattern
    ///
    /// # Examples
    /// ```
    /// # use midas_processor::app::services::station_registry::StationRegistry;
    /// # use std::path::PathBuf;
    /// let registry = StationRegistry::new(PathBuf::from("/cache"));
    /// let heathrow_stations = registry.find_stations_by_name("heathrow");
    /// let london_stations = registry.find_stations_by_name("london");
    /// ```
    pub fn find_stations_by_name(&self, pattern: &str) -> Vec<&Station> {
        let pattern_lower = pattern.to_lowercase();
        self.stations
            .values()
            .filter(|station| station.src_name.to_lowercase().contains(&pattern_lower))
            .collect()
    }

    /// Find stations within a geographic bounding box
    ///
    /// Returns all stations whose coordinates fall within the specified
    /// rectangular geographic region defined by minimum and maximum
    /// latitude and longitude values.
    ///
    /// # Arguments
    /// * `min_lat` - Southern boundary (minimum latitude)
    /// * `max_lat` - Northern boundary (maximum latitude)  
    /// * `min_lon` - Western boundary (minimum longitude)
    /// * `max_lon` - Eastern boundary (maximum longitude)
    ///
    /// # Returns
    /// Vector of stations within the bounding box
    ///
    /// # Examples
    /// ```
    /// # use midas_processor::app::services::station_registry::StationRegistry;
    /// # use std::path::PathBuf;
    /// let registry = StationRegistry::new(PathBuf::from("/cache"));
    ///
    /// // Find stations in Greater London area
    /// let london_stations = registry.find_stations_in_region(
    ///     51.3, 51.7,  // Latitude range
    ///     -0.5, 0.2    // Longitude range
    /// );
    ///
    /// // Find all UK stations
    /// let uk_stations = registry.find_stations_in_region(
    ///     49.0, 61.0,  // UK latitude range
    ///     -8.0, 2.0    // UK longitude range
    /// );
    /// ```
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
    ///
    /// Returns stations whose operational period overlaps with the specified
    /// time range. A station is considered active if its operational period
    /// (from src_bgn_date to src_end_date) has any overlap with the query period.
    ///
    /// # Arguments
    /// * `start_date` - Beginning of the query time period
    /// * `end_date` - End of the query time period
    ///
    /// # Returns
    /// Vector of stations that were active during any part of the specified period
    ///
    /// # Examples
    /// ```
    /// # use midas_processor::app::services::station_registry::StationRegistry;
    /// # use std::path::PathBuf;
    /// # use chrono::{DateTime, Utc};
    /// let registry = StationRegistry::new(PathBuf::from("/cache"));
    ///
    /// let start = "2020-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
    /// let end = "2020-12-31T23:59:59Z".parse::<DateTime<Utc>>().unwrap();
    ///
    /// let active_2020 = registry.find_active_stations(start, end);
    /// ```
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

    /// Find stations by multiple criteria
    ///
    /// Combines name pattern, geographic region, and temporal range filters.
    /// Only stations matching ALL specified criteria are returned.
    ///
    /// # Arguments
    /// * `criteria` - Search criteria combining multiple filters
    ///
    /// # Returns
    /// Vector of stations matching all specified criteria
    pub fn find_stations_by_criteria(&self, criteria: &SearchCriteria) -> Vec<&Station> {
        self.stations
            .values()
            .filter(|station| {
                // Name pattern filter
                if let Some(ref pattern) = criteria.name_pattern {
                    let pattern_lower = pattern.to_lowercase();
                    if !station.src_name.to_lowercase().contains(&pattern_lower) {
                        return false;
                    }
                }

                // Geographic region filter
                if let Some(ref region) = criteria.region {
                    if station.high_prcn_lat < region.min_lat
                        || station.high_prcn_lat > region.max_lat
                        || station.high_prcn_lon < region.min_lon
                        || station.high_prcn_lon > region.max_lon
                    {
                        return false;
                    }
                }

                // Temporal range filter
                if let Some(ref time_range) = criteria.active_period {
                    if station.src_bgn_date > time_range.end_date
                        || station.src_end_date < time_range.start_date
                    {
                        return false;
                    }
                }

                true
            })
            .collect()
    }

    /// Get stations within a certain distance from a point
    ///
    /// Uses simple Euclidean distance calculation in decimal degrees.
    /// For more accurate distance calculations, consider using a proper
    /// geographic distance formula (haversine).
    ///
    /// # Arguments
    /// * `lat` - Latitude of the center point
    /// * `lon` - Longitude of the center point
    /// * `max_distance_degrees` - Maximum distance in decimal degrees
    ///
    /// # Returns
    /// Vector of stations within the specified distance
    pub fn find_stations_near_point(
        &self,
        lat: f64,
        lon: f64,
        max_distance_degrees: f64,
    ) -> Vec<&Station> {
        self.stations
            .values()
            .filter(|station| {
                let lat_diff = station.high_prcn_lat - lat;
                let lon_diff = station.high_prcn_lon - lon;
                let distance = (lat_diff * lat_diff + lon_diff * lon_diff).sqrt();
                distance <= max_distance_degrees
            })
            .collect()
    }

    /// Get stations grouped by historic county
    ///
    /// Returns a map where keys are county names and values are vectors
    /// of stations in each county.
    pub fn group_stations_by_county(&self) -> std::collections::HashMap<String, Vec<&Station>> {
        let mut county_map = std::collections::HashMap::new();

        for station in self.stations.values() {
            county_map
                .entry(station.historic_county.clone())
                .or_insert_with(Vec::new)
                .push(station);
        }

        county_map
    }

    /// Get basic statistics about the station registry
    pub fn get_statistics(&self) -> RegistryStatistics {
        let stations: Vec<&Station> = self.stations.values().collect();

        if stations.is_empty() {
            return RegistryStatistics::default();
        }

        let mut min_lat = stations[0].high_prcn_lat;
        let mut max_lat = stations[0].high_prcn_lat;
        let mut min_lon = stations[0].high_prcn_lon;
        let mut max_lon = stations[0].high_prcn_lon;
        let mut min_height = stations[0].height_meters;
        let mut max_height = stations[0].height_meters;

        for station in &stations {
            min_lat = min_lat.min(station.high_prcn_lat);
            max_lat = max_lat.max(station.high_prcn_lat);
            min_lon = min_lon.min(station.high_prcn_lon);
            max_lon = max_lon.max(station.high_prcn_lon);
            min_height = min_height.min(station.height_meters);
            max_height = max_height.max(station.height_meters);
        }

        // Count unique counties
        let unique_counties: std::collections::HashSet<&String> =
            stations.iter().map(|s| &s.historic_county).collect();

        RegistryStatistics {
            total_stations: stations.len(),
            unique_counties: unique_counties.len(),
            geographic_bounds: GeographicBounds {
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            },
            elevation_range: ElevationRange {
                min_height_meters: min_height,
                max_height_meters: max_height,
            },
        }
    }
}

/// Search criteria for multi-criteria station queries
#[derive(Debug, Clone, Default)]
pub struct SearchCriteria {
    /// Name pattern to search for (case-insensitive)
    pub name_pattern: Option<String>,

    /// Geographic region bounds
    pub region: Option<GeographicRegion>,

    /// Temporal activity period
    pub active_period: Option<TimeRange>,
}

/// Geographic region definition
#[derive(Debug, Clone)]
pub struct GeographicRegion {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

/// Time range definition
#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
}

/// Geographic bounds of all stations in the registry
#[derive(Debug, Clone)]
pub struct GeographicBounds {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

/// Elevation range of all stations in the registry
#[derive(Debug, Clone)]
pub struct ElevationRange {
    pub min_height_meters: f32,
    pub max_height_meters: f32,
}

/// Basic statistics about the station registry
#[derive(Debug, Clone, Default)]
pub struct RegistryStatistics {
    pub total_stations: usize,
    pub unique_counties: usize,
    pub geographic_bounds: GeographicBounds,
    pub elevation_range: ElevationRange,
}

impl Default for GeographicBounds {
    fn default() -> Self {
        Self {
            min_lat: 0.0,
            max_lat: 0.0,
            min_lon: 0.0,
            max_lon: 0.0,
        }
    }
}

impl Default for ElevationRange {
    fn default() -> Self {
        Self {
            min_height_meters: 0.0,
            max_height_meters: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::Station;
    use chrono::{DateTime, Utc};
    use std::path::PathBuf;

    #[allow(clippy::too_many_arguments)]
    fn create_test_station(
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

    fn create_test_registry() -> StationRegistry {
        let mut registry = StationRegistry::new(PathBuf::from("/test"));

        let station1 = create_test_station(
            1,
            "HEATHROW",
            51.4778,
            -0.4614,
            2000,
            2050,
            "Greater London",
            25.0,
        );
        let station2 = create_test_station(
            2,
            "BIRMINGHAM",
            52.4539,
            -1.7481,
            1990,
            2010,
            "West Midlands",
            161.0,
        );
        let station3 = create_test_station(
            3,
            "MANCHESTER",
            53.4808,
            -2.2426,
            1995,
            2025,
            "Greater Manchester",
            78.0,
        );

        registry.stations.insert(station1.src_id, station1);
        registry.stations.insert(station2.src_id, station2);
        registry.stations.insert(station3.src_id, station3);

        registry
    }

    #[test]
    fn test_find_stations_by_name() {
        let registry = create_test_registry();

        let heathrow_stations = registry.find_stations_by_name("HEATH");
        assert_eq!(heathrow_stations.len(), 1);
        assert_eq!(heathrow_stations[0].src_name, "HEATHROW");

        let birmingham_stations = registry.find_stations_by_name("birmingham");
        assert_eq!(birmingham_stations.len(), 1);
        assert_eq!(birmingham_stations[0].src_name, "BIRMINGHAM");

        let man_stations = registry.find_stations_by_name("MAN");
        assert_eq!(man_stations.len(), 1);
        assert_eq!(man_stations[0].src_name, "MANCHESTER");

        let no_match = registry.find_stations_by_name("NONEXISTENT");
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test_find_stations_in_region() {
        let registry = create_test_registry();

        // All UK stations
        let uk_stations = registry.find_stations_in_region(50.0, 55.0, -5.0, 2.0);
        assert_eq!(uk_stations.len(), 3);

        // Only London area
        let london_stations = registry.find_stations_in_region(51.0, 52.0, -1.0, 0.0);
        assert_eq!(london_stations.len(), 1);
        assert_eq!(london_stations[0].src_name, "HEATHROW");

        // Northern England
        let north_stations = registry.find_stations_in_region(52.5, 54.0, -3.0, -1.0);
        assert_eq!(north_stations.len(), 1);
        assert_eq!(north_stations[0].src_name, "MANCHESTER");

        // Empty region
        let empty_region = registry.find_stations_in_region(60.0, 61.0, -1.0, 0.0);
        assert_eq!(empty_region.len(), 0);
    }

    #[test]
    fn test_find_active_stations() {
        let registry = create_test_registry();

        // 2005 - all should be active
        let active_2005 = registry.find_active_stations(
            DateTime::parse_from_rfc3339("2005-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2005-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(active_2005.len(), 3);

        // 2015 - only Heathrow and Manchester should be active (Birmingham ended 2010)
        let active_2015 = registry.find_active_stations(
            DateTime::parse_from_rfc3339("2015-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2015-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(active_2015.len(), 2);

        // 1985 - none should be active (all started 1990 or later)
        let active_1985 = registry.find_active_stations(
            DateTime::parse_from_rfc3339("1985-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("1985-12-31T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(active_1985.len(), 0);
    }

    #[test]
    fn test_find_stations_by_criteria() {
        let registry = create_test_registry();

        // Search by name pattern only
        let criteria = SearchCriteria {
            name_pattern: Some("MAN".to_string()),
            ..Default::default()
        };
        let name_results = registry.find_stations_by_criteria(&criteria);
        assert_eq!(name_results.len(), 1);
        assert_eq!(name_results[0].src_name, "MANCHESTER");

        // Search by region only (London area)
        let criteria = SearchCriteria {
            region: Some(GeographicRegion {
                min_lat: 51.0,
                max_lat: 52.0,
                min_lon: -1.0,
                max_lon: 0.0,
            }),
            ..Default::default()
        };
        let region_results = registry.find_stations_by_criteria(&criteria);
        assert_eq!(region_results.len(), 1);
        assert_eq!(region_results[0].src_name, "HEATHROW");

        // Combined criteria - Manchester in northern England after 1994
        let criteria = SearchCriteria {
            name_pattern: Some("MAN".to_string()),
            region: Some(GeographicRegion {
                min_lat: 52.5,
                max_lat: 54.0,
                min_lon: -3.0,
                max_lon: -1.0,
            }),
            active_period: Some(TimeRange {
                start_date: DateTime::parse_from_rfc3339("1994-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                end_date: DateTime::parse_from_rfc3339("2020-12-31T23:59:59Z")
                    .unwrap()
                    .with_timezone(&Utc),
            }),
        };
        let combined_results = registry.find_stations_by_criteria(&criteria);
        assert_eq!(combined_results.len(), 1);
        assert_eq!(combined_results[0].src_name, "MANCHESTER");
    }

    #[test]
    fn test_find_stations_near_point() {
        let registry = create_test_registry();

        // Find stations near London (should find Heathrow)
        let near_london = registry.find_stations_near_point(51.5, -0.1, 1.0);
        assert_eq!(near_london.len(), 1);
        assert_eq!(near_london[0].src_name, "HEATHROW");

        // Find stations near Birmingham
        let near_birmingham = registry.find_stations_near_point(52.5, -1.9, 1.0);
        assert_eq!(near_birmingham.len(), 1);
        assert_eq!(near_birmingham[0].src_name, "BIRMINGHAM");

        // Large radius should find all stations
        let all_near = registry.find_stations_near_point(52.0, -1.5, 5.0);
        assert_eq!(all_near.len(), 3);
    }

    #[test]
    fn test_group_stations_by_county() {
        let registry = create_test_registry();
        let county_groups = registry.group_stations_by_county();

        assert_eq!(county_groups.len(), 3); // 3 different counties
        assert_eq!(county_groups.get("Greater London").unwrap().len(), 1);
        assert_eq!(county_groups.get("West Midlands").unwrap().len(), 1);
        assert_eq!(county_groups.get("Greater Manchester").unwrap().len(), 1);
    }

    #[test]
    fn test_get_statistics() {
        let registry = create_test_registry();
        let stats = registry.get_statistics();

        assert_eq!(stats.total_stations, 3);
        assert_eq!(stats.unique_counties, 3);

        // Check geographic bounds
        assert!((stats.geographic_bounds.min_lat - 51.4778).abs() < 0.001);
        assert!((stats.geographic_bounds.max_lat - 53.4808).abs() < 0.001);
        assert!((stats.geographic_bounds.min_lon - (-2.2426)).abs() < 0.001);
        assert!((stats.geographic_bounds.max_lon - (-0.4614)).abs() < 0.001);

        // Check elevation range
        assert!((stats.elevation_range.min_height_meters - 25.0).abs() < 0.001);
        assert!((stats.elevation_range.max_height_meters - 161.0).abs() < 0.001);
    }

    #[test]
    fn test_station_ids_and_stations() {
        let registry = create_test_registry();

        let ids = registry.station_ids();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));

        let stations = registry.stations();
        assert_eq!(stations.len(), 3);
    }
}
