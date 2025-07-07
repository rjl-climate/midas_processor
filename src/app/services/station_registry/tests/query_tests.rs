//! Tests for station query and search functionality

use super::*;
use crate::app::services::station_registry::{StationRegistry, query::*};
use chrono::{DateTime, Utc};
use std::path::PathBuf;

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

    // 2015 - only Heathrow and Manchester should be active
    let active_2015 = registry.find_active_stations(
        DateTime::parse_from_rfc3339("2015-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        DateTime::parse_from_rfc3339("2015-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc),
    );
    assert_eq!(active_2015.len(), 2);

    // 1985 - none should be active
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

    // Search by region only
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
}

#[test]
fn test_find_stations_near_point() {
    let registry = create_test_registry();

    // Find stations near London
    let near_london = registry.find_stations_near_point(51.5, -0.1, 1.0);
    assert_eq!(near_london.len(), 1);
    assert_eq!(near_london[0].src_name, "HEATHROW");

    // Large radius should find all stations
    let all_near = registry.find_stations_near_point(52.0, -1.5, 5.0);
    assert_eq!(all_near.len(), 3);
}

#[test]
fn test_group_stations_by_county() {
    let registry = create_test_registry();
    let county_groups = registry.group_stations_by_county();

    assert_eq!(county_groups.len(), 3);
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
