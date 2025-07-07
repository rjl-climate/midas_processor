//! Tests for parsing statistics functionality

use super::super::stats::ParseStats;

#[test]
fn test_parse_stats_calculation() {
    let stats = ParseStats {
        total_records: 100,
        observations_parsed: 95,
        records_skipped: 5,
        errors: vec!["Error 1".to_string(), "Error 2".to_string()],
    };

    assert_eq!(stats.success_rate(), 95.0);
    assert!(stats.is_successful());

    let poor_stats = ParseStats {
        total_records: 100,
        observations_parsed: 80,
        records_skipped: 20,
        errors: vec![],
    };

    assert_eq!(poor_stats.success_rate(), 80.0);
    assert!(!poor_stats.is_successful());
}

#[test]
fn test_parse_stats_empty() {
    let empty_stats = ParseStats::new();

    assert_eq!(empty_stats.total_records, 0);
    assert_eq!(empty_stats.observations_parsed, 0);
    assert_eq!(empty_stats.records_skipped, 0);
    assert!(empty_stats.errors.is_empty());
    assert_eq!(empty_stats.success_rate(), 0.0);
    assert!(!empty_stats.is_successful());
}

#[test]
fn test_parse_stats_perfect() {
    let perfect_stats = ParseStats {
        total_records: 50,
        observations_parsed: 50,
        records_skipped: 0,
        errors: vec![],
    };

    assert_eq!(perfect_stats.success_rate(), 100.0);
    assert!(perfect_stats.is_successful());
}
