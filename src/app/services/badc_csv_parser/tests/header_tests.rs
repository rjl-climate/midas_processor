//! Tests for BADC-CSV header parsing functionality

use super::super::header::SimpleHeader;

#[test]
fn test_header_parsing_complete() {
    let header_lines = vec![
        "Conventions,G,BADC-CSV,1".to_string(),
        "title,G,Test Temperature Data".to_string(),
        "source,G,MIDAS Test".to_string(),
        "missing_value,G,NA".to_string(),
        "long_name,max_air_temp,maximum air temperature,degC".to_string(),
    ];

    let header = SimpleHeader::parse(&header_lines).unwrap();

    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, Some("Test Temperature Data".to_string()));
    assert_eq!(header.source, Some("MIDAS Test".to_string()));
}

#[test]
fn test_header_parsing_minimal() {
    let header_lines = vec![
        "Conventions,G,BADC-CSV,1".to_string(),
        "missing_value,G,NULL".to_string(),
    ];

    let header = SimpleHeader::parse(&header_lines).unwrap();

    assert_eq!(header.missing_value, "NULL");
    assert_eq!(header.title, None);
    assert_eq!(header.source, None);
}

#[test]
fn test_header_parsing_defaults() {
    let header_lines = vec![
        "Conventions,G,BADC-CSV,1".to_string(),
        "some_other_attr,G,Other Value".to_string(),
    ];

    let header = SimpleHeader::parse(&header_lines).unwrap();

    // Should use default missing value when not specified
    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, None);
    assert_eq!(header.source, None);
}

#[test]
fn test_is_missing_value() {
    let header = SimpleHeader {
        missing_value: "NA".to_string(),
        title: None,
        source: None,
    };

    assert!(header.is_missing_value("NA"));
    assert!(header.is_missing_value(" NA "));
    assert!(header.is_missing_value(""));
    assert!(header.is_missing_value("   "));

    assert!(!header.is_missing_value("0"));
    assert!(!header.is_missing_value("NULL"));
    assert!(!header.is_missing_value("15.5"));
}

#[test]
fn test_custom_missing_value() {
    let header = SimpleHeader {
        missing_value: "-999".to_string(),
        title: None,
        source: None,
    };

    assert!(header.is_missing_value("-999"));
    assert!(header.is_missing_value(" -999 "));
    assert!(header.is_missing_value(""));

    assert!(!header.is_missing_value("NA"));
    assert!(!header.is_missing_value("0"));
}
