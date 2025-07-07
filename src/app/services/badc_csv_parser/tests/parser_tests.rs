//! Tests for the main BADC-CSV parser functionality

use super::*;

#[test]
fn test_section_splitting() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
missing_value,G,NA
data
col1,col2,col3
val1,val2,val3
end data"#;

    // Test section splitting logic directly
    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert_eq!(header_lines.len(), 3);
    assert!(header_lines[0].contains("Conventions"));
    assert!(header_lines[1].contains("title"));
    assert!(header_lines[2].contains("missing_value"));

    let data_content = data_section.unwrap();
    assert!(data_content.contains("col1,col2,col3"));
    assert!(data_content.contains("val1,val2,val3"));
    assert!(data_content.contains("end data"));
}

#[test]
fn test_section_splitting_no_data() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
missing_value,G,NA"#;

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data");

    assert!(data_start.is_none());
}

#[test]
fn test_section_splitting_empty_data() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
data"#;

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert_eq!(header_lines.len(), 2);
    assert!(data_section.is_none());
}

#[test]
fn test_header_and_data_integration() {
    let temp_file = create_temp_file(&create_test_badc_csv());
    let content = std::fs::read_to_string(temp_file.path()).unwrap();

    // Test file reading and section splitting
    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    // Verify header parsing
    assert!(!header_lines.is_empty());
    assert!(data_section.is_some());

    // Test header extraction
    let header = super::super::header::SimpleHeader::parse(&header_lines).unwrap();
    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, Some("Test Temperature Data".to_string()));
}

#[test]
fn test_minimal_file_structure() {
    let temp_file = create_temp_file(&create_minimal_badc_csv());
    let content = std::fs::read_to_string(temp_file.path()).unwrap();

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert!(!header_lines.is_empty());
    assert!(data_section.is_some());

    let header = super::super::header::SimpleHeader::parse(&header_lines).unwrap();
    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, None);
}
