//! Data models for MIDAS processing

use chrono::{DateTime, Utc};
// use std::collections::HashMap;  // Currently unused

/// Station metadata structure (placeholder)
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Station {
    pub src_id: i32,
    // TODO: Implement full Station structure
}

/// Observation record structure (placeholder)
#[derive(Debug, Clone)]
pub struct Observation {
    pub ob_end_time: DateTime<Utc>,
    pub id: i32,
    // TODO: Implement full Observation structure
}

/// Quality control flag enumeration (placeholder)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QualityFlag {
    Valid = 0,
    Suspect = 1,
    Erroneous = 2,
    NotChecked = 3,
    Missing = 9,
}
