# Station ID Evolution Analysis Guide

## Overview

MIDAS weather stations often change their observation IDs over time while remaining the same physical station. This guide explains how the ID evolution parsing capability enables sophisticated analysis of long-term temperature trends despite these ID changes.

## Problem Statement

Weather stations like London Weather Centre (station 19144) used different reporting IDs during different periods:
- **1958-1992**: ID 5046 (DCNN/MODLERAD)
- **1992-1997**: ID 5037 (DCNN/MODLERAD)  
- **1997-2006**: ID 5047 (DCNN/MODLERAD)
- **2004-2004**: ID 2188 (DCNN/MODLERAD) - backup system

Without ID evolution tracking, these would appear as separate stations, fragmenting 48 years of continuous temperature data into disconnected segments.

## Implementation

### Data Structures

The system extends the `Station` model with ID evolution history:

```rust
pub struct Station {
    // ... existing fields
    pub id_history: Vec<IdPeriod>,
}

pub struct IdPeriod {
    pub observation_id: String,     // e.g., "5046"
    pub id_type: String,            // e.g., "DCNN"
    pub met_domain_name: String,    // e.g., "MODLERAD"
    pub first_year: i32,            // Start year
    pub last_year: i32,             // End year
    pub context: String,            // "primary" or "backup"
}
```

### Parsing Process

1. **Capability File Structure**: The parser now reads both header and data sections
   ```
   Conventions,G,BADC-CSV,1
   observation_station,G,LONDON WEATHER CENTRE
   src_id,G,19144
   data
   id,id_type,met_domain_name,first_year,last_year
   5046,DCNN,MODLERAD,1958,1992
   5037,DCNN,MODLERAD,1992,1997
   5047,DCNN,MODLERAD,1997,2006
   2188,DCNN,MODLERAD,2004,2004
   end data
   ```

2. **Automatic Analysis**: The system detects gaps and overlaps in ID coverage
   - Overlapping periods indicate backup systems or transitions
   - Gaps indicate potential data availability issues

## Usage Examples

### Temporal ID Lookup

```rust
let station = registry.get_station(19144).unwrap();

// Find which ID was active in 1980
let id_1980 = station.find_observation_id_for_year(1980);
assert_eq!(id_1980.unwrap().observation_id, "5046");

// Find which ID was active in 2002  
let id_2002 = station.find_observation_id_for_year(2002);
assert_eq!(id_2002.unwrap().observation_id, "5047");
```

### Complete Station History

```rust
// Get all observation IDs ever used by this station
let all_ids = station.get_all_observation_ids();
// Returns: ["5046", "5037", "5047", "2188"]

// Check if station has ID evolution data
if station.has_id_history() {
    println!("Station has {} ID periods", station.id_history.len());
}
```

### Quality Analysis

```rust
// Detect issues in ID evolution
let issues = station.analyze_id_evolution();
for issue in issues {
    println!("Issue: {}", issue);
}
// Output:
// "Overlapping ID periods: 5047 (1997-2006) and 2188 (2004-2004)"
```

## Parquet Integration

### Enhanced Schema

The Parquet output will include ID evolution context:

```rust
// For each observation record
observation_id: String,                    // Original ID from CSV
station_id: i32,                          // Canonical station ID  
observation_id_period_start: DateTime,    // When this ID became valid
observation_id_period_end: DateTime,      // When this ID stopped being valid
observation_id_context: String,           // "primary" or "backup"
```

### Partitioning Strategy

Files are organized to enable efficient queries across ID changes:

```
/weather_data.parquet/
├── year=1980/dataset=uk-radiation-obs/station_id=19144/
│   ├── obs_id=5046_modlerad.parquet
├── year=1995/dataset=uk-radiation-obs/station_id=19144/
│   ├── obs_id=5037_modlerad.parquet
└── year=2002/dataset=uk-radiation-obs/station_id=19144/
    ├── obs_id=5047_modlerad.parquet
```

## Research Applications

### Urban Heat Island Analysis

The ID evolution capability enables sophisticated UHI research:

1. **Continuous Station Histories**: Analyze 48-year temperature trends for London Weather Centre despite 4 different observation IDs

2. **Instrument Change Detection**: Identify potential bias from equipment upgrades by analyzing periods when IDs changed

3. **Data Quality Assessment**: Detect gaps or overlaps that might affect trend analysis

4. **Validation Support**: Understand why certain observations might be flagged during ID transition periods

### Query Examples

**Station-Level Analysis:**
```sql
-- Get all radiation data for London Weather Centre across all ID changes
SELECT * FROM weather_data
WHERE station_id = 19144
  AND dataset = 'uk-radiation-obs'
  AND year BETWEEN 1958 AND 2006;
```

**ID Evolution Analysis:**
```sql
-- Understand measurement continuity across ID changes
SELECT observation_id,
       observation_id_period_start,
       observation_id_period_end,
       observation_id_context,
       COUNT(*) as observation_count
FROM weather_data
WHERE station_id = 19144
GROUP BY observation_id, observation_id_period_start, observation_id_period_end, observation_id_context
ORDER BY observation_id_period_start;
```

**Gap Detection:**
```sql
-- Identify potential data gaps during ID transitions
SELECT station_id,
       LAG(observation_id_period_end) OVER (PARTITION BY station_id ORDER BY observation_id_period_start) as prev_end,
       observation_id_period_start as curr_start,
       observation_id
FROM weather_data
WHERE station_id = 19144
  AND prev_end < curr_start;
```

## Benefits for Climate Research

### Data Provenance
- Preserves exact original observation context
- Maintains complete audit trail of ID changes
- Documents instrument transitions and their timing

### Analytical Power
- Enables analysis across ID transitions seamlessly
- Supports detection of bias from equipment changes
- Facilitates validation of long-term climate trends

### Quality Assurance
- Automatic detection of coverage gaps and overlaps
- Context classification for different measurement systems
- Historical reconstruction of station operations

## Implementation Status

- ✅ **Core Parsing**: Capability files now extract ID evolution data
- ✅ **Data Structures**: Station model extended with ID history
- ✅ **Analysis Methods**: Gap/overlap detection implemented
- ✅ **Testing**: Comprehensive test coverage with real-world examples
- 🚧 **Parquet Export**: Enhanced schema design documented (future work)
- 🚧 **Observation Enrichment**: Add ID context to processed observations (future work)

This capability transforms fragmented station data into continuous, analyzable histories essential for detecting urban heat island effects that develop over multiple decades.