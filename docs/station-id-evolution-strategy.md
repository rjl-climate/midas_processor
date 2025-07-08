# Station ID Evolution Context

  From the London Weather Centre capability file, I can see that Station 19144 used multiple reporting IDs
   over its operational period:

  - 1958-1992: ID 5046 (DCNN/MODLERAD)
  - 1992-1997: ID 5037 (DCNN/MODLERAD)
  - 1997-2006: ID 5047 (DCNN/MODLERAD)
  - 2004-2004: ID 2188 (DCNN/MODLERAD) - overlapping period

  This shows that the same physical station (src_id 19144) used different observation IDs during different
   time periods, sometimes with overlapping periods for different measurement systems.

  Parquet Schema Design for Station ID Evolution

  Core Principles

  1. Maintain data provenance: Store exactly what was in the original observation
  2. Enable station-level analysis: Support queries across all IDs for the same station
  3. Support temporal analysis: Understand when ID changes occurred
  4. Preserve measurement context: Different IDs may represent different instrument setups

  Recommended Parquet Schema

  // Current fields (preserved)
  observation_id: String,           // Original observation ID from CSV (e.g., "5046", "5037", "5047")
  station_id: i32,                 // Canonical MIDAS src_id (e.g., 19144)
  id_type: String,                 // ID classification (e.g., "DCNN")
  met_domain_name: String,         // Dataset/measurement type (e.g., "MODLERAD")

  // Enhanced fields (new)
  observation_id_period_start: DateTime<Utc>,  // When this obs_id became valid for this station
  observation_id_period_end: DateTime<Utc>,    // When this obs_id stopped being valid
  observation_id_context: String,              // Why this ID was used (e.g., "instrument_upgrade",
  "reporting_change")

  // Station metadata (denormalized but frozen at observation time)
  station_name: String,            // Station name at time of observation
  station_latitude: f64,           // Coordinates at time of observation
  station_longitude: f64,
  station_elevation: f32,
  station_authority: String,       // Operating authority at time of observation

  Data Processing Strategy

  1. During Station Registry Loading

  // Build ID evolution mapping during registry initialization
  pub struct StationIdHistory {
      station_id: i32,
      id_periods: Vec<IdPeriod>,
  }

  pub struct IdPeriod {
      observation_id: String,
      id_type: String,
      met_domain_name: String,
      valid_from: DateTime<Utc>,
      valid_to: DateTime<Utc>,
      context: String,  // "primary", "backup", "instrument_change", etc.
  }

  2. During Observation Processing

  // Enrich each observation with ID context
  impl Observation {
      pub fn enrich_with_id_history(&mut self, id_history: &StationIdHistory) {
          if let Some(period) = id_history.find_period_for_time(self.ob_end_time) {
              self.observation_id_period_start = period.valid_from;
              self.observation_id_period_end = period.valid_to;
              self.observation_id_context = period.context.clone();
          }
      }
  }

  Parquet Partitioning Strategy

  /weather_data.parquet/
  ├── year=1968/dataset=uk-radiation-obs/station_id=19144/
  │   ├── obs_id=5046_modlerad.parquet       # 1958-1992 data
  ├── year=1992/dataset=uk-radiation-obs/station_id=19144/
  │   ├── obs_id=5037_modlerad.parquet       # 1992-1997 data
  ├── year=1997/dataset=uk-radiation-obs/station_id=19144/
  │   ├── obs_id=5047_modlerad.parquet       # 1997-2006 data
  └── year=2004/dataset=uk-radiation-obs/station_id=19144/
      ├── obs_id=2188_modlerad.parquet       # 2004 overlapping data

  Query Benefits

  Station-Level Analysis

  -- Get all radiation data for London Weather Centre
  SELECT * FROM weather_data
  WHERE station_id = 19144
    AND dataset = 'uk-radiation-obs'
    AND year BETWEEN 1968 AND 2006;

  ID Evolution Analysis

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

  Data Quality Assessment

  -- Identify potential data gaps during ID transitions
  SELECT station_id,
         LAG(observation_id_period_end) OVER (PARTITION BY station_id ORDER BY
  observation_id_period_start) as prev_end,
         observation_id_period_start as curr_start,
         observation_id
  FROM weather_data
  WHERE station_id = 19144
    AND prev_end < curr_start; -- Gap detection

  Benefits for UHI Analysis

  1. Continuous station histories: Analyze temperature trends across ID changes
  2. Instrument change detection: Identify potential bias from equipment upgrades
  3. Data quality assessment: Spot gaps or overlaps in measurement periods
  4. Validation support: Understand why observations might appear "invalid" during transition periods

  This approach preserves the original observation context while enabling sophisticated analysis of
  station evolution over time, which is crucial for detecting urban heat island effects that develop over
  decades.

---


│ │ Phase 5: Enhanced Parquet Schema (Future Enhancement)                                                   │ │
│ │                                                                                                         │ │
│ │ - Add ID evolution fields: observation_id_period_start/end, observation_id_context                      │ │
│ │ - Partition by station and ID: Enable efficient queries across station evolution                        │ │
│ │ - Preserve provenance: Maintain exact original observation context while enabling station-level         │ │
│ │ analysis
