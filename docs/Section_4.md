## **Summary: MIDAS Tables (Section 4.2 & 4.3) for an AI Agent**

This summary details the metadata structures that describe the data sources (weather stations) and the individual observation records. Understanding these is essential for correctly joining, filtering, and interpreting the weather data.

### **Part 1: Section 4.2 - Source Capability (The "Who" and "Where")**

This section describes the metadata for each data **source** (e.g., a weather station, rain gauge, etc.). This information is relatively static and provides context about where the measurements are coming from.

#### **Core Concept**

The "Source Capability" data acts as a master reference table for all stations. Every observation record in the dataset can be linked back to a source described here. Your application must be able to ingest this source information and use it to identify, locate, and filter stations.

#### **Key Data Fields in the Source Table**

Your agent's data model should have a "Source" or "Station" entity with the following critical attributes:

*   **`src_id` (Source ID)**
    *   **Description:** A unique integer identifier for each data source.
    *   **AI/App Usage:** This is the **Primary Key** for all source-related information. It is used to link the observation records (from Section 4.3) to the station's metadata. This is the most important field for joining tables.

*   **`src_name` (Source Name)**
    *   **Description:** The official name of the station (e.g., "HEATHROW").
    *   **AI/App Usage:** For display purposes, labeling maps, and providing human-readable search functionality.

*   **Location Coordinates (`high_prcn_lat`, `high_prcn_lon`)**
    *   **Description:** The high-precision latitude and longitude of the station in WGS84 decimal degrees.
    *   **AI/App Usage:** Essential for mapping stations, performing spatial queries (e.g., "find all stations within 50km of a point"), and spatial consistency checks.

*   **Location Grid Reference (`east_grid_ref`, `north_grid_ref`, `grid_ref_type`)**
    *   **Description:** The station's location using a specified UK grid reference system (e.g., "OSGB").
    *   **AI/App Usage:** Alternative location system, primarily for UK-specific mapping or analysis. The `grid_ref_type` tells the app which grid system to use.

*   **Operational Dates (`src_bgn_date`, `src_end_date`)**
    *   **Description:** The start and end dates of the station's operational life. An `src_end_date` in the future or far past (e.g., 9999-12-31) indicates the station is still active.
    *   **AI/App Usage:** Critical for filtering. The app should only query for observation data from a station that falls within its operational date range. This prevents searching for data from stations that were not active at a given time.

*   **`rec_st_ind` (Record Status Indicator)**
    *   **Description:** An integer indicating the status of the station record itself (e.g., if it was updated). `9` is the definitive version.
    *   **AI/App Usage:** When multiple records exist for the same `src_id` (e.g., due to updates), the application should **always use the record where `rec_st_ind` is 9**.

---

### **Part 2: Section 4.3 - Attributes for Each Observation Record (The "What" and "When")**

This section describes the columns that are present in every single row of the main data files (e.g., the hourly or daily weather observation tables). These attributes describe the measurement itself.

#### **Core Concept**

Each row in a data file is a unique "observation record." It contains the timestamp, a link back to the source station, and the actual weather measurements with their associated quality flags.

#### **Key Data Fields in Observation Records**

Your agent's data parser for the main data files must be able to process these columns:

*   **`ob_end_time` (Observation End Time)**
    *   **Description:** The timestamp for the observation in the format `YYYY-MM-DD HH24:MI:SS`. This marks the *end* of the observation period (e.g., for an hourly rainfall total, 10:00:00 refers to the rain that fell between 09:00:01 and 10:00:00).
    *   **AI/App Usage:** The primary time-series index. Used for filtering by date/time, plotting data over time, and temporal analysis. The agent must parse this specific string format into a proper datetime object.

*   **`id` and `id_type` (Identifier and Type)**
    *   **Description:** These two fields link the observation record back to the source. `id_type` specifies the type of identifier used (it will almost always be `'SRCE'`). The `id` field contains the numerical value that corresponds to the `src_id` from the Source Capability table.
    *   **AI/App Usage:** This is the **Foreign Key**. To get the station's name or location for an observation, the application must perform a join: `observation_table.id` = `source_table.src_id` where `observation_table.id_type` = `'SRCE'`.

*   **`met_domain_name` (Meteorological Domain Name)**
    *   **Description:** A string that identifies the type of data contained in the file (e.g., `'UK-HOURLY-WEATHER-OBS'`, `'UK-DAILY-RAIN-OBS'`).
    *   **AI/App Usage:** Allows the agent to identify the schema of the file it is processing. The app can use this to know which measurement columns (like `air_temperature`, `prcp_amt`) to expect in the file.

*   **`rec_st_ind` (Record Status Indicator)**
    *   **Description:** An integer code indicating the history and validity of this specific observation record.
    *   **AI/App Usage:** This is **critical for data integrity**. An observation may be reported and then later corrected. The application should use this field to select the most definitive version of a record for a given station and time. The key codes are:
        *   `9`: **Definitive original record.**
        *   `1`: **Corrected/updated record.** This supersedes any previous version.
        *   **Rule:** If multiple records exist for the same station (`id`) and time (`ob_end_time`), the application should prioritize the one with `rec_st_ind = 1`. If none have a `1`, it should use the one with `rec_st_ind = 9`.

*   **Measurement and Quality Flag Pairs**
    *   **Description:** The actual data, presented in pairs of columns: one for the value and one for its quality.
    *   **Format:** `[measurement_name]` and `q_[measurement_name]`.
    *   **Examples:** `air_temperature` and `q_air_temperature`; `wind_speed` and `q_wind_speed`.
    *   **AI/App Usage:** The agent must be programmed to recognize this pairing. When it reads `air_temperature`, it must also read `q_air_temperature` to determine its validity. The full list of possible measurement names is found in **Appendix A** of the guide.

### **Recommended Logic & Data Flow for the AI Agent**

1.  **Ingest Source Data:** First, load the "Source Capability" data into a dedicated "Stations" table or data structure, using `src_id` as the unique key. Filter to keep only records where `rec_st_ind = 9`.

2.  **Ingest Observation Data:** When processing an observation file:
    a.  Use `met_domain_name` to identify the data type and determine the expected measurement columns.
    b.  For each row, parse the `ob_end_time` into a standard datetime object.
    c.  Use the `id` and `id_type` fields to link this observation back to the correct station in your "Stations" table.

3.  **Handle Data Updates:** Implement logic to handle `rec_st_ind`. A simple approach is to process all data and then, for any duplicates (same `id` and `ob_end_time`), discard the record with `rec_st_ind = 9` if a record with `rec_st_ind = 1` exists.

4.  **Process Measurements:** For each measurement column (e.g., `air_temperature`), immediately look up its corresponding quality flag (`q_air_temperature`) to decide how to handle the value, as detailed in the QC summary.
