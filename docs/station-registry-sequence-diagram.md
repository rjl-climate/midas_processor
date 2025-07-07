# Station Registry Mermaid Sequence Diagram

This document shows the complete execution flow of the MIDAS Station Registry from loading station metadata to performing various query operations.

## Components

- **Client** - Code calling the station registry (e.g., CLI commands, BADC parser)
- **StationRegistry** - Main registry coordinating operations and providing O(1) lookups
- **Loader** - File discovery and loading orchestration module
- **Parser** - Station record parsing from different CSV formats
- **FileSystem** - MIDAS cache file access and directory traversal
- **ProgressBar** - Optional progress reporting with throughput metrics
- **Station** - Domain model with validation and geographic methods

## Main Loading Sequence Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant SR as StationRegistry
    participant L as Loader
    participant P as Parser
    participant FS as FileSystem
    participant PB as ProgressBar
    participant S as Station

    Note over C,S: Station Registry Loading Flow

    %% Initialization
    C->>+SR: load_from_cache(cache_path, datasets, show_progress)
    SR->>SR: new(cache_path)
    SR->>L: LoadStats::new()

    %% Cache Validation
    SR->>+FS: validate cache_path exists
    alt Cache path exists
        FS-->>-SR: Valid ✓
    else Cache path missing
        FS-->>-SR: Error ❌
        SR-->>C: Error::StationRegistry
    end

    %% File Discovery Phase
    SR->>+L: discover_station_files(cache_path, datasets)
    L->>+FS: scan datasets for capability/ directories
    FS-->>-L: capability_files[]
    L->>+FS: scan datasets for station-metadata.csv files
    FS-->>-L: metadata_files[]
    L-->>-SR: (capability_files, metadata_files)

    %% Progress Setup
    alt show_progress = true
        SR->>+PB: ProgressBar::new(total_files)
        PB->>PB: set_style(template)
        PB-->>-SR: progress_bar
    end

    %% Capability Files Processing
    Note over SR,S: Process Capability Files (Station metadata in headers)
    
    loop For each capability file
        alt show_progress
            SR->>PB: set_position(file_index)
            SR->>PB: set_message("Processing capability file...")
        end

        SR->>+L: load_capability_file(file_path)
        L->>+FS: open CSV file with flexible reader
        FS-->>-L: csv_reader

        L->>L: initialize station_metadata HashMap
        
        loop Read header records until "data" marker
            L->>+FS: read_record()
            FS-->>-L: StringRecord
            
            alt Record contains global attributes
                L->>+P: extract_capability_metadata(record, metadata)
                P->>P: parse key,scope,value format
                P->>P: handle multi-field values (location, height, dates)
                P-->>-L: updated metadata HashMap
            end
        end

        L->>+P: parse_capability_station_metadata(metadata, file_path)
        P->>P: extract required fields (src_id, station_name, etc.)
        P->>P: parse location (lat,lon from single field)
        P->>P: parse height (value,unit format)
        P->>P: parse date_valid (start,end dates)
        P->>+S: Station::new(all_fields)
        S->>S: validate coordinates, dates, required fields
        S-->>-P: validated Station
        P-->>-L: Station

        L-->>-SR: (vec![station], total_records)

        SR->>SR: check for duplicate station ID
        alt Station ID not exists
            SR->>SR: insert station into HashMap
            SR->>SR: increment stations_loaded
        else Duplicate found
            SR->>SR: log warning, keep existing
        end

        SR->>SR: increment files_processed
    end

    %% Metadata Files Processing  
    Note over SR,S: Process Metadata Files (Station records in data section)

    loop For each metadata file
        alt show_progress
            SR->>PB: set_position(file_index)
            SR->>PB: set_message("Processing metadata file...")
        end

        SR->>+L: load_metadata_file(file_path)
        L->>+FS: open CSV file with BADC format reader
        FS-->>-L: csv_reader

        L->>L: skip header section until "data" marker
        L->>+FS: read first record after "data"
        FS-->>-L: header_record (column names)

        loop Read data records until "end data" marker
            L->>+FS: read_record()
            FS-->>-L: StringRecord
            
            L->>+P: parse_station_record(record, headers)
            P->>P: create field name→value mapping
            P->>P: check rec_st_ind = 9 (definitive records only)
            
            alt Record is definitive
                P->>P: parse required fields (src_id, coords, dates, etc.)
                P->>P: parse optional fields (grid references, etc.)
                P->>+S: Station::new(all_fields)
                S->>S: validate and create station
                S-->>-P: validated Station
                P-->>-L: Some(station)
            else Record filtered
                P-->>-L: None (filtered out)
            end
        end

        L-->>-SR: (stations[], total_records)

        loop For each parsed station
            SR->>SR: check for duplicate station ID
            alt New station
                SR->>SR: insert station into HashMap
                SR->>SR: increment stations_loaded
            else Duplicate (prefer metadata over capability)
                SR->>SR: replace existing with metadata version
            end
        end

        SR->>SR: increment files_processed
    end

    %% Finalization
    alt show_progress
        SR->>PB: finish_with_message("Loading complete")
    end

    SR->>SR: update registry metadata (datasets, load_time, etc.)
    SR->>L: finalize LoadStats (duration, filter_rate, etc.)
    
    SR-->>-C: (StationRegistry, LoadStats)
```

## Query Operations Sequence Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant SR as StationRegistry
    participant Q as Query
    participant S as Station

    Note over C,S: Station Registry Query Operations

    %% Basic Lookups (O(1) operations)
    rect rgb(230, 255, 230)
        Note over C,S: O(1) Hash Lookups
        
        C->>+SR: get_station(src_id)
        SR->>SR: HashMap.get(src_id)
        SR-->>-C: Option<&Station>

        C->>+SR: contains_station(src_id)
        SR->>SR: HashMap.contains_key(src_id)
        SR-->>-C: bool

        C->>+SR: station_count()
        SR->>SR: HashMap.len()
        SR-->>-C: usize
    end

    %% Collection Operations
    rect rgb(255, 245, 230)
        Note over C,S: Collection Operations
        
        C->>+SR: station_ids()
        SR->>Q: collect all HashMap keys
        Q-->>-C: Vec<i32>

        C->>+SR: stations()
        SR->>Q: collect all HashMap values
        Q-->>-C: Vec<&Station>
    end

    %% Search Operations (O(n) filtering)
    rect rgb(240, 240, 255)
        Note over C,S: Search and Filter Operations

        C->>+SR: find_stations_by_name(pattern)
        SR->>+Q: filter by name pattern
        Q->>Q: pattern.to_lowercase()
        loop For each station
            Q->>S: check station.src_name.contains(pattern)
        end
        Q-->>-SR: Vec<&Station>
        SR-->>-C: matching stations

        C->>+SR: find_stations_in_region(min_lat, max_lat, min_lon, max_lon)
        SR->>+Q: filter by geographic bounds
        loop For each station
            Q->>S: check coordinates within bounds
            Q->>Q: lat ∈ [min_lat, max_lat] && lon ∈ [min_lon, max_lon]
        end
        Q-->>-SR: Vec<&Station>
        SR-->>-C: stations in region

        C->>+SR: find_active_stations(start_date, end_date)
        SR->>+Q: filter by temporal overlap
        loop For each station
            Q->>S: check operational period overlap
            Q->>Q: station.src_bgn_date ≤ end_date && station.src_end_date ≥ start_date
        end
        Q-->>-SR: Vec<&Station>
        SR-->>-C: active stations

        C->>+SR: find_stations_by_criteria(criteria)
        SR->>+Q: apply multiple filters
        Q->>Q: combine name, region, and temporal filters
        loop For each station
            Q->>S: check all criteria (AND logic)
        end
        Q-->>-SR: Vec<&Station>
        SR-->>-C: stations matching all criteria
    end

    %% Advanced Operations
    rect rgb(255, 240, 245)
        Note over C,S: Advanced Analysis Operations

        C->>+SR: find_stations_near_point(lat, lon, max_distance)
        SR->>+Q: calculate distances and filter
        loop For each station
            Q->>S: calculate Euclidean distance
            Q->>Q: distance = √((lat_diff)² + (lon_diff)²)
        end
        Q-->>-SR: Vec<&Station>
        SR-->>-C: nearby stations

        C->>+SR: group_stations_by_county()
        SR->>+Q: group by historic_county
        Q->>Q: HashMap<String, Vec<&Station>>
        loop For each station
            Q->>S: get station.historic_county
            Q->>Q: add to county group
        end
        Q-->>-SR: HashMap<county, stations>
        SR-->>-C: grouped stations

        C->>+SR: get_statistics()
        SR->>+Q: calculate registry statistics
        Q->>Q: count unique counties
        Q->>Q: find geographic bounds (min/max lat/lon)
        Q->>Q: find elevation range (min/max height)
        Q-->>-SR: RegistryStatistics
        SR-->>-C: statistics summary
    end
```

## Error Handling Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant SR as StationRegistry
    participant L as Loader
    participant FS as FileSystem

    Note over C,FS: Error Handling Strategy

    C->>+SR: load_from_cache(invalid_path, datasets, false)
    
    SR->>+FS: check cache_path.exists()
    FS-->>-SR: false
    
    SR->>SR: Error::station_registry("Cache path does not exist")
    SR-->>-C: Err(Error::StationRegistry)

    Note over C,FS: File Processing Errors (Graceful Degradation)

    C->>+SR: load_from_cache(valid_path, datasets, false)
    
    loop For each file
        SR->>+L: load_capability_file(corrupted_file)
        L->>+FS: open CSV file
        
        alt File corrupted
            FS-->>-L: csv::Error
            L-->>-SR: Err(Error::CsvParsing)
            SR->>SR: log warning, add to stats.errors
            SR->>SR: continue processing next file ✓
        else File valid
            FS-->>-L: valid csv_reader
            L-->>-SR: Ok((stations, records))
            SR->>SR: add stations to registry ✓
        end
    end
    
    SR-->>-C: Ok((registry, stats_with_some_errors))

    Note over C,FS: Individual Record Errors

    rect rgb(255, 250, 250)
        Note over SR,FS: Graceful record-level error handling
        Note over SR,FS: • Invalid station records are skipped
        Note over SR,FS: • Parsing continues for remaining records  
        Note over SR,FS: • Error counts tracked in LoadStats
        Note over SR,FS: • Warnings logged with context
    end
```

## Performance Characteristics

```mermaid
graph TB
    subgraph "Loading Performance"
        L1[File Discovery: O(f)]
        L2[File Reading: O(n)]
        L3[Record Parsing: O(r)]
        L4[Station Validation: O(1)]
        L5[HashMap Insert: O(1)]
    end
    
    subgraph "Query Performance"
        Q1[Basic Lookup: O(1)]
        Q2[Pattern Search: O(n)]
        Q3[Geographic Filter: O(n)]
        Q4[Temporal Filter: O(n)]
        Q5[Statistics: O(n)]
    end
    
    subgraph "Memory Usage"
        M1[Station Data: O(stations)]
        M2[File Buffers: O(file_size)]
        M3[HashMap Index: O(stations)]
        M4[Temporary Parsing: O(record_size)]
    end
    
    L1 --> L2 --> L3 --> L4 --> L5
    
    style Q1 fill:#c8e6c9
    style L4 fill:#c8e6c9
    style L5 fill:#c8e6c9
```

## Key Design Features

### **Loading Strategy**
- **Progressive Loading**: Process files one at a time to minimize memory usage
- **Error Isolation**: Individual file failures don't stop the entire loading process
- **Duplicate Handling**: Metadata files override capability files for the same station
- **Progress Reporting**: Optional progress bars with throughput metrics

### **Query Optimization**
- **O(1) Lookups**: Primary station access via HashMap for constant-time performance
- **Lazy Filtering**: Search operations only iterate when called, not pre-computed
- **Memory Efficient**: Return references to avoid copying station data
- **Flexible Criteria**: Support for combining multiple search filters

### **Data Integrity**
- **Validation**: All stations validated through domain model constructors
- **Filtering**: Only definitive records (rec_st_ind = 9) included in registry
- **Error Context**: Detailed error messages with file paths and parsing context
- **Statistics Tracking**: Comprehensive loading metrics for monitoring

### **Extensibility**
- **Modular Design**: Separate modules for loading, parsing, querying, and metadata
- **New File Formats**: Easy to add support for additional MIDAS file types
- **Query Methods**: Simple to add new search and filtering capabilities
- **Statistics**: Extensible metrics collection for analysis and debugging

## Usage Examples

### Loading a Registry
```rust
use midas_processor::app::services::station_registry::StationRegistry;

let datasets = vec!["uk-daily-temperature-obs".to_string()];
let (registry, stats) = StationRegistry::load_from_cache(
    &cache_path, 
    &datasets, 
    true  // show progress
).await?;

println!("Loaded {} stations in {:.2}s", 
    stats.stations_loaded, 
    stats.load_duration.as_secs_f64()
);
```

### Querying Stations
```rust
// O(1) lookup
if let Some(station) = registry.get_station(1330) {
    println!("Found station: {}", station.src_name);
}

// Geographic search
let london_stations = registry.find_stations_in_region(
    51.3, 51.7,  // Latitude range
    -0.5, 0.2    // Longitude range
);

// Temporal search
let active_2020 = registry.find_active_stations(
    start_date, end_date
);

// Combined criteria
let criteria = SearchCriteria {
    name_pattern: Some("heathrow".to_string()),
    region: Some(london_region),
    active_period: Some(year_2020),
};
let results = registry.find_stations_by_criteria(&criteria);
```

The station registry provides a robust, high-performance foundation for MIDAS data processing with comprehensive error handling and flexible query capabilities.