# BADC-CSV Parser Mermaid Sequence Diagram

This document shows the complete execution flow of the MIDAS BADC-CSV parser using Mermaid sequence diagram format.

## Components

- **Client** - Code calling the parser (e.g., CLI commands)
- **BadcCsvParser** - Main parser orchestrator
- **SimpleHeader** - BADC-CSV header metadata extractor
- **ColumnMapping** - Dynamic column analysis and categorization
- **RecordParser** - Individual CSV record processing
- **StationRegistry** - O(1) station metadata lookup service
- **Observation** - Domain model with validation

## Mermaid Sequence Diagram

```mermaid
sequenceDiagram
    participant C as Client
    participant P as BadcCsvParser
    participant H as SimpleHeader
    participant CM as ColumnMapping
    participant RP as RecordParser
    participant SR as StationRegistry
    participant O as Observation

    Note over C,O: BADC-CSV Parser Complete Flow

    %% File Processing Phase
    C->>+P: parse_file(path)
    Note over P: Read file content
    P->>P: read_file()
    P->>P: split_sections(content)
    Note over P: Split into header and data sections

    %% Header Parsing Phase
    P->>+H: parse(header_lines)
    Note over H: Extract global attributes
    H->>H: extract_global_attrs()
    H-->>-P: SimpleHeader(missing_value, title, source)

    %% Data Section Setup
    P->>+CM: parse_data_section(data_content, header)
    Note over CM: Setup CSV reader
    CM->>CM: create_csv_reader()
    CM->>CM: analyze(csv_headers)
    Note over CM: Categorize columns into quality/measurement
    CM->>CM: categorize_columns()

    %% Record Processing Loop
    Note over CM,O: For each CSV record

    loop Each CSV Record
        CM->>+RP: parse_observation_record(record, mapping, header, registry)

        %% Required Field Parsing
        Note over RP: Parse required fields
        RP->>RP: parse_required_fields()
        Note over RP: Extract station_id, datetime, etc.

        %% Station Lookup
        RP->>+SR: get_station(station_id)
        SR-->>-RP: Option<Station>

        %% Dynamic Data Parsing
        Note over RP: Parse measurements & quality flags
        RP->>RP: parse_measurements()
        RP->>RP: parse_quality_flags()

        %% Observation Creation
        RP->>+O: new(all_fields)
        Note over O: Validate observation data
        O->>O: validate()
        O-->>-RP: Observation

        RP-->>-CM: Observation
    end

    CM-->>-P: ColumnMapping + observations
    P-->>-C: ParseResult(observations, stats)

    %% Error Handling Notes
    Note over P,O: Error Handling Strategy
    Note over P: • Individual record failures don't stop processing
    Note over P: • Detailed error context at each level
    Note over P: • Statistics track success/failure rates
    Note over P: • Graceful degradation throughout
```

## Alternative Detailed View with Error Handling

```mermaid
sequenceDiagram
    participant C as Client
    participant P as BadcCsvParser
    participant H as SimpleHeader
    participant CM as ColumnMapping
    participant RP as RecordParser
    participant SR as StationRegistry
    participant O as Observation

    Note over C,O: Detailed Flow with Error Handling

    C->>+P: parse_file(path)

    alt File exists and readable
        P->>P: read_file() ✓
        P->>P: split_sections() ✓

        alt Has "data" marker
            P->>+H: parse(header_lines)
            H->>H: extract_global_attrs()
            H-->>-P: SimpleHeader ✓

            P->>+CM: parse_data_section()
            CM->>CM: create_csv_reader() ✓
            CM->>CM: analyze_headers() ✓

            loop Each Record
                CM->>+RP: parse_observation_record()

                alt Valid record format
                    RP->>RP: parse_required_fields() ✓
                    RP->>+SR: get_station(station_id)

                    alt Station found
                        SR-->>-RP: Station ✓
                        RP->>RP: parse_measurements() ✓
                        RP->>RP: parse_quality_flags() ✓
                        RP->>+O: new()

                        alt Validation passes
                            O->>O: validate() ✓
                            O-->>-RP: Observation ✓
                        else Validation fails
                            O-->>-RP: ValidationError ❌
                            Note over RP: Log error, increment stats
                        end
                    else Station not found
                        SR-->>-RP: None ❌
                        Note over RP: Log error, increment stats
                    end
                else Invalid record
                    Note over RP: Parse error ❌
                    Note over RP: Log error, increment stats
                end

                RP-->>-CM: Result<Observation>
            end

            CM-->>-P: ParseResult ✓
        else No "data" marker
            Note over P: Format error ❌
            P-->>C: Error::BadcFormat
        end
    else File not readable
        Note over P: I/O error ❌
        P-->>C: Error::Io
    end

    P-->>-C: Result<ParseResult>
```

## Component Interaction Overview

```mermaid
graph TB
    subgraph "Parser Core"
        P[BadcCsvParser]
        H[SimpleHeader]
        CM[ColumnMapping]
        RP[RecordParser]
    end

    subgraph "External Dependencies"
        SR[StationRegistry]
        FS[FileSystem]
    end

    subgraph "Domain Models"
        O[Observation]
        PS[ParseStats]
        PR[ParseResult]
    end

    %% Dependencies
    P --> FS
    P --> H
    P --> CM
    CM --> RP
    RP --> SR
    RP --> O
    P --> PS
    P --> PR

    %% Data Flow
    FS -.->|file content| P
    H -.->|metadata| CM
    SR -.->|station data| RP
    O -.->|validated obs| PR
    PS -.->|statistics| PR
```

## Key Data Structures Flow

```mermaid
flowchart TD
    Start([File Path]) --> Read[Read File Content]
    Read --> Split[Split Sections]
    Split --> Header[Header Lines]
    Split --> Data[Data Content]

    Header --> ParseH[Parse Header]
    ParseH --> SH[SimpleHeader]

    Data --> CSV[CSV Reader]
    CSV --> Headers[CSV Headers]
    Headers --> Analyze[Analyze Columns]
    Analyze --> CM[ColumnMapping]

    CSV --> Records[CSV Records]
    Records --> Loop{For Each Record}

    Loop --> ParseR[Parse Record]
    ParseR --> Fields[Required Fields]
    ParseR --> Station[Station Lookup]
    ParseR --> Measurements[Parse Measurements]
    ParseR --> Quality[Parse Quality Flags]

    Fields --> Valid{Valid?}
    Station --> Found{Found?}

    Valid -->|Yes| Found
    Found -->|Yes| Create[Create Observation]
    Create --> Validate[Validate]
    Validate --> Success{Valid?}
    Success -->|Yes| Add[Add to Results]
    Success -->|No| Error1[Log Error]
    Found -->|No| Error2[Log Error]
    Valid -->|No| Error3[Log Error]

    Add --> More{More Records?}
    Error1 --> More
    Error2 --> More
    Error3 --> More

    More -->|Yes| Loop
    More -->|No| Result[ParseResult]

    SH -.-> ParseR
    CM -.-> ParseR

    style Start fill:#e1f5fe
    style Result fill:#c8e6c9
    style Error1 fill:#ffcdd2
    style Error2 fill:#ffcdd2
    style Error3 fill:#ffcdd2
```

## Performance Characteristics

```mermaid
graph LR
    subgraph "Time Complexity"
        F[File Read: O(n)]
        H[Header Parse: O(h)]
        C[Column Map: O(c)]
        R[Records: O(r)]
        S[Station Lookup: O(1)]
        V[Validation: O(1)]
    end

    subgraph "Space Complexity"
        M[Memory: O(n + r)]
        B[Buffer: O(record_size)]
        I[Indexes: O(stations)]
    end

    F --> H --> C --> R
    R --> S --> V

    style S fill:#c8e6c9
    style V fill:#c8e6c9
```

## Usage Example

To render these diagrams:

1. **GitHub/GitLab**: Diagrams render automatically in markdown
2. **VS Code**: Install Mermaid Preview extension
3. **Online**: Use [mermaid.live](https://mermaid.live) to visualize
4. **CLI**: Use `mermaid-cli` to generate images

```bash
# Generate PNG from markdown
mmdc -i csv-parser-mermaid-diagram.md -o parser-diagram.png
```

## Error Handling Strategy

The parser implements comprehensive error handling:

- **File Level**: I/O errors, missing files
- **Format Level**: Invalid BADC-CSV structure
- **Record Level**: Malformed CSV records
- **Data Level**: Invalid field values
- **Validation Level**: Business rule violations

All errors are captured with context while processing continues for maximum data recovery.
