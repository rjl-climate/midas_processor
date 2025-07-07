# BADC-CSV Parser Comprehensive Guide

## Overview

This guide provides a complete understanding of the BADC-CSV parser implementation in the MIDAS processor. It explains the file formats, parsing mechanics, failure modes, and real-world complexities encountered when processing MIDAS weather observation data.

## File Types in MIDAS System

### 1. Capability Files
**Purpose**: Define station metadata and reporting capabilities
**Location**: `{cache_root}/{dataset}/capability/{county}/{station_id}_{station_name}/`
**Filename Pattern**: `midas-open_{dataset}_dv-{version}_{county}_{station_id}_{station_name}_capability.csv`

**Contents**:
- **Header Section**: Station metadata (lines 1-N until "data" marker)
  - Global attributes: `src_id`, `location`, `height`, `date_valid`
  - Column definitions with metadata
  - Missing value markers
- **Data Section**: Capability records (after "data" marker)
  - ID mappings for different reporting types
  - Date ranges for each capability

**Example Structure**:
```csv
Conventions,G,BADC-CSV,1
title,G,Midas Open: Station capability information
src_id,G,01330
location,G,50.77,-4.346
height,G,120,m
date_valid,G,1960-01-01 00:00:00,1965-12-31 23:59:59
data
id,id_type,met_domain_name,first_year,last_year
8803,DCNN,DLY3208,1960,1965
end data
```

### 2. Observation Data Files
**Purpose**: Contain actual weather measurements
**Location**: `{cache_root}/{dataset}/qcv-{version}/{county}/{station_id}_{station_name}/`
**Filename Pattern**: `midas-open_{dataset}_dv-{version}_{county}_{station_id}_{station_name}_qcv-{version}_{year}.csv`

**Contents**:
- **Header Section**: Observation metadata and column definitions
  - Global attributes: `title`, `source`, `missing_value`
  - Column definitions for all measurement types
  - Quality flag definitions
- **Data Section**: Time-series observations
  - Temporal columns: `ob_end_time`, `ob_hour_count`
  - Station identifiers: `id`, `id_type`, `src_id`
  - Weather measurements: `max_air_temp`, `min_air_temp`, etc.
  - Quality flags: `max_air_temp_q`, `min_air_temp_q`, etc.

**Example Structure**:
```csv
Conventions,G,BADC-CSV,1
title,G,uk-daily-temperature-obs
missing_value,G,NA
long_name,max_air_temp,maximum air temperature,degC
type,max_air_temp,float
data
ob_end_time,id_type,id,src_id,max_air_temp,max_air_temp_q
1960-08-01 09:00:00,DCNN,8803,1330,17.8,0
```

### 3. Centralized Metadata Files (Future)
**Purpose**: Station metadata in a single file per dataset
**Status**: Not yet implemented, but parser is designed to handle them

## BADC-CSV Format Structure

### Two-Section Format
All BADC-CSV files follow a consistent two-section structure:

1. **Header Section** (Lines 1 to "data" marker)
   - Global attributes: `key,G,value[,units]`
   - Column definitions: `attribute,column_name,value[,units]`
   - Common attributes: `long_name`, `type`, `units`, `comments`

2. **Data Section** (After "data" marker to "end data" or EOF)
   - Column headers: Comma-separated field names
   - Data records: Comma-separated values matching header order

### Key Features
- **Self-Documenting**: Header contains complete metadata
- **Flexible**: Column order can vary between files
- **Standardized**: Consistent attribute naming across datasets
- **Quality-Aware**: Built-in quality flag system

#
