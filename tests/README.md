# MIDAS Processor Tests

This directory contains validation and testing tools for the MIDAS processor.

## validate_parquet.py

Comprehensive validation tool for MIDAS parquet files that ensures data integrity, optimal performance, and correct formatting.

### Purpose
Validates that generated parquet files maintain:
- Data integrity compared to original CSV sources
- Correct sorting (station_id, then timestamp)
- Optimal row grouping for query performance
- Proper compression and statistics
- Expected schema and data types

### Usage

```bash
python tests/validate_parquet.py PARQUET_FILE CSV_SOURCE_DIR [OPTIONS]
```

**Arguments:**
- `PARQUET_FILE`: Path to parquet file to validate
- `CSV_SOURCE_DIR`: Path to directory containing original CSV files

**Options:**
- `--performance-tests`: Run query performance validation
- `--full-validation`: Run extended validation tests
- `--json-report FILENAME`: Save detailed JSON report (saves to tests/ directory)
- `--verbose`: Verbose output

### Example

```bash
# Full validation with JSON report
python tests/validate_parquet.py \
  "/path/to/uk-radiation-obs-202507.parquet" \
  "/path/to/csv-source" \
  --performance-tests \
  --full-validation \
  --json-report validation_report.json
```

### Test Categories

1. **Basic Properties**: File existence, readability, size checks
2. **Data Integrity**: Row counts, station counts, date ranges
3. **Schema**: Column presence, data types, reasonable column count
4. **Sorting**: Station and timestamp ordering validation
5. **Parquet Metadata**: Row groups, compression, statistics
6. **Performance**: Query speed tests (optional)
7. **Extended**: Memory usage, aggregation performance (optional)

### Dependencies

```bash
pip install polars pandas pyarrow rich
```

### Exit Codes
- `0`: All tests passed
- `1`: One or more tests failed