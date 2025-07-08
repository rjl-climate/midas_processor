# MIDAS Validation Commands Guide

This guide covers all available validation commands for testing the MIDAS processing pipeline with real data.

## 🧪 **MIDAS Validation Commands**

### **Basic Command Structure**
```bash
midas-processor validate [OPTIONS]
```

## **Core Options**

### **Input/Output Control**
```bash
# Specify cache directory (required)
--input <PATH>
--input '/Users/richardlyon/Library/Application Support/midas-fetcher/cache'

# Set output directory for results  
--output <PATH>
--output ./validation_results

# Control number of files to test
--max-files <COUNT>
--max-files 100    # Test 100 files (default: 1000)
```

### **Dataset Filtering**
```bash
# Test specific datasets only
--datasets <LIST>
--datasets uk-daily-temperature-obs
--datasets uk-daily-temperature-obs,uk-daily-rain-obs

# Available datasets:
# uk-daily-temperature-obs, uk-daily-weather-obs, uk-daily-rain-obs
# uk-hourly-weather-obs, uk-hourly-rain-obs, uk-mean-wind-obs  
# uk-radiation-obs, uk-soil-temperature-obs
```

### **File Size Filtering**
```bash
# Minimum file size to include (default: 1KB)
--min-file-size 5120    # 5KB minimum

# Maximum file size to include (default: 100MB)  
--max-file-size 52428800    # 50MB maximum
```

### **Quality Control Options**
```bash
# Include suspect quality data in validation
--include-suspect

# Include unchecked quality data  
--include-unchecked

# Set minimum QC version (default: 1)
--qc-version 2
```

### **Processing Control**
```bash
# Continue processing after errors (default: true)
--continue-on-error

# Stop on first error
--no-continue-on-error

# Maximum processing time per file (default: 300s)
--max-processing-time 60    # 1 minute timeout
```

### **Output Formats**
```bash
# Human-readable output (default)
--output-format human

# JSON output for scripting
--output-format json  

# CSV output for data analysis
--output-format csv
```

### **Logging Control**
```bash
# Verbose output levels
--verbose        # -v: info level
--verbose --verbose    # -vv: debug level  
--verbose --verbose --verbose    # -vvv: trace level

# Quiet mode (errors only)
--quiet
```

## **Common Usage Examples**

### **Quick Test (5 files)**
```bash
midas-processor validate \
  --input '/Users/richardlyon/Library/Application Support/midas-fetcher/cache' \
  --output ./quick_test \
  --max-files 5 \
  --verbose
```

### **Temperature Data Focus**
```bash
midas-processor validate \
  --input ~/midas-cache \
  --output ./temp_validation \
  --datasets uk-daily-temperature-obs \
  --max-files 50 \
  --include-suspect \
  --verbose
```

### **Comprehensive Test**
```bash
midas-processor validate \
  --input '/Users/richardlyon/Library/Application Support/midas-fetcher/cache' \
  --output ./full_validation \
  --max-files 1000 \
  --min-file-size 1024 \
  --max-file-size 10485760 \
  --continue-on-error \
  --verbose
```

### **Production Validation**
```bash
midas-processor validate \
  --input ~/production-cache \
  --output ./production_validation \
  --max-files 500 \
  --datasets uk-daily-temperature-obs,uk-daily-rain-obs \
  --qc-version 1 \
  --output-format json \
  --verbose > validation.log 2>&1
```

### **Performance Testing**
```bash
midas-processor validate \
  --input ~/cache \
  --output ./performance_test \
  --max-files 100 \
  --max-processing-time 30 \
  --min-file-size 10240 \
  --verbose
```

## **Output Files Generated**

Every validation run creates:

```bash
📁 <output-directory>/
├── detailed_stats.json     # Complete statistics and metrics
├── test_summary.md         # Human-readable report  
└── issues.csv             # List of all issues found
```

### **Reading Results**
```bash
# View summary report
cat ./validation_output/test_summary.md

# Analyze issues  
less ./validation_output/issues.csv

# Process JSON results
jq '.issue_counts' ./validation_output/detailed_stats.json
```

## **Validation Process**

The validation system:

1. **Discovers Files**: Scans cache directory for MIDAS CSV files
2. **Intelligent Sampling**: Selects representative files across:
   - Different datasets (temperature, rain, radiation, etc.)
   - Various time periods (historical to recent)
   - Multiple geographic regions
   - Different file sizes

3. **Complete Pipeline Testing**:
   - ✅ BADC-CSV parsing
   - ✅ Station metadata enrichment
   - ✅ Record deduplication  
   - ✅ Quality control filtering
   - ✅ Statistics generation

4. **Issue Detection**: Identifies:
   - Parse errors (malformed CSV, invalid dates)
   - Data quality problems (missing fields, invalid values)
   - Performance issues (slow processing, memory usage)
   - Station metadata problems

5. **Comprehensive Reporting**: Generates actionable reports with:
   - Overall success rates
   - Issue categorization and severity
   - File-level problem identification
   - Performance metrics

## **Interpreting Results**

### **Success Indicators**
- **✅ Overall Status: PASS** - No critical issues detected
- **High success rate** (>95%) - Most records processed successfully
- **No critical issues** - Pipeline ready for production

### **Warning Signs**
- **❌ Overall Status: FAIL** - Critical issues found
- **Low success rate** (<90%) - Systematic processing problems
- **Many parse errors** - Data format issues
- **Performance problems** - Memory/timing issues

### **Next Steps Based on Results**

**If validation passes:**
- ✅ Ready to proceed with parquet writer
- ✅ Pipeline is robust for production use

**If validation fails:**
- 🔍 Review issues.csv for specific problems
- 🛠️ Fix identified parsing/processing issues
- 🔄 Re-run validation after fixes

## **Help Commands**
```bash
# Get full help
midas-processor validate --help

# Get general help
midas-processor --help

# List all commands
midas-processor help
```

## **Technical Details**

### **Sampling Strategy**
- Stratified sampling across datasets
- Temporal distribution (old to recent data)
- Geographic coverage (different regions)
- File size diversity (small to large files)

### **Quality Assurance**
- Tests both capability and observation files
- Validates station metadata enrichment
- Checks deduplication logic
- Verifies quality control filtering

### **Performance Monitoring**
- Processing time per file
- Memory usage tracking
- Error rate analysis
- Throughput measurement

The validation system provides confidence that your MIDAS processing pipeline works correctly with real data complexity before proceeding to production processing or parquet file generation.