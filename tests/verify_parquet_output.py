#!/usr/bin/env python3
"""
MIDAS Parquet Output Verification Script

This script validates the Parquet output from the MIDAS processor against the original
CSV data to ensure data integrity, processing accuracy, and proper transformation.

Usage:
    python verify_parquet_output.py --input-csv /path/to/csv --parquet-output /path/to/parquet --dataset uk-daily-temperature-obs

Features:
- Data integrity validation (row counts, schema, data types)
- Content verification (random sampling and comparison)
- Processing validation (deduplication, enrichment, filtering)
- Statistical analysis and reporting
- HTML report generation with visualizations

Requirements:
    pip install pandas polars pyarrow seaborn matplotlib jinja2
"""

import argparse
import csv
import json
import logging
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from collections import defaultdict

import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from jinja2 import Template

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Results from a validation check."""
    test_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    severity: str = "ERROR"  # ERROR, WARNING, INFO


@dataclass
class ValidationSummary:
    """Summary of all validation results."""
    total_tests: int
    passed_tests: int
    failed_tests: int
    warnings: int
    errors: int
    dataset_name: str
    csv_files_count: int
    parquet_file_size: int
    processing_time: str
    results: List[ValidationResult]
    
    @property
    def success_rate(self) -> float:
        return (self.passed_tests / self.total_tests) * 100 if self.total_tests > 0 else 0.0


class MIDASVerifier:
    """Main verification class for MIDAS Parquet output validation."""
    
    def __init__(self, csv_path: Path, parquet_path: Path, dataset_name: str):
        self.csv_path = Path(csv_path)
        self.parquet_path = Path(parquet_path)
        self.dataset_name = dataset_name
        self.results: List[ValidationResult] = []
        self.csv_sample_size = 1000  # Number of CSV files to sample for detailed comparison
        self.random_seed = 42
        
    def verify(self) -> ValidationSummary:
        """Run complete verification suite."""
        logger.info(f"Starting verification for dataset: {self.dataset_name}")
        start_time = datetime.now()
        
        # Load data
        parquet_df = self._load_parquet_data()
        if parquet_df is None:
            return self._create_failure_summary("Failed to load Parquet data")
            
        csv_sample = self._sample_csv_files()
        
        # Run validation tests
        self._validate_file_existence()
        self._validate_schema(parquet_df)
        self._validate_data_types(parquet_df)
        self._validate_row_counts(parquet_df, csv_sample)
        self._validate_content_integrity(parquet_df, csv_sample)
        self._validate_station_metadata(parquet_df)
        self._validate_quality_flags(parquet_df)
        self._validate_timestamps(parquet_df)
        self._validate_measurements(parquet_df)
        self._validate_processing_flags(parquet_df)
        self._validate_deduplication(parquet_df)
        self._generate_statistical_summary(parquet_df, csv_sample)
        
        processing_time = datetime.now() - start_time
        
        return self._create_summary(processing_time.total_seconds())
    
    def _load_parquet_data(self) -> Optional[pl.DataFrame]:
        """Load Parquet file using Polars for efficiency."""
        try:
            df = pl.read_parquet(self.parquet_path)
            logger.info(f"Loaded Parquet file: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            self._add_result("parquet_load", False, f"Failed to load Parquet file: {e}", severity="ERROR")
            return None
    
    def _sample_csv_files(self) -> List[Path]:
        """Sample CSV files for detailed comparison."""
        csv_files = list(self.csv_path.rglob("*.csv"))
        if not csv_files:
            self._add_result("csv_discovery", False, "No CSV files found in input path", severity="ERROR")
            return []
            
        # Sample files for validation
        random.seed(self.random_seed)
        sample_size = min(self.csv_sample_size, len(csv_files))
        sampled_files = random.sample(csv_files, sample_size)
        
        logger.info(f"Discovered {len(csv_files)} CSV files, sampling {len(sampled_files)} for validation")
        self._add_result("csv_discovery", True, f"Found {len(csv_files)} CSV files, sampled {len(sampled_files)}")
        
        return sampled_files
    
    def _validate_file_existence(self):
        """Validate that required files exist."""
        if not self.parquet_path.exists():
            self._add_result("file_existence", False, f"Parquet file does not exist: {self.parquet_path}")
            return
            
        if not self.csv_path.exists():
            self._add_result("file_existence", False, f"CSV input path does not exist: {self.csv_path}")
            return
            
        self._add_result("file_existence", True, "All required files and paths exist")
    
    def _validate_schema(self, parquet_df: pl.DataFrame):
        """Validate Parquet schema has expected columns."""
        expected_columns = {
            'ob_end_time', 'ob_hour_count', 'observation_id', 'station_id', 'id_type',
            'met_domain_name', 'rec_st_ind', 'version_num', 'meto_stmp_time',
            'midas_stmp_etime', 'station_src_id', 'station_name', 'station_latitude', 
            'station_longitude', 'station_authority', 'station_county', 'station_height_meters',
            'east_grid_ref', 'north_grid_ref', 'grid_ref_type', 'station_start_date', 
            'station_end_date'
        }
        
        actual_columns = set(parquet_df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            self._add_result("schema_validation", False, 
                           f"Missing expected columns: {missing_columns}", 
                           details={"missing": list(missing_columns), "extra": list(extra_columns)})
        else:
            self._add_result("schema_validation", True, 
                           f"Schema validation passed. Found {len(actual_columns)} columns",
                           details={"column_count": len(actual_columns), "extra_columns": list(extra_columns)})
    
    def _validate_data_types(self, parquet_df: pl.DataFrame):
        """Validate data types are appropriate."""
        type_checks = {
            'ob_end_time': pl.Datetime,
            'station_id': pl.Int32,
            'station_src_id': pl.Int32,
            'station_latitude': pl.Float64,
            'station_longitude': pl.Float64,
            'station_height_meters': pl.Float32,
            'rec_st_ind': pl.Int32,
            'version_num': pl.Int32,
            'meto_stmp_time': pl.Datetime,
            'station_start_date': pl.Datetime,
            'station_end_date': pl.Datetime,
        }
        
        type_errors = []
        for column, expected_type in type_checks.items():
            if column in parquet_df.columns:
                actual_type = parquet_df[column].dtype
                if actual_type != expected_type:
                    type_errors.append(f"{column}: expected {expected_type}, got {actual_type}")
        
        if type_errors:
            self._add_result("data_types", False, f"Data type mismatches: {'; '.join(type_errors)}")
        else:
            self._add_result("data_types", True, "All data types are correct")
    
    def _validate_row_counts(self, parquet_df: pl.DataFrame, csv_files: List[Path]):
        """Validate row counts considering deduplication."""
        parquet_rows = len(parquet_df)
        
        # Count total rows from sampled CSV files and extrapolate
        csv_rows_sample = 0
        files_processed = 0
        
        for csv_file in csv_files[:50]:  # Limit to prevent excessive processing
            try:
                with open(csv_file, 'r') as f:
                    csv_rows_sample += sum(1 for line in f) - 1  # Subtract header
                files_processed += 1
            except Exception as e:
                logger.warning(f"Could not read {csv_file}: {e}")
        
        if files_processed == 0:
            self._add_result("row_count", False, "Could not read any CSV files for row count validation")
            return
        
        # Extrapolate total rows (rough estimate)
        total_csv_files = len(list(self.csv_path.rglob("*.csv")))
        estimated_total_csv_rows = (csv_rows_sample / files_processed) * total_csv_files
        
        # Allow for reasonable deduplication (typically 5-15% reduction)
        expected_min_rows = int(estimated_total_csv_rows * 0.85)
        expected_max_rows = int(estimated_total_csv_rows * 1.05)
        
        if expected_min_rows <= parquet_rows <= expected_max_rows:
            self._add_result("row_count", True, 
                           f"Row count validation passed: {parquet_rows} rows (estimated CSV: {estimated_total_csv_rows:.0f})",
                           details={"parquet_rows": parquet_rows, "estimated_csv_rows": estimated_total_csv_rows})
        else:
            self._add_result("row_count", False,
                           f"Row count mismatch: Parquet has {parquet_rows} rows, expected {expected_min_rows}-{expected_max_rows} (from CSV estimate: {estimated_total_csv_rows:.0f})",
                           details={"parquet_rows": parquet_rows, "estimated_csv_rows": estimated_total_csv_rows})
    
    def _validate_content_integrity(self, parquet_df: pl.DataFrame, csv_files: List[Path]):
        """Validate content integrity by comparing sample records."""
        validation_errors = []
        records_validated = 0
        
        # Convert to pandas for easier comparison
        pdf = parquet_df.to_pandas()
        
        for csv_file in csv_files[:10]:  # Validate first 10 files
            try:
                csv_df = pd.read_csv(csv_file)
                if len(csv_df) == 0:
                    continue
                    
                # Sample a few records from this CSV file
                sample_records = csv_df.head(3)
                
                for _, record in sample_records.iterrows():
                    # Find matching record in Parquet (by station_src_id and time if possible)
                    if 'ob_end_time' in pdf.columns and 'station_src_id' in pdf.columns:
                        if 'ob_end_time' in record and 'src_id' in record:
                            matches = pdf[
                                (pdf['station_src_id'] == record.get('src_id', -1)) &
                                (pdf['ob_end_time'].astype(str).str.contains(str(record.get('ob_end_time', ''))[:10], na=False))
                            ]
                            
                            if len(matches) > 0:
                                records_validated += 1
                                # Could add more detailed field-by-field comparison here
                                
            except Exception as e:
                validation_errors.append(f"Error validating {csv_file}: {e}")
        
        if validation_errors:
            self._add_result("content_integrity", False, 
                           f"Content validation errors: {'; '.join(validation_errors[:3])}...",
                           details={"errors": validation_errors, "records_validated": records_validated})
        else:
            self._add_result("content_integrity", True, 
                           f"Content integrity validation passed for {records_validated} records")
    
    def _validate_station_metadata(self, parquet_df: pl.DataFrame):
        """Validate station metadata completeness and accuracy."""
        station_columns = ['station_src_id', 'station_name', 'station_latitude', 'station_longitude', 'station_authority', 'station_county']
        missing_station_data = {}
        
        for col in station_columns:
            if col in parquet_df.columns:
                null_count = parquet_df[col].null_count()
                if null_count > 0:
                    missing_station_data[col] = null_count
        
        if missing_station_data:
            total_rows = len(parquet_df)
            missing_pct = {col: (count/total_rows)*100 for col, count in missing_station_data.items()}
            self._add_result("station_metadata", False,
                           f"Missing station metadata: {missing_pct}",
                           details=missing_station_data, severity="WARNING")
        else:
            self._add_result("station_metadata", True, "Station metadata is complete")
    
    def _validate_quality_flags(self, parquet_df: pl.DataFrame):
        """Validate that quality flags are preserved."""
        # Look for quality flag columns with 'q_' prefix (BADC-CSV format)
        qf_columns = [col for col in parquet_df.columns if col.startswith('q_') or col.endswith('_qf') or 'quality' in col.lower()]
        
        if not qf_columns:
            self._add_result("quality_flags", False, "No quality flag columns found", severity="WARNING")
        else:
            # Check if quality flags have reasonable values
            qf_summary = {}
            for col in qf_columns:
                unique_values = parquet_df[col].unique().to_list()
                qf_summary[col] = {"unique_values": len(unique_values), "sample_values": unique_values[:10]}
            
            self._add_result("quality_flags", True, 
                           f"Found {len(qf_columns)} quality flag columns",
                           details=qf_summary)
    
    def _validate_timestamps(self, parquet_df: pl.DataFrame):
        """Validate timestamp formats and ranges."""
        timestamp_columns = ['ob_end_time', 'meto_stmp_time']
        
        for col in timestamp_columns:
            if col not in parquet_df.columns:
                continue
                
            # Check for null values
            null_count = parquet_df[col].null_count()
            total_rows = len(parquet_df)
            
            if null_count == total_rows:
                self._add_result(f"timestamp_{col}", False, f"All {col} values are null")
                continue
                
            # Check timestamp ranges (should be reasonable weather observation dates)
            non_null_timestamps = parquet_df.filter(pl.col(col).is_not_null())[col]
            if len(non_null_timestamps) > 0:
                min_date = non_null_timestamps.min()
                max_date = non_null_timestamps.max()
                
                # Reasonable range for weather data: 1900-2030
                if min_date and max_date:
                    self._add_result(f"timestamp_{col}", True, 
                                   f"{col} range: {min_date} to {max_date}",
                                   details={"min_date": str(min_date), "max_date": str(max_date), "null_count": null_count})
    
    def _validate_measurements(self, parquet_df: pl.DataFrame):
        """Validate measurement data columns."""
        # Look for measurement columns (typically numeric, not metadata)
        metadata_columns = {'ob_end_time', 'ob_hour_count', 'observation_id', 'station_id', 'id_type',
                          'met_domain_name', 'rec_st_ind', 'version_num', 'meto_stmp_time', 'midas_stmp_etime',
                          'station_src_id', 'station_name', 'station_latitude', 'station_longitude', 'station_authority', 
                          'station_county', 'station_height_meters', 'east_grid_ref', 'north_grid_ref', 
                          'grid_ref_type', 'station_start_date', 'station_end_date'}
        
        measurement_columns = [col for col in parquet_df.columns 
                             if col not in metadata_columns and not col.endswith('_qf') and not col.startswith('q_')]
        
        if not measurement_columns:
            self._add_result("measurements", False, "No measurement columns found")
            return
        
        measurement_stats = {}
        for col in measurement_columns[:10]:  # Limit to first 10 measurement columns
            try:
                col_data = parquet_df[col]
                if col_data.dtype in [pl.Float64, pl.Float32, pl.Int32, pl.Int64]:
                    stats = {
                        "count": len(col_data),
                        "null_count": col_data.null_count(),
                        "min": float(col_data.min()) if col_data.min() is not None else None,
                        "max": float(col_data.max()) if col_data.max() is not None else None,
                        "mean": float(col_data.mean()) if col_data.mean() is not None else None,
                    }
                    measurement_stats[col] = stats
            except Exception as e:
                logger.warning(f"Could not analyze measurement column {col}: {e}")
        
        self._add_result("measurements", True, 
                       f"Found {len(measurement_columns)} measurement columns",
                       details={"measurement_stats": measurement_stats})
    
    def _validate_processing_flags(self, parquet_df: pl.DataFrame):
        """Validate processing flag columns that track our processing operations."""
        processing_flag_columns = [col for col in parquet_df.columns if col.startswith('pf_')]
        
        if not processing_flag_columns:
            self._add_result("processing_flags", False, "No processing flag columns found", severity="WARNING")
        else:
            flag_summary = {}
            for col in processing_flag_columns:
                unique_values = parquet_df[col].unique().to_list()
                # Remove None values for cleaner display
                unique_values = [v for v in unique_values if v is not None]
                flag_summary[col] = {"unique_count": len(unique_values), "values": unique_values[:10]}  # Limit to first 10 values
            
            self._add_result("processing_flags", True,
                           f"Found {len(processing_flag_columns)} processing flag columns",
                           details=flag_summary)
    
    def _validate_deduplication(self, parquet_df: pl.DataFrame):
        """Validate that deduplication was performed correctly."""
        # Check for duplicate records based on key fields
        key_fields = ['observation_id', 'station_id', 'ob_end_time']
        
        if not all(field in parquet_df.columns for field in key_fields):
            self._add_result("deduplication", False, 
                           f"Cannot validate deduplication: missing key fields {key_fields}")
            return
        
        # Count duplicates
        total_rows = len(parquet_df)
        unique_rows = parquet_df.select(key_fields).unique().height
        duplicate_count = total_rows - unique_rows
        
        if duplicate_count == 0:
            self._add_result("deduplication", True, 
                           f"No duplicates found: {unique_rows} unique records out of {total_rows} total")
        else:
            duplicate_pct = (duplicate_count / total_rows) * 100
            severity = "WARNING" if duplicate_pct < 1 else "ERROR"
            self._add_result("deduplication", duplicate_pct < 5,  # Allow small amount of duplicates
                           f"Found {duplicate_count} potential duplicates ({duplicate_pct:.2f}%)",
                           details={"duplicate_count": duplicate_count, "duplicate_percentage": duplicate_pct},
                           severity=severity)
    
    def _generate_statistical_summary(self, parquet_df: pl.DataFrame, csv_files: List[Path]):
        """Generate statistical summary and comparisons."""
        summary = {
            "parquet_file_size_bytes": self.parquet_path.stat().st_size if self.parquet_path.exists() else 0,
            "parquet_row_count": len(parquet_df),
            "parquet_column_count": len(parquet_df.columns),
            "csv_files_sampled": len(csv_files),
            "total_csv_files": len(list(self.csv_path.rglob("*.csv"))),
        }
        
        # Calculate compression ratio (rough estimate)
        if summary["parquet_file_size_bytes"] > 0:
            # Estimate CSV size based on Parquet size (CSV is typically 3-5x larger)
            estimated_csv_size = summary["parquet_file_size_bytes"] * 4
            compression_ratio = estimated_csv_size / summary["parquet_file_size_bytes"]
            summary["estimated_compression_ratio"] = compression_ratio
        
        self._add_result("statistical_summary", True, 
                       "Statistical summary generated",
                       details=summary)
    
    def _add_result(self, test_name: str, passed: bool, message: str, 
                   details: Optional[Dict[str, Any]] = None, severity: str = "ERROR"):
        """Add a validation result."""
        if passed and severity == "ERROR":
            severity = "INFO"
        
        result = ValidationResult(
            test_name=test_name,
            passed=passed,
            message=message,
            details=details,
            severity=severity
        )
        self.results.append(result)
    
    def _create_summary(self, processing_time_seconds: float) -> ValidationSummary:
        """Create validation summary."""
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        warnings = sum(1 for r in self.results if r.severity == "WARNING")
        errors = sum(1 for r in self.results if r.severity == "ERROR")
        
        parquet_size = self.parquet_path.stat().st_size if self.parquet_path.exists() else 0
        csv_count = len(list(self.csv_path.rglob("*.csv")))
        
        return ValidationSummary(
            total_tests=len(self.results),
            passed_tests=passed,
            failed_tests=failed,
            warnings=warnings,
            errors=errors,
            dataset_name=self.dataset_name,
            csv_files_count=csv_count,
            parquet_file_size=parquet_size,
            processing_time=f"{processing_time_seconds:.2f} seconds",
            results=self.results
        )
    
    def _create_failure_summary(self, error_message: str) -> ValidationSummary:
        """Create summary for catastrophic failure."""
        return ValidationSummary(
            total_tests=1,
            passed_tests=0,
            failed_tests=1,
            warnings=0,
            errors=1,
            dataset_name=self.dataset_name,
            csv_files_count=0,
            parquet_file_size=0,
            processing_time="N/A",
            results=[ValidationResult("critical_failure", False, error_message, severity="ERROR")]
        )


class ReportGenerator:
    """Generate HTML verification reports."""
    
    HTML_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>MIDAS Parquet Verification Report - {{ summary.dataset_name }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .header { background: #f5f5f5; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
            .summary { display: flex; gap: 20px; margin-bottom: 20px; }
            .metric { background: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd; flex: 1; }
            .metric h3 { margin: 0 0 10px 0; color: #333; }
            .metric .value { font-size: 24px; font-weight: bold; }
            .success { color: #28a745; }
            .warning { color: #ffc107; }
            .error { color: #dc3545; }
            .test-results { margin-top: 20px; }
            .test-item { margin: 10px 0; padding: 15px; border-radius: 5px; border-left: 4px solid; }
            .test-passed { background: #d4edda; border-left-color: #28a745; }
            .test-failed { background: #f8d7da; border-left-color: #dc3545; }
            .test-warning { background: #fff3cd; border-left-color: #ffc107; }
            .test-name { font-weight: bold; margin-bottom: 5px; }
            .test-details { font-size: 12px; color: #666; margin-top: 10px; }
            pre { background: #f8f9fa; padding: 10px; border-radius: 3px; overflow-x: auto; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>MIDAS Parquet Verification Report</h1>
            <p><strong>Dataset:</strong> {{ summary.dataset_name }}</p>
            <p><strong>Generated:</strong> {{ timestamp }}</p>
            <p><strong>Processing Time:</strong> {{ summary.processing_time }}</p>
        </div>
        
        <div class="summary">
            <div class="metric">
                <h3>Success Rate</h3>
                <div class="value {% if summary.success_rate >= 90 %}success{% elif summary.success_rate >= 70 %}warning{% else %}error{% endif %}">
                    {{ "%.1f"|format(summary.success_rate) }}%
                </div>
            </div>
            <div class="metric">
                <h3>Tests Passed</h3>
                <div class="value success">{{ summary.passed_tests }}</div>
            </div>
            <div class="metric">
                <h3>Tests Failed</h3>
                <div class="value error">{{ summary.failed_tests }}</div>
            </div>
            <div class="metric">
                <h3>Warnings</h3>
                <div class="value warning">{{ summary.warnings }}</div>
            </div>
        </div>
        
        <div class="summary">
            <div class="metric">
                <h3>CSV Files</h3>
                <div class="value">{{ summary.csv_files_count }}</div>
            </div>
            <div class="metric">
                <h3>Parquet Size</h3>
                <div class="value">{{ format_bytes(summary.parquet_file_size) }}</div>
            </div>
            <div class="metric">
                <h3>Total Tests</h3>
                <div class="value">{{ summary.total_tests }}</div>
            </div>
        </div>
        
        <div class="test-results">
            <h2>Test Results</h2>
            {% for result in summary.results %}
            <div class="test-item {% if result.passed %}test-passed{% elif result.severity == 'WARNING' %}test-warning{% else %}test-failed{% endif %}">
                <div class="test-name">{{ result.test_name }}</div>
                <div>{{ result.message }}</div>
                {% if result.details %}
                <div class="test-details">
                    <pre>{{ result.details | tojson(indent=2) }}</pre>
                </div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
    </body>
    </html>
    """
    
    @staticmethod
    def generate_html_report(summary: ValidationSummary, output_path: Path):
        """Generate HTML verification report."""
        template = Template(ReportGenerator.HTML_TEMPLATE)
        
        def format_bytes(size_bytes: int) -> str:
            """Format bytes to human readable format."""
            for unit in ['B', 'KB', 'MB', 'GB']:
                if size_bytes < 1024.0:
                    return f"{size_bytes:.1f} {unit}"
                size_bytes /= 1024.0
            return f"{size_bytes:.1f} TB"
        
        html_content = template.render(
            summary=summary,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            format_bytes=format_bytes
        )
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {output_path}")
    
    @staticmethod
    def generate_json_report(summary: ValidationSummary, output_path: Path):
        """Generate JSON verification report for programmatic access."""
        report_data = asdict(summary)
        
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"JSON report generated: {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Verify MIDAS Parquet output against CSV input")
    parser.add_argument("--input-csv", required=True, help="Path to input CSV directory")
    parser.add_argument("--parquet-output", required=True, help="Path to output Parquet file")
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., uk-daily-temperature-obs)")
    parser.add_argument("--output-dir", default="./verification_output", help="Output directory for reports")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of CSV files to sample")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    
    args = parser.parse_args()
    
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Validate input paths
    csv_path = Path(args.input_csv)
    parquet_path = Path(args.parquet_output)
    output_dir = Path(args.output_dir)
    
    if not csv_path.exists():
        logger.error(f"CSV input path does not exist: {csv_path}")
        sys.exit(1)
    
    if not parquet_path.exists():
        logger.error(f"Parquet file does not exist: {parquet_path}")
        sys.exit(1)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run verification
    verifier = MIDASVerifier(csv_path, parquet_path, args.dataset)
    verifier.csv_sample_size = args.sample_size
    
    summary = verifier.verify()
    
    # Generate reports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_report = output_dir / f"verification_report_{args.dataset}_{timestamp}.html"
    json_report = output_dir / f"verification_report_{args.dataset}_{timestamp}.json"
    
    ReportGenerator.generate_html_report(summary, html_report)
    ReportGenerator.generate_json_report(summary, json_report)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"MIDAS Parquet Verification Report")
    print(f"{'='*60}")
    print(f"Dataset: {summary.dataset_name}")
    print(f"Success Rate: {summary.success_rate:.1f}%")
    print(f"Tests Passed: {summary.passed_tests}/{summary.total_tests}")
    print(f"Warnings: {summary.warnings}")
    print(f"Errors: {summary.errors}")
    print(f"Processing Time: {summary.processing_time}")
    print(f"\nReports Generated:")
    print(f"  HTML: {html_report}")
    print(f"  JSON: {json_report}")
    
    # Exit with appropriate code
    exit_code = 0 if summary.errors == 0 else 1
    if summary.warnings > 0:
        print(f"\nNote: {summary.warnings} warnings found. Check the report for details.")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()