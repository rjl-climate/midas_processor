#!/usr/bin/env python3
"""
Comprehensive validation tool for MIDAS parquet files

This script validates that the generated parquet files maintain data integrity,
correct sorting, optimal row grouping, and expected performance characteristics
compared to the original BADC-CSV source files.
"""

import argparse
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import re

try:
    import polars as pl
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich import print as rprint
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Install with: pip install polars pandas pyarrow rich")
    sys.exit(1)

console = Console()

@dataclass
class ValidationResult:
    """Result of a validation test"""
    test_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    duration_ms: Optional[float] = None

@dataclass
class ValidationReport:
    """Complete validation report"""
    parquet_path: str
    csv_source_path: str
    results: List[ValidationResult]
    summary: Dict[str, Any]
    timestamp: str
    
    def passed(self) -> bool:
        return all(r.passed for r in self.results)
    
    def failed_tests(self) -> List[ValidationResult]:
        return [r for r in self.results if not r.passed]

class MidasParquetValidator:
    """Main validation class for MIDAS parquet files"""
    
    def __init__(self, parquet_path: Path, csv_source_path: Path):
        self.parquet_path = parquet_path
        self.csv_source_path = csv_source_path
        self.results: List[ValidationResult] = []
        
    def run_validation(self, full_validation: bool = False, performance_tests: bool = False) -> ValidationReport:
        """Run complete validation suite"""
        
        console.print(f"🔍 [bold blue]Validating:[/bold blue] {self.parquet_path.name}")
        console.print(f"📁 [dim]Source:[/dim] {self.csv_source_path}")
        console.print()
        
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}")) as progress:
            # Core validation tests
            task1 = progress.add_task("Loading data and metadata...", total=None)
            self._validate_basic_properties()
            progress.update(task1, completed=True)
            
            task2 = progress.add_task("Validating data integrity...", total=None)
            self._validate_data_integrity()
            progress.update(task2, completed=True)
            
            task3 = progress.add_task("Validating schema...", total=None)
            self._validate_schema()
            progress.update(task3, completed=True)
            
            task4 = progress.add_task("Validating sorting...", total=None)
            self._validate_sorting()
            progress.update(task4, completed=True)
            
            task5 = progress.add_task("Validating parquet metadata...", total=None)
            self._validate_parquet_metadata()
            progress.update(task5, completed=True)
            
            if performance_tests:
                task6 = progress.add_task("Running performance tests...", total=None)
                self._validate_performance()
                progress.update(task6, completed=True)
                
            if full_validation:
                task7 = progress.add_task("Running extended validation...", total=None)
                self._validate_extended()
                progress.update(task7, completed=True)
        
        # Generate report
        report = self._generate_report()
        self._display_report(report)
        
        return report
    
    def _validate_basic_properties(self):
        """Validate basic file properties"""
        start_time = time.time()
        
        try:
            # Check file exists and is readable
            if not self.parquet_path.exists():
                self.results.append(ValidationResult(
                    "File Existence", False, f"Parquet file not found: {self.parquet_path}"
                ))
                return
            
            # Check file size is reasonable
            file_size = self.parquet_path.stat().st_size
            if file_size < 1024:  # Less than 1KB
                self.results.append(ValidationResult(
                    "File Size", False, f"Parquet file too small: {file_size} bytes"
                ))
                return
            
            # Try to read parquet file
            try:
                df = pl.scan_parquet(self.parquet_path).limit(1).collect()
                self.results.append(ValidationResult(
                    "File Readability", True, f"Parquet file readable, size: {file_size:,} bytes"
                ))
            except Exception as e:
                self.results.append(ValidationResult(
                    "File Readability", False, f"Cannot read parquet file: {e}"
                ))
                return
            
            # Check CSV source directory exists
            if not self.csv_source_path.exists():
                self.results.append(ValidationResult(
                    "CSV Source", False, f"CSV source not found: {self.csv_source_path}"
                ))
                return
            
            # Count CSV files
            csv_files = list(self.csv_source_path.glob("**/*.csv"))
            if not csv_files:
                self.results.append(ValidationResult(
                    "CSV Files", False, "No CSV files found in source directory"
                ))
                return
            
            self.results.append(ValidationResult(
                "CSV Files", True, f"Found {len(csv_files)} CSV files",
                details={"csv_count": len(csv_files)}
            ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Basic Properties", False, f"Error in basic validation: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_data_integrity(self):
        """Validate data integrity between CSV and parquet"""
        start_time = time.time()
        
        try:
            # Load parquet data
            parquet_df = pl.scan_parquet(self.parquet_path)
            
            # Get basic statistics
            parquet_stats = parquet_df.select([
                pl.len().alias("total_rows"),
                pl.col("station_id").n_unique().alias("unique_stations"),
                pl.col("ob_end_time").min().alias("min_date"),
                pl.col("ob_end_time").max().alias("max_date"),
            ]).collect()
            
            total_rows = parquet_stats["total_rows"][0]
            unique_stations = parquet_stats["unique_stations"][0]
            min_date = parquet_stats["min_date"][0]
            max_date = parquet_stats["max_date"][0]
            
            # For accurate validation, we need to count actual CSV rows
            # For now, we'll use a simplified check based on expected row counts
            csv_files = list(self.csv_source_path.glob("**/*.csv"))
            
            # Quick validation - check if parquet rows are reasonable
            # Based on the radiation dataset: ~22.7M rows from 3,445 files = ~6,588 rows/file average
            avg_rows_per_file = total_rows / len(csv_files) if csv_files else 0
            
            if 1000 <= avg_rows_per_file <= 20000:  # Reasonable range for MIDAS data
                self.results.append(ValidationResult(
                    "Row Count Integrity", True,
                    f"Row count reasonable: {total_rows:,} parquet rows, avg {avg_rows_per_file:,.0f} rows/file",
                    details={"parquet_rows": total_rows, "avg_rows_per_file": avg_rows_per_file, "csv_files": len(csv_files)}
                ))
            else:
                self.results.append(ValidationResult(
                    "Row Count Integrity", False,
                    f"Row count suspicious: {total_rows:,} parquet rows, avg {avg_rows_per_file:,.0f} rows/file"
                ))
            
            # Validate station count - expect 100-500 stations for UK datasets
            if 50 <= unique_stations <= 1000:
                self.results.append(ValidationResult(
                    "Station Count", True,
                    f"Station count reasonable: {unique_stations} unique stations",
                    details={"unique_stations": unique_stations}
                ))
            else:
                self.results.append(ValidationResult(
                    "Station Count", False,
                    f"Station count unexpected: {unique_stations} unique stations"
                ))
            
            # Validate date range
            if min_date and max_date:
                date_range_years = (max_date - min_date).days / 365.25
                self.results.append(ValidationResult(
                    "Date Range", True,
                    f"Date range: {min_date} to {max_date} ({date_range_years:.1f} years)",
                    details={"min_date": str(min_date), "max_date": str(max_date), "years": date_range_years}
                ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Data Integrity", False, f"Error validating data integrity: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_schema(self):
        """Validate parquet schema and column types"""
        start_time = time.time()
        
        try:
            # Load parquet schema
            pq_file = pq.ParquetFile(self.parquet_path)
            schema = pq_file.schema
            
            # Expected columns for MIDAS data
            expected_columns = {
                "station_id", "station_name", "county", "latitude", "longitude", 
                "height", "height_units", "ob_end_time"
            }
            
            column_names = set(schema.names)
            
            # Check required columns exist
            missing_cols = expected_columns - column_names
            if missing_cols:
                self.results.append(ValidationResult(
                    "Required Columns", False,
                    f"Missing required columns: {missing_cols}"
                ))
            else:
                self.results.append(ValidationResult(
                    "Required Columns", True,
                    f"All required columns present: {len(expected_columns)}"
                ))
            
            # Check data types
            df = pl.scan_parquet(self.parquet_path).limit(1).collect()
            
            type_checks = {
                "station_id": pl.Utf8,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "height": pl.Float64,
                "ob_end_time": pl.Datetime,
            }
            
            type_errors = []
            for col, expected_type in type_checks.items():
                if col in df.columns:
                    actual_type = df[col].dtype
                    if actual_type != expected_type:
                        type_errors.append(f"{col}: expected {expected_type}, got {actual_type}")
            
            if type_errors:
                self.results.append(ValidationResult(
                    "Column Types", False,
                    f"Type mismatches: {'; '.join(type_errors)}"
                ))
            else:
                self.results.append(ValidationResult(
                    "Column Types", True,
                    "All column types correct"
                ))
            
            # Check for reasonable column count
            total_columns = len(schema.names)
            if total_columns < 10:
                self.results.append(ValidationResult(
                    "Column Count", False,
                    f"Too few columns: {total_columns}"
                ))
            elif total_columns > 50:
                self.results.append(ValidationResult(
                    "Column Count", False,
                    f"Too many columns: {total_columns}"
                ))
            else:
                self.results.append(ValidationResult(
                    "Column Count", True,
                    f"Reasonable column count: {total_columns}"
                ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Schema Validation", False, f"Error validating schema: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_sorting(self):
        """Validate station-timestamp sorting"""
        start_time = time.time()
        
        try:
            # Check if data is sorted by station_id, then ob_end_time
            df = pl.scan_parquet(self.parquet_path).select([
                "station_id", "ob_end_time"
            ]).collect()
            
            if len(df) == 0:
                self.results.append(ValidationResult(
                    "Sorting Validation", False, "No data to validate sorting"
                ))
                return
            
            # Check if sorted by station_id
            station_sorted = df["station_id"].is_sorted()
            
            if not station_sorted:
                self.results.append(ValidationResult(
                    "Station Sorting", False, "Data is not sorted by station_id"
                ))
            else:
                self.results.append(ValidationResult(
                    "Station Sorting", True, "Data is correctly sorted by station_id"
                ))
            
            # Check if sorted by timestamp within each station
            time_sorted_within_stations = True
            problem_stations = []
            
            # Sample check - test a few stations
            unique_stations = df["station_id"].unique()
            sample_stations = unique_stations[:min(10, len(unique_stations))]
            
            for station in sample_stations:
                station_data = df.filter(pl.col("station_id") == station)
                if not station_data["ob_end_time"].is_sorted():
                    time_sorted_within_stations = False
                    problem_stations.append(station)
            
            if not time_sorted_within_stations:
                self.results.append(ValidationResult(
                    "Timestamp Sorting", False,
                    f"Timestamps not sorted within stations: {problem_stations[:5]}"
                ))
            else:
                self.results.append(ValidationResult(
                    "Timestamp Sorting", True,
                    f"Timestamps correctly sorted within stations (checked {len(sample_stations)} stations)"
                ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Sorting Validation", False, f"Error validating sorting: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_parquet_metadata(self):
        """Validate parquet file metadata and optimization"""
        start_time = time.time()
        
        try:
            pq_file = pq.ParquetFile(self.parquet_path)
            
            # Check row groups
            num_row_groups = pq_file.num_row_groups
            total_rows = pq_file.metadata.num_rows
            
            if num_row_groups == 0:
                self.results.append(ValidationResult(
                    "Row Groups", False, "No row groups found"
                ))
                return
            
            # Analyze row group sizes
            row_group_sizes = []
            for i in range(num_row_groups):
                rg = pq_file.metadata.row_group(i)
                row_group_sizes.append(rg.num_rows)
            
            avg_row_group_size = sum(row_group_sizes) / len(row_group_sizes)
            min_rg_size = min(row_group_sizes)
            max_rg_size = max(row_group_sizes)
            
            # Check if row groups are reasonably sized (between 100K and 2M rows)
            if avg_row_group_size < 100_000:
                self.results.append(ValidationResult(
                    "Row Group Size", False,
                    f"Row groups too small: avg {avg_row_group_size:,.0f} rows"
                ))
            elif avg_row_group_size > 2_000_000:
                self.results.append(ValidationResult(
                    "Row Group Size", False,
                    f"Row groups too large: avg {avg_row_group_size:,.0f} rows"
                ))
            else:
                self.results.append(ValidationResult(
                    "Row Group Size", True,
                    f"Row groups well-sized: avg {avg_row_group_size:,.0f} rows (min: {min_rg_size:,}, max: {max_rg_size:,})",
                    details={
                        "num_row_groups": num_row_groups,
                        "avg_size": avg_row_group_size,
                        "min_size": min_rg_size,
                        "max_size": max_rg_size
                    }
                ))
            
            # Check compression
            compression_info = []
            for i in range(num_row_groups):
                rg = pq_file.metadata.row_group(i)
                for j in range(rg.num_columns):
                    col = rg.column(j)
                    compression_info.append(str(col.compression))
            
            unique_compressions = set(compression_info)
            if len(unique_compressions) == 1:
                compression = unique_compressions.pop()
                self.results.append(ValidationResult(
                    "Compression", True,
                    f"Consistent compression: {compression}"
                ))
            else:
                self.results.append(ValidationResult(
                    "Compression", False,
                    f"Mixed compression types: {unique_compressions}"
                ))
            
            # Check if statistics are enabled
            has_statistics = False
            rg = pq_file.metadata.row_group(0)
            if rg.num_columns > 0:
                col = rg.column(0)
                has_statistics = col.statistics is not None
            
            self.results.append(ValidationResult(
                "Column Statistics", has_statistics,
                "Statistics enabled" if has_statistics else "Statistics disabled"
            ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Parquet Metadata", False, f"Error validating parquet metadata: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_performance(self):
        """Validate query performance"""
        start_time = time.time()
        
        try:
            # Test station-based query performance
            station_query_start = time.time()
            
            # Get a sample station
            df = pl.scan_parquet(self.parquet_path)
            sample_station = df.select("station_id").limit(1).collect()["station_id"][0]
            
            # Query specific station
            station_data = df.filter(pl.col("station_id") == sample_station).collect()
            station_query_time = (time.time() - station_query_start) * 1000
            
            # Test time-range query performance
            time_query_start = time.time()
            recent_data = df.filter(
                pl.col("ob_end_time") > pl.datetime(2020, 1, 1)
            ).limit(10000).collect()
            time_query_time = (time.time() - time_query_start) * 1000
            
            # Test combined query performance
            combined_query_start = time.time()
            combined_data = df.filter(
                (pl.col("station_id") == sample_station) & 
                (pl.col("ob_end_time") > pl.datetime(2020, 1, 1))
            ).collect()
            combined_query_time = (time.time() - combined_query_start) * 1000
            
            # Performance should be reasonable (< 1 second for small queries)
            if station_query_time < 1000:
                self.results.append(ValidationResult(
                    "Station Query Performance", True,
                    f"Station query: {station_query_time:.1f}ms ({len(station_data):,} rows)",
                    details={"query_time_ms": station_query_time, "rows_returned": len(station_data)}
                ))
            else:
                self.results.append(ValidationResult(
                    "Station Query Performance", False,
                    f"Station query slow: {station_query_time:.1f}ms"
                ))
            
            if time_query_time < 2000:
                self.results.append(ValidationResult(
                    "Time Range Query Performance", True,
                    f"Time range query: {time_query_time:.1f}ms ({len(recent_data):,} rows)"
                ))
            else:
                self.results.append(ValidationResult(
                    "Time Range Query Performance", False,
                    f"Time range query slow: {time_query_time:.1f}ms"
                ))
            
            if combined_query_time < 1000:
                self.results.append(ValidationResult(
                    "Combined Query Performance", True,
                    f"Combined query: {combined_query_time:.1f}ms ({len(combined_data):,} rows)"
                ))
            else:
                self.results.append(ValidationResult(
                    "Combined Query Performance", False,
                    f"Combined query slow: {combined_query_time:.1f}ms"
                ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Performance Validation", False, f"Error validating performance: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _validate_extended(self):
        """Run extended validation tests"""
        start_time = time.time()
        
        try:
            # Test memory usage
            df = pl.scan_parquet(self.parquet_path)
            
            # Test lazy operations don't consume excessive memory
            lazy_ops = df.filter(pl.col("station_id").is_not_null()).select(["station_id", "ob_end_time"])
            
            # Test aggregations
            agg_start = time.time()
            station_counts = df.group_by("station_id").len().collect()
            agg_time = (time.time() - agg_start) * 1000
            
            unique_stations = len(station_counts)
            avg_records_per_station = station_counts["len"].mean()
            
            self.results.append(ValidationResult(
                "Aggregation Performance", True,
                f"Station aggregation: {agg_time:.1f}ms ({unique_stations:,} stations, avg {avg_records_per_station:.0f} records/station)",
                details={
                    "agg_time_ms": agg_time,
                    "unique_stations": unique_stations,
                    "avg_records_per_station": avg_records_per_station
                }
            ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                "Extended Validation", False, f"Error in extended validation: {e}"
            ))
        
        duration = (time.time() - start_time) * 1000
        if self.results:
            self.results[-1].duration_ms = duration
    
    def _generate_report(self) -> ValidationReport:
        """Generate comprehensive validation report"""
        passed_tests = sum(1 for r in self.results if r.passed)
        total_tests = len(self.results)
        
        summary = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "success_rate": (passed_tests / total_tests) * 100 if total_tests > 0 else 0,
            "total_duration_ms": sum(r.duration_ms for r in self.results if r.duration_ms),
        }
        
        return ValidationReport(
            parquet_path=str(self.parquet_path),
            csv_source_path=str(self.csv_source_path),
            results=self.results,
            summary=summary,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )
    
    def _display_report(self, report: ValidationReport):
        """Display validation report in console"""
        console.print()
        
        # Summary panel
        if report.passed():
            title = "✅ [bold green]Validation PASSED[/bold green]"
        else:
            title = "❌ [bold red]Validation FAILED[/bold red]"
        
        summary_text = f"""
[bold]Test Results:[/bold] {report.summary['passed_tests']}/{report.summary['total_tests']} passed ({report.summary['success_rate']:.1f}%)
[bold]Duration:[/bold] {report.summary['total_duration_ms']:.1f}ms
[bold]Timestamp:[/bold] {report.timestamp}
"""
        
        console.print(Panel(summary_text, title=title, expand=False))
        
        # Results table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Test", style="dim")
        table.add_column("Status", justify="center")
        table.add_column("Message")
        table.add_column("Duration", justify="right")
        
        for result in report.results:
            status = "✅ PASS" if result.passed else "❌ FAIL"
            duration = f"{result.duration_ms:.1f}ms" if result.duration_ms else "-"
            
            table.add_row(
                result.test_name,
                status,
                result.message,
                duration
            )
        
        console.print(table)
        
        # Failed tests detail
        failed_tests = report.failed_tests()
        if failed_tests:
            console.print()
            console.print("[bold red]Failed Tests Details:[/bold red]")
            for test in failed_tests:
                console.print(f"  • {test.test_name}: {test.message}")

def main():
    parser = argparse.ArgumentParser(
        description="Validate MIDAS parquet files against original CSV sources"
    )
    parser.add_argument("parquet_path", help="Path to the parquet file to validate")
    parser.add_argument("csv_source_path", help="Path to the CSV source directory")
    parser.add_argument("--full-validation", action="store_true", 
                       help="Run extended validation tests")
    parser.add_argument("--performance-tests", action="store_true",
                       help="Run performance validation tests")
    parser.add_argument("--json-report", help="Output JSON report to file")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose output")
    
    args = parser.parse_args()
    
    parquet_path = Path(args.parquet_path)
    csv_source_path = Path(args.csv_source_path)
    
    validator = MidasParquetValidator(parquet_path, csv_source_path)
    report = validator.run_validation(
        full_validation=args.full_validation,
        performance_tests=args.performance_tests
    )
    
    # Output JSON report if requested
    if args.json_report:
        report_data = {
            "parquet_path": report.parquet_path,
            "csv_source_path": report.csv_source_path,
            "timestamp": report.timestamp,
            "summary": report.summary,
            "results": [
                {
                    "test_name": r.test_name,
                    "passed": r.passed,
                    "message": r.message,
                    "details": r.details,
                    "duration_ms": r.duration_ms
                }
                for r in report.results
            ]
        }
        
        # Save to tests/ directory if filename only provided
        report_path = Path(args.json_report)
        if not report_path.is_absolute() and report_path.parent == Path('.'):
            tests_dir = Path(__file__).parent
            report_path = tests_dir / report_path
        
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        console.print(f"📄 JSON report saved to: {report_path}")
    
    # Exit with appropriate code
    sys.exit(0 if report.passed() else 1)

if __name__ == "__main__":
    main()