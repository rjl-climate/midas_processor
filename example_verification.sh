#!/bin/bash

# MIDAS Parquet Verification Example Script
# 
# This script demonstrates how to use the verification script to validate
# Parquet output from the MIDAS processor.

set -e  # Exit on any error

echo "MIDAS Parquet Verification Example"
echo "=================================="

# Configuration - adjust these paths as needed
INPUT_CSV_PATH="/Users/richardlyon/Library/Application Support/midas-fetcher/cache"
OUTPUT_DIR="./output"
VERIFICATION_OUTPUT="./verification_results"
DATASET="uk-radiation-obs"

echo "Configuration:"
echo "  Input CSV Path: $INPUT_CSV_PATH"
echo "  Output Directory: $OUTPUT_DIR"
echo "  Dataset: $DATASET"
echo "  Verification Output: $VERIFICATION_OUTPUT"
echo

# Step 1: Process the dataset with MIDAS processor
echo "Step 1: Processing dataset with MIDAS processor..."
echo "Command: cargo run process --datasets '$DATASET' --quiet"
echo

# Uncomment this line to actually run the processor
# cargo run process --datasets "$DATASET" --quiet

echo "Note: Processing command shown above. Uncomment to run."
echo

# Step 2: Find the generated Parquet file
CURRENT_MONTH=$(date +"%Y%m")
PARQUET_FILE="$OUTPUT_DIR/${DATASET}_${CURRENT_MONTH}.parquet"

echo "Step 2: Looking for generated Parquet file..."
echo "Expected file: $PARQUET_FILE"

if [ -f "$PARQUET_FILE" ]; then
    echo "✓ Parquet file found!"
    
    # Show file information
    echo "File size: $(du -h "$PARQUET_FILE" | cut -f1)"
    echo "File modified: $(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$PARQUET_FILE")"
else
    echo "✗ Parquet file not found. Expected: $PARQUET_FILE"
    echo
    echo "Available Parquet files in output directory:"
    find "$OUTPUT_DIR" -name "*.parquet" -exec ls -lh {} \; 2>/dev/null || echo "No Parquet files found"
    echo
    echo "Please run the MIDAS processor first, or adjust the PARQUET_FILE path."
    exit 1
fi

echo

# Step 3: Install Python dependencies (if needed)
echo "Step 3: Checking Python dependencies..."
if command -v python3 >/dev/null 2>&1; then
    echo "✓ Python 3 found"
    
    # Check if required packages are installed
    python3 -c "import pandas, polars, pyarrow, matplotlib, seaborn, jinja2" 2>/dev/null && \
        echo "✓ All required Python packages are installed" || \
        {
            echo "Installing Python dependencies..."
            pip3 install -r requirements.txt
        }
else
    echo "✗ Python 3 not found. Please install Python 3."
    exit 1
fi

echo

# Step 4: Run verification
echo "Step 4: Running Parquet verification..."
echo "Command: python3 verify_parquet_output.py \\"
echo "    --input-csv '$INPUT_CSV_PATH/$DATASET' \\"
echo "    --parquet-output '$PARQUET_FILE' \\"
echo "    --dataset '$DATASET' \\"
echo "    --output-dir '$VERIFICATION_OUTPUT'"

python3 verify_parquet_output.py \
    --input-csv "$INPUT_CSV_PATH/$DATASET" \
    --parquet-output "$PARQUET_FILE" \
    --dataset "$DATASET" \
    --output-dir "$VERIFICATION_OUTPUT"

echo
echo "Verification complete!"
echo

# Step 5: Show generated reports
echo "Step 5: Generated verification reports:"
find "$VERIFICATION_OUTPUT" -name "*$DATASET*" -type f | while read -r file; do
    echo "  $file"
    if [[ "$file" == *.html ]]; then
        echo "    → Open in browser: file://$PWD/$file"
    fi
done

echo
echo "Example completed successfully!"
echo
echo "Next steps:"
echo "1. Review the HTML report in your browser"
echo "2. Check any warnings or errors in the verification results"
echo "3. If issues are found, investigate the processing pipeline"
echo "4. Use the JSON report for automated validation in CI/CD"