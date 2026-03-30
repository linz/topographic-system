# Carto Text Processing Workflow

This README documents the comprehensive workflow for processing cartographic text data using the `process_carto_text_newfields.py` script. The workflow transforms cartographic text data by enriching it with new styling fields based on complex mapping rules.

## Overview

The `CartoTextProcessor` class processes cartographic text data through a multi-stage workflow that:
1. Reads mapping data from Excel spreadsheets
2. Processes geographic data from GeoPackage files
3. Applies complex mapping rules to update styling attributes
4. Exports enriched data with comprehensive logging

## Workflow Steps

### 1. Initialization and Setup
- **Purpose**: Initialize the processor with output directory and logging configuration
- **Actions**: 
  - Creates output directory if it doesn't exist
  - Sets up dual logging (file + console) with timestamps
  - Defines field specifications for new styling fields

### 2. Excel Spreadsheet Processing

#### 2.1 Full Layers Sheet Processing (`process_fulllayers_tab`)
- **Purpose**: Extract ATT (attribute) sections and consolidate SYMBOL information
- **Actions**:
  - Reads Excel file and locates 'Full layers' sheet
  - Removes completely empty rows
  - Identifies ATT sections (rows starting with 'ATT')
  - Consolidates 4-row ATT sections into single rows
  - Combines SYMBOL values from header and 3 data rows
  - Exports processed data to `formatted_rows.csv`

**Exit Conditions:**
- **File not found**: Process stops if Excel file doesn't exist
- **Sheet not found**: Process stops if specified sheet doesn't exist
- **No ATT sections**: Process continues but with warnings

#### 2.2 New Values Sheet Processing (`process_new_values_tab`)
- **Purpose**: Process styling values and handle empty 'Text Bend' fields
- **Actions**:
  - Reads 'New values' sheet
  - Sets empty 'Text Bend' values to 0
  - Exports processed data to `new_values_processed.csv`

**Exit Conditions:**
- **Missing Text Bend column**: Process continues with warning
- **File errors**: Process stops with error logging

#### 2.3 Font Mapping Sheet Processing (`process_font_mapping_tab`)
- **Purpose**: Process font mapping rules and handle empty values
- **Actions**:
  - Reads 'Font mapping' sheet
  - Skips rows where LAMPS = 'Others:'
  - Sets empty LAMPS and FONT values to 'empty'
  - Exports processed data to `font_mapping_processed.csv`

**Exit Conditions:**
- **Missing required columns**: Process stops if LAMPS or FONT columns missing
- **File errors**: Process stops with error logging

### 3. GeoPackage Layer Processing

#### 3.1 Layer Loading and Field Addition (`process_carto_text_layer`)
- **Purpose**: Load geographic data and add new styling fields
- **Actions**:
  - Reads carto_text layer from GeoPackage
  - Adds 10 new styling fields with proper data types:
    - `font` (string, 20 chars)
    - `style` (string, 30 chars)
    - `colour` (string, 20 chars)
    - `size` (float64)
    - `placement` (string, 2 chars)
    - `offset` (float64)
    - `textanchor` (string, 20 chars)
    - `labelanchor` (float64)
    - `charplace` (string, 32 chars)
    - `chardistance` (float64)

**Exit Conditions:**
- **GeoPackage not found**: Process stops with error
- **Layer not found**: Process stops with error
- **Memory issues**: Process may fail with large datasets

### 4. Symbol Text Processing and Query Generation

#### 4.1 Symbol Parsing (`parse_symbol_text`)
- **Purpose**: Parse SYMBOL text into field=value conditions
- **Example Input**: `"text_font = ATTriumMou-Cond and text_colour = 9 and text_height = 0.0013 and text_place in (34)"`
- **Actions**:
  - Splits SYMBOL text by 'and' operators
  - Handles both `field = value` and `field in (value)` formats
  - Converts values to appropriate data types (int, float, string)

**Exit Conditions:**
- **Empty SYMBOL text**: Individual symbols are skipped
- **Malformed SYMBOL**: Parsing errors are logged, processing continues

#### 4.2 Query Generation (`process_symbol_text_to_queries`)
- **Purpose**: Convert parsed symbols into pandas queries for feature selection
- **Actions**:
  - Converts `text_place` to `text_placement`
  - Handles multiple placement values (e.g., "1,4" → `text_placement = 1 or text_placement = 4`)
  - Generates pandas query strings
  - Tests queries against the geodataframe
  - Records match counts and feature indices

**Exit Conditions when Selection is Empty:**
- **No valid conditions**: Query skipped if no parseable conditions found
- **Query execution fails**: Logged as error, processing continues with next query
- **Zero matches**: Query processes but affects no features

### 5. Complex Field Updates

#### 5.1 Feature Matching and Grouping (`update_layer_fields_from_queries`)
- **Purpose**: Apply styling updates based on complex multi-criteria matching
- **Process Flow**:
  
  **Step 1: Base Query Matching**
  - Apply initial query conditions to find candidate features
  - **EXIT POINT**: If no features match base query → Log "Base Query - No matched features" error

  **Step 2: Text Bend Grouping**
  - Group matched features by `text_bend` values
  - For each group, perform multi-level filtering

  **Step 3: Text Bend Matching**
  - Query `new_values` table for matching `text_bend` value
  - **EXIT POINT**: If no match → Log "Text Bend: [layer_id] - text_bend_value = X" error

  **Step 4: Text Height Matching**
  - Filter `new_values` for matching `text_height`
  - **EXIT POINT**: If no match → Log "Text Height: [layer_id] - text_bend = X, text_height = Y" error

  **Step 5: Colour Matching**
  - Apply hardcoded colour mapping:
    - `text_colour = 9` → `black`
    - Other values → `steelblue`
  - Filter `new_values` for matching colour
  - **EXIT POINT**: If no match → Process continues (may be handled at next level)

  **Step 6: Placement Matching**
  - Handle single or multiple placement values
  - Filter `new_values` for matching `Text Place` values
  - **EXIT POINT**: If no match → Log "Placement: [layer_id] - [full criteria]" error

  **Step 7: Font/Style Processing**
  - Extract `text_font` from symbol conditions
  - Look up font in `font_mapping` table
  - Extract style information
  
  **Step 8: Style Matching**
  - Filter `new_values` for matching style
  - **EXIT POINT**: If no match → Log "Style: [layer_id] - [full criteria]" error

  **Step 9: Field Updates**
  - **Success Path**: Update all 10 styling fields with values from `new_values`
  - **Failure Path**: Set all fields to null/empty values and log "Setting Zeros" error

#### 5.2 Error Tracking
The process maintains comprehensive error tracking in two categories:

**Unmatched IDs (`self.unmatched_ids`)**:
- Base Query failures: No features match initial conditions
- Text Bend failures: No `new_values` rows for specific `text_bend`
- Text Height failures: No matching height values
- Placement failures: No matching placement values
- Style failures: No matching style values
- Setting Zeros: Cases where null values are assigned

**Multiple Value Rows (`self.multiple_value_rows`)**:
- Cases where multiple `new_values` rows match the same criteria
- Requires manual review for data quality

### 6. Data Export

#### 6.1 Field Constraint Enforcement
- **Purpose**: Ensure data integrity before export
- **Actions**:
  - Truncate string fields to maximum lengths
  - Ensure proper data types (float64, string)
  - Replace `<NA>` values with empty strings
  - Handle null value assignments

#### 6.2 Export Options
- **GeoPackage**: Default format, preserves spatial data and field constraints
- **Parquet**: Alternative format for analytical workflows

## Process Stop Conditions

### Critical Failures (Process Termination)
1. **Input File Missing**: Excel spreadsheet not found
2. **GeoPackage Missing**: Carto text data file not accessible
3. **Layer Missing**: Specified layer not found in GeoPackage
4. **Required Columns Missing**: Critical columns missing from Excel sheets
5. **Memory Errors**: Insufficient memory for large datasets

### Non-Critical Failures (Process Continues with Warnings)
1. **Empty Symbol Text**: Individual symbols skipped
2. **Query Execution Errors**: Failed queries logged, other queries continue
3. **No Feature Matches**: Queries with zero matches are logged
4. **Missing Lookup Values**: Features get null values instead of styled values
5. **Multiple Matches**: First match used, others logged for review

### Selection Empty Scenarios

**Most Common Empty Selection Points:**

1. **Text Bend Mismatch** (Most frequent)
   - **When**: `text_bend` value from feature doesn't exist in `new_values` table
   - **Example**: Feature has `text_bend = 1.5` but `new_values` only has `1.0, 2.0, 3.0`
   - **Result**: Feature gets null styling values
   - **Log**: "Text Bend: ATT-layer-name - text_bend_value = 1.5"

2. **Height Mismatch**
   - **When**: `text_height` from symbol doesn't match available heights in `new_values`
   - **Example**: Symbol specifies `text_height = 0.0015` but `new_values` only has `0.0013`
   - **Result**: Feature gets null styling values
   - **Log**: "Text Height: ATT-layer-name - text_bend = X, text_height = 0.0015"

3. **Placement Mismatch**
   - **When**: `text_place` values from symbol don't match `Text Place` in `new_values`
   - **Example**: Symbol has `text_place in (45)` but `new_values` only has placements 1,2,3,4,34
   - **Result**: Feature gets null styling values
   - **Log**: "Placement: ATT-layer-name - [full criteria]"

4. **Style Mismatch**
   - **When**: Font mapping resolves to a style not available in `new_values`
   - **Example**: Font maps to style "Bold" but `new_values` only has "Regular, Italic"
   - **Result**: Feature gets null styling values
   - **Log**: "Style: ATT-layer-name - [full criteria]"

5. **Base Query No Matches**
   - **When**: Initial symbol conditions don't match any features in geodataframe
   - **Example**: Symbol looks for `text_colour = 5` but all features have colours 1,2,9
   - **Result**: No features affected by this rule
   - **Log**: "Base Query - ATT-layer-name - No matched features for query conditions"

## Configuration

### Required Input Files
- **Excel Spreadsheet**: Contains mapping rules in multiple sheets
  - Full layers: ATT sections with SYMBOL definitions
  - New values: Styling values and parameters
  - Font mapping: Font name translations
- **GeoPackage**: Contains carto_text layer with spatial features

### Output Files
- **Logs**: Timestamped processing logs with detailed information
- **CSV Files**: Processed data from Excel sheets
- **GeoPackage/Parquet**: Final enriched carto text data

### Key Parameters
- `mapping_spreadsheet`: Path to Excel file with mapping rules
- `carto_text_folder`: Directory containing GeoPackage
- `product_database`: GeoPackage filename
- `carto_text_layer`: Layer name within GeoPackage
- `new_font_name`: Target font name for styling
- `output_directory`: Location for all output files

## Error Analysis and Debugging

### Log File Analysis
Each run generates a timestamped log file containing:
- Processing steps and timing
- Data quality statistics
- Error details with specific criteria
- Performance metrics

### Common Issues and Solutions

1. **High Unmatched Rate**
   - Review `new_values` table completeness
   - Check for missing `text_bend` or `text_height` combinations
   - Verify placement value ranges

2. **Multiple Matches Warning**
   - Indicates potential data quality issues
   - Review duplicate entries in `new_values`
   - Consider more specific matching criteria

3. **Processing Performance**
   - Large datasets may require memory optimization
   - Consider processing in chunks for very large layers
   - Monitor log file size for extremely verbose logging

4. **Font Mapping Failures**
   - Verify font names match between symbol text and mapping table
   - Check for case sensitivity issues
   - Ensure 'LAMPS' column is properly populated

## Best Practices

### Data Preparation
- Ensure `new_values` table has comprehensive coverage of expected parameter combinations
- Validate font names are consistent across all input sources
- Test with small datasets before full production runs

### Monitoring
- Always review log files for error patterns
- Monitor unmatched ID counts as data quality indicators
- Validate output data through spot checks

### Troubleshooting
- Use detailed logging to identify specific matching failures
- Test individual SYMBOL strings manually for complex debugging
- Verify data types match expected formats in all input sources