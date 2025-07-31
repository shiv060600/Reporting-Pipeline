# Sales Reporting Pipeline

A Python-based system for processing and analyzing sales data from multiple sources (Ingram and SAGE) to generate comprehensive sales reports with advanced mapping and categorization capabilities.

## Overview

This system combines sales data from Ingram and SAGE databases to create detailed sales reports. It processes historical sales data, combines it from multiple sources, and generates various reports including YTD analysis, 12-month rolling analysis, and historical trends. The system includes advanced mapping functionality for customer names and sales categories to ensure data consistency and accurate reporting.

## Key Features

- Processes sales data from both Ingram and SAGE sources
- Handles customer data with advanced name mapping system using master mapping tables
- Advanced mapping system for customer names and sales categories
- Monthly sales upload functionality with automatic categorization
- Generates comprehensive sales reports including:
  - Year-to-Date (YTD) analysis
  - 12-month rolling analysis
  - Historical trends (3 years)
  - Customer-specific reporting
  - Title/ISBN level reporting

## System Architecture

### Core Processing Components

#### Main Processing Script (`main.py`)
- Fetches sales data from both Ingram and SAGE databases
- Processes and combines sales data
- Generates various sales reports
- Handles data aggregation and analysis

#### Ingram Sales Processing (`create_ing_sales.py`)
- Processes historical Ingram sales data from Excel files
- Cleans and transforms the data
- Creates SQL Server table with processed data

### Mapping and Categorization System

#### Master Name Mapping (`upload_master_name_mapping.py`)
- Uploads customer name mapping data from Excel to SQL Server
- Maintains `MASTER_INGRAM_NAME_MAPPING` table
- Ensures consistent customer identification across data sources
- Handles customer name standardization and deduplication

#### Sales Category Mapping (`upload_master_sales_category.py`)
- Manages sales category classifications for both Ingram and SAGE customers
- Uploads category data to `INGRAM_MASTER_CATEGORIES` and `SAGE_MASTER_CATEGORIES` tables
- Provides standardized sales category mapping across all data sources
- Supports hierarchical account structures (SL Account Number to HQ Account Number mapping)

#### Monthly Sales Upload (`monthly_sales_upload.py`)
- Processes monthly Ingram sales data with automatic categorization
- Applies master sales category mappings to new sales data
- Integrates with book mapping system for title standardization
- Handles data cleaning and transformation for reporting consistency

### Configuration (`helpers/paths.py`)
- Manages file paths and connection strings
- Centralizes configuration settings
- Handles database connection parameters

## Data Processing Flow

### 1. Data Collection and Mapping
- **Customer Name Mapping**: Standardizes customer names across Ingram and SAGE sources
- **Sales Category Mapping**: Applies master sales categories to all customer accounts
- **Book Title Mapping**: Standardizes book titles using ISBN-based mapping

### 2. Data Processing
- Maps Ingram customer names to SAGE customer names using master mapping tables
- Standardizes ISBN formats
- Combines data from both sources with proper categorization
- Groups and aggregates sales data by standardized categories

### 3. Report Generation
- Calculates YTD values
- Generates 12-month rolling analysis
- Creates historical trend reports
- Outputs consolidated data to Excel with proper categorization

## Technical Stack

### Core Technologies
- **Python 3.x**: Primary programming language
- **SQL Server**: Database for data storage and retrieval
- **Excel Integration**: Uses xlwings for Excel file processing

### Key Python Libraries
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **sqlalchemy**: Database ORM and connection management
- **xlwings**: Excel automation and file processing
- **pyodbc**: SQL Server connectivity
- **rapidfuzz**: Fuzzy string matching for data cleaning
- **openpyxl**: Excel file reading and writing
- **polars**: High-performance data processing
- **pyarrow**: Arrow format data handling

### Database Schema
- **MASTER_INGRAM_NAME_MAPPING**: Customer name standardization
- **INGRAM_MASTER_CATEGORIES**: Ingram customer sales categories
- **SAGE_MASTER_CATEGORIES**: SAGE customer sales categories
- **Sales Data Tables**: Historical and current sales data storage

## Data Quality and Consistency

### Mapping System Benefits
- **Standardized Customer Names**: Eliminates duplicates and variations in customer naming
- **Consistent Sales Categories**: Ensures all sales are properly categorized for reporting
- **Title Standardization**: Uses ISBN-based mapping to standardize book titles
- **Data Deduplication**: Removes duplicate records while maintaining source identification

### Error Handling and Logging
- Comprehensive logging system tracks all data processing steps
- Error handling for database connections and file operations
- Data validation and quality checks throughout the pipeline
- Null value handling and data cleaning procedures

## Output and Reporting

The system generates Excel reports containing:
- Combined sales data from both sources with proper categorization
- YTD analysis with standardized customer and category groupings
- 12-month rolling analysis
- Historical trends with consistent categorization
- Customer-specific reports with mapped names and categories
- Title/ISBN level analysis with standardized titles

## System Notes

- The system uses master mapping tables to standardize customer names between Ingram and SAGE sources
- Data is aggregated to remove duplicates while maintaining source identification
- Reports are generated on a monthly basis with automatic categorization
- Historical data is maintained for 3 years with proper mapping applied
- The mapping system ensures data quality and reporting accuracy across all sources 