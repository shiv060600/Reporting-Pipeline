# Sales Reporting Pipeline

A Python-based system for processing and analyzing sales data from multiple sources (Ingram and SAGE) to generate comprehensive sales reports.

## Overview

This system combines sales data from Ingram and SAGE databases to create detailed sales reports. It processes historical sales data, combines it from multiple sources, and generates various reports including YTD analysis, 12-month rolling analysis, and historical trends.

## Key Features

- Processes sales data from both Ingram and SAGE sources
- Handles customer data with clear source identification (appends "(INGRAM)" to Ingram customer names)
- Generates comprehensive sales reports including:
  - Year-to-Date (YTD) analysis
  - 12-month rolling analysis
  - Historical trends (3 years)
  - Customer-specific reporting
  - Title/ISBN level reporting

## System Components

### Main Processing Script (`main.py`)
- Fetches sales data from both Ingram and SAGE databases
- Processes and combines sales data
- Generates various sales reports
- Handles data aggregation and analysis

### Ingram Sales Processing (`create_ing_sales.py`)
- Processes historical Ingram sales data from Excel files
- Cleans and transforms the data
- Creates SQL Server table with processed data

### Configuration (`helpers/paths.py`)
- Manages file paths and connection strings
- Centralizes configuration settings

## Data Processing Flow

1. **Data Collection**
   - Fetches Ingram sales data from SQL Server
   - Fetches SAGE sales data from SQL Server
   - Excludes current month's data
   - Includes last 3 years of historical data

2. **Data Processing**
   - Appends "(INGRAM)" to Ingram customer names for clear source identification
   - Standardizes ISBN formats
   - Combines data from both sources
   - Groups and aggregates sales data

3. **Report Generation**
   - Calculates YTD values
   - Generates 12-month rolling analysis
   - Creates historical trend reports
   - Outputs consolidated data to Excel

## Technical Requirements

- Python 3.x
- SQL Server
- Required Python packages:
  - pandas
  - numpy
  - sqlalchemy
  - xlwings
  - pyodbc

## Setup and Configuration

1. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure database connections in `helpers/paths.py`

3. Ensure all required file paths are correctly set in the configuration

## Usage
ALL COMMANDS NEED TO BE EXECUTED ON TUTPUB5 THROUGH POWERSHELL FOR MANUAL DEBUGGING AND CHANGES
1. Run the Ingram sales processing script:
   ```bash
   activate venv:
   cd H:\Upgrading_Database_Reporting_Systems\REPORTING_PIPELINE\venv
   Scripts\Activate
   
   cd to location on TUTPUB5 (H:\Upgrading_Database_Reporting_Systems\REPORTING_PIPELINE)
   cd src
   py create_ing_sales.py
   ```

2. Run the main reporting script:
   ```bash
   py main.py
   
   ```

## Output

The system generates Excel reports containing:
- Combined sales data from both sources
- YTD analysis
- 12-month rolling analysis
- Historical trends
- Customer-specific reports
- Title/ISBN level analysis

## Notes

- The system uses a simple but effective approach to handle customer data by appending "(INGRAM)" to Ingram customer names
- Data is aggregated to remove duplicates while maintaining source identification
- Reports are generated on a monthly basis
- Historical data is maintained for 3 years 