# Sales Reporting Pipeline

An automated reporting pipeline that selects data from MSSQL databases, transforms it, and generates multiple sales reports. The pipeline runs on Apache Airflow for scheduling and orchestration.

## What It Does

This pipeline pulls sales data from multiple sources, transforms and combines the data, then generates comprehensive sales reports. The system handles customer name mapping, sales categorization, and produces various analytical reports including YTD analysis, rolling trends, and historical comparisons.

## Tech Stack

- **Windows Environment**: Python 3.13
- **WSL Ubuntu Environment**: Python 3.12.3 + Apache Airflow 3.0.6
- **Database**: Microsoft SQL Server (MSSQL)
- **Orchestration**: Apache Airflow (running on WSL Ubuntu)
- **Key Libraries**: pandas, polars, sqlalchemy, pyodbc, xlwings

## Starting and Stopping Airflow

Airflow runs on WSL Ubuntu and is controlled via startup scripts.

### Start Airflow

```bash
# From Windows Command Prompt or PowerShell
wsl -d Ubuntu

# Navigate to the airflow directory
cd /mnt/h/Upgrading_Database_Reporting_Systems/REPORTING_PIPELINE/airflow

# Run the startup script
./start_airflow.sh
```

The script will start both the web server and scheduler in the background.

### Stop Airflow

```bash
# From WSL Ubuntu
cd /mnt/h/Upgrading_Database_Reporting_Systems/REPORTING_PIPELINE/airflow

# Run the stop script
./stop_airflow.sh
```

### Access the Web Interface

Once Airflow is running, open your browser:
- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

## Project Structure

```
REPORTING_PIPELINE/
├── airflow/                 # Airflow home directory
│   ├── dags/               # DAG files for workflow orchestration
│   └── airflow_env_linux/  # Python virtual environment (Linux)
├── src/
│   ├── pipelines/          # Report generation scripts
│   ├── database_uploads/   # Data upload scripts
│   └── helpers/            # Utility functions and configuration
└── requirements.txt        # Python dependencies
```

## Reports Generated

The pipeline generates multiple reports including:
- Combined sales reports from multiple sources
- Year-to-Date (YTD) analysis
- 12-month rolling analysis
- Historical trends
- Customer-specific reports
- Title/ISBN level reports

## How It Works

1. **Data Selection**: Pulls sales data from MSSQL databases
2. **Data Transformation**: Standardizes customer names, categories, and ISBNs
3. **Data Combination**: Merges data from multiple sources
4. **Report Generation**: Creates Excel reports with analysis
5. **Scheduling**: Airflow manages when and how often reports run