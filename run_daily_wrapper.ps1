# PowerShell wrapper for run_daily.py
# Activates conda environment and runs the daily sales report pipeline

# Change to project directory
Set-Location "H:\Upgrading_Database_Reporting_Systems\REPORTING_PIPELINE"

# Activate conda environment (adjust the environment name if different)
& conda activate reporting_env

# Run the Python script
python src\run_daily.py

# Exit with the Python script's exit code
exit $LASTEXITCODE


