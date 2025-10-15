#!/bin/bash

# Startup script for Apache Airflow
echo "Starting Apache Airflow..."

# Set Airflow Home
export AIRFLOW_HOME=/mnt/h/Upgrading_Database_Reporting_Systems/REPORTING_PIPELINE/airflow

# Set password file path (overrides config)
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/mnt/h/Upgrading_Database_Reporting_Systems/REPORTING_PIPELINE/airflow/simple_auth_manager_passwords.json

# Activate virtual environment
source /mnt/h/Upgrading_Database_Reporting_Systems/REPORTING_PIPELINE/airflow/airflow_env_linux/bin/activate

# Start API server in background (replaces webserver in Airflow 3.x)
echo "Starting Airflow API Server on port 8080..."
nohup airflow api-server --port 8080 > logs/api_server.log 2>&1 &

# Wait a moment for API server to initialize
sleep 3

# Start scheduler in background
echo "Starting Airflow Scheduler..."
nohup airflow scheduler > logs/scheduler.log 2>&1 &

echo ""
echo "Airflow is starting up!"
echo "Web UI: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "Logs:"
echo "  API Server: tail -f logs/api_server.log"
echo "  Scheduler: tail -f logs/scheduler.log"
echo ""
echo "To stop Airflow, run: ./stop_airflow.sh"
