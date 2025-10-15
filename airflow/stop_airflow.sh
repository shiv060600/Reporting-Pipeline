#!/bin/bash

echo "Stopping Apache Airflow..."

# Kill all airflow processes
pkill -9 -f "airflow"

echo "Airflow stopped!"
