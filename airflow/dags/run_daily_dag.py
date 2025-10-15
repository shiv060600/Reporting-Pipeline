#type: ignore
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner' : 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024,1,1),
    'retries': 0
}

daily_dag = DAG(
    'daily_mapping_upload',
    default_args = default_args,
    description = 'Daily upload of mapping files and rerun comined_report and report_three_combined',
    schedule = '0 21 * * *',
    catchup = False,
    tags = ['sales','daily']
)

run_daily = BashOperator(
    task_id='run_daily_pipeline',
    bash_command='powershell.exe -Command "cd H:\\Upgrading_Database_Reporting_Systems\\REPORTING_PIPELINE\\src; conda activate base; python run_daily.py"',
    dag=daily_dag,
)