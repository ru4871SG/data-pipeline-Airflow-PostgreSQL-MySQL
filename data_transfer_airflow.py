"""
Airflow DAG script to automate data transfer process from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora.
"""

# Libraries
import sys
# You must change the below path to the folder where you store the Python scripts locally
# (data_transfer_from_mysql.py and data_transfer_from_staging_to_aurora.py)
sys.path.insert(0, '/home/ruddy/Documents/GitHub/data-pipeline-Airflow-PostgreSQL-MySQL')

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import data_transfer_from_mysql
import data_transfer_from_staging_to_aurora

# DAG arguments
default_args = {
    'owner': 'Ruddy Gunawan',
    'start_date': days_ago(0),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition with 3 minutes interval
dag = DAG(
    'data_transfer_airflow',
    default_args=default_args,
    description='Data Transfer Process (100 records at a time) from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora',
    schedule_interval=timedelta(minutes=3),
    catchup=False,
)

# Task to transfer data from MySQL to PostgreSQL
task1 = PythonOperator(
    task_id='data_transfer_from_mysql_to_postgresql',
    python_callable=data_transfer_from_mysql.main,
    dag=dag,
)

# Task to transfer data from staging to production in Amazon Aurora
task2 = PythonOperator(
    task_id='data_transfer_from_staging_to_production',
    python_callable=data_transfer_from_staging_to_aurora.main,
    dag=dag,
)

# Task Pipeline
task1 >> task2
