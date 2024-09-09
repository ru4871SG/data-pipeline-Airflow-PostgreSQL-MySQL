"""
Airflow DAG script to automate data transfer process from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora.
"""

# Libraries
import sys
import os

# You must change the below path to the folder where you store the Python data transfer scripts
# For this repository, the path points to the parent directory of this file, since that's where we store the data transfer scripts
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

import data_transfer_from_mysql
import etl_from_staging_to_aurora

# DAG arguments
default_args = {
    'owner': 'Ruddy Gunawan',
    'start_date': days_ago(0),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
}
# Note: for start_date, per my experience, using fixed schedule with datetime.datetime() is much more organized, but this example uses the default
#  start_date=days_ago(0)


# DAG definition with 5 minutes interval
dag = DAG(
    'airflow_data_transfer_original',
    default_args=default_args,
    description='Data Transfer Process (100 records at a time) from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

# Task to transfer data from MySQL to PostgreSQL
task1 = PythonOperator(
    task_id='data_transfer_from_mysql_to_postgresql',
    python_callable=data_transfer_from_mysql.main,
    dag=dag,
)

# Task to transfer data from staging in local PostgreSQL to production in Amazon Aurora
task2 = PythonOperator(
    task_id='data_transfer_from_staging_to_production',
    python_callable=etl_from_staging_to_aurora.main,
    dag=dag,
)

# Task Pipeline
task1 >> task2
