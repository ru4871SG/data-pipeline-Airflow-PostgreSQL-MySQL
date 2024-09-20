"""
Airflow DAG script to automate data transfer process from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora.

This script automatically runs the next DAG after the current DAG finishes.
"""

# Libraries
import sys
import os
import time

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from datetime import timedelta

# Set the path to the folder where the data transfer and ETL scripts are stored
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

import data_transfer_from_mysql
import etl_from_staging_to_aurora
import check_mysql


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

# DAG definition
dag = DAG(
    'airflow_data_transfer_automatic_runs',
    default_args=default_args,
    description='Data Transfer Process from MySQL to Staging Area in PostgreSQL, then to Production in Amazon Aurora',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
)


# Function to check if it's the first DAG run
def check_first_run(**context):
    dag_id = context['dag'].dag_id
    dag_runs = DagRun.find(dag_id=dag_id, state=State.SUCCESS)
    
    if len(dag_runs) == 0:  # If there are no successful runs, this is considered the first run
        return 'check_mysql'
    return 'skip_check_mysql'


# Function to wait for 60 seconds before triggering the next run
def wait_60_seconds(**context):
    print("Starting 60-second countdown before triggering next run.")
    for i in range(6, 0, -1):
        print(f"{i * 10} seconds remaining...")
        time.sleep(10)
    print("Countdown finished. Proceeding to trigger next run.")

# Branch operator to check if it's the first run
branch_op = BranchPythonOperator(
    task_id='check_first_run',
    python_callable=check_first_run,
    provide_context=True,
    dag=dag,
)


# Task to check the first 5 rows in MySQL (task0) - triggered only if it's the first run
task0 = PythonOperator(
    task_id='check_mysql',
    python_callable=check_mysql.main,
    dag=dag,
)

# Task to transfer data from MySQL to PostgreSQL (task1)
task1 = PythonOperator(
    task_id='data_transfer_from_mysql_to_postgresql',
    python_callable=data_transfer_from_mysql.main,
    dag=dag,
)

# Task to transfer data from staging in local PostgreSQL to production in Amazon Aurora (task2)
task2 = PythonOperator(
    task_id='data_transfer_from_staging_to_production',
    python_callable=etl_from_staging_to_aurora.main,
    dag=dag,
)

# Dummy operator to be triggered when it's not the first run (skip the check_mysql task)
skip_check_mysql = DummyOperator(
    task_id='skip_check_mysql',
    dag=dag,
)

# Waiting task that counts down for 60 seconds
wait_task = PythonOperator(
    task_id='wait_60_seconds',
    python_callable=wait_60_seconds,
    dag=dag,
)

# Trigger the next DAG run after the current DAG finishes
trigger_next_run = TriggerDagRunOperator(
    task_id='trigger_next_run',
    trigger_dag_id='airflow_data_transfer_automatic_runs',
    dag=dag,
)

# Add a new dummy operator to join the branches
join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Modify the task dependencies based on the different branches
branch_op >> [task0, skip_check_mysql]
task0 >> join
skip_check_mysql >> join
join >> task1 >> task2 >> wait_task >> trigger_next_run
