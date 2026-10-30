from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'dinah',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_batch_pipeline',
    default_args=default_args,
    description='Batch ETL pipeline for Olist E-commerce data',
    schedule='@daily', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ecommerce'],
) as dag:

    # Task 1: Run ingestion (Raw -> Bronze)
    ingest_task = BashOperator(
        task_id='ingest_data',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow/common && python /opt/airflow/batch/ingest.py'
    )

    # Task 2: Run cleaning (Bronze -> Silver)
    clean_task = BashOperator(
        task_id='clean_data',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow/common && python /opt/airflow/batch/clean.py'
    )

    # Task 3: Run transformations (Silver -> Gold)
    transform_task = BashOperator(
        task_id='transform_data',
        bash_command='cd /opt/airflow && export PYTHONPATH=/opt/airflow/common && python /opt/airflow/batch/transform.py'
    )
    
    # Define the execution order (dependencies)
    ingest_task >> clean_task >> transform_task