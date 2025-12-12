from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your ETL functions
from extract import extract_files
from Transform import transform_files
from Load import Load_Data

# Default settings for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='nyc_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',     # Runs everyday — change if needed
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_files
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_files
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=Load_Data
    )

    extract_task >> transform_task >> load_task
