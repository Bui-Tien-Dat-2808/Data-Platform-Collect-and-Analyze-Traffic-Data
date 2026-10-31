from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_platform_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'traffic_data_platform_pipeline',
    default_args=default_args,
    description='Automated pipeline for TomTom Traffic Data: Ingest -> Bronze -> Silver -> Gold',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['traffic', 'lakehouse', 'elt'],
) as dag:

    # 1. Ingestion: Call TomTom API and append to Bronze
    task_ingest_bronze = BashOperator(
        task_id='ingest_tomtom_to_bronze',
        bash_command='python /opt/airflow/collector/tomtom_collector.py',
    )

    # 2. PySpark Clean & Transform Bronze -> Silver
    task_spark_silver = BashOperator(
        task_id='spark_clean_bronze_to_silver',
        bash_command='python /opt/airflow/spark/jobs/bronze_to_silver.py',
    )

    # 3. dbt Transform Silver -> Gold in PostgreSQL
    task_dbt_run = BashOperator(
        task_id='dbt_run_gold_models',
        bash_command='cd /opt/airflow/dbt && dbt run',
    )

    # 4. dbt Test Data Quality
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command='cd /opt/airflow/dbt && dbt test',
    )

    task_ingest_bronze >> task_spark_silver >> task_dbt_run >> task_dbt_test

