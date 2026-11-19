
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ==========================================
# 1. DEFAULT ARGUMENTS
# ==========================================

default_args = {
    'owner': 'data_platform_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


# ==========================================
# 2. DEFINE DAG
# ==========================================

with DAG(
    dag_id='traffic_data_platform_pipeline',
    default_args=default_args,
    description='Multi-City Traffic Data Pipeline: Flow and Incidents to Bronze, Silver and Gold',
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['traffic', 'multi-city', 'lakehouse', 'elt', 'incident'],
) as dag:

    # ==========================================
    # 3. BRONZE - DATA INGESTION
    # ==========================================

    # Traffic Flow Collector
    task_ingest_flow = BashOperator(
        task_id='ingest_tomtom_flow_to_bronze',
        bash_command='python /opt/airflow/collector/tomtom_collector.py',
    )

    # Multi-City Incident Collector
    task_ingest_incidents = BashOperator(
        task_id='ingest_multi_city_incidents_to_bronze',
        bash_command='python /opt/airflow/collector/multi_city_collector.py',
    )


    # ==========================================
    # 4. SILVER - SPARK PROCESSING
    # ==========================================

    # Traffic Flow Processing
    task_spark_flow = BashOperator(
        task_id='spark_clean_flow_bronze_to_silver',
        bash_command='python /opt/airflow/spark/jobs/bronze_to_silver.py',
    )

    # Multi-City Incident Processing
    task_spark_incidents = BashOperator(
        task_id='spark_clean_incidents_bronze_to_silver',
        bash_command='python /opt/airflow/spark/jobs/bronze_to_silver_incidents.py',
    )


    # ==========================================
    # 5. LOAD INCIDENTS TO POSTGRESQL
    # ==========================================

    task_load_incidents = BashOperator(
        task_id='load_incidents_to_postgres',
        bash_command='python /opt/airflow/spark/jobs/load_incidents_to_postgres.py',
    )


    # ==========================================
    # 6. GOLD - DBT TRANSFORMATION
    # ==========================================

    task_dbt_run = BashOperator(
        task_id='dbt_run_gold_models',
        bash_command='cd /opt/airflow/dbt && dbt run',
    )


    # ==========================================
    # 7. DATA QUALITY - DBT TEST
    # ==========================================

    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command='cd /opt/airflow/dbt && dbt test',
    )


    # ==========================================
    # 8. TASK DEPENDENCIES
    # ==========================================

    # Branch 1: Traffic Flow
    task_ingest_flow >> task_spark_flow

    # Branch 2: Multi-City Incidents
    (
        task_ingest_incidents
        >> task_spark_incidents
        >> task_load_incidents
    )

    # dbt waits for both branches
    [
        task_spark_flow,
        task_load_incidents
    ] >> task_dbt_run

    # Data quality checks after dbt
    task_dbt_run >> task_dbt_test
