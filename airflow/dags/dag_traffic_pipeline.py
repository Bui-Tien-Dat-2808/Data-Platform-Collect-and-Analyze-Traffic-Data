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
    description='Automated pipeline for Multi-City Traffic Incidents: Ingest -> Bronze -> Silver -> Gold',
    schedule_interval='*/5 * * * *',  # Every 5 minutes (Real-time Urban Traffic)
    catchup=False,
    tags=['traffic', 'multi-city', 'realtime', 'lakehouse', 'elt'],
) as dag:

    # 1. Ingestion: Thu thập toàn bộ sự cố/điểm tắc trên 3 đô thị (Hà Nội, TP.HCM, Đà Nẵng) -> MinIO Bronze
    task_ingest_incidents_bronze = BashOperator(
        task_id='ingest_multi_city_incidents_to_bronze',
        bash_command='python /opt/airflow/collector/multi_city_collector.py',
    )

    # 2. Silver Transformation: Chuẩn hóa, lọc lỗi, tính delay_minutes, length_km -> MinIO Silver Parquet
    task_spark_silver_incidents = BashOperator(
        task_id='silver_clean_and_transform_incidents',
        bash_command='python /opt/airflow/spark/jobs/bronze_to_silver_incidents.py',
    )

    # 3. Serving Staging: Nạp Parquet từ Silver vào PostgreSQL staging table
    task_load_incidents_postgres = BashOperator(
        task_id='load_incidents_silver_to_postgres',
        bash_command='python /opt/airflow/spark/jobs/load_incidents_to_postgres.py',
    )

    # 4. Gold Transformation: dbt build Gold Data Marts (fact_traffic_incidents, agg_hourly_incident_summary)
    task_dbt_run = BashOperator(
        task_id='dbt_run_incident_gold_models',
        bash_command='cd /opt/airflow/dbt && dbt run --select tag:incidents fact_traffic_incidents dim_incident_type agg_hourly_incident_summary',
    )

    # 5. Data Quality Assurance: dbt test tự động kiểm tra tính duy nhất, not null
    task_dbt_test = BashOperator(
        task_id='dbt_test_incident_quality',
        bash_command='cd /opt/airflow/dbt && dbt test --select fact_traffic_incidents dim_incident_type agg_hourly_incident_summary',
    )

    # Pipeline tuần tự cho toàn bộ sự cố Hà Nội:
    (
        task_ingest_incidents_bronze
        >> task_spark_silver_incidents
        >> task_load_incidents_postgres
        >> task_dbt_run
        >> task_dbt_test
    )

