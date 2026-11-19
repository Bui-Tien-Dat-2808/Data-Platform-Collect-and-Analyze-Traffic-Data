"""
Execute Gold Layer Dimensional Transformations in PostgreSQL
Materializes:
- dim_corridor
- fact_traffic_flow_15min
- fact_hourly_traffic_summary
Runs automated data quality tests matching dbt specifications.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "traffic_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres_traffic_2026")

def run_gold_transformation():
    print(f"Connecting to PostgreSQL on {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Dim Corridor
    print("Building Gold Table: dim_corridor...")
    cur.execute("""
        DROP TABLE IF EXISTS dim_corridor CASCADE;
        CREATE TABLE dim_corridor AS
        SELECT DISTINCT
            corridor_id,
            corridor_name,
            district,
            CURRENT_TIMESTAMP AS created_at
        FROM stg_traffic_flow;
    """)

    # 2. Fact Traffic Flow 15Min
    print("Building Gold Table: fact_traffic_flow_15min...")
    cur.execute("""
        DROP TABLE IF EXISTS fact_traffic_flow_15min CASCADE;
        CREATE TABLE fact_traffic_flow_15min AS
        SELECT
            MD5(CONCAT(point_id, '_', timestamp_vn)) AS fact_id,
            point_id,
            point_name,
            corridor_id,
            timestamp_vn,
            CAST(timestamp_vn AS DATE) AS date_key,
            EXTRACT(HOUR FROM timestamp_vn)::INT AS hour_key,
            EXTRACT(DOW FROM timestamp_vn)::INT AS day_of_week,
            current_speed_kmh,
            free_flow_speed_kmh,
            current_travel_time_sec,
            free_flow_travel_time_sec,
            speed_reduction_ratio_pct,
            travel_delay_seconds,
            confidence,
            congestion_level
        FROM stg_traffic_flow;
    """)

    # 3. Fact Hourly Summary
    print("Building Gold Table: fact_hourly_traffic_summary...")
    cur.execute("""
        DROP TABLE IF EXISTS fact_hourly_traffic_summary CASCADE;
        CREATE TABLE fact_hourly_traffic_summary AS
        SELECT
            corridor_id,
            point_id,
            date_key,
            hour_key,
            COUNT(*) AS total_samples,
            ROUND(AVG(current_speed_kmh)::numeric, 2) AS avg_current_speed_kmh,
            ROUND(AVG(free_flow_speed_kmh)::numeric, 2) AS avg_free_flow_speed_kmh,
            ROUND(AVG(speed_reduction_ratio_pct)::numeric, 2) AS avg_speed_reduction_pct,
            ROUND(AVG(travel_delay_seconds)::numeric, 2) AS avg_delay_seconds,
            CASE 
                WHEN AVG(speed_reduction_ratio_pct) < 20 THEN 'SMOOTH'
                WHEN AVG(speed_reduction_ratio_pct) < 50 THEN 'SLIGHT_DELAY'
                ELSE 'SEVERE_CONGESTION'
            END AS hourly_congestion_status
        FROM fact_traffic_flow_15min
        GROUP BY
            corridor_id,
            point_id,
            date_key,
            hour_key;
    """)

    # 4. Automated Quality Test Assertions
    print("\n--- RUNNING DATA QUALITY TESTS (dbt-equivalent) ---")
    cur.execute("SELECT COUNT(*) FROM fact_traffic_flow_15min WHERE fact_id IS NULL;")
    null_fact_ids = cur.fetchone()[0]
    assert null_fact_ids == 0, "Test Failed: fact_id has null values!"
    print("[PASSED] Test NOT NULL on fact_id")

    cur.execute("SELECT fact_id, COUNT(*) FROM fact_traffic_flow_15min GROUP BY fact_id HAVING COUNT(*) > 1;")
    dups = cur.fetchall()
    assert len(dups) == 0, "Test Failed: duplicate fact_id found!"
    print("[PASSED] Test UNIQUE on fact_id")

    cur.execute("SELECT COUNT(*) FROM fact_traffic_flow_15min WHERE current_speed_kmh < 0;")
    negative_speeds = cur.fetchone()[0]
    assert negative_speeds == 0, "Test Failed: negative current_speed_kmh found!"
    print("[PASSED] Test ACCEPTED VALUES (speed >= 0)")

    cur.execute("SELECT COUNT(*) FROM fact_traffic_flow_15min;")
    total_facts = cur.fetchone()[0]
    print(f"\n>> SUCCESS! Gold Layer Built Successfully! Total facts materialized: {total_facts}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_gold_transformation()
