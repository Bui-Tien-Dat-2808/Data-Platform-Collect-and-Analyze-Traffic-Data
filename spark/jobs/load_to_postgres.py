"""
Load Cleaned Silver Parquet Data into PostgreSQL Staging Table
Serves as the bridge between Lakehouse Silver (Parquet) and Data Warehouse Gold (dbt)
"""

import os
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "traffic_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres_traffic_2026")

def load_silver_to_postgres(parquet_path: str = "data/silver_traffic_parquet"):
    print(f"Reading Parquet datasets from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    if df.empty:
        print("No records found to load.")
        return

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_traffic_flow (
            point_id VARCHAR(50),
            point_name VARCHAR(100),
            corridor_id VARCHAR(50),
            corridor_name VARCHAR(100),
            district VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            frc VARCHAR(20),
            current_speed_kmh DOUBLE PRECISION,
            free_flow_speed_kmh DOUBLE PRECISION,
            current_travel_time_sec INT,
            free_flow_travel_time_sec INT,
            confidence DOUBLE PRECISION,
            road_closure BOOLEAN,
            speed_reduction_ratio_pct DOUBLE PRECISION,
            travel_delay_seconds INT,
            congestion_level VARCHAR(50),
            timestamp_vn TIMESTAMP
        );
        TRUNCATE TABLE stg_traffic_flow;
    """)

    cols = list(df.columns)
    col_str = ", ".join(cols)
    values = [tuple(None if pd.isna(val) else val for val in row) for row in df.itertuples(index=False)]
    insert_query = f"INSERT INTO stg_traffic_flow ({col_str}) VALUES %s"
    
    execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Successfully loaded {len(values)} records into PostgreSQL table 'stg_traffic_flow'!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_silver_to_postgres()

