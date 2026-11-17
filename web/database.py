"""
Database connection pool and query utilities for Traffic Intelligence Web API.
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_NAME = os.getenv("POSTGRES_DB", "traffic_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "***REMOVED***")

logger = logging.getLogger("TrafficDB")

# Connection Pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    logger.info("Database connection pool initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize database pool: {e}")
    db_pool = None

def get_db_connection():
    if db_pool is None:
        raise ConnectionError("Database connection pool is not available.")
    return db_pool.getconn()

def release_db_connection(conn):
    if db_pool and conn:
        db_pool.putconn(conn)

def get_monitored_cities() -> List[Dict[str, Any]]:
    """Retrieve all monitored metropolitan cities."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT city_id, city_name, center_latitude, center_longitude, default_zoom
                FROM dim_city
                ORDER BY city_name ASC;
            """)
            return cur.fetchall()
    finally:
        release_db_connection(conn)

def get_live_city_incidents(city_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve real-time traffic incidents for the selected city from the latest snapshot.
    Includes severity color mapping (Green/Yellow/Red) like Google Maps.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                WITH latest_snapshot AS (
                    SELECT MAX(fetched_at_utc) AS max_utc
                    FROM fact_traffic_incidents
                    WHERE city_id = %s
                )
                SELECT 
                    f.incident_id,
                    f.city_id,
                    f.city_name,
                    f.incident_type,
                    f.type_label,
                    f.magnitude,
                    f.delay_seconds,
                    f.delay_minutes,
                    f.length_km,
                    f.length_m,
                    f.road_from,
                    f.road_to,
                    f.description,
                    f.latitude,
                    f.longitude,
                    f.fetched_at_vn,
                    -- Color code & congestion level similar to Google Maps
                    CASE 
                        WHEN f.incident_type = 1 THEN 'accident'       -- Orange/Purple
                        WHEN f.incident_type IN (7, 8, 9) THEN 'closure'-- Gray/Blue
                        WHEN f.delay_minutes >= 5.0 OR f.magnitude >= 3 THEN 'severe'   -- Red (Tắc nghẽn nghiêm trọng)
                        WHEN f.delay_minutes >= 2.0 OR f.magnitude = 2 THEN 'moderate'  -- Yellow/Orange (Ùn ứ vừa)
                        ELSE 'light'                                   -- Green / Yellow nhạt (Chậm nhẹ)
                    END AS congestion_severity,
                    CASE 
                        WHEN f.incident_type = 1 THEN '#E056FD'        -- Tím/Cam tai nạn
                        WHEN f.incident_type IN (7, 8, 9) THEN '#30336B'-- Đóng đường / Thi công
                        WHEN f.delay_minutes >= 5.0 OR f.magnitude >= 3 THEN '#EA2027'   -- Đỏ rực
                        WHEN f.delay_minutes >= 2.0 OR f.magnitude = 2 THEN '#FFC312'  -- Vàng cam
                        ELSE '#10AC84'                                   -- Xanh ngọc / Nhẹ
                    END AS color_hex
                FROM fact_traffic_incidents f
                JOIN latest_snapshot s ON f.fetched_at_utc = s.max_utc
                WHERE f.city_id = %s
                ORDER BY f.delay_minutes DESC NULLS LAST;
            """
            cur.execute(query, (city_id, city_id))
            return cur.fetchall()
    finally:
        release_db_connection(conn)

def get_live_city_kpi(city_id: str) -> Dict[str, Any]:
    """Retrieve key operational metrics for the latest snapshot of a city."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                WITH latest_snapshot AS (
                    SELECT MAX(fetched_at_utc) AS max_utc
                    FROM fact_traffic_incidents
                    WHERE city_id = %s
                ),
                city_data AS (
                    SELECT f.*
                    FROM fact_traffic_incidents f
                    JOIN latest_snapshot s ON f.fetched_at_utc = s.max_utc
                    WHERE f.city_id = %s
                )
                SELECT 
                    COUNT(*) AS total_incidents,
                    COALESCE(ROUND(SUM(length_km)::numeric, 2), 0.0) AS total_congestion_length_km,
                    COALESCE(ROUND(AVG(delay_minutes)::numeric, 1), 0.0) AS avg_delay_minutes,
                    COALESCE(ROUND(MAX(delay_minutes)::numeric, 1), 0.0) AS max_delay_minutes,
                    (
                        SELECT CONCAT(road_from, ' -> ', road_to)
                        FROM city_data
                        WHERE road_from IS NOT NULL AND road_from != ''
                        ORDER BY delay_minutes DESC NULLS LAST
                        LIMIT 1
                    ) AS top_congested_corridor,
                    (
                        SELECT TO_CHAR(MAX(fetched_at_vn), 'HH24:MI:SS DD/MM/YYYY')
                        FROM city_data
                    ) AS last_updated_time
                FROM city_data;
            """
            cur.execute(query, (city_id, city_id))
            result = cur.fetchone()
            return result or {
                "total_incidents": 0,
                "total_congestion_length_km": 0.0,
                "avg_delay_minutes": 0.0,
                "max_delay_minutes": 0.0,
                "top_congested_corridor": "Không có",
                "last_updated_time": "Chưa có dữ liệu"
            }
    finally:
        release_db_connection(conn)

def get_incident_type_distribution(city_id: str) -> List[Dict[str, Any]]:
    """Retrieve breakdown of incident categories for charting."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                WITH latest_snapshot AS (
                    SELECT MAX(fetched_at_utc) AS max_utc
                    FROM fact_traffic_incidents
                    WHERE city_id = %s
                )
                SELECT 
                    f.type_label,
                    COUNT(*) AS incident_count,
                    ROUND(SUM(f.length_km)::numeric, 2) AS sum_length_km
                FROM fact_traffic_incidents f
                JOIN latest_snapshot s ON f.fetched_at_utc = s.max_utc
                WHERE f.city_id = %s
                GROUP BY f.type_label
                ORDER BY incident_count DESC;
            """
            cur.execute(query, (city_id, city_id))
            return cur.fetchall()
    finally:
        release_db_connection(conn)

