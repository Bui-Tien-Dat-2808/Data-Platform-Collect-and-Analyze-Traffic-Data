"""
Bronze to Silver Batch Transformation Job
Supports both Native PyArrow (optimized for Windows/local environments without winutils errors)
and direct push to MinIO Silver Lakehouse (S3).

- Reads raw TomTom traffic JSON records from MinIO or local Bronze
- Flattens nested JSON structures
- Standardizes timestamps & timezones (UTC -> Asia/Ho_Chi_Minh)
- Applies data quality checks (filter nulls, speed > 0, confidence >= 0.5)
- Computes Speed Reduction Rate (SRR) and Travel Delay (TDT)
- Assigns congestion level classification
- Deduplicates on (point_id, timestamp_vn)
- Writes partitioned Parquet and uploads to MinIO lakehouse-silver
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("BronzeToSilver")

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "lakehouse-silver")

def process_bronze_to_silver(input_json_path: str, output_parquet_dir: str = "data/silver_traffic_parquet"):
    logger.info(f"Reading raw Bronze data from {input_json_path}...")
    with open(input_json_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    cleaned_rows = []
    for item in raw_data:
        meta = item.get("metadata", {})
        payload = item.get("payload", {})

        # 1. Filter only successful API responses
        if meta.get("status") != "SUCCESS" or not payload:
            continue

        flow = payload.get("flowSegmentData", {})
        if not flow:
            continue

        current_speed = float(flow.get("currentSpeed", 0.0))
        free_flow_speed = float(flow.get("freeFlowSpeed", 0.0))
        confidence = float(flow.get("confidence", 0.0))

        # 2. Data Quality Filtering
        if free_flow_speed <= 0 or current_speed < 0 or confidence < 0.5:
            continue

        # 3. Timestamp conversion & Timezone handling (UTC -> VN UTC+7)
        ingest_time_str = meta.get("ingestion_timestamp_utc")
        if ingest_time_str:
            dt_utc = datetime.fromisoformat(ingest_time_str)
            dt_vn = dt_utc.astimezone(timezone.utc).replace(tzinfo=None) + pd.Timedelta(hours=7)
        else:
            dt_vn = datetime.now()

        current_travel_time = int(flow.get("currentTravelTime", 0))
        free_flow_travel_time = int(flow.get("freeFlowTravelTime", 0))

        # 4. Derived Traffic Analytics Metrics
        # Speed Reduction Rate: ((freeFlowSpeed - currentSpeed) / freeFlowSpeed) * 100
        speed_reduction = round(((free_flow_speed - current_speed) / free_flow_speed) * 100.0, 2)
        travel_delay = current_travel_time - free_flow_travel_time

        if speed_reduction < 20:
            congestion_level = "LOW / FREE_FLOW"
        elif speed_reduction < 50:
            congestion_level = "MODERATE"
        else:
            congestion_level = "HEAVY / CONGESTED"

        cleaned_rows.append({
            "corridor_id": meta.get("corridor_id"),
            "corridor_name": meta.get("corridor_name"),
            "district": meta.get("district"),
            "point_id": meta.get("point_id"),
            "point_name": meta.get("point_name"),
            "latitude": meta.get("request_latitude"),
            "longitude": meta.get("request_longitude"),
            "frc": flow.get("frc"),
            "current_speed_kmh": current_speed,
            "free_flow_speed_kmh": free_flow_speed,
            "current_travel_time_sec": current_travel_time,
            "free_flow_travel_time_sec": free_flow_travel_time,
            "confidence": confidence,
            "road_closure": bool(flow.get("roadClosure", False)),
            "speed_reduction_ratio_pct": speed_reduction,
            "travel_delay_seconds": travel_delay,
            "congestion_level": congestion_level,
            "timestamp_vn": dt_vn
        })

    if not cleaned_rows:
        logger.warning("No valid records found after data quality cleaning.")
        return

    df = pd.DataFrame(cleaned_rows)

    # 5. Deduplication by (point_id, timestamp_vn)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["point_id", "timestamp_vn"])
    logger.info(f"Cleaned {len(df)} records (deduplicated {before_dedup - len(df)} duplicate records).")

    # 6. Save as Parquet partitioned by corridor_id
    os.makedirs(output_parquet_dir, exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=output_parquet_dir,
        partition_cols=["corridor_id"],
        compression="snappy"
    )
    logger.info(f"Partitioned Parquet written successfully to: {output_parquet_dir}")

    # 7. Upload to MinIO lakehouse-silver
    upload_silver_to_minio(output_parquet_dir)

def upload_silver_to_minio(parquet_dir: str):
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        if not client.bucket_exists(MINIO_BUCKET_SILVER):
            client.make_bucket(MINIO_BUCKET_SILVER)

        uploaded_count = 0
        for root, dirs, files in os.walk(parquet_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, parquet_dir).replace("\\", "/")
                    s3_path = f"traffic_flow/{rel_path}"
                    client.fput_object(MINIO_BUCKET_SILVER, s3_path, local_path)
                    uploaded_count += 1

        logger.info(f"Uploaded {uploaded_count} Parquet files to MinIO: s3://{MINIO_BUCKET_SILVER}/traffic_flow/")
    except Exception as e:
        logger.error(f"Failed to upload Silver Parquet to MinIO: {e}")

if __name__ == "__main__":
    bronze_path = sys.argv[1] if len(sys.argv) > 1 else "collector/sample_batch.json"
    silver_path = sys.argv[2] if len(sys.argv) > 2 else "data/silver_traffic_parquet"
    process_bronze_to_silver(bronze_path, silver_path)
