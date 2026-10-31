import os
import json
import time
import logging
from datetime import datetime, timezone
import requests
from minio import Minio
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("TomTomCollector")

# Load environment variables
load_dotenv()

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "lakehouse-bronze")

FLOW_SEGMENT_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"

def upload_to_minio_bronze(file_path: str, batch_timestamp: datetime):
    """Uploads collected raw JSON batch to MinIO Bronze Lakehouse partition."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        if not client.bucket_exists(MINIO_BUCKET_BRONZE):
            client.make_bucket(MINIO_BUCKET_BRONZE)

        ymd = batch_timestamp.strftime("%Y/%m/%d")
        fname = batch_timestamp.strftime("%Y%m%d_%H%M%S")
        s3_object_name = f"tomtom/flow/{ymd}/traffic_raw_{fname}.json"
        
        client.fput_object(MINIO_BUCKET_BRONZE, s3_object_name, file_path)
        logger.info(f"Successfully pushed raw data to MinIO: s3://{MINIO_BUCKET_BRONZE}/{s3_object_name}")
    except Exception as e:
        logger.error(f"Failed to upload to MinIO Bronze: {e}")

def fetch_traffic_flow(lat: float, lon: float, api_key: str, max_retries: int = 3) -> dict:
    """
    Fetch traffic flow data for a specific coordinate from TomTom API.
    Includes retry mechanism with exponential backoff.
    """
    params = {
        "point": f"{lat},{lon}",
        "unit": "KMPH",
        "key": api_key
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            start_time = time.time()
            response = requests.get(FLOW_SEGMENT_URL, params=params, timeout=10)
            elapsed_ms = round((time.time() - start_time) * 1000, 2)
            
            if response.status_code == 200:
                payload = response.json()
                return {
                    "status": "SUCCESS",
                    "http_status": 200,
                    "elapsed_ms": elapsed_ms,
                    "data": payload
                }
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded (429). Retrying attempt {attempt}/{max_retries}...")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"TomTom API HTTP {response.status_code}: {response.text}")
                return {
                    "status": "FAILED",
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": response.text
                }
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error on attempt {attempt}: {str(e)}")
            time.sleep(1.5 * attempt)
            
    return {
        "status": "FAILED",
        "http_status": None,
        "elapsed_ms": None,
        "error": "Max retries reached without success."
    }

def run_collector(config_path: str = "config/hanoi_corridors.json"):
    """
    Main ingestion execution loop across all configured monitoring points.
    Appends audit metadata for data lineage & lakehouse ingestion.
    """
    if not TOMTOM_API_KEY:
        raise ValueError("TOMTOM_API_KEY is not defined in environment variables or .env file.")

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    corridors = config.get("corridors", [])
    collected_records = []
    batch_timestamp = datetime.now(timezone.utc).isoformat()

    logger.info(f"Starting ingestion cycle at {batch_timestamp} for {len(corridors)} corridors...")

    for corridor in corridors:
        c_id = corridor["corridor_id"]
        c_name = corridor["corridor_name"]
        district = corridor["district"]

        for point in corridor.get("monitoring_points", []):
            p_id = point["point_id"]
            p_name = point["name"]
            lat = point["latitude"]
            lon = point["longitude"]

            logger.info(f"Querying [{c_id}] Point: {p_name} ({lat}, {lon})...")
            result = fetch_traffic_flow(lat, lon, TOMTOM_API_KEY)

            record = {
                "metadata": {
                    "ingestion_timestamp_utc": batch_timestamp,
                    "collector_version": "1.0.0",
                    "source": "TomTom_Traffic_API_FlowSegment",
                    "corridor_id": c_id,
                    "corridor_name": c_name,
                    "district": district,
                    "point_id": p_id,
                    "point_name": p_name,
                    "request_latitude": lat,
                    "request_longitude": lon,
                    "latency_ms": result.get("elapsed_ms"),
                    "status": result.get("status")
                },
                "payload": result.get("data") if result.get("status") == "SUCCESS" else None,
                "error": result.get("error") if result.get("status") != "SUCCESS" else None
            }
            collected_records.append(record)
            time.sleep(0.3)  # Gentle pacing to prevent sudden quota burst

    logger.info(f"Cycle completed. Collected {len(collected_records)} raw records.")

    # Save local copy
    sample_file = "collector/sample_batch.json"
    with open(sample_file, "w", encoding="utf-8") as f:
        json.dump(collected_records, f, indent=2, ensure_ascii=False)
    logger.info(f"Sample data persisted to {sample_file}")

    # Automatically push to MinIO Bronze Lakehouse
    upload_to_minio_bronze(sample_file, datetime.now(timezone.utc))

    return collected_records

if __name__ == "__main__":
    records = run_collector()

