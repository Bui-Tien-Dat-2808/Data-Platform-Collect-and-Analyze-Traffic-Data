"""
Multi-City Incident Collector (Hanoi, HCMC, Da Nang).
Polls TomTom Incident Details API for all configured cities,
tags city metadata, normalizes geometry/delays, and saves to Bronze Lakehouse.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from minio import Minio

# ----------------------------------------------------
# 1. CONFIGURATION
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

MINIO_ENDPOINT_RAW = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "://" in MINIO_ENDPOINT_RAW:
    parsed_endpoint = urlparse(MINIO_ENDPOINT_RAW)
    MINIO_ENDPOINT = parsed_endpoint.netloc
    MINIO_SECURE = parsed_endpoint.scheme == "https"
else:
    MINIO_ENDPOINT = MINIO_ENDPOINT_RAW
    MINIO_SECURE = False

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "lakehouse-bronze")

API_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"
CONFIG_PATH = PROJECT_ROOT / "config" / "cities_config.json"
SAMPLE_OUTPUT_DIR = PROJECT_ROOT / "collector"

FIELDS = (
    "{incidents{type,geometry{type,coordinates},properties{"
    "id,iconCategory,magnitudeOfDelay,delay,length,from,to,"
    "events{description}"
    "}}}"
)

TYPE_MAP = {
    1: "Accident",
    2: "Fog",
    3: "Dangerous Conditions",
    4: "Rain",
    5: "Ice",
    6: "Traffic Jam",
    7: "Lane Closed",
    8: "Road Closed",
    9: "Road Works",
    10: "Wind",
    11: "Flooding",
    14: "Broken Down Vehicle"
}

# ----------------------------------------------------
# 2. LOGGING
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] MultiCityCollector: %(message)s"
)
logger = logging.getLogger("MultiCityCollector")

# ----------------------------------------------------
# 3. HELPER FUNCTIONS
# ----------------------------------------------------
def load_cities_config(config_path=CONFIG_PATH):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)

def build_bbox_str(bbox):
    return f"{bbox['min_lon']},{bbox['min_lat']},{bbox['max_lon']},{bbox['max_lat']}"

def extract_coordinates(geometry):
    if not geometry:
        return None, None
    coordinates = geometry.get("coordinates")
    if not coordinates:
        return None, None
    geometry_type = geometry.get("type")

    try:
        if geometry_type == "Point":
            lon, lat = coordinates[0], coordinates[1]
        elif geometry_type == "LineString":
            lon, lat = coordinates[0][0], coordinates[0][1]
        else:
            return None, None
        return float(lon), float(lat)
    except (TypeError, ValueError, IndexError):
        return None, None

def normalize_incident(incident, city_id, city_name):
    properties = incident.get("properties") or {}
    geometry = incident.get("geometry") or {}
    longitude, latitude = extract_coordinates(geometry)
    events = properties.get("events") or []
    description = events[0].get("description") if events and isinstance(events[0], dict) else None

    return {
        "city_id": city_id,
        "city_name": city_name,
        "incident_id": properties.get("id"),
        "incident_type": properties.get("iconCategory"),
        "type_label": TYPE_MAP.get(properties.get("iconCategory"), "Unknown"),
        "magnitude": properties.get("magnitudeOfDelay"),
        "delay_seconds": properties.get("delay"),
        "length_m": properties.get("length"),
        "road_from": properties.get("from"),
        "road_to": properties.get("to"),
        "description": description,
        "longitude": longitude,
        "latitude": latitude,
        "geometry_type": geometry.get("type"),
        "geometry_coordinates": geometry.get("coordinates")
    }

def fetch_incidents(bbox_str, api_key, max_retries=3):
    params = {
        "key": api_key,
        "bbox": bbox_str,
        "fields": FIELDS,
        "timeValidityFilter": "present",
        "language": "en-GB"
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params, timeout=20)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:
                logger.warning(f"Rate limit 429 (Attempt {attempt + 1}/{max_retries})")
            else:
                response.raise_for_status()
        except requests.Timeout:
            logger.warning(f"Request timeout (Attempt {attempt + 1}/{max_retries})")
        except requests.RequestException as error:
            status_code = getattr(error.response, "status_code", None)
            if status_code != 429:
                logger.error(f"HTTP Error: {error}")
                raise
            logger.warning("TomTom API rate limit 429 via exception")

        if attempt < max_retries - 1:
            wait_seconds = 2 ** (attempt + 1)
            logger.info(f"Retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed to fetch incidents after {max_retries} attempts.")

def upload_to_minio(file_path, city_id, batch_dt):
    try:
        import urllib3
        http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=1.5, read=3.0),
            retries=urllib3.Retry(total=1, backoff_factor=0.2)
        )
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
            http_client=http_client
        )
        if not client.bucket_exists(MINIO_BUCKET_BRONZE):
            client.make_bucket(MINIO_BUCKET_BRONZE)

        ymd = batch_dt.strftime("%Y/%m/%d")
        ts = batch_dt.strftime("%Y%m%d_%H%M%S")
        object_name = f"tomtom/incidents/city={city_id}/{ymd}/incidents_{city_id}_{ts}.json"

        client.fput_object(
            MINIO_BUCKET_BRONZE,
            object_name,
            str(file_path),
            content_type="application/json"
        )
        logger.info(f"Uploaded to MinIO: s3://{MINIO_BUCKET_BRONZE}/{object_name}")
        return object_name
    except Exception as e:
        logger.warning(f"Could not upload to MinIO ({e}). Local copy is safely persisted.")
        return None

# ----------------------------------------------------
# 4. MAIN COLLECTOR EXECUTION
# ----------------------------------------------------
def run_multi_city_collector(config_path=CONFIG_PATH):
    if not TOMTOM_API_KEY:
        raise ValueError("TOMTOM_API_KEY is not defined in environment variables or .env")

    cities = load_cities_config(config_path)
    all_city_results = {}
    batch_timestamp = datetime.now(timezone.utc)
    ts_str = batch_timestamp.strftime("%Y%m%d_%H%M%S")

    logger.info(f"Starting Multi-City collection for {len(cities)} cities...")

    for city in cities:
        c_id = city["city_id"]
        c_name = city["city_name"]
        bbox_str = build_bbox_str(city["bbox"])

        logger.info(f"Fetching incidents for {c_name} ({c_id}) [bbox={bbox_str}]...")
        raw_response = fetch_incidents(bbox_str, TOMTOM_API_KEY)
        incidents_raw = raw_response.get("incidents") or []

        incidents = [normalize_incident(item, c_id, c_name) for item in incidents_raw]
        logger.info(f"-> {c_name}: Found {len(incidents)} incidents.")

        city_payload = {
            "metadata": {
                "city_id": c_id,
                "city_name": c_name,
                "ingestion_timestamp_utc": batch_timestamp.isoformat(),
                "source": "TomTom Incident Details API",
                "bbox": bbox_str,
                "total_incidents": len(incidents)
            },
            "incidents": incidents,
            "raw_response": raw_response
        }

        # Save city sample locally
        city_file = SAMPLE_OUTPUT_DIR / f"sample_incidents_{c_id.lower()}.json"
        with open(city_file, "w", encoding="utf-8") as f:
            json.dump(city_payload, f, ensure_ascii=False, indent=2)

        # Upload to MinIO Bronze
        upload_to_minio(city_file, c_id, batch_timestamp)
        all_city_results[c_id] = city_payload
        time.sleep(0.5)

    # Save combined sample
    combined_file = SAMPLE_OUTPUT_DIR / "sample_incidents.json"
    with open(combined_file, "w", encoding="utf-8") as f:
        json.dump(all_city_results, f, ensure_ascii=False, indent=2)

    logger.info("Multi-City ingestion cycle completed successfully!")
    return all_city_results

if __name__ == "__main__":
    run_multi_city_collector()
