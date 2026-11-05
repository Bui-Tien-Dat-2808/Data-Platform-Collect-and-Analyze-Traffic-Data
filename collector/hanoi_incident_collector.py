import os
import json
import time
import logging

from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from minio import Minio


# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT", "http://localhost:9000"
).replace("http://", "").replace("https://", "")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_BUCKET_BRONZE = os.getenv(
    "MINIO_BUCKET_BRONZE", "lakehouse-bronze"
)

API_URL = (
    "https://api.tomtom.com/traffic/services/5/incidentDetails"
)

CONFIG_PATH = PROJECT_ROOT / "config" / "hanoi_area_config.json"

SAMPLE_OUTPUT_PATH = (
    PROJECT_ROOT / "collector" / "sample_incidents.json"
)

# Các trường cần lấy từ TomTom API.
# requests sẽ tự encode ký tự đặc biệt trong params.
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


# =====================================================
# 2. LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =====================================================
# 3. LOAD HANOI BBOX CONFIG
# =====================================================

def load_area_config(config_path=CONFIG_PATH):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


def build_bbox(config):
    bbox = config["bbox"]

    return ",".join([
        str(bbox["min_lon"]),
        str(bbox["min_lat"]),
        str(bbox["max_lon"]),
        str(bbox["max_lat"])
    ])


# =====================================================
# 4. EXTRACT INCIDENT DATA
# =====================================================

def extract_coordinates(geometry):
    """
    Hỗ trợ cả Point và LineString.

    Point:
        [lon, lat]

    LineString:
        [[lon1, lat1], [lon2, lat2], ...]

    Với LineString, lấy tọa độ đầu tiên làm vị trí đại diện.
    """

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
            logger.warning(
                "Geometry type không được hỗ trợ: %s",
                geometry_type
            )
            return None, None

        return float(lon), float(lat)

    except (TypeError, ValueError, IndexError):
        logger.warning("Không thể đọc geometry coordinates")
        return None, None


def normalize_incident(incident):
    """
    Chuyển một incident từ cấu trúc API TomTom
    thành cấu trúc dữ liệu thống nhất của dự án.
    """

    properties = incident.get("properties") or {}
    geometry = incident.get("geometry") or {}

    longitude, latitude = extract_coordinates(geometry)

    events = properties.get("events") or []

    description = None

    if events and isinstance(events[0], dict):
        description = events[0].get("description")

    return {
        "incident_id": properties.get("id"),
        "incident_type": properties.get("iconCategory"),
        "type_label": TYPE_MAP.get(
            properties.get("iconCategory"), "Unknown"
        ),
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


# =====================================================
# 5. CALL TOMTOM INCIDENT DETAILS API
# =====================================================

def fetch_incidents(bbox, api_key, max_retries=3):

    params = {
        "key": api_key,
        "bbox": bbox,
        "fields": FIELDS,
        "timeValidityFilter": "present",
        "language": "en-GB"
    }

    for attempt in range(max_retries):

        try:
            logger.info(
                "Đang gọi TomTom Incident API - lần %s/%s",
                attempt + 1,
                max_retries
            )

            response = requests.get(
                API_URL,
                params=params,
                timeout=20
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                logger.warning("TomTom API giới hạn request (429)")

            else:
                response.raise_for_status()

        except requests.Timeout:
            logger.warning("Request timeout")

        except requests.RequestException as error:
            # Không retry các lỗi HTTP thông thường,
            # trừ lỗi 429 đã được xử lý phía trên.
            if (
                getattr(error.response, "status_code", None)
                != 429
            ):
                raise

            logger.warning("API trả về lỗi 429")

        if attempt < max_retries - 1:
            wait_seconds = 2 ** (attempt + 1)

            logger.info(
                "Chờ %s giây trước khi thử lại...",
                wait_seconds
            )

            time.sleep(wait_seconds)

    raise RuntimeError(
        "Không thể lấy dữ liệu Incident sau "
        f"{max_retries} lần thử."
    )


# =====================================================
# 6. SAVE BRONZE DATA LOCALLY
# =====================================================

def save_local_json(data, output_path=SAMPLE_OUTPUT_PATH):

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4
        )

    logger.info("Đã lưu dữ liệu local: %s", output_path)

    return output_path


# =====================================================
# 7. UPLOAD TO MINIO BRONZE
# =====================================================

def upload_to_minio_bronze(file_path, batch_timestamp):

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET_BRONZE):
        client.make_bucket(MINIO_BUCKET_BRONZE)

    object_name = (
        f"tomtom/incidents/"
        f"{batch_timestamp.strftime('%Y/%m/%d')}/"
        f"incidents_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    )

    client.fput_object(
        MINIO_BUCKET_BRONZE,
        object_name,
        str(file_path),
        content_type="application/json"
    )

    logger.info(
        "Đã upload MinIO: %s/%s",
        MINIO_BUCKET_BRONZE,
        object_name
    )

    return object_name


# =====================================================
# 8. MAIN COLLECTOR
# =====================================================

def run_collector(config_path=CONFIG_PATH):

    if not TOMTOM_API_KEY:
        raise ValueError(
            "Chưa cấu hình TOMTOM_API_KEY trong file .env"
        )

    config = load_area_config(config_path)

    bbox = build_bbox(config)

    logger.info("Bắt đầu thu thập Incident tại Hà Nội")
    logger.info("BBOX: %s", bbox)

    # Gọi API
    raw_response = fetch_incidents(
        bbox=bbox,
        api_key=TOMTOM_API_KEY
    )

    # Lấy danh sách incidents
    incidents_raw = raw_response.get("incidents") or []

    # Chuẩn hóa dữ liệu
    incidents = [
        normalize_incident(item)
        for item in incidents_raw
    ]

    fetched_at = datetime.now(timezone.utc)

    batch_timestamp = fetched_at.strftime("%Y%m%d_%H%M%S")

    # Payload Bronze
    output_data = {
        "metadata": {
            "ingestion_timestamp_utc": fetched_at.isoformat(),
            "source": "TomTom Incident Details API",
            "bbox_area": config.get("bbox_area", "HANOI"),
            "bbox": bbox,
            "total_incidents": len(incidents)
        },
        "incidents": incidents,
        "raw_response": raw_response
    }

    # Lưu local
    local_file = save_local_json(output_data)

    # Upload MinIO
    object_name = upload_to_minio_bronze(
        local_file,
        fetched_at
    )

    logger.info("Thu thập hoàn tất")
    logger.info("Tổng số incidents: %s", len(incidents))
    logger.info("MinIO object: %s", object_name)

    return output_data


if __name__ == "__main__":
    run_collector()