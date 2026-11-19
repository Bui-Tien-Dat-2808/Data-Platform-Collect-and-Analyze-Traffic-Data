
import os
import json
import math
import logging
import argparse

from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
from minio import Minio
from dotenv import load_dotenv


# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

MINIO_ENDPOINT_RAW = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

if "://" in MINIO_ENDPOINT_RAW:
    parsed_endpoint = urlparse(MINIO_ENDPOINT_RAW)
    MINIO_ENDPOINT = parsed_endpoint.netloc
    MINIO_SECURE = parsed_endpoint.scheme == "https"
else:
    MINIO_ENDPOINT = MINIO_ENDPOINT_RAW
    MINIO_SECURE = False

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

MINIO_BUCKET_BRONZE = os.getenv(
    "MINIO_BUCKET_BRONZE",
    "lakehouse-bronze"
)

MINIO_BUCKET_SILVER = os.getenv(
    "MINIO_BUCKET_SILVER",
    "lakehouse-silver"
)

LOCAL_INPUT = PROJECT_ROOT / "collector" / "sample_incidents.json"

LOCAL_OUTPUT = PROJECT_ROOT / "data" / "silver_incidents"

TIMEZONE_VN = "Asia/Ho_Chi_Minh"


# =====================================================
# 2. INCIDENT TYPE MAPPING
# =====================================================

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
# 3. SILVER SCHEMA
# =====================================================

SILVER_COLUMNS = [
    "city_id",
    "city_name",
    "incident_id",
    "incident_type",
    "type_label",
    "magnitude",
    "delay_seconds",
    "delay_minutes",
    "length_m",
    "length_km",
    "road_from",
    "road_to",
    "description",
    "longitude",
    "latitude",
    "fetched_at_utc",
    "fetched_at_vn",
    "dt"
]


# =====================================================
# 4. LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =====================================================
# 5. MINIO CLIENT
# =====================================================

def get_minio_client():

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


# =====================================================
# 6. READ BRONZE JSON
# =====================================================

def read_local_json(input_path):

    input_path = Path(input_path)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file Bronze: {input_path}"
        )

    with open(input_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, dict):
        raise ValueError("Cấu trúc Bronze không hợp lệ.")

    logger.info("Đã đọc Bronze local: %s", input_path)

    return data


def read_minio_json(object_name):

    client = get_minio_client()

    response = client.get_object(
        MINIO_BUCKET_BRONZE,
        object_name
    )

    try:
        raw_data = response.read()
        data = json.loads(raw_data.decode("utf-8"))

    finally:
        response.close()
        response.release_conn()

    if not isinstance(data, dict):
        raise ValueError("Bronze trên MinIO không phải JSON object.")

    logger.info(
        "Đã đọc Bronze từ MinIO: %s/%s",
        MINIO_BUCKET_BRONZE,
        object_name
    )

    return data


# =====================================================
# 7. DATA CONVERSION HELPERS
# =====================================================

def to_numeric(value):

    if value is None:
        return None

    result = pd.to_numeric(value, errors="coerce")

    if pd.isna(result):
        return None

    result = float(result)

    # Loại bỏ NaN và Infinity
    if not math.isfinite(result):
        return None

    return result


def to_integer(value):

    numeric_value = to_numeric(value)

    if numeric_value is None:
        return None

    return int(numeric_value)


def get_coordinates(incident):

    longitude = incident.get("longitude")
    latitude = incident.get("latitude")

    if longitude is not None and latitude is not None:
        return longitude, latitude

    geometry = incident.get("geometry") or {}

    coordinates = (
        incident.get("geometry_coordinates")
        or geometry.get("coordinates")
    )

    geometry_type = (
        incident.get("geometry_type")
        or geometry.get("type")
    )

    if not coordinates:
        return None, None

    try:

        if geometry_type == "Point":
            return coordinates[0], coordinates[1]

        if geometry_type == "LineString":
            return coordinates[0][0], coordinates[0][1]

    except (TypeError, IndexError, KeyError):
        logger.warning(
            "Không thể đọc tọa độ incident: %s",
            incident.get("incident_id")
        )

    return None, None


# =====================================================
# 8. TRANSFORM BRONZE TO SILVER
# =====================================================

def process_incidents(data):

    if not isinstance(data, dict):
        raise ValueError("Cấu trúc Bronze không hợp lệ.")

    # Hỗ trợ file đơn thành phố và file tổng hợp đa thành phố
    if "incidents" in data:
        city_payloads = [data]

    else:
        city_payloads = [
            value
            for value in data.values()
            if isinstance(value, dict) and "incidents" in value
        ]

    all_records = []

    for payload in city_payloads:

        metadata = payload.get("metadata") or {}
        incidents = payload.get("incidents") or []

        if not isinstance(incidents, list):
            raise ValueError("Trường incidents phải là danh sách.")

        if not incidents:
            logger.warning("Không có incident trong payload này.")
            continue

        fetched_at = metadata.get("ingestion_timestamp_utc")

        if not fetched_at:
            logger.warning("Thiếu ingestion_timestamp_utc.")
            continue

        fetched_at_utc = pd.to_datetime(
            fetched_at,
            utc=True,
            errors="coerce"
        )

        if pd.isna(fetched_at_utc):
            logger.warning("Thời gian thu thập không hợp lệ.")
            continue

        fetched_at_vn = fetched_at_utc.tz_convert(TIMEZONE_VN)

        dt_value = fetched_at_vn.strftime("%Y-%m-%d")

        city_id = metadata.get("city_id") or "HAN"
        city_name = metadata.get("city_name") or "Hà Nội"

        for incident in incidents:

            if not isinstance(incident, dict):
                continue

            incident_id = incident.get("incident_id")

            if incident_id is None or str(incident_id).strip() == "":
                continue

            incident_id = str(incident_id).strip()

            delay_seconds = to_integer(
                incident.get("delay_seconds")
            )

            length_m = to_numeric(
                incident.get("length_m")
            )

            incident_type = to_integer(
                incident.get("incident_type")
            )

            magnitude = to_integer(
                incident.get("magnitude")
            )

            if delay_seconds is not None and delay_seconds < 0:
                continue

            type_label = TYPE_MAP.get(
                incident_type,
                "Unknown"
            ) if incident_type is not None else "Unknown"

            delay_minutes = (
                delay_seconds / 60
                if delay_seconds is not None
                else None
            )

            length_km = (
                length_m / 1000
                if length_m is not None
                else None
            )

            longitude, latitude = get_coordinates(incident)

            longitude = to_numeric(longitude)
            latitude = to_numeric(latitude)

            all_records.append({

                "city_id": incident.get("city_id") or city_id,
                "city_name": incident.get("city_name") or city_name,

                "incident_id": incident_id,
                "incident_type": incident_type,
                "type_label": type_label,
                "magnitude": magnitude,

                "delay_seconds": delay_seconds,
                "delay_minutes": delay_minutes,

                "length_m": length_m,
                "length_km": length_km,

                "road_from": incident.get("road_from"),
                "road_to": incident.get("road_to"),
                "description": incident.get("description"),

                "longitude": longitude,
                "latitude": latitude,

                "fetched_at_utc": fetched_at_utc,
                "fetched_at_vn": fetched_at_vn,

                "dt": dt_value
            })

    if not all_records:

        logger.warning("Không có incident hợp lệ sau khi xử lý.")

        return pd.DataFrame(columns=SILVER_COLUMNS)

    df = pd.DataFrame(all_records)

    # =================================================
    # DEDUPLICATION
    # =================================================

    before_dedup = len(df)

    df = df.drop_duplicates(
        subset=[
            "city_id",
            "incident_id",
            "fetched_at_utc"
        ],
        keep="last"
    )

    logger.info(
        "Số bản ghi trùng đã loại bỏ: %s",
        before_dedup - len(df)
    )

    # =================================================
    # DATA TYPE STANDARDIZATION
    # =================================================

    integer_columns = [
        "incident_type",
        "magnitude",
        "delay_seconds"
    ]

    for column in integer_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).astype("Int64")

    double_columns = [
        "delay_minutes",
        "length_m",
        "length_km",
        "longitude",
        "latitude"
    ]

    for column in double_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        ).astype("float64")

    string_columns = [
        "city_id",
        "city_name",
        "incident_id",
        "type_label",
        "road_from",
        "road_to",
        "description",
        "dt"
    ]

    for column in string_columns:
        df[column] = df[column].astype("string")

    df = df[SILVER_COLUMNS]

    logger.info("Số incident hợp lệ: %s", len(df))
    logger.info("Silver columns: %s", list(df.columns))

    return df


# =====================================================
# 9. SAVE SILVER PARQUET
# =====================================================

def save_silver_parquet(df, output_dir=LOCAL_OUTPUT):

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if df.empty:
        logger.warning("Không có dữ liệu. Không tạo Parquet.")
        return []

    generated_files = []

    for dt_value, partition_df in df.groupby("dt", dropna=False):

        if pd.isna(dt_value):
            continue

        partition_dir = output_dir / f"dt={dt_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        file_path = partition_dir / "incidents.parquet"

        # Đọc các file Parquet cũ trong partition
        existing_files = sorted(partition_dir.glob("*.parquet"))

        frames = []

        for existing_file in existing_files:
            if existing_file.is_file():
                frames.append(pd.read_parquet(existing_file))

        frames.append(partition_df)

        merged_df = pd.concat(frames, ignore_index=True)

        merged_df = merged_df.drop_duplicates(
            subset=[
                "city_id",
                "incident_id",
                "fetched_at_utc"
            ],
            keep="last"
        )

        temp_file_path = partition_dir / ".incidents.tmp"

        try:

            merged_df.to_parquet(
                temp_file_path,
                engine="pyarrow",
                compression="snappy",
                index=False
            )

            os.replace(temp_file_path, file_path)

        finally:

            if temp_file_path.exists():
                temp_file_path.unlink()

        # Chỉ xóa file cũ sau khi file mới được ghi thành công
        for existing_file in existing_files:

            if existing_file != file_path:
                existing_file.unlink()

        generated_files.append(file_path)

        logger.info(
            "Đã gộp %s file Parquet vào: %s",
            len(existing_files) + 1,
            file_path
        )

    return generated_files


# =====================================================
# 10. UPLOAD SILVER TO MINIO
# =====================================================

def upload_silver_to_minio(generated_files, output_dir=LOCAL_OUTPUT):

    if not generated_files:
        logger.warning("Không có file Parquet để upload.")
        return

    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_SILVER):

        client.make_bucket(MINIO_BUCKET_SILVER)

        logger.info(
            "Đã tạo bucket: %s",
            MINIO_BUCKET_SILVER
        )

    output_dir = Path(output_dir)

    uploaded_count = 0

    for file_path in generated_files:

        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(
                f"Không tìm thấy file Parquet: {file_path}"
            )

        relative_path = file_path.relative_to(output_dir)

        object_name = (
            "incidents/"
            + relative_path.as_posix()
        )

        # Upload file Parquet tổng hợp
        client.fput_object(
            MINIO_BUCKET_SILVER,
            object_name,
            str(file_path),
            content_type="application/octet-stream"
        )

        # Chỉ dọn file cũ sau khi upload thành công
        partition_prefix = (
            f"incidents/{relative_path.parent.as_posix()}/"
        )

        old_objects = list(
            client.list_objects(
                MINIO_BUCKET_SILVER,
                prefix=partition_prefix,
                recursive=True
            )
        )

        for old_object in old_objects:

            if (
                old_object.object_name != object_name
                and old_object.object_name.endswith(".parquet")
            ):

                client.remove_object(
                    MINIO_BUCKET_SILVER,
                    old_object.object_name
                )

                logger.info(
                    "Đã xóa file Parquet cũ: %s",
                    old_object.object_name
                )

        uploaded_count += 1

        logger.info(
            "Đã upload Silver: %s/%s",
            MINIO_BUCKET_SILVER,
            object_name
        )

    logger.info(
        "Tổng số file Parquet đã upload: %s",
        uploaded_count
    )


# =====================================================
# 11. MAIN PIPELINE
# =====================================================

def run_silver_pipeline(
    input_path=LOCAL_INPUT,
    minio_object=None,
    output_dir=LOCAL_OUTPUT,
    upload=True
):

    logger.info(
        "========== BRONZE TO SILVER INCIDENT =========="
    )

    # 1. Read Bronze

    if minio_object:

        bronze_data = read_minio_json(minio_object)

    else:

        bronze_data = read_local_json(input_path)

    # 2. Transform Bronze to Silver

    silver_df = process_incidents(bronze_data)

    if silver_df.empty:

        logger.warning(
            "Pipeline kết thúc: không có dữ liệu hợp lệ."
        )

        return silver_df

    # 3. Save local Parquet

    generated_files = save_silver_parquet(
        silver_df,
        output_dir
    )

    # 4. Upload to MinIO

    if upload:

        upload_silver_to_minio(
            generated_files,
            output_dir
        )

    logger.info("Bronze to Silver Incident hoàn tất.")
    logger.info("Tổng số dòng Silver: %s", len(silver_df))
    logger.info("Tổng số file Parquet: %s", len(generated_files))

    return silver_df


# =====================================================
# 12. COMMAND LINE
# =====================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Bronze to Silver Incident Pipeline"
    )

    parser.add_argument(
        "--input",
        default=str(LOCAL_INPUT),
        help="Đường dẫn JSON Bronze local"
    )

    parser.add_argument(
        "--minio-object",
        default=None,
        help="Đọc trực tiếp JSON object từ MinIO Bronze"
    )

    parser.add_argument(
        "--output",
        default=str(LOCAL_OUTPUT),
        help="Thư mục lưu Parquet Silver"
    )

    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Chỉ lưu Parquet local, không upload MinIO"
    )

    args = parser.parse_args()

    run_silver_pipeline(
        input_path=args.input,
        minio_object=args.minio_object,
        output_dir=args.output,
        upload=not args.no_upload
    )
