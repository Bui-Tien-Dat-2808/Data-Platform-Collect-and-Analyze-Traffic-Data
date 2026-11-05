
import os
import json
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

# Hỗ trợ cả endpoint có và không có http:// hoặc https://
if "://" in MINIO_ENDPOINT_RAW:
    parsed_endpoint = urlparse(MINIO_ENDPOINT_RAW)
    MINIO_ENDPOINT = parsed_endpoint.netloc
    MINIO_SECURE = parsed_endpoint.scheme == "https"
else:
    MINIO_ENDPOINT = MINIO_ENDPOINT_RAW
    MINIO_SECURE = False

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin"
)

MINIO_BUCKET_BRONZE = os.getenv(
    "MINIO_BUCKET_BRONZE",
    "lakehouse-bronze"
)

MINIO_BUCKET_SILVER = os.getenv(
    "MINIO_BUCKET_SILVER",
    "lakehouse-silver"
)

LOCAL_INPUT = (
    PROJECT_ROOT
    / "collector"
    / "sample_incidents.json"
)

LOCAL_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "silver_incidents"
)

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


# Thứ tự và tên 16 trường trong Silver
SILVER_COLUMNS = [
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
# 3. LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =====================================================
# 4. MINIO CLIENT
# =====================================================

def get_minio_client():

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


# =====================================================
# 5. READ BRONZE JSON
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
        raise ValueError(
            "Cấu trúc Bronze không hợp lệ: dữ liệu gốc phải là JSON object."
        )

    logger.info(
        "Đã đọc Bronze local: %s",
        input_path
    )

    return data


def read_minio_json(object_name):

    client = get_minio_client()

    response = client.get_object(
        MINIO_BUCKET_BRONZE,
        object_name
    )

    try:
        raw_data = response.read()

        data = json.loads(
            raw_data.decode("utf-8")
        )

    finally:
        response.close()
        response.release_conn()

    if not isinstance(data, dict):
        raise ValueError(
            "Cấu trúc Bronze trên MinIO không phải JSON object."
        )

    logger.info(
        "Đã đọc Bronze từ MinIO: %s/%s",
        MINIO_BUCKET_BRONZE,
        object_name
    )

    return data


# =====================================================
# 6. DATA CONVERSION HELPERS
# =====================================================

def to_numeric(value):

    if value is None:
        return None

    result = pd.to_numeric(
        value,
        errors="coerce"
    )

    if pd.isna(result):
        return None

    return float(result)


def to_integer(value):

    numeric_value = to_numeric(value)

    if numeric_value is None:
        return None

    return int(numeric_value)


def get_coordinates(incident):

    # Ưu tiên tọa độ đã được chuẩn hóa từ Collector.
    longitude = incident.get("longitude")
    latitude = incident.get("latitude")

    if longitude is not None and latitude is not None:
        return longitude, latitude

    # Nếu chưa có, đọc từ geometry gốc.
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
            "Không thể đọc tọa độ của incident: %s",
            incident.get("incident_id")
        )

    return None, None


# =====================================================
# 7. TRANSFORM BRONZE TO SILVER
# =====================================================

def process_incidents(data):

    metadata = data.get("metadata") or {}

    incidents = data.get("incidents") or []

    if not isinstance(incidents, list):
        raise ValueError(
            "Trường incidents phải là một danh sách."
        )

    if not incidents:
        logger.warning(
            "Bronze không có incident nào để xử lý."
        )

        return pd.DataFrame(columns=SILVER_COLUMNS)

    fetched_at = metadata.get(
        "ingestion_timestamp_utc"
    )

    if not fetched_at:
        raise ValueError(
            "Thiếu ingestion_timestamp_utc trong metadata."
        )

    # Chuyển thời gian thu thập sang UTC.
    fetched_at_utc = pd.to_datetime(
        fetched_at,
        utc=True,
        errors="coerce"
    )

    if pd.isna(fetched_at_utc):
        raise ValueError(
            "ingestion_timestamp_utc không hợp lệ."
        )

    # Chuyển UTC sang giờ Việt Nam.
    fetched_at_vn = fetched_at_utc.tz_convert(
        TIMEZONE_VN
    )

    # Ngày partition lấy theo giờ Việt Nam.
    dt_value = fetched_at_vn.strftime("%Y-%m-%d")

    records = []

    for incident in incidents:

        if not isinstance(incident, dict):
            continue

        # -------------------------------------------------
        # 1. Validate incident_id
        # -------------------------------------------------

        incident_id = incident.get("incident_id")

        if (
            incident_id is None
            or str(incident_id).strip() == ""
        ):
            continue

        incident_id = str(incident_id).strip()

        # -------------------------------------------------
        # 2. Convert numeric fields
        # -------------------------------------------------

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

        # -------------------------------------------------
        # 3. Filter invalid delay
        # -------------------------------------------------

        if (
            delay_seconds is not None
            and delay_seconds < 0
        ):
            continue

        # -------------------------------------------------
        # 4. Map incident type
        # -------------------------------------------------

        if incident_type is not None:
            type_label = TYPE_MAP.get(
                incident_type,
                "Unknown"
            )
        else:
            type_label = "Unknown"

        # -------------------------------------------------
        # 5. Calculate delay_minutes and length_km
        # -------------------------------------------------

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

        # -------------------------------------------------
        # 6. Extract coordinates
        # -------------------------------------------------

        longitude, latitude = get_coordinates(
            incident
        )

        longitude = to_numeric(longitude)
        latitude = to_numeric(latitude)

        # -------------------------------------------------
        # 7. Build Silver record
        # -------------------------------------------------

        records.append({

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

    if not records:

        logger.warning(
            "Không có incident hợp lệ sau khi xử lý."
        )

        return pd.DataFrame(columns=SILVER_COLUMNS)

    df = pd.DataFrame(records)

    # =================================================
    # 8. DEDUPLICATION
    # =================================================

    before_dedup = len(df)

    df = df.drop_duplicates(
        subset=[
            "incident_id",
            "fetched_at_utc"
        ],
        keep="last"
    )

    after_dedup = len(df)

    logger.info(
        "Số bản ghi trùng lặp đã loại bỏ: %s",
        before_dedup - after_dedup
    )

    # =================================================
    # 9. DATA TYPE STANDARDIZATION
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
        "incident_id",
        "type_label",
        "road_from",
        "road_to",
        "description",
        "dt"
    ]

    for column in string_columns:

        df[column] = df[column].astype("string")

    # Đảm bảo đúng thứ tự 16 trường.
    df = df[SILVER_COLUMNS]

    logger.info(
        "Số incident hợp lệ sau xử lý: %s",
        len(df)
    )

    logger.info(
        "Silver columns: %s",
        list(df.columns)
    )

    return df


# =====================================================
# 8. SAVE SILVER PARQUET
# =====================================================

def save_silver_parquet(
    df,
    output_dir=LOCAL_OUTPUT
):

    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    if df.empty:

        logger.warning(
            "Không có dữ liệu. Không tạo Parquet."
        )

        return []

    batch_timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S_%f")

    generated_files = []

    # Ghi Parquet theo partition dt.
    for dt_value, partition_df in df.groupby(
        "dt",
        dropna=False
    ):

        if pd.isna(dt_value):
            continue

        partition_dir = (
            output_dir
            / f"dt={dt_value}"
        )

        partition_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        file_path = (
            partition_dir
            / f"incidents_{batch_timestamp}.parquet"
        )

        partition_df.to_parquet(
            file_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        generated_files.append(file_path)

        logger.info(
            "Đã tạo Parquet: %s",
            file_path
        )

    return generated_files


# =====================================================
# 9. UPLOAD SILVER TO MINIO
# =====================================================

def upload_silver_to_minio(
    generated_files,
    output_dir=LOCAL_OUTPUT
):

    if not generated_files:

        logger.warning(
            "Không có file Parquet để upload."
        )

        return

    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_SILVER):

        client.make_bucket(
            MINIO_BUCKET_SILVER
        )

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

        relative_path = file_path.relative_to(
            output_dir
        )

        object_name = (
            "incidents/"
            + relative_path.as_posix()
        )

        client.fput_object(
            MINIO_BUCKET_SILVER,
            object_name,
            str(file_path),
            content_type="application/octet-stream"
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
# 10. MAIN PIPELINE
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

    # -------------------------------------------------
    # 1. Read Bronze
    # -------------------------------------------------

    if minio_object:

        bronze_data = read_minio_json(
            minio_object
        )

    else:

        bronze_data = read_local_json(
            input_path
        )

    # -------------------------------------------------
    # 2. Transform Bronze to Silver
    # -------------------------------------------------

    silver_df = process_incidents(
        bronze_data
    )

    if silver_df.empty:

        logger.warning(
            "Pipeline kết thúc: không có dữ liệu hợp lệ."
        )

        return silver_df

    # -------------------------------------------------
    # 3. Save local Parquet
    # -------------------------------------------------

    generated_files = save_silver_parquet(
        silver_df,
        output_dir
    )

    # -------------------------------------------------
    # 4. Upload to MinIO
    # -------------------------------------------------

    if upload:

        upload_silver_to_minio(
            generated_files,
            output_dir
        )

    logger.info(
        "Bronze to Silver Incident hoàn tất."
    )

    logger.info(
        "Tổng số dòng Silver: %s",
        len(silver_df)
    )

    logger.info(
        "Tổng số file Parquet: %s",
        len(generated_files)
    )

    return silver_df


# =====================================================
# 11. COMMAND LINE
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
