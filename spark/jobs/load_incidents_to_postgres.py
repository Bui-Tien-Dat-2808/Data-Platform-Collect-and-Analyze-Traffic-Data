
"""
Load Silver Incident Parquet into PostgreSQL Staging Table.

Pipeline:
Silver Parquet -> PostgreSQL staging table

Features:
- Read all Parquet files from data/silver_incidents/
- Create staging table if it does not exist
- Preserve historical incident snapshots
- Prevent duplicates using (incident_id, fetched_at_utc)
- Normalize Pandas / NumPy values before PostgreSQL insertion
- Track actual inserted records
"""

import os
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

SILVER_DIR = PROJECT_ROOT / "data" / "silver_incidents"

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "traffic_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

TABLE_NAME = "stg_traffic_incidents"


# --------------------------------------------------
# 2. LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("LoadIncidentsToPostgres")


# --------------------------------------------------
# 3. READ SILVER PARQUET
# --------------------------------------------------

def read_silver_parquet(parquet_dir: Path) -> pd.DataFrame:

    logger.info(f"Reading Silver Parquet from: {parquet_dir}")

    parquet_files = sorted(parquet_dir.rglob("*.parquet"))

    if not parquet_files:
        logger.warning("No Parquet files found.")
        return pd.DataFrame()

    dataframes = []

    for file_path in parquet_files:

        logger.info(f"Reading: {file_path}")

        df = pd.read_parquet(file_path)

        dataframes.append(df)

    result = pd.concat(
        dataframes,
        ignore_index=True
    )

    logger.info(f"Total records read: {len(result)}")

    return result


# --------------------------------------------------
# 4. CREATE POSTGRESQL STAGING TABLE
# --------------------------------------------------

def create_staging_table(cursor):

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

            incident_id VARCHAR(100) NOT NULL,
            incident_type INTEGER,
            type_label VARCHAR(100),
            magnitude INTEGER,

            delay_seconds INTEGER,
            delay_minutes DOUBLE PRECISION,

            length_m DOUBLE PRECISION,
            length_km DOUBLE PRECISION,

            road_from TEXT,
            road_to TEXT,
            description TEXT,

            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,

            fetched_at_utc TIMESTAMPTZ NOT NULL,
            fetched_at_vn TIMESTAMPTZ,

            dt DATE,

            CONSTRAINT uq_traffic_incident_snapshot
            UNIQUE (incident_id, fetched_at_utc)
        );
    """

    cursor.execute(create_table_sql)

    logger.info(f"PostgreSQL table {TABLE_NAME} is ready.")


# --------------------------------------------------
# 5. PREPARE DATA
# --------------------------------------------------

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:

    required_columns = [
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

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    df = df[required_columns].copy()

    # Normalize incident ID
    df["incident_id"] = (
        df["incident_id"]
        .astype("string")
        .str.strip()
    )

    df["incident_id"] = df["incident_id"].replace("", pd.NA)

    # Normalize UTC timestamp
    df["fetched_at_utc"] = pd.to_datetime(
        df["fetched_at_utc"],
        utc=True,
        errors="coerce"
    )

    # Normalize Vietnam timestamp
    df["fetched_at_vn"] = pd.to_datetime(
        df["fetched_at_vn"],
        errors="coerce"
    )

    if df["fetched_at_vn"].dt.tz is None:
        df["fetched_at_vn"] = (
            df["fetched_at_vn"]
            .dt.tz_localize("Asia/Ho_Chi_Minh")
        )
    else:
        df["fetched_at_vn"] = (
            df["fetched_at_vn"]
            .dt.tz_convert("Asia/Ho_Chi_Minh")
        )

    # Remove records without required identifiers
    before = len(df)

    df = df.dropna(
        subset=["incident_id", "fetched_at_utc"]
    )

    logger.info(
        f"Removed {before - len(df)} records with invalid identifiers/timestamps."
    )

    # Normalize date
    df["dt"] = pd.to_datetime(
        df["dt"],
        errors="coerce"
    ).dt.date

    # Remove duplicates inside current batch
    before = len(df)

    df = df.drop_duplicates(
        subset=["incident_id", "fetched_at_utc"],
        keep="last"
    )

    logger.info(
        f"Removed {before - len(df)} duplicate records in batch."
    )

    logger.info(
        f"Valid incident records: {len(df)}"
    )

    return df


# --------------------------------------------------
# 6. CONVERT PANDAS / NUMPY VALUES
# --------------------------------------------------

def convert_numpy_types(value):

    # Convert NaN, NaT, pd.NA to SQL NULL
    if pd.isna(value):
        return None

    # Convert Pandas Timestamp to Python datetime
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    # Convert NumPy scalar to Python native type
    if isinstance(value, np.generic):
        return value.item()

    return value


# --------------------------------------------------
# 7. INSERT DATA INTO POSTGRESQL
# --------------------------------------------------

def load_to_postgres(df: pd.DataFrame):

    if df.empty:
        logger.warning("No valid incident records to load.")
        return

    columns = [
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

    # Convert DataFrame rows into PostgreSQL-compatible tuples
    values = [
        tuple(
            convert_numpy_types(value)
            for value in row
        )
        for row in df[columns].itertuples(
            index=False,
            name=None
        )
    ]

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            {", ".join(columns)}
        )
        VALUES %s

        ON CONFLICT (incident_id, fetched_at_utc)
        DO NOTHING

        RETURNING incident_id;
    """

    connection = None

    try:

        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        with connection:

            with connection.cursor() as cursor:

                create_staging_table(cursor)

                inserted_rows = execute_values(
                    cursor,
                    insert_sql,
                    values,
                    page_size=1000,
                    fetch=True
                )

                inserted_count = len(inserted_rows)

                skipped_count = (
                    len(values) - inserted_count
                )

        logger.info("-----------------------------------")
        logger.info("POSTGRESQL LOAD SUMMARY")
        logger.info("-----------------------------------")

        logger.info(f"Records submitted: {len(values)}")
        logger.info(f"Records inserted: {inserted_count}")
        logger.info(f"Records skipped: {skipped_count}")

        logger.info(f"Target table: {TABLE_NAME}")

    except Exception:

        logger.exception(
            "Failed to load incidents into PostgreSQL."
        )

        raise

    finally:

        if connection is not None:
            connection.close()


# --------------------------------------------------
# 8. MAIN PIPELINE
# --------------------------------------------------

def main():

    logger.info(
        "Starting Silver Incident -> PostgreSQL pipeline."
    )

    if not SILVER_DIR.exists():

        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_DIR}"
        )

    # Step 1: Read Silver Parquet
    df = read_silver_parquet(SILVER_DIR)

    if df.empty:

        logger.warning(
            "Pipeline stopped: no Silver data."
        )

        return

    # Step 2: Prepare data
    df = prepare_data(df)

    if df.empty:

        logger.warning(
            "Pipeline stopped: no valid incident records."
        )

        return

    # Step 3: Load PostgreSQL
    load_to_postgres(df)

    logger.info(
        "Silver Incident -> PostgreSQL completed."
    )


if __name__ == "__main__":
    main()
