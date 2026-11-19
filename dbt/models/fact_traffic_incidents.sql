
{{ config(
    materialized='incremental',
    unique_key='fact_id',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        MD5(
            CONCAT(
                COALESCE(city_id, 'HAN'),
                '_',
                incident_id,
                '_',
                fetched_at_utc::TEXT
            )
        ) AS fact_id,

        COALESCE(city_id, 'HAN') AS city_id,
        COALESCE(city_name, 'Hà Nội') AS city_name,

        incident_id,
        incident_type,
        type_label,
        magnitude,
        delay_seconds,
        delay_minutes,
        length_m,
        length_km,
        road_from,
        road_to,
        description,
        longitude,
        latitude,
        fetched_at_utc,
        fetched_at_vn,

        CAST(fetched_at_vn AS DATE) AS date_key,
        EXTRACT(HOUR FROM fetched_at_vn)::INT AS hour_key,

        dt

    FROM {{ source('silver_traffic', 'stg_traffic_incidents') }}

    {% if is_incremental() %}
    WHERE fetched_at_utc >= (
        SELECT COALESCE(
            MAX(fetched_at_utc),
            TIMESTAMPTZ '1900-01-01 00:00:00+00'
        ) - INTERVAL '1 day'
        FROM {{ this }}
    )
    {% endif %}
)

SELECT *
FROM source_data
