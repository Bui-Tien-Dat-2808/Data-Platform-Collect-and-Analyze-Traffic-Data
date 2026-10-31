{{ config(
    materialized='incremental',
    unique_key='fact_id'
) }}

WITH source_data AS (
    SELECT
        MD5(CONCAT(point_id, '_', timestamp_vn)) AS fact_id,
        point_id,
        corridor_id,
        timestamp_vn,
        CAST(timestamp_vn AS DATE) AS date_key,
        EXTRACT(HOUR FROM timestamp_vn) AS hour_key,
        EXTRACT(DOW FROM timestamp_vn) AS day_of_week,
        current_speed_kmh,
        free_flow_speed_kmh,
        current_travel_time_sec,
        free_flow_travel_time_sec,
        speed_reduction_ratio_pct,
        travel_delay_seconds,
        confidence,
        congestion_level
    FROM {{ source('silver_traffic', 'stg_traffic_flow') }}
)

SELECT * FROM source_data
{% if is_incremental() %}
    WHERE timestamp_vn > (SELECT MAX(timestamp_vn) FROM {{ this }})
{% endif %}

