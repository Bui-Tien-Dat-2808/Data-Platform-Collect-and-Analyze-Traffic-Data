{{ config(
    materialized='incremental',
    unique_key='fact_id'
) }}

WITH source_data AS (
    SELECT
        MD5(CONCAT(incident_id, '_', fetched_at_utc::TEXT)) AS fact_id,
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
        EXTRACT(HOUR FROM fetched_at_vn) AS hour_key,
        dt
    FROM {{ source('silver_traffic', 'stg_traffic_incidents') }}
)

SELECT *
FROM source_data
{% if is_incremental() %}
    WHERE fetched_at_utc > (SELECT MAX(fetched_at_utc) FROM {{ this }})
{% endif %}