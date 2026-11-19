
{{ config(
    materialized='incremental',
    unique_key=['city_id', 'date_key', 'hour_key', 'incident_type'],
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT
        COALESCE(city_id, 'HAN') AS city_id,
        COALESCE(city_name, 'Hà Nội') AS city_name,
        date_key,
        hour_key,
        incident_id,
        incident_type,
        type_label,
        delay_minutes,
        length_km,
        fetched_at_vn
    FROM {{ ref('fact_traffic_incidents') }}

    {% if is_incremental() %}
    WHERE date_key >= (
        SELECT COALESCE(
            MAX(date_key),
            DATE '1900-01-01'
        ) - 3
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    city_id,
    city_name,
    date_key,
    hour_key,
    EXTRACT(DOW FROM fetched_at_vn)::INT AS day_of_week,
    incident_type,
    type_label,

    COUNT(DISTINCT incident_id) AS total_incidents,
    ROUND(AVG(delay_minutes)::NUMERIC, 2) AS avg_delay_minutes,
    ROUND(SUM(delay_minutes)::NUMERIC, 2) AS total_delay_minutes,
    ROUND(AVG(length_km)::NUMERIC, 2) AS avg_length_km,
    ROUND(SUM(length_km)::NUMERIC, 2) AS total_length_km

FROM source_data

GROUP BY
    city_id,
    city_name,
    date_key,
    hour_key,
    EXTRACT(DOW FROM fetched_at_vn),
    incident_type,
    type_label
