{{ config(
    materialized='table'
) }}

SELECT
    COALESCE(city_id, 'HAN') AS city_id,
    COALESCE(city_name, 'Hà Nội') AS city_name,
    date_key,
    hour_key,
    EXTRACT(DOW FROM fetched_at_vn) AS day_of_week,
    incident_type,
    type_label,
    COUNT(DISTINCT incident_id) AS total_incidents,
    ROUND(AVG(delay_minutes)::NUMERIC, 2) AS avg_delay_minutes,
    ROUND(SUM(delay_minutes)::NUMERIC, 2) AS total_delay_minutes,
    ROUND(AVG(length_km)::NUMERIC, 2) AS avg_length_km,
    ROUND(SUM(length_km)::NUMERIC, 2) AS total_length_km
FROM {{ ref('fact_traffic_incidents') }}
GROUP BY 
    COALESCE(city_id, 'HAN'),
    COALESCE(city_name, 'Hà Nội'),
    date_key,
    hour_key,
    EXTRACT(DOW FROM fetched_at_vn),
    incident_type,
    type_label
