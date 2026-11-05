{{ config(
    materialized='table'
) }}

SELECT
    date_key,
    hour_key,
    incident_type,
    type_label,
    COUNT(DISTINCT incident_id) AS total_incidents,
    ROUND(AVG(delay_minutes)::NUMERIC, 2) AS avg_delay_minutes,
    ROUND(SUM(delay_minutes)::NUMERIC, 2) AS total_delay_minutes,
    ROUND(AVG(length_km)::NUMERIC, 2) AS avg_length_km,
    ROUND(SUM(length_km)::NUMERIC, 2) AS total_length_km
FROM {{ ref('fact_traffic_incidents') }}
GROUP BY 
    date_key,
    hour_key,
    incident_type,
    type_label