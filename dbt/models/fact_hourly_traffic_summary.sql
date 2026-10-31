{{ config(materialized='table') }}

SELECT
    corridor_id,
    point_id,
    date_key,
    hour_key,
    COUNT(*) AS total_samples,
    ROUND(AVG(current_speed_kmh)::numeric, 2) AS avg_current_speed_kmh,
    ROUND(AVG(free_flow_speed_kmh)::numeric, 2) AS avg_free_flow_speed_kmh,
    ROUND(AVG(speed_reduction_ratio_pct)::numeric, 2) AS avg_speed_reduction_pct,
    ROUND(AVG(travel_delay_seconds)::numeric, 2) AS avg_delay_seconds,
    CASE 
        WHEN AVG(speed_reduction_ratio_pct) < 20 THEN 'SMOOTH'
        WHEN AVG(speed_reduction_ratio_pct) < 50 THEN 'SLIGHT_DELAY'
        ELSE 'SEVERE_CONGESTION'
    END AS hourly_congestion_status
FROM {{ ref('fact_traffic_flow_15min') }}
GROUP BY
    corridor_id,
    point_id,
    date_key,
    hour_key

