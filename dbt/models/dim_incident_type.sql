{{ config(
    materialized='table'
) }}

WITH incident_types AS (
    SELECT 1 AS incident_type, 'Accident' AS type_label, 'Hazard' AS type_group
    UNION ALL SELECT 2, 'Fog', 'Weather'
    UNION ALL SELECT 3, 'Dangerous Conditions', 'Hazard'
    UNION ALL SELECT 4, 'Rain', 'Weather'
    UNION ALL SELECT 5, 'Ice', 'Weather'
    UNION ALL SELECT 6, 'Traffic Jam', 'Congestion'
    UNION ALL SELECT 7, 'Lane Closed', 'Roadwork'
    UNION ALL SELECT 8, 'Road Closed', 'Roadwork'
    UNION ALL SELECT 9, 'Road Works', 'Roadwork'
    UNION ALL SELECT 10, 'Wind', 'Weather'
    UNION ALL SELECT 11, 'Flooding', 'Weather'
    UNION ALL SELECT 14, 'Broken Down Vehicle', 'Hazard'
)

SELECT * FROM incident_types