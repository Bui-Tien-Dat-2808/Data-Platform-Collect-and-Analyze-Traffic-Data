{{ config(
    materialized='table'
) }}

WITH cities AS (
    SELECT 'HAN' AS city_id, 'Hà Nội' AS city_name, 21.0285 AS center_latitude, 105.8542 AS center_longitude, 12 AS default_zoom
    UNION ALL
    SELECT 'SGN' AS city_id, 'TP. Hồ Chí Minh' AS city_name, 10.7769 AS center_latitude, 106.7009 AS center_longitude, 12 AS default_zoom
    UNION ALL
    SELECT 'DAD' AS city_id, 'Đà Nẵng' AS city_name, 16.0544 AS center_latitude, 108.2022 AS center_longitude, 13 AS default_zoom
)

SELECT * FROM cities
