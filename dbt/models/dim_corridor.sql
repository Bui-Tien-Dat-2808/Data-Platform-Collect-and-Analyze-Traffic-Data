{{ config(materialized='table') }}

SELECT DISTINCT
    corridor_id,
    corridor_name,
    district,
    CURRENT_TIMESTAMP AS created_at
FROM {{ source('silver_traffic', 'stg_traffic_flow') }}

