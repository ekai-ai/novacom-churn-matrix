{{ config(materialized='table') }}

WITH source_data AS (
    SELECT DISTINCT
        PRIORITY
    FROM {{ ref('stg_sup__tickets') }}
    WHERE PRIORITY IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['standardized_priority']) }} AS TicketPrioritySK,
    standardized_priority AS TicketPriorityName
FROM (
    SELECT
        UPPER(TRIM(PRIORITY)) AS standardized_priority
    FROM source_data
) AS standardized_data