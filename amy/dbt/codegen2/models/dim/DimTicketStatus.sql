{{ config(materialized='table') }}

WITH source_data AS (
    SELECT DISTINCT
        STATUS
    FROM
        {{ ref('stg_sup__tickets') }}
),

standardized_status AS (
    SELECT
        COALESCE(UPPER(TRIM(STATUS)), 'UNKNOWN') AS TicketStatusName
    FROM
        source_data
)

SELECT
    -- Generate surrogate key based on the standardized status name
    {{ dbt_utils.generate_surrogate_key(['TicketStatusName']) }} AS TicketStatusSK,
    TicketStatusName
FROM
    standardized_status