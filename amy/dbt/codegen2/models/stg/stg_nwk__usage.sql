{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        "USAGE_ID",
        "ASSIGNMENT_ID",
        "USAGE_DATE",
        "USAGE_TYPE",
        "DATA_CONSUMED",
        "USAGE_UNIT",
        "USAGE_PEAK",
        "USAGE_COST",
        "CREATED_AT",
        "UPDATED_AT"
    FROM {{ source('raw', 'NWK_USAGE') }}
)

SELECT *
FROM source_data