{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'NWK_USAGE') }}
),

renamed AS (
    SELECT
        USAGE_ID,
        ASSIGNMENT_ID,
        USAGE_DATE,
        USAGE_TYPE,
        DATA_CONSUMED,
        USAGE_UNIT,
        USAGE_PEAK,
        USAGE_COST,
        CREATED_AT,
        UPDATED_AT
    FROM
        source
)

SELECT
    *
FROM
    renamed