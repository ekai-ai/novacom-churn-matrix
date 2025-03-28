{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        OUTAGE_ID,
        ASSIGNMENT_ID,
        START_TIME,
        END_TIME,
        OUTAGE_TYPE,
        REGION,
        IMPACTED_CUSTOMERS_COUNT,
        CAUSE,
        RESOLUTION,
        CREATED_AT,
        UPDATED_AT
    FROM
        {{ source('raw', 'NWK_OUTAGES') }}
)

SELECT
    OUTAGE_ID,
    ASSIGNMENT_ID,
    START_TIME,
    END_TIME,
    OUTAGE_TYPE,
    REGION,
    IMPACTED_CUSTOMERS_COUNT,
    CAUSE,
    RESOLUTION,
    CREATED_AT,
    UPDATED_AT
FROM
    source