{{ config(materialized='table', schema='analytics') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref('stg_nwk_outages') }}
),

renamed AS (
    SELECT
        OUTAGE_ID,
        ASSIGNMENT_ID,
        START_TIME,
        END_TIME,
        OUTAGE_TYPE,
        REGION,
        IMPACTED_CUSTOMERS_COUNT,
        CAUSE,
        RESOLUTION
    FROM
        source
    WHERE
        OUTAGE_ID IS NOT NULL
        AND IMPACTED_CUSTOMERS_COUNT >= 0
        AND OUTAGE_TYPE IN ('Planned', 'Unplanned')
)

SELECT
    OUTAGE_ID,
    ASSIGNMENT_ID,
    CAST(START_TIME as datetime) as START_TIME_KEY,
    CAST(END_TIME as datetime) as END_TIME_KEY,
    OUTAGE_TYPE,
    REGION,
    IMPACTED_CUSTOMERS_COUNT,
    CAUSE,
    RESOLUTION
FROM
    renamed