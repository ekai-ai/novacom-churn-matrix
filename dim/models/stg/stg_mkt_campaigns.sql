{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'MKT_CAMPAIGNS') }}
),

renamed AS (
    SELECT
        CAMPAIGN_ID,
        NAME,
        CAMPAIGN_TYPE,
        OBJECTIVE,
        TARGET_AUDIENCE,
        CONVERSION_GOAL,
        START_DATE,
        END_DATE,
        BUDGET,
        CREATED_AT,
        UPDATED_AT
    FROM
        source
)

SELECT
    *
FROM
    renamed