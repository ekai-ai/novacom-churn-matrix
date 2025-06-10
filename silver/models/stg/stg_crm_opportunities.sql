{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'CRM_OPPORTUNITIES') }}
),

renamed AS (
    SELECT
        OPPORTUNITY_ID,
        ACCOUNT_ID,
        STAGE,
        PIPELINE_STAGE,
        AMOUNT,
        PROBABILITY_OF_CLOSE,
        CLOSE_DATE,
        ASSIGNED_SALES_REP,
        NEXT_STEP,
        NOTE,
        CREATED_AT,
        UPDATED_AT
    FROM
        source
)

SELECT
    *
FROM
    renamed