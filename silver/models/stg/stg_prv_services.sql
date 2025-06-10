{{ config(materialized='table', schema='staging') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'PRV_SERVICES') }}
),
renamed AS (
    SELECT
        SERVICE_ID,
        SERVICE_NAME,
        SERVICE_TYPE,
        DESCRIPTION,
        MONTHLY_COST,
        ACTIVE_FLAG,
        PROVISIONING_TEAM,
        CREATED_DATE,
        UPDATED_AT
    FROM
        source
)
SELECT
    *
FROM
    renamed
