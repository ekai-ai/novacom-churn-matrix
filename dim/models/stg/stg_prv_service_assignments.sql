{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'PRV_SERVICE_ASSIGNMENTS') }}
),

renamed AS (
    SELECT
        ASSIGNMENT_ID,
        SERVICE_ID,
        CUSTOMER_ID,
        START_DATE,
        END_DATE,
        STATUS,
        PROVISIONING_STATUS,
        LAST_MODIFIED_DATE,
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