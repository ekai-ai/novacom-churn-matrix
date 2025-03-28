{{ config(materialized='view') }}

WITH source AS (
    SELECT
        TARGET_ID,
        CAMPAIGN_ID,
        CUSTOMER_ID,
        ASSIGNED_DATE,
        STATUS,
        LAST_CONTACT_DATE,
        CHANNEL,
        RESPONSE_FLAG,
        CREATED_AT,
        UPDATED_AT
    FROM {{ source('raw', 'MKT_CAMPAIGN_TARGETS') }}
)

SELECT
    TARGET_ID,
    CAMPAIGN_ID,
    CUSTOMER_ID,
    ASSIGNED_DATE,
    STATUS,
    LAST_CONTACT_DATE,
    CHANNEL,
    RESPONSE_FLAG,
    CREATED_AT,
    UPDATED_AT
FROM source