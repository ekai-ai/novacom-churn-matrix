{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'CRM_CUSTOMERS') }}
),

renamed AS (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME,
        MIDDLE_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        DATE_OF_BIRTH,
        ADDRESS_LINE1,
        ADDRESS_LINE2,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY,
        CUSTOMER_SEGMENT,
        CUSTOMER_TIER,
        START_DATE,
        STATUS,
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