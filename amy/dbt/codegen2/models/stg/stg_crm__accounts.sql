{{ config(materialized='view') }}

WITH source AS (
    SELECT
        ACCOUNT_ID,
        CUSTOMER_ID,
        ACCOUNT_TYPE,
        CREATION_DATE,
        STATUS,
        CURRENCY_CODE,
        BILLING_ADDRESS,
        SHIPPING_ADDRESS,
        ACCOUNT_BALANCE,
        NOTE,
        CREATED_AT,
        UPDATED_AT
    FROM
        {{ source('raw', 'CRM_ACCOUNTS') }}
)

SELECT
    ACCOUNT_ID,
    CUSTOMER_ID,
    ACCOUNT_TYPE,
    CREATION_DATE,
    STATUS,
    CURRENCY_CODE,
    BILLING_ADDRESS,
    SHIPPING_ADDRESS,
    ACCOUNT_BALANCE,
    NOTE,
    CREATED_AT,
    UPDATED_AT
FROM
    source