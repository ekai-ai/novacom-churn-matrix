{{ config(materialized='view', alias='stg_crm_accounts', schema='staging') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'CRM_ACCOUNTS') }}
),

renamed AS (
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
)

SELECT
    *
FROM
    renamed