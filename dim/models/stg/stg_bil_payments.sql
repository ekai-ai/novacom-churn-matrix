{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'BIL_PAYMENTS') }}
),

renamed AS (
    SELECT
        PAYMENT_ID,
        INVOICE_ID,
        PAYMENT_DATE,
        AMOUNT,
        PAYMENT_METHOD,
        TRANSACTION_ID,
        CURRENCY_CODE,
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