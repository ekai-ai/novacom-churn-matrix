{{ config(materialized='view') }}

WITH source AS (
    SELECT
        ITEM_ID,
        INVOICE_ID,
        ITEM_CODE,
        DESCRIPTION,
        QUANTITY,
        UNIT_PRICE,
        TAX_RATE,
        DISCOUNT_RATE,
        LINE_TOTAL,
        CREATED_AT,
        UPDATED_AT
    FROM
        {{ source('raw', 'BIL_INVOICE_ITEMS') }}
)

SELECT
    ITEM_ID,
    INVOICE_ID,
    ITEM_CODE,
    DESCRIPTION,
    QUANTITY,
    UNIT_PRICE,
    TAX_RATE,
    DISCOUNT_RATE,
    LINE_TOTAL,
    CREATED_AT,
    UPDATED_AT
FROM
    source
