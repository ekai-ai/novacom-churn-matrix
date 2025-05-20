{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'BIL_INVOICES') }}
),

renamed AS (
    SELECT
        invoice_id,
        customer_id,
        invoice_date,
        due_date,
        total_amount,
        tax_amount,
        discount_amount,
        currency_code,
        status,
        note,
        created_at,
        updated_at
    FROM
        source
)

SELECT
    *
FROM
    renamed