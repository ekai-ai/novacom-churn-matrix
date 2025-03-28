{{ config(materialized='view') }}

with source as (

    select
        "INVOICE_ID",
        "CUSTOMER_ID",
        "INVOICE_DATE",
        "DUE_DATE",
        "TOTAL_AMOUNT",
        "TAX_AMOUNT",
        "DISCOUNT_AMOUNT",
        "CURRENCY_CODE",
        "STATUS",
        "NOTE",
        "CREATED_AT",
        "UPDATED_AT"
    from {{ source('raw', 'BIL_INVOICES') }}

),

final as (

    select
        "INVOICE_ID",
        "CUSTOMER_ID",
        "INVOICE_DATE",
        "DUE_DATE",
        "TOTAL_AMOUNT",
        "TAX_AMOUNT",
        "DISCOUNT_AMOUNT",
        "CURRENCY_CODE",
        "STATUS",
        "NOTE",
        "CREATED_AT",
        "UPDATED_AT"
    from source

)

select * from final
