{{ config(materialized='table') }}

with inv as (
    select
        INVOICE_ID,
        CUSTOMER_ID,
        invoice_date as INVOICE_DATE_KEY,
        due_date as DUE_DATE_KEY,
        round(total_amount, 2) as TOTAL_AMOUNT,
        round(tax_amount, 2) as TAX_AMOUNT,
        discount_amount as DISCOUNT_AMOUNT,
        currency_code as CURRENCY_CODE,
        status as STATUS
    from {{ ref('stg_bil_invoices') }}
    where INVOICE_ID is not null
      and total_amount >= 0
      and tax_amount >= 0
      and currency_code regexp '^[A-Z]{3}$'
      and status in ('Issued', 'Paid', 'Overdue', 'Cancelled')
),

items as (
    select
        INVOICE_ID,
        ITEM_CODE,
        QUANTITY,
        UNIT_PRICE,
        TAX_RATE,
        DISCOUNT_RATE,
        LINE_TOTAL
    from {{ ref('stg_bil_invoice_items') }}
    where INVOICE_ID is not null
      and QUANTITY >= 0
      and UNIT_PRICE >= 0
      and TAX_RATE >= 0
      and DISCOUNT_RATE >= 0
      and LINE_TOTAL >= 0
),

payments as (
    select
        INVOICE_ID,
        min(PAYMENT_DATE) as PAYMENT_DATE_KEY,
        sum(AMOUNT) as PAYMENT_AMOUNT,
        max(PAYMENT_METHOD) as PAYMENT_METHOD,
        max(TRANSACTION_ID) as TRANSACTION_ID
    from {{ ref('stg_bil_payments') }}
    group by INVOICE_ID
)

select
    inv.INVOICE_ID,
    inv.CUSTOMER_ID,
    inv.INVOICE_DATE_KEY,
    inv.DUE_DATE_KEY,
    inv.TOTAL_AMOUNT,
    inv.TAX_AMOUNT,
    inv.DISCOUNT_AMOUNT,
    coalesce(pay.PAYMENT_AMOUNT, 0) as PAYMENT_AMOUNT,
    inv.CURRENCY_CODE,
    inv.STATUS,
    items.ITEM_CODE,
    items.QUANTITY,
    items.UNIT_PRICE,
    items.TAX_RATE,
    items.DISCOUNT_RATE,
    items.LINE_TOTAL,
    pay.PAYMENT_DATE_KEY,
    pay.PAYMENT_METHOD,
    pay.TRANSACTION_ID
from inv
join items on inv.INVOICE_ID = items.INVOICE_ID
left join payments pay on inv.INVOICE_ID = pay.INVOICE_ID;