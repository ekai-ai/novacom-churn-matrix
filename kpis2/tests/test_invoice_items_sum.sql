-- Test to ensure invoice items sum up to the invoice total
-- Checks if the sum of line_total from invoice_items matches total_amount in invoices

with invoice_totals as (
    select
        invoice_id,
        total_amount
    from {{ source('Combined Source', 'bil_invoices') }}
),

item_sums as (
    select
        invoice_id,
        sum(line_total) as calculated_total
    from {{ source('Combined Source', 'bil_invoice_items') }}
    group by 1
),

comparison as (
    select
        it.invoice_id,
        it.total_amount,
        is.calculated_total,
        abs(it.total_amount - is.calculated_total) as difference
    from invoice_totals it
    inner join item_sums is on it.invoice_id = is.invoice_id
)

select *
from comparison
where difference > 0.01  -- Allow for small rounding differences
