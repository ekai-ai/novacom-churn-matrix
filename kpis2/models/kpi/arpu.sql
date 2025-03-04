-- This dbt model calculates the Average Revenue Per User (ARPU) for a given month.
-- The calculation takes the sum of invoice amounts and divides it by the count of distinct customers 
-- that have an invoice within the specified monthly date range.

{% set start_date = var('start_date', '2023-10-01') %}
{% set end_date = var('end_date', '2023-10-31') %}

with filtered_invoices as (
    select
        customer_id,
        total_amount
    from {{ source('Combined Source', 'bil_invoices') }}
    where invoice_date between '{{ start_date }}' and '{{ end_date }}'
)

select
    sum(total_amount) as total_revenue,
    count(distinct customer_id) as total_customers,
    case 
        when count(distinct customer_id) = 0 then 0
        else sum(total_amount) / count(distinct customer_id)
    end as arpu,
    '{{ start_date }}' as period_start,
    '{{ end_date }}' as period_end
from filtered_invoices