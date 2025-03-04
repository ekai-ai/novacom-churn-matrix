-- customer_retention_rate.sql

-- This model calculates the Customer Retention Rate KPI.
-- It calculates the percentage of customers that remain subscribed to services
-- over a defined period. The period boundaries are defined using the
-- variables 'period_start' and 'period_end'.

{% set period_start = var("period_start", "2023-01-01") %}
{% set period_end = var("period_end", "2023-12-31") %}

with total_customers as (
    select
        count(distinct customer_id) as customers_start
    from {{ source('Combined Source', 'prv_service_assignments') }}
    where start_date <= '{{ period_start }}'
      and (end_date is null or end_date >= '{{ period_start }}')
),
retained_customers as (
    select
        count(distinct customer_id) as customers_retained
    from {{ source('Combined Source', 'prv_service_assignments') }}
    where start_date <= '{{ period_start }}'
      and (end_date is null or end_date >= '{{ period_end }}')
),
end_customers as (
    select
        count(distinct customer_id) as customers_end
    from {{ source('Combined Source', 'prv_service_assignments') }}
    where start_date <= '{{ period_end }}'
      and (end_date is null or end_date >= '{{ period_end }}')
)

select
    '{{ period_start }}' as period_start,
    '{{ period_end }}' as period_end,
    s.customers_start,
    e.customers_end,
    r.customers_retained,
    case
        when s.customers_start = 0 then 0
        else (r.customers_retained::float / s.customers_start)
    end as retention_rate
from total_customers s
cross join retained_customers r
cross join end_customers e
