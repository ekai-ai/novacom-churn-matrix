-- customer_retention_rate.sql

-- This model calculates the Customer Retention Rate KPI.
-- It calculates the percentage of customers that remain subscribed to services
-- over a defined period. The period boundaries are defined using the
-- variables 'period_start' and 'period_end'.

with total_customers as (
    select
        count(distinct customer_id) as total_customers
    from {{ ref('prv_service_assignments') }}
    where start_date <= '{{ var("period_start") }}'
      and (end_date is null or end_date >= '{{ var("period_start") }}')
),
retained_customers as (
    select
        count(distinct customer_id) as retained_customers
    from {{ ref('prv_service_assignments') }}
    where start_date <= '{{ var("period_start") }}'
      and (end_date is null or end_date >= '{{ var("period_end") }}')
)

select
    r.retained_customers,
    t.total_customers,
    case
        when t.total_customers = 0 then 0
        else (r.retained_customers * 100.0 / t.total_customers)
    end as customer_retention_rate
from retained_customers r
cross join total_customers t
