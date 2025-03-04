-- customer_retention_rate.sql

-- This model calculates the Customer Retention Rate KPI.
-- It calculates the percentage of customers that remain subscribed to services
-- over a defined period. The period boundaries are defined using the
-- variables 'period_start' and 'period_end'.




with total_customers as (
    select
        count(distinct customer_id) as customers_start
    from NOVACOM.BRONZE.prv_service_assignments
    where start_date <= '2023-01-01'
      and (end_date is null or end_date >= '2023-01-01')
),
retained_customers as (
    select
        count(distinct customer_id) as customers_retained
    from NOVACOM.BRONZE.prv_service_assignments
    where start_date <= '2023-01-01'
      and (end_date is null or end_date >= '2023-12-31')
),
end_customers as (
    select
        count(distinct customer_id) as customers_end
    from NOVACOM.BRONZE.prv_service_assignments
    where start_date <= '2023-12-31'
      and (end_date is null or end_date >= '2023-12-31')
)

select
    '2023-01-01' as period_start,
    '2023-12-31' as period_end,
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