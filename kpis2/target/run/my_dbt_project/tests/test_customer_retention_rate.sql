select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure retention rate calculation is correct
-- Retention rate should be between 0 and 1 (or 0% and 100%)

with retention_data as (
    select
        period_start,
        period_end,
        customers_start,
        customers_end,
        customers_retained,
        retention_rate
    from NOVACOM.KPIS.customer_retention_rate
),

validation as (
    select
        period_start,
        period_end,
        customers_start,
        customers_end,
        customers_retained,
        retention_rate,
        case
            when customers_start = 0 then null
            else customers_retained::float / customers_start
        end as calculated_retention_rate
    from retention_data
)

select *
from validation
where retention_rate < 0 
   or retention_rate > 1
   or (abs(coalesce(retention_rate, 0) - coalesce(calculated_retention_rate, 0)) > 0.01)  -- Allow for small rounding differences
      
    ) dbt_internal_test