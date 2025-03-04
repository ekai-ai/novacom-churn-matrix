select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure service adoption rate calculation is correct
-- Adoption rate should be between 0 and 1 (or 0% and 100%)

with adoption_data as (
    select
        service_id,
        service_name,
        total_customers,
        customers_using_service,
        adoption_rate,
        period_start,
        period_end
    from NOVACOM.KPIS.service_adoption_rate
),

validation as (
    select
        service_id,
        service_name,
        total_customers,
        customers_using_service,
        adoption_rate,
        case
            when total_customers = 0 then null
            else customers_using_service::float / total_customers
        end as calculated_adoption_rate
    from adoption_data
)

select *
from validation
where adoption_rate < 0 
   or adoption_rate > 1
   or (abs(coalesce(adoption_rate, 0) - coalesce(calculated_adoption_rate, 0)) > 0.01)  -- Allow for small rounding differences
      
    ) dbt_internal_test