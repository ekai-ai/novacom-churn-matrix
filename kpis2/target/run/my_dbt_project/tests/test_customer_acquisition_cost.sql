select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure CAC calculation is correct
-- CAC should equal total_budget / acquired_customers

with cac_data as (
    select
        total_budget as total_acquisition_cost,
        acquired_customers as new_customers,
        customer_acquisition_cost as cac
    from NOVACOM.KPIS.customer_acquisition_cost
),

validation as (
    select
        total_acquisition_cost,
        new_customers,
        cac,
        case
            when new_customers = 0 then null
            else total_acquisition_cost / new_customers
        end as calculated_cac
    from cac_data
)

select *
from validation
where (cac is null and calculated_cac is not null)
   or (calculated_cac is null and cac is not null)
   or (abs(coalesce(cac, 0) - coalesce(calculated_cac, 0)) > 0.01)  -- Allow for small rounding differences
      
    ) dbt_internal_test