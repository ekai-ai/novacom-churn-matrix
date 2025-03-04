-- Test to ensure CAC calculation is correct
-- CAC should equal total_acquisition_cost / new_customers

with cac_data as (
    select
        total_acquisition_cost,
        new_customers,
        cac,
        period_start,
        period_end
    from {{ ref('customer_acquisition_cost') }}
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
