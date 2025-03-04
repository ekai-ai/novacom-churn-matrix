-- Test to ensure ARPU calculation is correct
-- ARPU should equal total_revenue / total_customers

with arpu_data as (
    select
        total_revenue,
        total_customers,
        arpu,
        period_start,
        period_end
    from NOVACOM.KPIS.arpu
),

validation as (
    select
        total_revenue,
        total_customers,
        arpu,
        case
            when total_customers = 0 then 0
            else total_revenue / total_customers
        end as calculated_arpu
    from arpu_data
)

select *
from validation
where abs(arpu - calculated_arpu) > 0.01  -- Allow for small rounding differences