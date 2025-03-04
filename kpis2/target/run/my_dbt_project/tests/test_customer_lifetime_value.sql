select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure CLV calculation is correct

with clv_data as (
    select
        customer_id,
        lifetime_value,
        total_revenue,
        customer_age_months
    from NOVACOM.KPIS.customer_lifetime_value
),

validation as (
    select
        customer_id,
        lifetime_value,
        total_revenue,
        customer_age_months
    from clv_data
    where lifetime_value < 0  -- CLV should never be negative
       or (customer_age_months > 0 and total_revenue > 0 and lifetime_value = 0)  -- If customer has revenue and age, CLV shouldn't be zero
       or (total_revenue = 0 and lifetime_value > 0)  -- If no revenue, CLV should be zero
)

select *
from validation
      
    ) dbt_internal_test