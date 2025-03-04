select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test to ensure referential integrity between tables
-- Checks if all customer_ids in invoices exist in the customers table

with invoice_customers as (
    select distinct
        customer_id
    from NOVACOM.BRONZE.bil_invoices
),

all_customers as (
    select distinct
        customer_id
    from NOVACOM.BRONZE.crm_customers
),

missing_customers as (
    select
        ic.customer_id
    from invoice_customers ic
    left join all_customers ac on ic.customer_id = ac.customer_id
    where ac.customer_id is null
)

select *
from missing_customers
      
    ) dbt_internal_test