-- service_adoption_rate.sql
with multi_service_customers as (
    select
        customer_id
    from {{ ref('prv_service_assignments') }}
    where status = 'Active'
    group by customer_id
    having count(*) > 1
), total_customers as (
    select count(*) as total_customers
    from {{ ref('crm_customers') }}
)

select
    (select count(*) from multi_service_customers)::numeric / nullif(total_customers,0) * 100 as service_adoption_rate
from total_customers
;