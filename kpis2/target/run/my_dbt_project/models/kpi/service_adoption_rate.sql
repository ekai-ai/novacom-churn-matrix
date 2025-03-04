
  create or replace   view NOVACOM.KPIS.service_adoption_rate
  
   as (
    -- service_adoption_rate.sql
-- This model calculates the adoption rate for each service
-- The adoption rate is the percentage of customers using a specific service




with services as (
    select distinct
        service_id,
        service_name
    from NOVACOM.BRONZE.prv_services
),

total_customers as (
    select 
        count(distinct customer_id) as total_customers
    from NOVACOM.BRONZE.crm_customers
    where status = 'Active'
),

service_usage as (
    select
        a.service_id,
        count(distinct a.customer_id) as customers_using_service
    from NOVACOM.BRONZE.prv_service_assignments a
    where a.status = 'Active'
    group by 1
)

select
    s.service_id,
    s.service_name,
    t.total_customers,
    coalesce(u.customers_using_service, 0) as customers_using_service,
    case
        when t.total_customers = 0 then 0
        else coalesce(u.customers_using_service, 0)::float / t.total_customers
    end as adoption_rate,
    '2023-01-01' as period_start,
    '2023-12-31' as period_end
from services s
cross join total_customers t
left join service_usage u on s.service_id = u.service_id
  );

