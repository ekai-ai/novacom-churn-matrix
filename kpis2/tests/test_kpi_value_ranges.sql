-- Test to ensure KPI values fall within expected ranges
with kpi_ranges as (
    select
        'service_adoption_rate' as model_name,
        0 as min_value,
        1 as max_value
    union all
    select
        'customer_retention_rate' as model_name,
        0 as min_value,
        1 as max_value
    union all
    select
        'customer_acquisition_cost' as model_name,
        0 as min_value,
        null as max_value -- No upper limit, but should be positive
    union all
    select
        'customer_lifetime_value' as model_name,
        0 as min_value,
        null as max_value -- No upper limit, but should be positive
)

-- Service Adoption Rate check
select
    'Service Adoption Rate out of range' as failure_reason,
    s.service_id,
    s.service_name,
    s.adoption_rate
from {{ ref('service_adoption_rate') }} s
where s.adoption_rate < 0 or s.adoption_rate > 1

union all

-- Customer Retention Rate check
select
    'Customer Retention Rate out of range' as failure_reason,
    null as service_id,
    null as service_name,
    r.retention_rate as adoption_rate
from {{ ref('customer_retention_rate') }} r
where r.retention_rate < 0 or r.retention_rate > 1

union all

-- Customer Acquisition Cost check
select
    'Customer Acquisition Cost negative' as failure_reason,
    null as service_id,
    null as service_name,
    c.customer_acquisition_cost as adoption_rate
from {{ ref('customer_acquisition_cost') }} c
where c.customer_acquisition_cost < 0

union all

-- Customer Lifetime Value check
select
    'Customer Lifetime Value negative' as failure_reason,
    null as service_id,
    null as service_name,
    clv.lifetime_value as adoption_rate
from {{ ref('customer_lifetime_value') }} clv
where clv.lifetime_value < 0
